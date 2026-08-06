{{ config(materialized='table', tags=['intermediate']) }}

/*
    Turn a stream of events into a trip.

    This is where the hard logic lives: collapsing 2-6 events per trip into one
    row by resolving the state machine in event_contract.md 6.2.

    Assembly is ORDER-INDEPENDENT by construction. It joins on trip_id and reads
    business timestamps from each event - it never relies on arrival order. That
    is why a trip whose RideStarted arrived 31 minutes after its RideCompleted
    still assembles correctly, which the generator produces deliberately.

    Trips are anchored on the UNION of every trip_id seen, not on
    stg_ride_requested. A trip whose RideRequested was lost or quarantined still
    appears here, flagged invalid - quarantine, never discard. Discarding an
    orphan makes it unrecoverable when the missing anchor arrives late.
*/

with all_trip_ids as (

    select trip_id from {{ ref('stg_ride_requested') }}
    union
    select trip_id from {{ ref('stg_ride_accepted') }}
    union
    select trip_id from {{ ref('stg_driver_arrived') }}
    union
    select trip_id from {{ ref('stg_ride_started') }}
    union
    select trip_id from {{ ref('stg_ride_completed') }}
    union
    select trip_id from {{ ref('stg_ride_cancelled') }}
    union
    select trip_id from {{ ref('stg_payment_completed') }}

),

-- Pipeline-health facts, aggregated from the atomic event stream. These
-- describe the PIPELINE, not the business, and are kept separate for that
-- reason: a trip with late-arriving events is not a worse trip.
event_stats as (

    select
        correlation_id                          as trip_id,
        count(*)                                as event_count,
        max(lateness_sec)                       as max_lateness_sec,
        bool_or(is_late_arrival_event)          as had_late_events,
        min(ingested_at)                        as first_ingested_at,
        max(ingested_at)                        as last_ingested_at
    from {{ ref('stg_trip_events') }}
    group by 1

),

-- The horizon used to infer expiry. Anchored to the data itself rather than
-- now(), so the model stays idempotent: re-running tomorrow must not silently
-- reclassify yesterday's trips.
observation as (

    select max(requested_at) as latest_request
    from {{ ref('stg_ride_requested') }}

),

assembled as (

    select
        ids.trip_id,

        -- Identity, taken from whichever event carries it. Coalesced across
        -- events so an orphaned trip still has a rider and a city.
        coalesce(req.rider_id, acc.rider_id, arr.rider_id, sta.rider_id,
                 com.rider_id, can.rider_id, pay.rider_id)          as rider_id,
        coalesce(acc.driver_id, arr.driver_id, sta.driver_id,
                 com.driver_id, pay.driver_id, can.driver_id)       as driver_id,
        acc.vehicle_id,
        coalesce(req.city_id, acc.city_id, com.city_id, can.city_id) as city_id,

        -- Lifecycle timestamps. NULL means "did not reach this stage" - which
        -- is information, never missing data.
        req.requested_at,
        acc.accepted_at,
        arr.arrived_at,
        sta.started_at,
        com.completed_at,
        can.cancelled_at,
        pay.paid_at,

        -- Request attributes
        req.pickup_lat,
        req.pickup_lon,
        req.pickup_zone_id,
        req.dropoff_lat,
        req.dropoff_lon,
        req.dropoff_zone_id,
        req.vehicle_type          as requested_vehicle_type,
        req.estimated_fare,
        req.estimated_distance_km,
        req.estimated_duration_sec,
        req.surge_multiplier,
        req.payment_method        as requested_payment_method,
        req.customer_tier,
        req.is_airport_pickup,
        req.is_airport_dropoff,
        req.weather_code          as request_weather_code,
        req.traffic_level         as request_traffic_level,
        req.promo_code,
        req.device_platform,
        req.app_version,

        -- Matching
        acc.vehicle_type          as delivered_vehicle_type,
        acc.matching_duration_sec,
        acc.eta_to_pickup_sec,
        acc.distance_to_pickup_km,
        acc.driver_rating_at_accept,
        acc.dispatch_attempt_number,
        acc.driver_zone_id        as driver_pickup_zone_id,

        -- Arrival and pickup
        arr.actual_pickup_duration_sec,
        arr.arrival_delay_sec,
        arr.is_late_arrival,
        sta.rider_wait_duration_sec,
        sta.actual_pickup_lat,
        sta.actual_pickup_lon,

        -- Completion and fare
        com.distance_km,
        com.duration_sec,
        com.base_fare,
        com.distance_fare,
        com.time_fare,
        com.surge_amount,
        com.airport_fee,
        com.toll_amount,
        com.booking_fee,
        com.tax_amount,
        com.total_fare,
        com.driver_payout,
        com.platform_commission,
        com.currency,
        com.dropoff_zone_id       as actual_dropoff_zone_id,
        com.traffic_level         as trip_traffic_level,
        com.weather_code          as trip_weather_code,
        com.payment_method        as charged_payment_method,

        -- Cancellation
        can.cancelled_by,
        can.cancellation_reason_code,
        can.cancelled_at_status,
        can.seconds_since_request,
        can.cancellation_fee,
        can.is_fee_charged,
        can.is_driver_fault,

        -- Payment
        pay.payment_id,
        pay.attempt_number        as payment_attempt_number,
        pay.had_failed_attempts,

        -- Pipeline health
        coalesce(stats.event_count, 0)      as event_count,
        coalesce(stats.max_lateness_sec, 0) as max_lateness_sec,
        coalesce(stats.had_late_events, false) as had_late_events,

        obs.latest_request

    from all_trip_ids                            as ids
    left join {{ ref('stg_ride_requested') }}    as req   using (trip_id)
    left join {{ ref('stg_ride_accepted') }}     as acc   using (trip_id)
    left join {{ ref('stg_driver_arrived') }}    as arr   using (trip_id)
    left join {{ ref('stg_ride_started') }}      as sta   using (trip_id)
    left join {{ ref('stg_ride_completed') }}    as com   using (trip_id)
    left join {{ ref('stg_ride_cancelled') }}    as can   using (trip_id)
    left join {{ ref('stg_payment_completed') }} as pay   using (trip_id)
    left join event_stats                        as stats using (trip_id)
    cross join observation                       as obs

),

derived as (

    select
        *,

        -- Outcome flags
        completed_at is not null                          as is_completed,
        cancelled_at is not null                          as is_cancelled,
        paid_at      is not null                          as is_paid,
        driver_id    is not null                          as is_matched,

        /*
            is_expired is an INFERENCE, not an observation.

            No event produces it: EXPIRED is in the state machine but has no
            corresponding event type (event_contract.md 0.1, Gap 1). A trip
            flagged here is indistinguishable from one whose later events were
            lost, so it conflates a business outcome with a pipeline failure.

            Kept because unmatched demand is too important to omit, but every
            mart exposing it must label it as derived. Resolved when
            RideExpired ships in contract v1.1.

            The 10-minute horizon is measured against the latest request in the
            data, not now(), so re-running does not reclassify old trips.
        */
        requested_at is not null
            and accepted_at is null
            and cancelled_at is null
            and requested_at < latest_request - interval 10 minute
                                                          as is_expired,

        -- A trip missing its anchor cannot be trusted for business analysis.
        -- It is retained and flagged, so it can be recovered if the anchor
        -- arrives late, but excluded from aggregates.
        requested_at is not null                          as has_request_event,

        surge_multiplier > 1.00                           as is_surge_trip

    from assembled

)

select
    *,

    -- Funnel stage reached, mirroring dim_ride_status.funnel_stage.
    case
        when is_paid       then 6
        when is_completed  then 5
        when started_at   is not null then 4
        when arrived_at   is not null then 3
        when is_matched    then 2
        when has_request_event then 1
        else 0
    end                                                   as funnel_stage_reached,

    -- Final status code, resolved from which events exist.
    case
        when is_cancelled and cancelled_by = 'RIDER'  then 'CANCELLED_RIDER'
        when is_cancelled and cancelled_by = 'DRIVER' then 'CANCELLED_DRIVER'
        when is_cancelled                             then 'CANCELLED_SYSTEM'
        when is_paid              then 'PAID'
        when is_completed         then 'COMPLETED'
        when started_at is not null then 'STARTED'
        when arrived_at is not null then 'DRIVER_ARRIVED'
        when is_matched           then 'MATCHED'
        when is_expired           then 'EXPIRED'
        when has_request_event    then 'REQUESTED'
        else 'UNKNOWN'
    end                                                   as ride_status_code,

    -- Request to terminal state.
    case
        when requested_at is null then null
        else date_diff(
            'second',
            requested_at,
            coalesce(paid_at, completed_at, cancelled_at, started_at,
                     arrived_at, accepted_at, requested_at)
        )
    end                                                   as total_lifecycle_sec,

    -- Estimate accuracy, signed. A signed error shows systematic over- or
    -- under-estimation; an absolute one hides which direction is wrong.
    case when estimated_fare > 0 and total_fare is not null
         then round((total_fare - estimated_fare) / estimated_fare * 100, 2)
    end                                                   as estimate_accuracy_pct,
    case when estimated_distance_km > 0 and distance_km is not null
         then round((distance_km - estimated_distance_km) / estimated_distance_km * 100, 2)
    end                                                   as distance_accuracy_pct,
    case when eta_to_pickup_sec > 0 and actual_pickup_duration_sec is not null
         then round(
             (actual_pickup_duration_sec - eta_to_pickup_sec) * 100.0 / eta_to_pickup_sec, 2)
    end                                                   as eta_accuracy_pct,

    case when distance_km > 0 and total_fare is not null
         then round(total_fare / distance_km, 2)
    end                                                   as revenue_per_km,

    /*
        Sequence validity, checked against the ASSEMBLED trip rather than
        arrival order (rules S1-S8). A trip failing this is quarantined from
        aggregates but retained, because the missing event may still arrive.
    */
    (
        has_request_event
        and not (is_completed and is_cancelled)
        and (completed_at is null or started_at is not null)
        and (paid_at      is null or completed_at is not null)
        and (started_at   is null or accepted_at is not null)
        and (arrived_at   is null or accepted_at is not null)
    )                                                     as is_sequence_valid

from derived
