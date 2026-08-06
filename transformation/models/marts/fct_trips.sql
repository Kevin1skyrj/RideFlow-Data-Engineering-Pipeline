{{
    config(
        materialized = 'incremental',
        unique_key = 'trip_id',
        incremental_strategy = 'delete+insert',
        tags = ['marts','fact']
    )
}}

-- depends_on: {{ ref('stg_trip_events') }}
--
-- Required because the ref() below sits inside an is_incremental conditional,
-- so dbt cannot infer the dependency from a first-run compile. Without this
-- hint the model builds on a full refresh and fails on every incremental run -
-- a failure mode that hides itself, because the model keeps its previous
-- content and the marts still look correct.
--
-- NOTE: never write a Jinja block-tag opener literally in these comments.
-- Jinja renders before SQL is parsed, so a SQL comment does NOT protect it: an
-- unmatched opener inside a comment still counts and breaks the whole template.
-- That happened twice while writing this very explanation.

/*
    The primary fact table. GRAIN: one row per trip.

    Every requested trip appears exactly once, whether it completed, was
    cancelled, or expired. Restricting the grain to completions would make the
    conversion-funnel question unanswerable - a fact table containing only
    successes cannot measure failure.

    ── Incremental strategy ────────────────────────────────────────────────
    delete+insert, NOT append. Append double-counts on every retry, which
    defeats the idempotency objective outright. delete+insert over the lookback
    window makes re-running any period produce identical output, which is what
    makes backfill and retry safe.

    The incremental filter uses ingested_at (arrival time, monotonic); business
    grouping uses the event timestamps. Filtering on event time would let the
    high-water mark advance past a late-arriving event, missing it PERMANENTLY
    with no error (etl_design.md 6.2).
*/

with trips as (

    select * from {{ ref('int_trips_assembled') }}

    {% if is_incremental() %}
    -- Scoped by trip rather than by the trip's own timestamp: a trip is
    -- reprocessed when ANY of its events arrived in the window. A late
    -- RideCompleted must revise its trip even though requested_at is old.
    where trip_id in (
        select correlation_id
        from {{ ref('stg_trip_events') }}
        where {{ incremental_window('ingested_at') }}
    )
    {% endif %}

),

city as (

    select city_id, timezone from {{ ref('dim_city') }}

),

resolved as (

    select
        trips.*,
        city.timezone
    from trips
    left join city using (city_id)

)

select
    -- ── Identity ────────────────────────────────────────────────────────
    trip_id,
    rider_id,
    driver_id,
    vehicle_id,

    -- ── Conformed dimension keys ────────────────────────────────────────
    -- coalesce to -1, never null. A null FK silently drops the row from every
    -- inner join, and the loss appears in no count and no error log.
    {{ unknown_key('city_id') }}                       as city_id,
    {{ unknown_key('vt.vehicle_type_id') }}            as vehicle_type_id,
    {{ unknown_key('rvt.vehicle_type_id') }}           as requested_vehicle_type_id,
    {{ unknown_key('tier.customer_tier_id') }}         as customer_tier_id,
    {{ unknown_key('status.ride_status_id') }}         as ride_status_id,
    {{ unknown_key('cstatus.ride_status_id') }}        as cancelled_at_status_id,
    {{ unknown_key('reason.cancellation_reason_id') }} as cancellation_reason_id,
    {{ unknown_key('pickup_zone_id') }}                as pickup_zone_id,
    {{ unknown_key('dropoff_zone_id') }}               as dropoff_zone_id,
    {{ unknown_key('actual_dropoff_zone_id') }}        as actual_dropoff_zone_id,
    {{ unknown_key('driver_pickup_zone_id') }}         as driver_pickup_zone_id,
    {{ unknown_key('rweather.weather_id') }}           as request_weather_id,
    {{ unknown_key('tweather.weather_id') }}           as trip_weather_id,
    {{ unknown_key('rtraffic.traffic_level_id') }}     as request_traffic_level_id,
    {{ unknown_key('ttraffic.traffic_level_id') }}     as trip_traffic_level_id,

    /*
        LOCAL date and time keys, resolved through dim_city.timezone.

        08:12 IST is 02:42 UTC. Bucketing on UTC would put the Bengaluru
        morning peak at 2 a.m. - and the chart would look plausible enough to
        be rationalised rather than investigated.
    */
    {{ local_date_key('requested_at', 'timezone') }}   as request_date_id,
    {{ local_time_key('requested_at', 'timezone') }}   as request_time_id,
    {{ local_hour('requested_at', 'timezone') }}       as request_local_hour,

    -- ── Lifecycle timestamps (UTC) ──────────────────────────────────────
    requested_at,
    accepted_at,
    arrived_at,
    started_at,
    completed_at,
    cancelled_at,
    paid_at,

    -- ── Request attributes ──────────────────────────────────────────────
    pickup_lat,
    pickup_lon,
    dropoff_lat,
    dropoff_lon,
    estimated_fare,
    estimated_distance_km,
    estimated_duration_sec,
    surge_multiplier,
    is_airport_pickup,
    is_airport_dropoff,
    promo_code,
    device_platform,
    app_version,

    -- ── Matching ────────────────────────────────────────────────────────
    matching_duration_sec,
    eta_to_pickup_sec,
    distance_to_pickup_km,
    driver_rating_at_accept,
    dispatch_attempt_number,

    -- ── Pickup ──────────────────────────────────────────────────────────
    actual_pickup_duration_sec,
    arrival_delay_sec,
    is_late_arrival,
    rider_wait_duration_sec,
    actual_pickup_lat,
    actual_pickup_lon,

    -- ── Fare (additive measures) ────────────────────────────────────────
    distance_km,
    duration_sec,
    base_fare,
    distance_fare,
    time_fare,

    -- Surge as CURRENCY, not a ratio. AVG(surge_multiplier) averages ratios -
    -- a Rs50 trip at 3.0x and a Rs3000 trip at 1.1x average to 2.05x, which
    -- describes no real state of the marketplace. SUM(surge_amount) does.
    surge_amount,

    airport_fee,
    toll_amount,
    booking_fee,
    tax_amount,
    total_fare,
    driver_payout,
    platform_commission,
    currency,

    -- ── Cancellation ────────────────────────────────────────────────────
    cancelled_by,
    seconds_since_request,
    cancellation_fee,
    is_fee_charged,

    -- Qualified: dim_cancellation_reason also has is_driver_fault. The EVENT's
    -- value is authoritative - it was canonicalised at emission time, so it
    -- reflects the rule in force then. The dimension's copy reflects the rule
    -- in force now, and the two can legitimately differ after a policy change.
    resolved.is_driver_fault,

    -- ── Payment linkage ─────────────────────────────────────────────────
    payment_id,
    payment_attempt_number,
    had_failed_attempts,

    -- ── Derived flags ───────────────────────────────────────────────────
    is_completed,
    is_cancelled,
    is_paid,
    is_matched,
    is_surge_trip,
    funnel_stage_reached,
    total_lifecycle_sec,
    estimate_accuracy_pct,
    distance_accuracy_pct,
    eta_accuracy_pct,
    revenue_per_km,

    /*
        INFERRED, not observed. No event produces EXPIRED, so a trip flagged
        here is indistinguishable from one whose later events were lost. Named
        with the is_inferred_ prefix so nothing downstream can present it with
        the same confidence as is_completed (event_contract.md 0.1, Gap 1).
    */
    is_expired                                         as is_inferred_expired,

    -- ── Pipeline quality (about the PIPELINE, not the business) ─────────
    event_count,
    max_lateness_sec,
    had_late_events,
    is_sequence_valid,
    has_request_event,

    /*
        Quarantine flag. Business metrics must filter on
        `is_quarantined = false`.

        A quarantined trip is RETAINED, not dropped. If its missing event
        arrives late, the next run reassembles it and it leaves quarantine on
        its own. Dropping it would make recovery impossible - the evidence it
        existed would be gone. `quarantined_trips` lists them with reasons, so
        the exclusion is inspectable rather than invisible.
    */
    (not has_request_event or not is_sequence_valid)   as is_quarantined,

    -- ── Technical ───────────────────────────────────────────────────────
    '{{ invocation_id }}'                              as dbt_invocation_id,
    current_timestamp                                  as dbt_loaded_at

from resolved
left join {{ ref('dim_vehicle_type') }}        as vt
       on resolved.delivered_vehicle_type  = vt.vehicle_type_code
left join {{ ref('dim_vehicle_type') }}        as rvt
       on resolved.requested_vehicle_type  = rvt.vehicle_type_code
left join {{ ref('dim_customer_tier') }}       as tier
       on resolved.customer_tier           = tier.customer_tier_code
left join {{ ref('dim_ride_status') }}         as status
       on resolved.ride_status_code        = status.ride_status_code
left join {{ ref('dim_ride_status') }}         as cstatus
       on resolved.cancelled_at_status     = cstatus.ride_status_code
left join {{ ref('dim_cancellation_reason') }} as reason
       on resolved.cancellation_reason_code = reason.cancellation_reason_code
left join {{ ref('dim_weather') }}             as rweather
       on resolved.request_weather_code    = rweather.weather_code
left join {{ ref('dim_weather') }}             as tweather
       on resolved.trip_weather_code       = tweather.weather_code
left join {{ ref('dim_traffic_level') }}       as rtraffic
       on resolved.request_traffic_level   = rtraffic.traffic_level_code
left join {{ ref('dim_traffic_level') }}       as ttraffic
       on resolved.trip_traffic_level      = ttraffic.traffic_level_code
