{{ config(materialized='table', tags=['marts','quality']) }}

/*
    Daily pipeline health. One row per landed date.

    These metrics describe the PIPELINE, not the business. A day with many late
    arrivals is not a day with worse trips - conflating the two is how an
    infrastructure problem gets mistaken for a demand signal.

    Everything here is a WARN-class signal rather than a blocking one. Blocking
    tests answer "is this number wrong?"; these answer "is the pipeline
    degrading?" - a question that needs a trend, not a gate. A rising unknown-key
    rate or orphan count means the DLQ rate is rising upstream.
*/

with events as (

    select
        landed_date,
        count(*)                                             as events_landed,
        count(distinct event_id)                             as distinct_events,
        sum(case when is_late_arrival_event then 1 else 0 end) as late_arrivals,
        sum(case when lateness_sec < 0 then 1 else 0 end)     as clock_skewed,
        round(max(lateness_sec), 2)                          as max_lateness_sec,
        round(median(lateness_sec), 2)                       as median_lateness_sec
    from {{ ref('stg_trip_events') }}
    group by 1

),

presence as (

    select
        landed_date,
        count(*) as presence_events_landed
    from {{ ref('stg_driver_presence') }}
    group by 1

),

trips as (

    select
        /*
            Coalesce across the lifecycle, do NOT key on requested_at alone.

            An orphaned trip has no RideRequested and therefore no
            requested_at, so grouping on it - and filtering out nulls - drops
            exactly the rows this model exists to count. The first version of
            this model reported 0 orphaned trips while quarantined_trips listed
            12: a quality metric silently excluding the defect it measures.
        */
        cast(coalesce(requested_at, accepted_at, arrived_at, started_at,
                      completed_at, cancelled_at, paid_at) as date)
                                                              as landed_date,
        count(*)                                              as trips,
        sum(case when not has_request_event then 1 else 0 end) as orphaned_trips,
        sum(case when not is_sequence_valid then 1 else 0 end) as sequence_invalid_trips,
        sum(case when is_inferred_expired then 1 else 0 end)   as inferred_expired_trips,
        sum(case when had_late_events then 1 else 0 end)       as trips_with_late_events,

        -- Unknown-key rate. A -1 is not automatically a defect: an unmatched
        -- trip legitimately has no delivered vehicle type. It is tracked
        -- because a RISE in the rate means lookups are failing upstream.
        sum(case when city_id = -1 then 1 else 0 end)          as unknown_city_keys,
        sum(case when vehicle_type_id = -1 then 1 else 0 end)  as unknown_vehicle_keys,
        sum(case when customer_tier_id = -1 then 1 else 0 end) as unknown_tier_keys,
        sum(case when ride_status_id = -1 then 1 else 0 end)   as unknown_status_keys
    from {{ ref('fct_trips') }}
    group by 1

),

payments as (

    select
        cast(p.paid_at as date)                               as landed_date,
        count(*)                                              as payments,
        -- Money collected for trips the warehouse cannot describe, because the
        -- RideCompleted was quarantined.
        sum(case when t.completed_at is null then 1 else 0 end) as payments_without_completion,
        round(sum(case when t.completed_at is null
                       then p.amount_charged else 0 end), 2)   as unexplained_revenue
    from {{ ref('fct_payments') }} as p
    join {{ ref('fct_trips') }} as t using (trip_id)
    group by 1

),

sessions as (

    select
        cast(online_at as date)                               as landed_date,
        count(*)                                              as driver_sessions,
        sum(case when is_session_open then 1 else 0 end)       as open_sessions,
        -- Device-reported vs observed trip counts disagreeing. Expected in
        -- small numbers; a spike means devices are losing connectivity.
        sum(case when counts_disagree then 1 else 0 end)       as sessions_counts_disagree
    from {{ ref('fct_driver_sessions') }}
    group by 1

),

dates as (

    select landed_date from events
    union select landed_date from presence
    union select landed_date from trips
    union select landed_date from payments
    union select landed_date from sessions

)

select
    d.landed_date,

    -- Volume
    coalesce(e.events_landed, 0)                as trip_events_landed,
    coalesce(p2.presence_events_landed, 0)      as presence_events_landed,
    coalesce(t.trips, 0)                        as trips,
    coalesce(pay.payments, 0)                   as payments,
    coalesce(s.driver_sessions, 0)              as driver_sessions,

    -- Deduplication. Zero here means staging did its job: the landing zone is
    -- append-only and retains redelivered copies, so any surplus would show.
    coalesce(e.events_landed, 0) - coalesce(e.distinct_events, 0)
                                                as duplicate_events_remaining,

    -- Timeliness
    coalesce(e.late_arrivals, 0)                as late_arrivals,
    coalesce(e.clock_skewed, 0)                 as clock_skewed_events,
    e.max_lateness_sec,
    e.median_lateness_sec,

    -- Completeness
    coalesce(t.orphaned_trips, 0)               as orphaned_trips,
    coalesce(t.sequence_invalid_trips, 0)       as sequence_invalid_trips,
    coalesce(t.inferred_expired_trips, 0)       as inferred_expired_trips,
    coalesce(pay.payments_without_completion, 0) as payments_without_completion,
    coalesce(pay.unexplained_revenue, 0)        as unexplained_revenue,

    -- Referential health
    coalesce(t.unknown_city_keys, 0)            as unknown_city_keys,
    coalesce(t.unknown_vehicle_keys, 0)         as unknown_vehicle_keys,
    coalesce(t.unknown_tier_keys, 0)            as unknown_tier_keys,
    coalesce(t.unknown_status_keys, 0)          as unknown_status_keys,

    -- Supply
    coalesce(s.open_sessions, 0)                as open_sessions,
    coalesce(s.sessions_counts_disagree, 0)     as sessions_counts_disagree,

    -- Rates, for trending
    round(100.0 * coalesce(t.orphaned_trips, 0)
          / nullif(t.trips, 0), 3)              as orphaned_trip_pct,
    round(100.0 * coalesce(t.sequence_invalid_trips, 0)
          / nullif(t.trips, 0), 3)              as sequence_invalid_pct,
    round(100.0 * coalesce(e.late_arrivals, 0)
          / nullif(e.events_landed, 0), 3)      as late_arrival_pct,

    current_timestamp                           as dbt_loaded_at

from dates as d
left join events   as e   using (landed_date)
left join presence as p2  using (landed_date)
left join trips    as t   using (landed_date)
left join payments as pay using (landed_date)
left join sessions as s   using (landed_date)
order by d.landed_date
