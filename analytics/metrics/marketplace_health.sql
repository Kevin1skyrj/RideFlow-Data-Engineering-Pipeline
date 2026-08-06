/*
    Q1  MARKETPLACE HEALTH - where is unmet demand right now?

    The question is NOT "where are trips happening". It is "where did someone
    want a ride and not get one, and was supply the reason". Answering it needs
    demand and supply at the same grain, which is why fct_driver_sessions exists
    at all.

    ── This is a DRILL-ACROSS query: aggregate, THEN join ────────────────────
    Joining fct_trips to fct_driver_sessions directly through shared dimensions
    is a fan trap. A driver with 3 sessions and 12 trips yields 36 rows and
    every measure inflates. Each fact is aggregated to zone x hour separately
    and the aggregates are joined (star_schema.md 7.2).
*/

with demand as (

    select
        pickup_zone_id                                              as zone_id,
        request_local_hour                                          as local_hour,
        count(*)                                                    as requests,
        sum(case when not is_matched then 1 else 0 end)              as unmatched,
        sum(case when is_completed then 1 else 0 end)                as completed,
        sum(case when is_inferred_expired then 1 else 0 end)         as inferred_expired,
        round(avg(matching_duration_sec), 1)                        as avg_match_sec,
        round(avg(surge_multiplier), 3)                             as avg_surge_multiplier
    from read_parquet('data/processed/fct_trips.parquet')
    -- Quarantined trips are excluded from every business metric. They are
    -- retained in the mart and listed in quarantined_trips, but their request
    -- attributes are unknown, so counting them here would be guessing.
    where not is_quarantined
      and requested_at is not null
    group by 1, 2

),

supply as (

    select
        online_zone_id                                              as zone_id,
        /*
            LOCAL hour, to match demand's request_local_hour.

            This was `strftime(online_at, '%H')`, which is UTC - the session
            timestamps are timestamptz and the session pins TimeZone='UTC'.
            Comparing a UTC hour to a local one would have shifted the whole
            supply curve by 5h30m and put the Bengaluru morning peak in the
            middle of the night.

            online_time_id is already a local HHMM key into dim_time (verified:
            zero orphans), so integer-dividing by 100 is the hour with no
            conversion to get wrong.

            `//`, not `/`. DuckDB's `/` is TRUE division, so `640 / 100` is
            6.4 - which would never equal an integer hour and would silently
            produce an empty supply join. The same operator caused duplicate
            dim_time keys in M6.
        */
        online_time_id // 100                                       as local_hour,
        count(*)                                                    as driver_sessions,
        sum(observed_trips_completed)                               as trips_served,
        -- Open sessions have NULL duration and are excluded from the average
        -- rather than counted as zero: imputing an end time fabricates supply
        -- hours and inflates every utilisation denominator.
        round(avg(session_duration_sec) / 3600.0, 2)                as avg_online_hours
    from read_parquet('data/processed/fct_driver_sessions.parquet')
    group by 1, 2

)

select
    z.zone_name,
    z.zone_type,
    d.local_hour,
    d.requests,
    d.unmatched,
    d.inferred_expired,
    round(100.0 * d.unmatched / nullif(d.requests, 0), 2)           as unmet_demand_pct,
    coalesce(s.driver_sessions, 0)                                  as driver_sessions,
    d.avg_match_sec,
    d.avg_surge_multiplier,

    /*
        The diagnostic that makes this actionable.

        High unmet demand WITH high surge means genuine scarcity - the
        marketplace is working and needs more supply. High unmet demand with
        LOW surge means the pricing signal never fired, which is a pricing bug,
        not a supply problem. The two have opposite remedies, and a single
        "cancellation rate" number cannot distinguish them.
    */
    case
        when d.requests < 10 then 'INSUFFICIENT_DATA'
        when d.unmatched * 1.0 / d.requests > 0.10 and d.avg_surge_multiplier > 1.2
            then 'SUPPLY_SHORTAGE'
        when d.unmatched * 1.0 / d.requests > 0.10
            then 'UNPRICED_SHORTAGE'
        else 'HEALTHY'
    end                                                             as diagnosis

from demand as d
join read_parquet('data/processed/dim_zone.parquet') as z using (zone_id)
/*
    Join on BOTH keys.

    This was `on s.zone_id = d.zone_id` alone, which dropped the hour and
    matched every demand row to all 24 supply rows for its zone - a 24x
    fan-out producing 13,349 rows from 599 real zone-hours.

    The irony is that both CTEs aggregate correctly: the fan trap this file's
    header warns about was reintroduced by the join that was supposed to avoid
    it. Aggregating to a grain is only half of a drill-across; joining on the
    whole of that grain is the other half.

    Guarded by tests/integration/test_analytics.py: the result must not exceed
    the number of distinct zone-hours in demand.
*/
left join supply as s
    on  s.zone_id    = d.zone_id
    and s.local_hour = d.local_hour
order by unmet_demand_pct desc nulls last, d.requests desc
