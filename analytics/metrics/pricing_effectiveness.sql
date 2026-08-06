/*
    Q3  PRICING EFFECTIVENESS - is surge rebalancing supply, or just
        suppressing demand?

    ── The measure trap this query exists to avoid ──────────────────────────
    AVG(surge_multiplier) averages RATIOS. A Rs50 trip at 3.0x and a Rs3000
    trip at 1.1x average to 2.05x, which describes no real state of the
    marketplace and is dominated by cheap trips.

    The correct, revenue-weighted answer is:

        SUM(surge_amount) / SUM(base + distance + time)

    `surge_amount` is stored as CURRENCY precisely so the right calculation is
    also the easy one. The naive average is computed alongside it, purely to
    show how far apart they are (star_schema.md 2.3).
*/

with trips as (

    select *
    from read_parquet('data/processed/fct_trips.parquet')
    where not is_quarantined
      and is_completed
      and total_fare is not null

)

select
    t.request_local_hour                                            as local_hour,
    dt.day_part,
    dt.is_peak_hour,
    w.weather_code,
    count(*)                                                        as completed_trips,

    -- ── Correct: revenue-weighted ────────────────────────────────────────
    round(sum(t.surge_amount), 2)                                   as surge_revenue,
    round(sum(t.base_fare + t.distance_fare + t.time_fare), 2)      as pre_surge_revenue,
    round(100.0 * sum(t.surge_amount)
          / nullif(sum(t.base_fare + t.distance_fare + t.time_fare), 0), 2)
                                                                    as weighted_surge_pct,

    -- ── Wrong, shown for contrast ────────────────────────────────────────
    round(avg(t.surge_multiplier), 3)                               as naive_avg_multiplier,

    round(100.0 * sum(case when t.is_surge_trip then 1 else 0 end) / count(*), 2)
                                                                    as surged_trip_pct,

    /*
        Did surge actually WORK?

        If surge attracted supply, matching times should FALL as surge rises.
        If it only suppressed demand, matching times stay flat or worsen while
        volume drops. Carrying both measures is what makes that distinguishable
        - the surge percentage alone cannot tell the two apart.
    */
    round(avg(t.matching_duration_sec), 1)                          as avg_match_sec,
    round(avg(t.eta_to_pickup_sec), 1)                              as avg_eta_sec

from trips as t
join read_parquet('data/processed/dim_time.parquet') as dt
  on t.request_time_id = dt.time_id
left join read_parquet('data/processed/dim_weather.parquet') as w
  on t.request_weather_id = w.weather_id
group by 1, 2, 3, 4
order by weighted_surge_pct desc
