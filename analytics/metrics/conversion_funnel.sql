/*
    Q2  CONVERSION FUNNEL - where do riders drop out?

    Only answerable because fct_trips is grained on REQUESTED trips, not
    completed ones. A fact table containing only successes cannot measure
    failure - restricting the grain would have made this question permanently
    unanswerable (star_schema.md 2.1).

    `funnel_stage_reached` mirrors dim_ride_status.funnel_stage, so drop-off
    between consecutive stages is one ordering column rather than hard-coded
    status lists scattered across queries.
*/

with base as (

    select *
    from read_parquet('data/processed/fct_trips.parquet')
    where not is_quarantined

),

stages as (

    select 1 as stage, 'Requested'      as stage_name, count(*) as trips from base
    union all
    select 2, 'Matched',      count(*) from base where funnel_stage_reached >= 2
    union all
    select 3, 'Driver arrived', count(*) from base where funnel_stage_reached >= 3
    union all
    select 4, 'Started',      count(*) from base where funnel_stage_reached >= 4
    union all
    select 5, 'Completed',    count(*) from base where funnel_stage_reached >= 5
    union all
    select 6, 'Paid',         count(*) from base where funnel_stage_reached >= 6

),

funnel as (

    select
        stage,
        stage_name,
        trips,
        first_value(trips) over (order by stage)                    as top_of_funnel,
        lag(trips) over (order by stage)                            as previous_stage
    from stages

)

select
    stage,
    stage_name,
    trips,
    round(100.0 * trips / nullif(top_of_funnel, 0), 2)              as pct_of_requests,
    -- Step conversion is the actionable number: an 8% drop between two stages
    -- is a specific problem, whereas "62% overall conversion" is not.
    round(100.0 * trips / nullif(previous_stage, 0), 2)             as step_conversion_pct,
    coalesce(previous_stage, trips) - trips                         as lost_at_this_step
from funnel
order by stage
