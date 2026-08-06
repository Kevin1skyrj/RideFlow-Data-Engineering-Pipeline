{{ config(materialized='table', tags=['marts','dimension']) }}

/*
    Calendar dimension, one row per LOCAL date.

    Generated rather than curated. The range is derived from the data with
    padding, so it is deterministic for a given dataset - a fixed hard-coded
    range would either waste rows or silently fail to cover a backfill.

    Split from dim_time deliberately (star_schema.md 6.3): a combined date-time
    dimension at minute grain needs 1,440 rows per DAY - 525,600 a year - and
    makes "demand by hour of day across all dates" awkward. Split, dim_time is
    1,440 rows TOTAL, reused across every date.
*/

with bounds as (

    select
        min(cast(requested_at as date)) - interval 30 day as first_date,
        max(cast(requested_at as date)) + interval 30 day as last_date
    from {{ ref('int_trips_assembled') }}
    where requested_at is not null

),

calendar as (

    select cast(generated as date) as full_date
    from bounds,
         generate_series(bounds.first_date, bounds.last_date, interval 1 day) as t(generated)

)

select
    cast(strftime(full_date, '%Y%m%d') as integer)        as date_id,
    full_date,

    -- ISO weekday: 1 = Monday .. 7 = Sunday. DuckDB's isodow already does this,
    -- unlike dayofweek which is 0-based from Sunday - an easy off-by-one that
    -- would silently shift every weekend flag by a day.
    cast(extract(isodow from full_date) as smallint)      as day_of_week,
    strftime(full_date, '%A')                             as day_name,
    extract(isodow from full_date) >= 6                   as is_weekend,

    cast(extract(week from full_date) as smallint)        as week_of_year,
    cast(extract(day from full_date) as smallint)         as day_of_month,
    cast(extract(month from full_date) as smallint)       as month_number,
    strftime(full_date, '%B')                             as month_name,
    cast(extract(quarter from full_date) as smallint)     as quarter,
    cast(extract(year from full_date) as smallint)        as year,

    /*
        is_holiday is a KNOWN SIMPLIFICATION.

        Indian public holidays vary by state, so a single global flag is wrong
        for some cities. A bridge_city_holiday table is the correct fix
        (data_dictionary.md 8.1). Left false rather than populated with a
        national list that would be wrong per-city - a wrong flag is worse than
        an absent one, because it looks authoritative.
    */
    false                                                 as is_holiday,
    cast(null as varchar)                                 as holiday_name

from calendar
