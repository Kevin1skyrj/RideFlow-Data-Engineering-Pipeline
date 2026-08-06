{{ config(materialized='table', tags=['marts','dimension']) }}

/*
    Time-of-day dimension: exactly 1,440 rows, one per minute, reused across
    every date.

    Keyed as HHMM in LOCAL time. Peak-hour boundaries are a BUSINESS DEFINITION
    encoded once here so no two consumers disagree. Changing them changes
    historical metrics and must be versioned, not edited in place.
*/

with minutes as (

    select generated as minute_of_day
    from generate_series(0, 1439, 1) as t(generated)

),

expanded as (

    select
        minute_of_day,

        -- INTEGER division (//), not `/`.
        --
        -- DuckDB's `/` is true division, so `cast(90 / 60 as smallint)` rounds
        -- 1.5 to 2 and the hour is wrong for a large share of minutes. That
        -- produced duplicate time_id values and broke the FK from fct_trips -
        -- a silent corruption of every hour-of-day metric.
        cast(minute_of_day // 60 as smallint) as hour_24,
        cast(minute_of_day % 60 as smallint)  as minute
    from minutes

)

select
    -- HHMM, matching the local_time_key macro.
    cast(hour_24 * 100 + minute as integer)  as time_id,
    hour_24,
    minute,

    printf('%02d:%02d', hour_24, minute)     as time_of_day,

    case
        when hour_24 between 0  and 4  then 'NIGHT'
        when hour_24 between 5  and 6  then 'EARLY_MORNING'
        when hour_24 between 7  and 9  then 'MORNING_PEAK'
        when hour_24 between 10 and 16 then 'MIDDAY'
        when hour_24 between 17 and 20 then 'EVENING_PEAK'
        when hour_24 between 21 and 22 then 'EVENING'
        else 'LATE_NIGHT'
    end                                      as day_part,

    -- 07:00-09:59 and 17:00-20:59 LOCAL. Defined once; every consumer inherits
    -- the same boundary.
    (hour_24 between 7 and 9) or (hour_24 between 17 and 20) as is_peak_hour

from expanded
