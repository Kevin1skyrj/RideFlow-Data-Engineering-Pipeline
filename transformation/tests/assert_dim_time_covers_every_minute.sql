{{ config(severity='error') }}

/*
    dim_time must contain exactly 1,440 rows - one per minute of a day.

    A gap would make some trips join to nothing and silently vanish from every
    time-of-day aggregate; a duplicate would fan them out and double-count.
    Neither would raise an error anywhere else.
*/

select
    count(*)                     as actual_rows,
    1440                         as expected_rows,
    count(distinct time_id)      as distinct_keys
from {{ ref('dim_time') }}
having count(*) != 1440
    or count(distinct time_id) != 1440
