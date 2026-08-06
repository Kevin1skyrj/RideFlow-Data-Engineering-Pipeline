{{ config(severity='error') }}

/*
    Invariant N2: every distinct trip in staging must appear in fct_trips, and
    vice versa.

    A trip that exists in staging but not in the mart has been silently dropped
    - the most dangerous class of pipeline bug, because it produces no error and
    the mart still looks internally consistent. A trip in the mart with no
    staging events would mean the mart invented one.
*/

with staged as (

    select distinct correlation_id as trip_id
    from {{ ref('stg_trip_events') }}

),

marted as (

    select trip_id from {{ ref('fct_trips') }}

)

select 'missing_from_mart' as issue, trip_id
from staged
where trip_id not in (select trip_id from marted)

union all

select 'not_in_staging' as issue, trip_id
from marted
where trip_id not in (select trip_id from staged)
