{{ config(severity='error') }}

/*
    Invariant N4: no trip may exist in fct_trips without at least one row in
    fct_trip_events.

    fct_trips is an AGGREGATION of fct_trip_events. A trip with no underlying
    events would mean the aggregation fabricated a row - and because the trip
    would still carry plausible-looking measures, nothing downstream would
    notice.
*/

select
    t.trip_id,
    t.ride_status_id,
    t.event_count
from {{ ref('fct_trips') }} as t
left join (
    select distinct trip_id from {{ ref('fct_trip_events') }}
) as e using (trip_id)
where e.trip_id is null
