{{ config(severity='error') }}

/*
    Invariant F7: the surge multiplier on RideCompleted must equal the one on
    RideRequested.

    This is the proof that the rider was charged the rate they were quoted. A
    mismatch is not a rounding issue - it means pricing changed under the rider
    mid-trip, which is both a product defect and a regulatory problem.
*/

select
    requested.trip_id,
    requested.surge_multiplier as quoted_surge,
    completed.surge_multiplier as charged_surge
from {{ ref('stg_ride_requested') }} as requested
join {{ ref('stg_ride_completed') }} as completed
  on requested.trip_id = completed.trip_id
where requested.surge_multiplier != completed.surge_multiplier
