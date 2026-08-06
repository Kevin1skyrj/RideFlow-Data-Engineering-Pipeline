{{ config(severity='error') }}

/*
    Sequence rule S4: RideCompleted and RideCancelled are mutually exclusive.

    This is the rule that makes the maximum event count per trip 6 rather than
    7 - a fact that was wrong in the data dictionary until a validator caught
    it. A trip appearing in both would mean revenue was recognised on a trip
    that also counts as a funnel drop-out, corrupting both metrics at once.
*/

select
    completed.trip_id,
    completed.completed_at,
    cancelled.cancelled_at
from {{ ref('stg_ride_completed') }} as completed
join {{ ref('stg_ride_cancelled') }} as cancelled
  on completed.trip_id = cancelled.trip_id
