{{ config(severity='error') }}

/*
    Invariant R2: driver_id may be null on RideCancelled ONLY when the trip was
    cancelled before a driver was matched.

    Null here is MEANINGFUL ABSENCE - it is the fact that no driver had been
    assigned - not missing data. A null at any later stage means a driver
    vanished from a trip they were assigned to, which is corruption.

    The converse is also checked: a pre-match cancellation must NOT carry a
    driver, because there was none to carry.
*/

select
    trip_id,
    cancelled_at_status,
    driver_id,
    case
        when driver_id is null and cancelled_at_status != 'REQUESTED'
            then 'null driver_id after a driver was assigned'
        else 'driver_id present on a pre-match cancellation'
    end as violation
from {{ ref('stg_ride_cancelled') }}
where (driver_id is null and cancelled_at_status != 'REQUESTED')
   or (driver_id is not null and cancelled_at_status = 'REQUESTED')
