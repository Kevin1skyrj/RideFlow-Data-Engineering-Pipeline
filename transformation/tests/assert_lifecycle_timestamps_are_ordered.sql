{{ config(severity='error') }}

/*
    Invariant T2: requested < accepted < arrived <= started < completed <= paid.

    Note this is asserted on BUSINESS time (event_timestamp fields), not arrival
    time. Arrival order is routinely scrambled by buffering and redelivery - the
    generator injects that on purpose - and a trip whose events arrived out of
    order is still perfectly valid. Only the business timeline must be coherent.
*/

with lifecycle as (

    select
        requested.trip_id,
        requested.requested_at,
        accepted.accepted_at,
        arrived.arrived_at,
        started.started_at,
        completed.completed_at,
        payment.paid_at
    from {{ ref('stg_ride_requested') }} as requested
    left join {{ ref('stg_ride_accepted') }}  as accepted  using (trip_id)
    left join {{ ref('stg_driver_arrived') }} as arrived   using (trip_id)
    left join {{ ref('stg_ride_started') }}   as started   using (trip_id)
    left join {{ ref('stg_ride_completed') }} as completed using (trip_id)
    left join {{ ref('stg_payment_completed') }} as payment using (trip_id)

)

select *
from lifecycle
where accepted_at  <= requested_at
   or arrived_at   <= accepted_at
   or started_at   <  arrived_at
   or completed_at <= started_at
   or paid_at      <  completed_at
