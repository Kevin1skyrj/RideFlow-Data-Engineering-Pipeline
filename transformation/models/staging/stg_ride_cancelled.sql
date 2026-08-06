{{ config(materialized='view', tags=['staging','trips']) }}

/*
    RideCancelled - terminal, and mutually exclusive with RideCompleted (S4).
    That exclusivity is why the maximum event count per trip is 6, not 7.
*/

select
    event_id,
    event_timestamp,
    ingested_at,

    {{ payload_uuid('trip_id') }}                  as trip_id,
    {{ payload_uuid('rider_id') }}                 as rider_id,

    -- NULLABLE and meaningful: null means no driver had been matched yet.
    -- Invariant R2 permits it only when cancelled_at_status = 'REQUESTED'.
    {{ payload_uuid('driver_id') }}                as driver_id,

    {{ payload_int('city_id') }}                   as city_id,
    {{ payload_timestamp('cancelled_at') }}        as cancelled_at,
    {{ payload_text('cancelled_by') }}             as cancelled_by,

    -- Enum, not free text. Free text cannot be aggregated, and the reason
    -- distribution is the entire point of collecting it.
    {{ payload_text('cancellation_reason_code') }} as cancellation_reason_code,

    -- THE key funnel column: cancelling pre-match and post-arrival are
    -- completely different business problems with opposite remedies.
    {{ payload_text('cancelled_at_status') }}      as cancelled_at_status,

    {{ payload_int('seconds_since_request') }}     as seconds_since_request,
    {{ payload_money('cancellation_fee') }}        as cancellation_fee,

    -- Separate from the amount: a fee can be assessed and then waived. The gap
    -- between the two is the waiver rate.
    {{ payload_bool('is_fee_charged') }}           as is_fee_charged,
    {{ payload_bool('is_driver_fault') }}          as is_driver_fault,
    {{ payload_text('currency') }}                 as currency

from {{ ref('stg_trip_events') }}
where event_type = 'RideCancelled'
