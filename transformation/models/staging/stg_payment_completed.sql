{{ config(materialized='view', tags=['staging','trips']) }}

/*
    PaymentCompleted - asserts SUCCESS by name.

    payment_status is therefore always 'SUCCEEDED' in contract v1.0.0. That must
    NOT be read as a 100% payment success rate: a payment that never succeeds
    emits nothing at all (event_contract.md 0.1, Gap 2). had_failed_attempts is
    the only available proxy, and it undercounts - it can only see failures that
    were eventually followed by a success.
*/

select
    event_id,
    event_timestamp,
    ingested_at,

    -- Physical load time. fct_payments filters its incremental window on this,
    -- so it must be carried through - see stg_trip_events for why it is not
    -- ingested_at.
    landed_at,

    {{ payload_uuid('payment_id') }}                as payment_id,
    {{ payload_uuid('trip_id') }}                   as trip_id,
    {{ payload_uuid('rider_id') }}                  as rider_id,
    {{ payload_uuid('driver_id') }}                 as driver_id,
    {{ payload_int('city_id') }}                    as city_id,
    {{ payload_timestamp('paid_at') }}              as paid_at,

    {{ payload_text('payment_method') }}            as payment_method,
    {{ payload_text('payment_status') }}            as payment_status,

    {{ payload_money('trip_fare') }}                as trip_fare,

    -- Passed to the driver in full - never commissioned.
    {{ payload_money('tip_amount') }}               as tip_amount,

    -- Platform-funded: the driver is still paid on the full fare, so this
    -- reduces NET REVENUE, not driver payout.
    {{ payload_money('discount_amount') }}          as discount_amount,
    {{ payload_money('amount_charged') }}           as amount_charged,

    {{ payload_text('currency') }}                  as currency,
    {{ payload_text_nullable('promo_code') }}       as promo_code,
    {{ payload_int('attempt_number') }}             as attempt_number,
    {{ payload_text_nullable('previous_attempt_failure_reason') }}
                                                    as previous_attempt_failure_reason,

    -- The join key for finance reconciliation against the gateway ledger.
    {{ payload_text('gateway_reference') }}         as gateway_reference,

    {{ payload_int('attempt_number') }} > 1         as had_failed_attempts,

    -- Invariant F3 materialised for direct testing.
    {{ payload_money('trip_fare') }}
      + {{ payload_money('tip_amount') }}
      - {{ payload_money('discount_amount') }}      as expected_amount_charged

from {{ ref('stg_trip_events') }}
where event_type = 'PaymentCompleted'
