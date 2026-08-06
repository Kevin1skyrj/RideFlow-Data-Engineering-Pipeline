{{ config(severity='error') }}

/*
    Invariant F3: amount_charged = trip_fare + tip_amount - discount_amount.

    Also asserts amount_charged > 0. A discount that exceeded the fare would
    produce a negative charge, which is a refund - a completely different
    business event that this schema cannot represent.
*/

select
    payment_id,
    trip_id,
    trip_fare,
    tip_amount,
    discount_amount,
    amount_charged,
    expected_amount_charged
from {{ ref('stg_payment_completed') }}
where abs(expected_amount_charged - amount_charged) > 0.01
   or amount_charged <= 0
