{{ config(severity='error') }}

/*
    Invariant F2: driver_payout + platform_commission = total_fare - tax_amount.

    Commission is taken on the PRE-TAX subtotal. Tax is remitted to the
    government, not earned, so including it in the commission base would
    overstate platform revenue on every single trip.
*/

select
    trip_id,
    driver_payout,
    platform_commission,
    payout_split_sum,
    pre_tax_subtotal,
    abs(payout_split_sum - pre_tax_subtotal) as drift
from {{ ref('stg_ride_completed') }}
where abs(payout_split_sum - pre_tax_subtotal) > 0.01
