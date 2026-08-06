{{
    config(
        materialized = 'incremental',
        unique_key = 'payment_id',
        incremental_strategy = 'delete+insert',
        tags = ['marts','fact']
    )
}}

/*
    GRAIN: one row per payment.

    Separate from fct_trips for four reasons, and the fourth is decisive:
      1. Different grain - not every trip has a payment.
      2. Different timing - settlement happens after the trip.
      3. Different identity - payment_id is its own key.
      4. It WILL become one-to-many. The moment the first refund exists, a trip
         has multiple payment records. Folding payments into fct_trips would
         then require restructuring the primary fact table - the expensive kind
         of migration. Separating now costs one join and avoids it.
*/

with payments as (

    select * from {{ ref('stg_payment_completed') }}

    {% if is_incremental() %}
    where {{ incremental_window('landed_at') }}
    {% endif %}

),

city as (

    select city_id, timezone from {{ ref('dim_city') }}

)

select
    pay.payment_id,
    pay.trip_id,
    pay.rider_id,
    pay.driver_id,
    {{ unknown_key('pay.city_id') }}                     as city_id,
    {{ unknown_key('method.payment_method_id') }}        as payment_method_id,
    {{ unknown_key('status.payment_status_id') }}        as payment_status_id,

    {{ local_date_key('pay.paid_at', 'city.timezone') }} as paid_date_id,
    {{ local_time_key('pay.paid_at', 'city.timezone') }} as paid_time_id,

    pay.paid_at,

    -- Additive measures
    pay.trip_fare,
    pay.tip_amount,
    pay.discount_amount,
    pay.amount_charged,
    pay.currency,

    pay.promo_code,
    pay.attempt_number,
    pay.previous_attempt_failure_reason,
    pay.gateway_reference,
    pay.had_failed_attempts,

    -- Expected settlement, from the method's own delay. CASH settles instantly
    -- but the driver holds the money; CORPORATE settles at 30 days. A business
    -- reading revenue without settlement timing can be profitable and
    -- insolvent at the same time.
    pay.paid_at + (interval 1 hour * method.settlement_delay_hours)
                                                         as settlement_due_at,

    -- Real margin leakage, invisible in fare data. The 1.7-point spread
    -- between CARD and UPI is a material profitability lever at scale.
    round(pay.amount_charged * method.gateway_fee_pct / 100, 2)
                                                         as gateway_fee_amount,

    '{{ invocation_id }}'                                as dbt_invocation_id,
    current_timestamp                                    as dbt_loaded_at

from payments as pay
left join city                             on pay.city_id = city.city_id
left join {{ ref('dim_payment_method') }}  as method
       on pay.payment_method = method.payment_method_code
left join {{ ref('dim_payment_status') }}  as status
       on pay.payment_status = status.payment_status_code
