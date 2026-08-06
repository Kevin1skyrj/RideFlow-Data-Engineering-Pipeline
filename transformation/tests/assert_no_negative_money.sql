{{ config(severity='error') }}

/*
    Invariant F4: every monetary amount is non-negative.

    Refunds are not negative charges - they are a separate future event type.
    A negative value here means the fare engine produced something impossible.
*/

select trip_id, 'base_fare' as column_name, base_fare as value
from {{ ref('stg_ride_completed') }} where base_fare < 0
union all
select trip_id, 'distance_fare', distance_fare
from {{ ref('stg_ride_completed') }} where distance_fare < 0
union all
select trip_id, 'time_fare', time_fare
from {{ ref('stg_ride_completed') }} where time_fare < 0
union all
select trip_id, 'surge_amount', surge_amount
from {{ ref('stg_ride_completed') }} where surge_amount < 0
union all
select trip_id, 'airport_fee', airport_fee
from {{ ref('stg_ride_completed') }} where airport_fee < 0
union all
select trip_id, 'toll_amount', toll_amount
from {{ ref('stg_ride_completed') }} where toll_amount < 0
union all
select trip_id, 'tax_amount', tax_amount
from {{ ref('stg_ride_completed') }} where tax_amount < 0
union all
select trip_id, 'total_fare', total_fare
from {{ ref('stg_ride_completed') }} where total_fare <= 0
union all
select trip_id, 'driver_payout', driver_payout
from {{ ref('stg_ride_completed') }} where driver_payout < 0
union all
select trip_id, 'platform_commission', platform_commission
from {{ ref('stg_ride_completed') }} where platform_commission < 0
union all
select trip_id, 'cancellation_fee', cancellation_fee
from {{ ref('stg_ride_cancelled') }} where cancellation_fee < 0
