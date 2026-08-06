{{ config(materialized='view', tags=['staging','trips']) }}

/*
    RideCompleted - the revenue event. Every fare metric originates here.

    The fare is decomposed rather than stored as a single total. Storing only
    total_fare makes "how much revenue came from surge?" unanswerable without
    re-deriving it from assumptions. Components are kept so the total is
    EXPLAINED, not merely stated - and invariant F1 re-checks that they still
    reconcile.
*/

select
    event_id,
    event_timestamp,
    ingested_at,

    {{ payload_uuid('trip_id') }}                    as trip_id,
    {{ payload_uuid('rider_id') }}                   as rider_id,
    {{ payload_uuid('driver_id') }}                  as driver_id,
    {{ payload_int('city_id') }}                     as city_id,
    {{ payload_timestamp('completed_at') }}          as completed_at,

    {{ payload_coordinate('dropoff_lat') }}          as dropoff_lat,
    {{ payload_coordinate('dropoff_lon') }}          as dropoff_lon,
    {{ payload_int('dropoff_zone_id') }}             as dropoff_zone_id,
    {{ payload_decimal('distance_km', 8, 2) }}       as distance_km,
    {{ payload_int('duration_sec') }}                as duration_sec,

    -- Fare components. decimal(12,2) throughout - never DOUBLE.
    {{ payload_money('base_fare') }}                 as base_fare,
    {{ payload_money('distance_fare') }}             as distance_fare,
    {{ payload_money('time_fare') }}                 as time_fare,
    {{ payload_decimal('surge_multiplier', 4, 2) }}  as surge_multiplier,

    -- Surge isolated AS CURRENCY. This is what makes the pricing question
    -- answerable directly instead of via AVG(surge_multiplier), which averages
    -- ratios and describes nothing real.
    {{ payload_money('surge_amount') }}              as surge_amount,

    {{ payload_money('airport_fee') }}               as airport_fee,
    {{ payload_money('toll_amount') }}               as toll_amount,
    {{ payload_money('booking_fee') }}               as booking_fee,

    -- Separated because it is remitted, not earned.
    {{ payload_money('tax_amount') }}                as tax_amount,
    {{ payload_money('total_fare') }}                as total_fare,
    {{ payload_money('driver_payout') }}             as driver_payout,

    -- Net revenue: what the business actually earns.
    {{ payload_money('platform_commission') }}       as platform_commission,

    {{ payload_text('currency') }}                   as currency,
    {{ payload_text('traffic_level') }}              as traffic_level,
    {{ payload_text('weather_code') }}               as weather_code,
    {{ payload_text('payment_method') }}             as payment_method,

    -- Invariant F1 materialised, so a dbt test can assert it directly rather
    -- than re-deriving the sum in the test itself.
    {{ payload_money('base_fare') }}
      + {{ payload_money('distance_fare') }}
      + {{ payload_money('time_fare') }}
      + {{ payload_money('surge_amount') }}
      + {{ payload_money('airport_fee') }}
      + {{ payload_money('toll_amount') }}
      + {{ payload_money('booking_fee') }}
      + {{ payload_money('tax_amount') }}            as fare_component_sum,

    -- Invariant F2: payout + commission must equal the pre-tax subtotal.
    {{ payload_money('driver_payout') }}
      + {{ payload_money('platform_commission') }}   as payout_split_sum,

    {{ payload_money('total_fare') }}
      - {{ payload_money('tax_amount') }}            as pre_tax_subtotal

from {{ ref('stg_trip_events') }}
where event_type = 'RideCompleted'
