{{ config(materialized='view', tags=['staging','trips']) }}

/*
    Typed columns for RideRequested - the first event of every trip and the
    origin of the conversion funnel.

    Extraction happens per event type because the shape is only knowable here.
    Flattening all nine types into one table would give a very wide, very
    sparse structure where most columns are null for most rows.
*/

select
    event_id,
    event_timestamp,
    ingested_at,
    lateness_sec,

    {{ payload_uuid('trip_id') }}                    as trip_id,
    {{ payload_uuid('rider_id') }}                   as rider_id,
    {{ payload_int('city_id') }}                     as city_id,
    {{ payload_timestamp('requested_at') }}          as requested_at,

    {{ payload_coordinate('pickup_lat') }}           as pickup_lat,
    {{ payload_coordinate('pickup_lon') }}           as pickup_lon,
    {{ payload_int('pickup_zone_id') }}              as pickup_zone_id,
    {{ payload_coordinate('dropoff_lat') }}          as dropoff_lat,
    {{ payload_coordinate('dropoff_lon') }}          as dropoff_lon,
    {{ payload_int('dropoff_zone_id') }}             as dropoff_zone_id,

    {{ payload_text('vehicle_type') }}               as vehicle_type,
    {{ payload_money('estimated_fare') }}            as estimated_fare,
    {{ payload_decimal('estimated_distance_km', 8, 2) }} as estimated_distance_km,
    {{ payload_int('estimated_duration_sec') }}      as estimated_duration_sec,

    -- Frozen at request: the rider is charged what they were quoted. Invariant
    -- F7 asserts this matches the value on RideCompleted.
    {{ payload_decimal('surge_multiplier', 4, 2) }}  as surge_multiplier,

    {{ payload_text('payment_method') }}             as payment_method,

    -- Point-in-time snapshot. NEVER join dim_rider for this: a rider who was
    -- SILVER in March and GOLD in August must show as SILVER on March trips.
    {{ payload_text('customer_tier') }}              as customer_tier,

    {{ payload_bool('is_airport_pickup') }}          as is_airport_pickup,
    {{ payload_bool('is_airport_dropoff') }}         as is_airport_dropoff,
    {{ payload_text('weather_code') }}               as weather_code,
    {{ payload_text('traffic_level') }}              as traffic_level,
    {{ payload_text_nullable('promo_code') }}        as promo_code,
    {{ payload_text('device_platform') }}            as device_platform,
    {{ payload_text('app_version') }}                as app_version

from {{ ref('stg_trip_events') }}
where event_type = 'RideRequested'
