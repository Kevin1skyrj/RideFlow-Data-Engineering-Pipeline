{{ config(materialized='view', tags=['staging','trips']) }}

/*  DriverArrived - optional. A trip cancelled before arrival never emits it. */

select
    event_id,
    event_timestamp,
    ingested_at,

    {{ payload_uuid('trip_id') }}                  as trip_id,
    {{ payload_uuid('rider_id') }}                 as rider_id,
    {{ payload_uuid('driver_id') }}                as driver_id,
    {{ payload_int('city_id') }}                   as city_id,
    {{ payload_timestamp('arrived_at') }}          as arrived_at,

    {{ payload_coordinate('driver_lat') }}         as driver_lat,
    {{ payload_coordinate('driver_lon') }}         as driver_lon,
    {{ payload_int('actual_pickup_duration_sec') }} as actual_pickup_duration_sec,

    -- SIGNED on purpose. Negative means the driver arrived early. Taking the
    -- absolute value would hide systematic ETA over-estimation, which is just
    -- as damaging as under-estimation.
    {{ payload_int('arrival_delay_sec') }}         as arrival_delay_sec,

    -- The 3-minute threshold is encoded in the contract so every consumer
    -- agrees on what "late" means.
    {{ payload_bool('is_late_arrival') }}          as is_late_arrival

from {{ ref('stg_trip_events') }}
where event_type = 'DriverArrived'
