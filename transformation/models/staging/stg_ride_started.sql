{{ config(materialized='view', tags=['staging','trips']) }}

/*  RideStarted - the revenue-eligibility boundary. A trip that never starts
    can never generate fare revenue.  */

select
    event_id,
    event_timestamp,
    ingested_at,

    {{ payload_uuid('trip_id') }}                 as trip_id,
    {{ payload_uuid('rider_id') }}                as rider_id,
    {{ payload_uuid('driver_id') }}               as driver_id,
    {{ payload_int('city_id') }}                  as city_id,
    {{ payload_timestamp('started_at') }}         as started_at,

    {{ payload_coordinate('actual_pickup_lat') }} as actual_pickup_lat,
    {{ payload_coordinate('actual_pickup_lon') }} as actual_pickup_lon,
    {{ payload_int('rider_wait_duration_sec') }}  as rider_wait_duration_sec,
    {{ payload_int('pickup_zone_id') }}           as pickup_zone_id

from {{ ref('stg_trip_events') }}
where event_type = 'RideStarted'
