{{ config(materialized='view', tags=['staging','presence']) }}

/*
    DriverOnline - a driver going on duty.

    Supply side, not trip side. The grain is one row per driver session, and
    there is no trip_id at all. zone_id here is what makes marketplace health
    answerable: without it, "no demand" and "demand with no available supply"
    are indistinguishable, and they are opposite problems.
*/

select
    event_id,
    event_timestamp,
    ingested_at,

    {{ payload_uuid('session_id') }}       as session_id,
    {{ payload_uuid('driver_id') }}        as driver_id,
    {{ payload_int('city_id') }}           as city_id,
    {{ payload_timestamp('online_at') }}   as online_at,

    {{ payload_coordinate('driver_lat') }} as driver_lat,
    {{ payload_coordinate('driver_lon') }} as driver_lon,
    {{ payload_int('zone_id') }}           as zone_id,

    {{ payload_uuid('vehicle_id') }}       as vehicle_id,
    {{ payload_text('vehicle_type') }}     as vehicle_type,

    -- Captured at session START only. Intra-session transitions
    -- (AVAILABLE -> ON_TRIP -> AVAILABLE) are NOT observable in v1.0.0, so
    -- utilisation must be derived by overlapping trips against session windows
    -- rather than read from status history (reference_data.md 7).
    {{ payload_text('driver_status') }}    as driver_status,

    {{ payload_text('device_platform') }}  as device_platform,
    {{ payload_text('app_version') }}      as app_version

from {{ ref('stg_driver_presence') }}
where event_type = 'DriverOnline'
