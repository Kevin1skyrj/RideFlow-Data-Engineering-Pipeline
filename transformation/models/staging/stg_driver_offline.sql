{{ config(materialized='view', tags=['staging','presence']) }}

/*
    DriverOffline - closes the session opened by DriverOnline.

    A session may legitimately never close: a driver still on duty when the
    observation window ends, or an app that died without sending the event. A
    missing DriverOffline is an OPEN SESSION, not data loss - and it must never
    be closed with an assumed end time, because that fabricates supply hours and
    inflates every utilisation denominator (event_contract.md 5.9).

    online_at is repeated on this event so session duration is computable from
    the offline event alone, surviving the loss of its DriverOnline.
*/

select
    event_id,
    event_timestamp,
    ingested_at,

    {{ payload_uuid('session_id') }}                 as session_id,
    {{ payload_uuid('driver_id') }}                  as driver_id,
    {{ payload_int('city_id') }}                     as city_id,

    {{ payload_timestamp('online_at') }}             as online_at,
    {{ payload_timestamp('offline_at') }}            as offline_at,

    {{ payload_coordinate('driver_lat') }}           as driver_lat,
    {{ payload_coordinate('driver_lon') }}           as driver_lon,
    {{ payload_int('zone_id') }}                     as zone_id,

    -- The utilisation denominator.
    {{ payload_int('session_duration_sec') }}        as session_duration_sec,
    {{ payload_int('trips_completed_in_session') }}  as trips_completed_in_session,

    -- CONNECTION_LOST is not the same as a deliberate sign-off; they mean
    -- opposite things for supply planning.
    {{ payload_text('offline_reason') }}             as offline_reason

from {{ ref('stg_driver_presence') }}
where event_type = 'DriverOffline'
