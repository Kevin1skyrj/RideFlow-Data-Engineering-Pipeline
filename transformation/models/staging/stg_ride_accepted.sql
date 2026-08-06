{{ config(materialized='view', tags=['staging','trips']) }}

/*  RideAccepted - the first event carrying driver_id, and the end of the
    matching stage of the funnel.  */

select
    event_id,
    event_timestamp,
    ingested_at,

    {{ payload_uuid('trip_id') }}                     as trip_id,
    {{ payload_uuid('rider_id') }}                    as rider_id,
    {{ payload_uuid('driver_id') }}                   as driver_id,
    {{ payload_int('city_id') }}                      as city_id,
    {{ payload_timestamp('accepted_at') }}            as accepted_at,

    {{ payload_coordinate('driver_lat') }}            as driver_lat,
    {{ payload_coordinate('driver_lon') }}            as driver_lon,
    {{ payload_int('driver_zone_id') }}               as driver_zone_id,
    {{ payload_uuid('vehicle_id') }}                  as vehicle_id,
    {{ payload_text('vehicle_type') }}                as vehicle_type,

    -- Stored, not derived: survives even if RideRequested is lost.
    {{ payload_int('matching_duration_sec') }}        as matching_duration_sec,
    {{ payload_int('eta_to_pickup_sec') }}            as eta_to_pickup_sec,

    -- Deadhead: driven unpaid. Directly reduces driver earning efficiency.
    {{ payload_decimal('distance_to_pickup_km', 8, 2) }} as distance_to_pickup_km,

    -- Point-in-time. Joining dim_driver.current_rating would answer "what is
    -- this driver's rating today", not "what was it when the match was made".
    {{ payload_decimal('driver_rating_at_accept', 3, 2) }} as driver_rating_at_accept,

    -- > 1 means drivers declined first: a supply-quality signal invisible in
    -- trip counts.
    {{ payload_int('dispatch_attempt_number') }}      as dispatch_attempt_number

from {{ ref('stg_trip_events') }}
where event_type = 'RideAccepted'
