{{ config(materialized='table', tags=['intermediate']) }}

/*
    Pair each DriverOnline with its DriverOffline on session_id.

    An unmatched DriverOnline is an OPEN SESSION - the driver was still on duty
    when the observation window closed, or the app died without sending the
    event. It is legitimate data, not a gap.

    An open session must NEVER be closed with an assumed end time. Doing so
    fabricates supply hours and inflates every utilisation denominator
    (event_contract.md 5.9). session_duration_sec stays NULL, and open sessions
    are excluded from completed-session metrics rather than guessed at.

    The reverse - an Offline with no Online - also occurs and is expected: the
    Online was quarantined in the DLQ. Verified on real data
    (SCHEMA_VIOLATION: 'driver_status' is a required property).

    ── Trip attribution ────────────────────────────────────────────────────
    A trip belongs to the session that DISPATCHED it (accepted_at), not the one
    it happened to finish in (completed_at). Drivers routinely complete the fare
    they were on when their shift nominally ends: measured here, 392 of 531
    completed trips finished after their driver's offline event.

    Attributing on completed_at pushed those trips out of every session,
    collapsing agreement with the driver-reported count to 109 of 502 sessions.
    Attributing on accepted_at raises it to 490 of 502.
*/

with online as (

    select * from {{ ref('stg_driver_online') }}

),

offline as (

    select * from {{ ref('stg_driver_offline') }}

),

-- Window per session, resolved once. Doing this here rather than as a
-- correlated subquery inside a join condition keeps the plan simple and the
-- semantics obvious.
session_window as (

    select
        onl.session_id,
        onl.driver_id,
        onl.online_at,
        off.offline_at
    from online as onl
    left join offline as off using (session_id)

),

-- Trips dispatched during each session.
--
-- Necessary because driver_status is captured only at session START:
-- AVAILABLE -> ON_TRIP -> AVAILABLE transitions are not observable in contract
-- v1.0.0, so utilisation cannot be read from status history and must be
-- reconstructed by overlap (reference_data.md 7).
trips_in_session as (

    select
        win.session_id,
        count(trips.trip_id)                                   as trips_dispatched,
        count(trips.completed_at)                              as trips_completed,
        coalesce(sum(trips.total_fare), 0)                     as revenue_in_session,
        coalesce(sum(trips.driver_payout), 0)                  as payout_in_session,
        coalesce(sum(trips.duration_sec), 0)                   as on_trip_seconds
    from session_window as win
    left join {{ ref('int_trips_assembled') }} as trips
      on  trips.driver_id   = win.driver_id
      and trips.accepted_at is not null
      and trips.accepted_at >= win.online_at
      and (win.offline_at is null or trips.accepted_at < win.offline_at)
    group by 1

)

select
    onl.session_id,
    onl.driver_id,
    onl.city_id,
    onl.vehicle_id,
    onl.vehicle_type,
    onl.driver_status,
    onl.device_platform,
    onl.app_version,

    onl.online_at,
    off.offline_at,
    onl.zone_id                              as online_zone_id,
    off.zone_id                              as offline_zone_id,
    onl.driver_lat                           as online_lat,
    onl.driver_lon                           as online_lon,
    off.driver_lat                           as offline_lat,
    off.driver_lon                           as offline_lon,
    off.offline_reason,

    -- NULL for an open session, deliberately. This is the utilisation
    -- denominator, and a fabricated value corrupts every driver productivity
    -- metric downstream.
    off.session_duration_sec,

    -- What the driver's own device reported.
    off.trips_completed_in_session           as reported_trips_completed,

    -- What the trip data actually shows. Keeping both makes any disagreement
    -- visible instead of silently picking one - a device that lost
    -- connectivity may under-report.
    coalesce(tis.trips_dispatched, 0)        as dispatched_trips,
    coalesce(tis.trips_completed, 0)         as observed_trips_completed,
    coalesce(tis.revenue_in_session, 0)      as revenue_in_session,
    coalesce(tis.payout_in_session, 0)       as payout_in_session,
    coalesce(tis.on_trip_seconds, 0)         as on_trip_seconds,

    off.offline_at is null                   as is_session_open,

    case
        when off.session_duration_sec > 0
        then round(coalesce(tis.trips_completed, 0) * 3600.0 / off.session_duration_sec, 2)
    end                                      as trips_per_online_hour,

    -- Share of the shift actually spent carrying a rider. EN_ROUTE time is
    -- unpaid deadhead, so this is the metric that predicts driver churn.
    case
        when off.session_duration_sec > 0
        then round(coalesce(tis.on_trip_seconds, 0) * 100.0 / off.session_duration_sec, 2)
    end                                      as utilisation_pct,

    case
        when off.zone_id is not null then off.zone_id != onl.zone_id
    end                                      as zone_drift

from online as onl
left join offline as off using (session_id)
left join trips_in_session as tis using (session_id)
