{{
    config(
        materialized = 'incremental',
        unique_key = 'session_id',
        incremental_strategy = 'delete+insert',
        tags = ['marts','fact']
    )
}}

/*
    GRAIN: one row per driver duty session. The SUPPLY side of the marketplace.

    Without this table, "no demand" and "demand with no available supply" are
    indistinguishable - and they are opposite problems with opposite remedies.

    An OPEN session (offline_at null) is legitimate: the driver was still on
    duty when the window closed. session_duration_sec stays NULL rather than
    being imputed, because a fabricated end time inflates every utilisation
    denominator.
*/

with sessions as (

    select * from {{ ref('int_driver_sessions') }}

    {% if is_incremental() %}
    -- Open sessions are ALWAYS reprocessed regardless of the window: one that
    -- is open today may close tomorrow, and a plain high-water mark would
    -- freeze it open forever.
    where {{ incremental_window('online_at') }}
       or session_id in (select session_id from {{ this }} where offline_at is null)
    {% endif %}

),

city as (

    select city_id, timezone from {{ ref('dim_city') }}

)

select
    s.session_id,
    s.driver_id,
    s.vehicle_id,
    {{ unknown_key('s.city_id') }}                        as city_id,
    {{ unknown_key('vt.vehicle_type_id') }}               as vehicle_type_id,
    {{ unknown_key('ds.driver_status_id') }}              as driver_status_id,
    {{ unknown_key('s.online_zone_id') }}                 as online_zone_id,
    {{ unknown_key('s.offline_zone_id') }}                as offline_zone_id,

    {{ local_date_key('s.online_at', 'city.timezone') }}  as online_date_id,
    {{ local_time_key('s.online_at', 'city.timezone') }}  as online_time_id,

    s.online_at,
    s.offline_at,
    s.online_lat,
    s.online_lon,
    s.offline_lat,
    s.offline_lon,
    s.offline_reason,

    -- The utilisation denominator. NULL for open sessions, deliberately.
    s.session_duration_sec,

    s.reported_trips_completed,
    s.dispatched_trips,
    s.observed_trips_completed,
    s.revenue_in_session,
    s.payout_in_session,
    s.on_trip_seconds,

    s.trips_per_online_hour,
    s.utilisation_pct,
    s.zone_drift,
    s.is_session_open,

    -- A device that lost connectivity may under-report. Surfacing the
    -- disagreement beats silently trusting one source.
    s.reported_trips_completed != s.observed_trips_completed
                                                          as counts_disagree,

    s.device_platform,
    s.app_version,

    '{{ invocation_id }}'                                 as dbt_invocation_id,
    current_timestamp                                     as dbt_loaded_at

from sessions as s
left join city                            on s.city_id = city.city_id
left join {{ ref('dim_vehicle_type') }}   as vt on s.vehicle_type  = vt.vehicle_type_code
left join {{ ref('dim_driver_status') }}  as ds on s.driver_status = ds.driver_status_code
