{{
    config(
        materialized = 'incremental',
        unique_key = 'event_id',
        incremental_strategy = 'delete+insert',
        tags = ['marts','fact']
    )
}}

/*
    GRAIN: one row per event, post-deduplication. The atomic fact.

    fct_trips is an aggregation; this is the record it aggregates. It earns its
    place three ways:
      1. Auditability - any fct_trips number traces to the events behind it.
      2. Pipeline metrics - duplicate rate, lateness and ordering are properties
         of EVENTS, invisible at trip grain.
      3. Future re-modelling - a new mart can be built from atomic events
         without re-reading the landing zone.

    ── This table DELIBERATELY contains events that fail business assertions ──
    It depends only on stg_trip_events, so it builds before the financial tests
    on stg_ride_completed run. Events that violate F1/F2 therefore land here
    even when the build fails.

    That is correct, and it is the point of an audit table: it records what
    ARRIVED, not what was accepted. Verified by injecting two schema-valid but
    financially corrupt trips - all 10 of their events landed here, while
    fct_trips and fct_payments received ZERO of them, because dbt skipped every
    downstream model once the assertions failed.

    So the accurate claim is: bad data never reaches the BUSINESS marts. It
    does reach this one, on purpose, and consumers must join to fct_trips (and
    filter is_quarantined) rather than aggregating measures from here.
*/

with events as (

    select * from {{ ref('stg_trip_events') }}

    {% if is_incremental() %}
    where {{ incremental_window('ingested_at') }}
    {% endif %}

),

/*
    Causal position within the trip.

    Ordered by event_timestamp, NOT ingested_at. Arrival order is routinely
    scrambled by buffering and redelivery - the generator injects that on
    purpose - and a trip whose events arrived out of order is still valid.
*/
sequenced as (

    select
        *,
        row_number() over (
            partition by correlation_id
            order by event_timestamp, event_id
        ) as event_sequence_number,

        -- Did arrival order disagree with causal order for this event?
        row_number() over (partition by correlation_id order by event_timestamp, event_id)
          != row_number() over (partition by correlation_id order by ingested_at, event_id)
            as is_out_of_order
    from events

),

city as (

    select city_id, timezone from {{ ref('dim_city') }}

)

select
    e.event_id,
    e.event_type,
    e.event_version,

    e.correlation_id                                        as trip_id,
    e.partition_key,
    e.causation_id,

    {{ unknown_key('cast(json_extract_string(e.payload_json, \'$.city_id\') as integer)') }}
                                                            as city_id,

    e.event_timestamp,
    e.ingested_at,

    {{ local_date_key('e.event_timestamp', 'coalesce(city.timezone, \'UTC\')') }}
                                                            as event_date_id,
    {{ local_time_key('e.event_timestamp', 'coalesce(city.timezone, \'UTC\')') }}
                                                            as event_time_id,

    e.event_sequence_number,
    e.is_out_of_order,

    -- Signed. Negative means producer clock skew; clamping it to zero would
    -- hide a real defect.
    e.lateness_sec,
    e.is_late_arrival_event,

    -- Duplicates are removed from fct_trips but the DUPLICATE RATE is an
    -- operational metric about the pipeline. It is measured here rather than
    -- flagged, because staging already removed the extra copies - see the
    -- landing-zone reconciliation test.
    false                                                   as is_duplicate,

    e.producer_service,
    e.producer_version,
    e.environment,

    -- The original payload, unmodified. Redundant with fct_trips and kept
    -- deliberately: it makes the fact table self-auditing, so a disputed number
    -- can be checked without re-reading the landing zone.
    e.payload_json,

    e.kafka_topic,
    e.kafka_partition,
    e.kafka_offset,

    '{{ invocation_id }}'                                   as dbt_invocation_id,
    current_timestamp                                       as dbt_loaded_at

from sequenced as e
left join city
       on cast(json_extract_string(e.payload_json, '$.city_id') as integer) = city.city_id
