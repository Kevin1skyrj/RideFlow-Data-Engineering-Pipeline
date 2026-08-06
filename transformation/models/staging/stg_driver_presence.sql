{{
    config(
        materialized = 'view',
        tags = ['staging', 'presence']
    )
}}

/*
    Base staging for driver presence events.

    A separate model from stg_trip_events, mirroring the separate Kafka topic.
    Presence events have a different grain (one per driver session, not one per
    trip) and a different partition key (driver_id, not trip_id). Merging them
    would force a nullable trip_id that is null 100% of the time for these rows
    and invite incorrect joins (event_contract.md 3.3).

    Same three responsibilities as the trip base model: filter, deduplicate,
    type. Deduplication rationale is identical - see stg_trip_events.
*/

with source as (

    select * from {{ source('landing', 'presence') }}

),

in_scope as (

    select *
    from source
    where environment = '{{ var("target_environment") }}'

),

ranked as (

    select
        *,
        row_number() over (
            partition by event_id
            order by ingested_at asc, kafka_offset asc
        ) as _arrival_rank
    from in_scope

)

select
    event_id,
    event_type,
    event_version,
    event_timestamp,
    ingested_at,

    -- For presence events the partition key is driver_id and the correlation
    -- id is session_id - the join key that pairs an Online with its Offline.
    partition_key,
    correlation_id,
    causation_id,

    producer_service,
    producer_version,
    environment,
    payload_json,

    kafka_topic,
    kafka_partition,
    kafka_offset,

    date_diff('millisecond', event_timestamp, ingested_at) / 1000.0 as lateness_sec,
    date_diff('millisecond', event_timestamp, ingested_at) > 300000 as is_late_arrival_event,

    dt as landed_date,
    hour as landed_hour

from ranked
where _arrival_rank = 1
