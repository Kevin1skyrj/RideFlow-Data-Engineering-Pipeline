{{
    config(
        materialized = 'view',
        tags = ['staging', 'trips']
    )
}}

/*
    Base staging for trip lifecycle events.

    Does exactly three things, and nothing else:
      1. Filters to the target environment.
      2. Deduplicates on event_id  -- the AUTHORITATIVE pass.
      3. Types the envelope.

    Deliberately does NOT join, aggregate, or apply business rules. A staging
    model that joins is doing intermediate-layer work, and once that starts the
    layering stops being enforceable (etl_design.md 5).

    On deduplication
    ----------------
    The consumer also dedupes, but only within one in-memory batch - it cannot
    see a duplicate that arrives hours later after a network partition, or one
    produced by replaying the topic after a bug fix. THIS pass is the guarantee:
    it is recomputed from the entire immutable landing zone on every run, so it
    is correct regardless of how far apart the copies arrived.

    Earliest arrival wins, not latest. Both copies carry an identical payload by
    definition - event_id identifies the same event redelivered, not two
    occurrences - so the choice looks arbitrary. It is not: taking the earliest
    makes the result deterministic, which is what allows a re-run to produce
    byte-identical output. Taking the latest would yield the same payload with a
    different ingested_at, making lateness metrics irreproducible.
*/

with source as (

    select * from {{ source('landing', 'trips') }}

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
    -- Envelope identity
    event_id,
    event_type,
    event_version,

    -- Business time: when it happened on the device. Drives every metric.
    event_timestamp,

    -- Arrival time: when we learned about it. Drives incremental loading and
    -- lag monitoring. Both are mandatory - lateness is exactly their difference,
    -- and with only one of them it cannot be computed at all.
    ingested_at,

    partition_key,
    correlation_id,

    -- NULL means "this event starts a causal chain". Meaningful absence, never
    -- imputed.
    causation_id,

    producer_service,
    producer_version,
    environment,

    -- Retained so per-event-type models can extract their own typed columns,
    -- and so any warehouse number can be traced back to the original event.
    payload_json,

    -- Kafka coordinates: what makes a row traceable to the exact message.
    kafka_topic,
    kafka_partition,
    kafka_offset,

    -- Derived pipeline metrics. Lateness is signed on purpose: negative means
    -- producer clock skew, and clamping it to zero would hide a real defect.
    date_diff('millisecond', event_timestamp, ingested_at) / 1000.0 as lateness_sec,
    date_diff('millisecond', event_timestamp, ingested_at) > 300000 as is_late_arrival_event,

    /*
        PHYSICAL load time - the clock incremental models filter on.

        Distinct from ingested_at, which is a BUSINESS timestamp the generator
        sets so late arrivals are simulable. ingested_at is not monotonic with
        respect to the load: re-consuming a topic re-lands old events today
        while they keep an ingested_at from months ago, and a lookback window
        based on it silently skips them.

        coalesce for backward compatibility: files written before this column
        existed have NULL, and falling back to ingested_at is the best
        available approximation for them.

        Source column QUALIFIED - the output alias is the same name, and DuckDB
        rejects an unqualified self-reference with "column cannot be referenced
        before it is defined".
    */
    coalesce(ranked.landed_at, ranked.ingested_at) as landed_at,

    -- Landing-zone partition columns.
    dt as landed_date,
    hour as landed_hour

from ranked
where _arrival_rank = 1
