# 04 — Events and Data Contracts

## What is an event?

An event is an immutable statement that something happened in the past, such
as `RideRequested` or `PaymentCompleted`. A command asks a system to do
something, such as `AcceptRide`. Commands can be rejected; events describe an
outcome that already occurred.

RideFlow uses past-tense PascalCase names so intent is unambiguous.

## Envelope and payload

Every event has a common envelope—identity, type, version, timestamps,
partition key, correlation/causation IDs, producer, and environment—plus a
type-specific payload. The envelope supports transport and traceability while
the payload carries business attributes.

Important identifiers:

- `event_id` uniquely identifies one event and is the deduplication key.
- `partition_key` selects Kafka ordering scope: trip ID or driver ID.
- `correlation_id` groups a business conversation, normally a trip.
- `causation_id` points to the event that caused this event.

## What is a data contract?

A data contract is an explicit agreement between producers and consumers about
structure, meaning, allowed values, versions, and quality expectations. Without
one, a producer can rename a field and silently break analytics.

RideFlow keeps JSON Schemas in the event-contract documentation and tests
generated events against them.

## Schema evolution

- Patch: clarification or validation that does not change compatibility.
- Minor: backward-compatible addition such as an optional field.
- Major: breaking change such as removing/renaming a field or changing meaning.

Changing meaning without changing a field name is especially dangerous because
parsing succeeds while metrics become silently wrong.

RideFlow rejects unknown `event_type` because no payload schema exists for it,
but generally tolerates additional payload fields so an older consumer can
continue when a producer adds information.

## Three clocks

| Clock | Meaning | Used for |
|---|---|---|
| `event_timestamp` | When the action occurred | Business date/hour and sequence |
| `ingested_at` | When it entered the modelled ingestion flow | Lateness and bounds |
| `landed_at` | When this process physically wrote the row | Incremental replay selection |

This distinction is a high-probability interview topic. A late event happened
yesterday but arrived today. Business metrics belong to yesterday; operational
latency belongs to today; a replay-safe incremental loader needs physical load
time so it notices the new file.

## Validation order

RideFlow checks parseable JSON, object shape, required envelope fields, known
event type, semantic version, timestamps, timestamp bounds, envelope schema,
and payload schema. Ordering matters: malformed bytes should be reported as
malformed JSON, not as a confusing missing-field error.

## Interview answers

### “Why use events instead of storing the latest trip status?”

> Current status loses history. Events preserve how and when the trip moved
> through its lifecycle, so I can reconstruct funnels, durations, late arrival,
> and corrections. The cost is that consumers must assemble state and handle
> duplicates and ordering.

### “Does Kafka ordering solve out-of-order business events?”

> It preserves append order within a partition, not truth about causal order.
> A mobile client may upload an older event later. I therefore retain event
> timestamps and causation IDs and resolve the full sequence in dbt.

### “Why keep the original malformed bytes?”

> Parsing or reserialising may destroy the defect. The DLQ stores base64 raw
> bytes plus topic, partition, offset, reason, and time so the producer bug can
> be diagnosed and replayed after correction.

### “What is missing from contract v1?”

> There is no explicit `RideExpired` or `PaymentFailed`, pooled rides lack a
> shared segment model, and intra-session driver status changes are absent. I
> documented these rather than inventing metrics the contract cannot support.
