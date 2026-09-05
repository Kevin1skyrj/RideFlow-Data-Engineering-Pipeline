# 14 — High-Probability Interview Answers

## “Tell me about a challenging technical decision.”

> DuckDB is embedded and allows one writer. A continuous Kafka consumer writing
> into it would contend with scheduled dbt jobs. I considered PostgreSQL, which
> supports concurrent writes but weakens the zero-setup columnar analytical
> goal; explicit lock coordination, which tightly couples services; and an
> immutable Parquet boundary. I chose Parquet. The consumer only lands files,
> dbt is the only DuckDB writer, and Airflow allows one active run. The trade-off
> is file management, but I gained replayability and engine neutrality.

Follow-ups: Why not Postgres? What if two runs start? What is unrecoverable?

## “Tell me about a failure you debugged.”

> My CI warehouse job failed on a clean runner even though local tests passed.
> I reproduced it from a Git archive and found two hidden assumptions: the data
> directories did not exist, and the generator produced JSONL while dbt expected
> raw Parquet. I added explicit runtime directory creation and a direct landing
> adapter that reuses the production serializer, validator, and Parquet writer.
> The real Kafka path remains a separate CI job. The next workflow passed all
> four jobs. The lesson was that local state can create false confidence.

## “Tell me about a data bug.”

> Marketplace demand and supply were both zone-hour aggregates, but I joined
> them only by zone. That multiplied hours against each other and inflated the
> result while still looking plausible. I corrected the join to the complete
> zone-hour grain and added checks. I now declare grain before designing joins.

## “How do you handle late and out-of-order data?”

> I preserve event time, ingestion time, and physical landing time separately.
> I do not trust arrival order for lifecycle correctness; dbt assembles the full
> trip using timestamps and causation. Normal incremental runs reprocess a
> 48-hour lookback using delete-and-insert, while older valid corrections use a
> bounded backfill. This avoids assigning late events to the wrong business
> hour or missing them silently.

## “How do you ensure data quality?”

> I separate structural and business quality. The consumer validates raw JSON
> and routes unusable messages to DLQ. dbt enforces uniqueness, nullability,
> relationships, lifecycle ordering, and financial invariants. Schema-valid but
> questionable trips are quarantined and excluded from metrics, not deleted.
> Reconciliation proves conservation across layers, and Airflow blocks export
> when error-severity tests fail.

## “How do you avoid duplicates?”

> I accept at-least-once delivery because persistence happens before offset
> commit. Batch-local dedup reduces unnecessary files, while staging performs
> the correctness dedup across history by stable event ID and earliest arrival.
> Incremental windows replace existing rows. A forced crash recovered every
> distinct valid event and the 13 expected redeliveries disappeared downstream.

## “Why should we hire a full-stack developer for data engineering?”

> My application background helps me understand producers, API contracts,
> failure handling, observability, and deployment. I deliberately built RideFlow
> to close the data-specific gaps: event-time semantics, Kafka offsets,
> dimensional grain, incremental backfills, reconciliation, and BI metrics. I
> do not claim years of data-engineering experience, but I can demonstrate the
> concepts in a tested system and explain both the implementation and its limits.

## “What would you improve next?”

> I would not add tools without a measured need. The most valuable next steps
> are an explicit payment-failure and ride-expiry contract, external alert
> delivery, and empirical TLC calibration. For a production deployment I would
> first replace local raw storage and the single broker with durable replicated
> services and add security and observability.

## “What are three weaknesses?”

> First, replication factor 1 and local raw files are not highly available.
> Second, contract v1 cannot represent terminal payment failure or explicit ride
> expiry. Third, Power BI is a binary Windows-only artifact. I mitigate them
> through documented scope, immutable open-format exports, and clear migration
> triggers, but I do not present those mitigations as eliminating the limits.

## Behavioral STAR map

| Question | Story |
|---|---|
| Difficult decision | DuckDB lock → immutable Parquet boundary |
| Failure/debugging | Clean-checkout CI JSONL/Parquet mismatch |
| Mistake | Zone-only join caused zone-hour fan-out |
| Quality ownership | Financial chaos injection blocked business marts |
| Resilience | SIGKILL, redelivery, zero missing IDs |
| Learning quickly | Full-stack background → event/data modelling curriculum |
| Scope management | Deferred Spark/Kubernetes/cloud until justified |

For STAR: give Situation and Task briefly, spend most time on your Action, and
finish with a measured Result plus what you learned.

## Mock interview sequence

1. Give your 90-second pitch.
2. Draw the architecture.
3. Explain exactly-once versus RideFlow's actual guarantee.
4. Explain three clocks.
5. Explain grain and the fan-out incident.
6. Explain the DuckDB decision.
7. Handle “production ready?” without becoming defensive.
8. Scale the system using measured bottlenecks.
9. Give the CI debugging story.
10. Volunteer three limitations.

After each answer, ask yourself: “How do I know?” and “When would this choice
be wrong?”
