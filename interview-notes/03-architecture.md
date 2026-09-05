# 03 — RideFlow Architecture

## Architecture diagram

```text
Python Event Generator
        |
        v
Apache Kafka (trip topic + presence topic)
        |
        v                 invalid message
Python Consumer ------------------------------> Kafka DLQ
        |
        v
Immutable Parquet landing zone (data/raw)
        |
        v
dbt: staging → intermediate → marts
        |
        v
DuckDB warehouse
        |
        +-----------------> exported Parquet marts
                              |             |
                              v             v
                           Power BI    Static dashboard

Airflow schedules and controls the dbt/test/export path.
GitHub Actions independently validates code, warehouse, and Kafka paths.
```

## Component responsibilities

| Component | Owns | Must not own |
|---|---|---|
| Generator | Reproducible input and anomaly injection | Downstream correctness |
| Kafka | Durable transport, replay, partition order | Business interpretation |
| Consumer | Contract validation, DLQ, batching, persistence, offsets | Cross-event business rules |
| Raw Parquet | Immutable source history | Dashboard queries |
| dbt | Deduplication, business rules, modelling, tests | Scheduling |
| DuckDB | Local analytical tables | Continuous stream ingestion |
| Airflow | Dependencies, schedule, retry, backfill, concurrency | Transformation SQL |
| Power BI | Presentation and exploration | Canonical business logic |

## The most important boundary

The consumer and dbt never write to the same system. The consumer writes
Parquet. dbt is the only DuckDB writer. This prevents file-lock contention and
makes the raw layer independently replayable.

## End-to-end record journey

1. The generator creates an envelope and type-specific payload.
2. The producer serialises it to JSON and chooses a topic and partition key.
3. Kafka appends it to a partition and assigns an offset.
4. The consumer polls a batch and validates each raw byte sequence.
5. Invalid messages receive a rejection reason in DLQ.
6. Valid events are written to uniquely named Parquet files.
7. Only after persistence are Kafka offsets committed.
8. dbt reads raw files, deduplicates event IDs, and extracts typed payload fields.
9. Intermediate models assemble event histories into trip/session state.
10. Tests either block or warn according to business severity.
11. Marts are materialised in DuckDB and exported to Parquet.
12. Power BI reads stable marts rather than raw events.

## Why these boundaries matter

A boundary is a contract that limits the effect of failure. Kafka lets the
producer continue when transformation is down. Parquet lets ingestion continue
when DuckDB is locked or dbt fails. Exported marts let BI read stable files
without needing write access to DuckDB.

## Failure behavior

| Failure | Expected behavior |
|---|---|
| Broker unavailable | Producer retries, then reports an actionable failure |
| Malformed message | DLQ with original bytes and reason; partition continues |
| Consumer killed before write | Offset uncommitted; Kafka redelivers |
| Consumer killed after write, before commit | Redelivery creates raw duplicate; dbt removes it |
| dbt assertion fails | Downstream export/publication is blocked |
| Warehouse deleted | Rebuild from immutable raw Parquet |
| Dashboard file unavailable | Raw and warehouse history remain unaffected |

## Interview questions

### “Walk me through your architecture.”

Use the record journey, not a technology list. Lead with the business event,
follow it across every boundary, and end with failure behavior and metrics.

### “Why so many components for a portfolio project?”

> Each component proves a distinct concern: Kafka proves replayable transport,
> Parquet creates a durable decoupling boundary, dbt proves testable modelling,
> Airflow proves orchestration and backfill, and Power BI proves consumption.
> I kept each deployment deliberately small—single broker and single-node
> warehouse—and documented where that stops being appropriate.

### “What is the source of truth?”

> The immutable Parquet landing zone is the analytical system of record. Kafka
> is replayable only within retention, and DuckDB is rebuildable derived state.
> Losing raw Parquet is therefore the unrecoverable data-loss event in this
> design.

### “Where does business logic live?”

> In dbt. Ingestion validates structure but does not decide business outcomes,
> while Power BI presents governed mart columns and documented measures. This
> keeps logic testable, versioned, and replayable against history.

### “What would change in production?”

> I would replace local storage with durable object storage, use replicated or
> managed Kafka, move DuckDB to a concurrent cloud warehouse when workload
> requires it, use managed secret storage, external alerting, centralized logs
> and metrics, access controls, and tested disaster recovery. The event and raw
> data contracts should remain stable.
