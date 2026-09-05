# 02 — Data Engineering Foundations

## What is data engineering?

Data engineering is the discipline of moving, storing, transforming, governing,
and serving data so that other systems and people can use it reliably. The job
is not merely to copy records. A useful pipeline must preserve meaning, handle
failure, make history reproducible, expose quality problems, and deliver data at
an agreed freshness.

In RideFlow, the input is an event stream and the output is trusted analytical
tables and dashboards.

## Application engineering versus data engineering

| Full-stack viewpoint | Data-engineering viewpoint |
|---|---|
| Handle one request correctly | Process every record across a time window |
| Current database state matters | Historical changes and replay matter |
| Request latency is central | Throughput, freshness, and correctness are central |
| Roll back one transaction | Rebuild or backfill a dataset safely |
| Validate one API payload | Enforce contracts plus cross-record business rules |
| Avoid duplicate HTTP actions | Make retries and repeated batches idempotent |

Your software background transfers directly: APIs resemble producers,
background workers resemble consumers, schemas are contracts, queues decouple
services, and CI still protects changes. The new part is thinking in datasets,
grains, time windows, and replay.

## Pipeline

A data pipeline is a repeatable path that converts source data into a useful
destination. It includes dependencies, failure behavior, state, validation, and
operational ownership—not only a sequence of scripts.

RideFlow's pipeline is:

```text
generate → transport → validate → land → transform → test → export → visualise
```

## OLTP versus OLAP

**OLTP** systems serve operational transactions: create a ride, assign a
driver, update payment status. They favor many small reads/writes, normalized
tables, indexes, and low latency.

**OLAP** systems serve analytical queries: completion rate by zone and hour,
monthly commission, or funnel conversion. They favor large scans, columnar
storage, denormalised dimensional models, and aggregations.

RideFlow's hypothetical booking service would be OLTP. DuckDB and the marts are
OLAP. You avoid running large analytical scans on the operational database
because they can compete with customer-facing workloads and its normalized
schema is inconvenient for analysis.

## ETL versus ELT

- **ETL:** extract, transform, then load. Data is shaped before reaching the
  analytical destination.
- **ELT:** extract, load raw data, then transform inside or near the warehouse.

RideFlow is closer to ELT: the consumer performs only contract validation and
lands raw payloads; dbt applies business transformations afterward. This keeps
history replayable and business logic version-controlled.

## Batch versus streaming

Streaming continuously transports events with low delay. Batch processing
handles a bounded collection or time window. These are not enemies.

RideFlow uses both:

- Kafka and the consumer form the streaming ingestion path.
- Airflow and dbt transform landed data hourly in bounded batches.

This pattern is often called micro-batch or streaming ingestion with batch
transformation. It is suitable because dashboards need minute/hour freshness,
not millisecond decisions.

## Data lake, warehouse, and lakehouse

- A **data lake** stores raw or lightly processed files cheaply and flexibly.
- A **data warehouse** stores governed, query-oriented tables.
- A **lakehouse** combines lake-style files with warehouse-style modelling and
  governance.

RideFlow uses immutable Parquet as its lake/landing layer and DuckDB/dbt as its
warehouse layer. It is a small local lakehouse pattern, not a claim of having a
distributed commercial lakehouse platform.

## Data lineage

Lineage answers: “Where did this number come from?” In RideFlow a Power BI
measure traces to an exported mart, a dbt model, staging events, raw Parquet,
and ultimately a Kafka event. dbt's `ref()` relationships create model-level
lineage automatically.

## Data freshness

Freshness is the delay between the newest business event and the available
analytical result. A successful pipeline can still serve stale data, so success
and freshness are separate signals. RideFlow exports a freshness marker and
checks landing-zone recency before transformation.

## Backfill

A backfill reprocesses a historical time range, usually after late data, a bug
fix, or a new model. It must be bounded and idempotent. RideFlow passes start
and end dates through Airflow into dbt and uses `delete+insert` so the selected
window is replaced rather than duplicated.

## Idempotency

An operation is idempotent when repeating it produces the same final state.
Data pipelines need this because schedulers retry and operators replay history.
RideFlow deduplicates stable event IDs and replaces incremental windows.

## Interview questions

### “Is RideFlow batch or streaming?”

> It is hybrid. Kafka and the consumer continuously ingest events, while dbt
> builds analytical marts in hourly batches under Airflow. The dashboard does
> not require sub-second freshness, so this separates durable low-latency ingest
> from simpler, testable batch transformation.

### “Why not query the application database directly?”

> Operational databases optimise many small transactions; analytics requires
> wide scans and aggregations. Mixing them risks slowing customer operations,
> and the normalized OLTP schema does not expose stable analytical grains. I
> therefore preserve events and build a separate star-schema warehouse.

### “Is this ETL or ELT?”

> Primarily ELT. Ingestion performs only structural validation and durable
> landing; business logic runs later in dbt. That preserves raw history and lets
> me change or replay transformations without republishing source events.

### “What makes a pipeline reliable?”

> Durable input, explicit contracts, safe retry semantics, idempotency,
> reconciliation between layers, quality gates, freshness monitoring, and a
> recovery path. RideFlow tests each of those rather than treating a successful
> process exit as proof of correct data.

### “What is the difference between orchestration and processing?”

> Processing changes data; orchestration decides when processing runs, in what
> order, with which parameters, concurrency, retries, and failure handling. dbt
> performs RideFlow's transformations; Airflow orchestrates them.
