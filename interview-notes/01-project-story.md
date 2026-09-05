# 01 — The RideFlow Project Story

## What is RideFlow?

RideFlow is an end-to-end analytical data platform for a simulated ride-hailing
business. It models the path that data takes after a rider or driver action
occurs: an event is produced, transported, validated, stored, transformed into
business-ready tables, scheduled, tested, and visualised.

It is not the application that books a cab or matches a driver. That would be
an operational product system. RideFlow is the data platform that helps teams
understand what happened across many rides.

## What business problem does it solve?

The platform answers four groups of questions:

1. **Marketplace health:** where are riders requesting rides and where are
   drivers available?
2. **Conversion funnel:** how many requested rides become matched, started,
   completed, and paid?
3. **Pricing effectiveness:** when was surge applied and how did conversion
   behave at different surge levels?
4. **Financial truth:** how much was charged, paid to drivers, retained as
   commission, cancelled, or quarantined?

An operational database is designed to quickly create or update one ride. An
analytical platform is designed to scan thousands or millions of rides and
aggregate them by hour, zone, vehicle type, or outcome without slowing down the
customer-facing application.

## What did you build?

- A deterministic Python event generator for nine trip and driver event types.
- A versioned JSON event contract with schema and invariant tests.
- Kafka running in KRaft mode with deliberately partitioned trip, presence, and
  dead-letter topics.
- A Python consumer that validates messages, batches them, writes Parquet, and
  commits offsets only after persistence.
- An immutable, Hive-partitioned raw landing zone.
- A dbt project that stages, deduplicates, assembles trip lifecycles, tests
  business rules, and builds facts and dimensions in DuckDB.
- An Airflow DAG that runs the hourly transformation, tests, export,
  reconciliation, documentation, and success-marker tasks.
- Power BI and static dashboards backed by exported Parquet marts.
- Unit, contract, integration, chaos, and CI tests.

## Verified numbers

| Evidence | Value |
|---|---:|
| Landed events in the full dataset | 90,186 |
| Total assembled trips | 16,415 |
| Clean trips used for metrics | 16,203 |
| Quarantined trips | 212 |
| Completed clean trips | 13,897 |
| Completion rate | 85.77% |
| Gross bookings | ₹7,936,695.40 |
| Platform commission | ₹1,511,749.30 |
| dbt project | 21 models, 11 seeds, 135 tests |
| Airflow DAG | 9 tasks |
| Power BI | 5 pages, 30 measures |
| Recorded ingestion rate | 4,368 events/second |

## 30-second interview answer

> RideFlow is a synthetic ride-hailing analytics platform I built end to end.
> Python generates versioned trip and driver events, Kafka transports them, and
> a validating consumer lands immutable Parquet. dbt transforms that history
> into a tested DuckDB star schema, Airflow orchestrates the hourly workflow,
> and Power BI consumes exported marts. The important part is reliability: I
> tested malformed, duplicate, late, and out-of-order events and killed the
> consumer mid-batch to prove recovery rather than only showing a happy path.

## 90-second interview answer

> The business goal was to answer marketplace-health, conversion, pricing, and
> financial questions without querying a transactional ride-booking database.
> I created a nine-event domain contract and a deterministic Python generator
> that can inject controlled failure cases. Trip and driver-presence events go
> through separate Kafka topics because they require different partition keys.
> The consumer validates each record, routes invalid ones to DLQ, writes valid
> events to immutable Parquet, and commits Kafka offsets only after the write.
>
> DuckDB's single-writer constraint shaped the architecture. I did not let the
> continuous consumer write into DuckDB and contend with dbt. Instead, Parquet
> decouples streaming ingestion from scheduled transformation. dbt is the only
> warehouse writer, while Airflow prevents concurrent runs and performs tests
> before export and publication.
>
> Correctness is layered: event schemas at ingestion, deterministic event-ID
> deduplication in staging, lifecycle and financial assertions in dbt, and
> quarantine flags that exclude unreliable trips from business measures while
> preserving them for audit. In a forced-kill test, all 17,880 distinct valid
> events recovered; 13 expected redeliveries were removed downstream. GitHub
> Actions now verifies two Python versions, a warehouse build from a clean
> checkout, and the actual Kafka Compose path.

## Why did you build this project?

> I came from full-stack development and wanted to understand what happens
> after application events leave an API. I selected a ride-hailing domain
> because it naturally contains state transitions, late mobile events,
> duplicates, payments, geography, and two-sided marketplace metrics. That let
> me learn data engineering through correctness problems instead of assembling
> tools without a reason.

## “What was your individual contribution?”

> I designed the event contract and architecture, implemented the Python
> generator and ingestion path, built the dbt models and tests, containerised
> Kafka and Airflow, created the orchestration and CI workflows, defined the
> metrics, and built the Power BI report. I also recorded the trade-offs and
> failure evidence so the project can be reviewed from a clean checkout.

Only use that answer for work you can open and explain in the repository.

## Strongest technical decision

The strongest story is the DuckDB lock constraint. A streaming consumer writing
continuously to an embedded single-writer database would block dbt. You chose an
immutable Parquet boundary: the consumer writes files, dbt alone writes DuckDB,
and Airflow serialises dbt runs. This turned a local limitation into a clean
lakehouse separation.

## Strongest reliability story

The consumer writes before committing its Kafka offset. A crash in between
causes redelivery rather than data loss. The kill-and-restart test recovered all
17,880 distinct valid IDs and produced 13 duplicate rows, which staging removed.
The duplicates are evidence that at-least-once recovery behaved correctly.

## Honest limitations

- One Kafka broker and local storage are not highly available.
- Events are synthetic and parameters are hand-tuned.
- Power BI is binary and Windows-only.
- External alert delivery is not configured.
- Explicit `RideExpired`, `PaymentFailed`, pooled-ride segments, and intra-session
  driver status events are outside contract v1.

Never hide these. Explaining limitations makes the rest of your claims credible.
