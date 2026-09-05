# RideFlow Interview Preparation

This folder is a complete learning and interview-defense path for someone who
already knows Python and SQL but is new to data engineering. Read it in order.
Do not begin by memorising answers: first understand what problem each tool
solves, then connect it to RideFlow, and only then practise speaking.

## Learning order

| Order | File | Outcome |
|---:|---|---|
| 1 | [01-project-story.md](01-project-story.md) | Explain what RideFlow is, why it exists, and what you built |
| 2 | [02-data-engineering-foundations.md](02-data-engineering-foundations.md) | Learn pipelines, OLTP/OLAP, batch/streaming, ETL/ELT, lake/warehouse |
| 3 | [03-architecture.md](03-architecture.md) | Draw and defend the complete architecture |
| 4 | [04-events-and-contracts.md](04-events-and-contracts.md) | Understand events, schemas, time semantics, evolution, and DLQ boundaries |
| 5 | [04-python.md](04-python.md) | Defend Python, type hints, modules, validation, and testing choices |
| 6 | [05-kafka.md](05-kafka.md) | Understand topics, partitions, offsets, consumer groups, and delivery semantics |
| 7 | [06-ingestion.md](06-ingestion.md) | Explain the consumer, validation, batching, DLQ, and offset commits |
| 8 | [07-parquet.md](07-parquet.md) | Explain columnar files, schemas, partitions, atomic writes, and small files |
| 9 | [08-duckdb.md](08-duckdb.md) | Explain the local OLAP engine and its single-writer constraint |
| 10 | [09-dbt.md](09-dbt.md) | Explain transformation DAGs, incrementals, tests, seeds, and lineage |
| 11 | [10-sql.md](10-sql.md) | Defend the SQL patterns used in transformation and analytics |
| 12 | [08-data-modeling.md](08-data-modeling.md) | Defend grain, facts, dimensions, star schemas, keys, and additivity |
| 13 | [09-data-quality-and-reliability.md](09-data-quality-and-reliability.md) | Defend deduplication, idempotency, reconciliation, quarantine, and recovery |
| 14 | [10-airflow.md](10-airflow.md) | Explain orchestration, retries, scheduling, backfills, and concurrency |
| 15 | [11-docker.md](11-docker.md) | Explain images, containers, Compose, volumes, and health checks |
| 16 | [12-github-actions.md](12-github-actions.md) | Explain the CI/CD jobs and clean-checkout failure |
| 17 | [13-power-bi.md](13-power-bi.md) | Explain semantic models, relationships, measures, and filter context |
| 18 | [14-business-metrics.md](14-business-metrics.md) | Defend funnel, financial, pricing, and marketplace metrics |
| 19 | [13-scaling-security-production.md](13-scaling-security-production.md) | Answer production-readiness, scaling, security, cost, and observability questions |
| 20 | [14-interview-answers.md](14-interview-answers.md) | Practise high-probability technical and behavioural answers |
| 21 | [15-revision-cheatsheet.md](15-revision-cheatsheet.md) | Revise the project immediately before an interview |

The existing [`../INTERVIEW_NOTES.md`](../INTERVIEW_NOTES.md) remains the
extended 186-question bank. Use it only after completing files 1–13.

## How to study each file

For every concept, be able to answer five things:

1. What problem does it solve?
2. How does it work internally at interview depth?
3. Where exactly is it used in RideFlow?
4. What evidence shows it works?
5. What limitation or alternative did you accept?

Use this spoken-answer structure:

> **Context:** the problem I had. **Decision:** what I selected. **Mechanism:**
> how it solves the problem. **Evidence:** what I tested or measured.
> **Trade-off:** when the decision would stop being correct.

## Truth rules

- Events are synthetic; generator parameters are hand-tuned.
- The system demonstrates production engineering patterns, but it is not a
  production deployment: Kafka has replication factor 1 and storage is local.
- End-to-end delivery is at-least-once plus deterministic deduplication, not a
  magical exactly-once guarantee.
- Invalid atomic events may appear in the audit fact; quarantined trips are
  excluded from business measures.
- A 48-hour normal lookback does not cover every valid seven-day-late event;
  older corrections use an explicit backfill.

## Completion standard

You are ready when you can draw the system, give the 30-second and 90-second
project explanations, answer 20 random questions with two follow-up “why?”
questions, and volunteer three limitations without looking at these files.
