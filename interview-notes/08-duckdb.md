# 08 — DuckDB

## What it is

DuckDB is an embedded, vectorized, columnar OLAP database. It runs inside the
calling process and can persist to one local database file. It is optimized for
analytical scans and aggregations rather than high-concurrency transactional
application writes.

## Why RideFlow selected it

- No server or cloud account is required.
- Reviewers can reproduce the warehouse locally.
- It reads Parquet and JSON efficiently.
- It supports the SQL needed by dbt models.
- Columnar execution suits the project's analytical queries.

## Central limitation

Concurrent writers contend for the database file. RideFlow makes dbt the only
writer and uses Airflow `max_active_runs=1`. The Kafka consumer writes Parquet
instead of opening DuckDB.

## Questions and answers

### “DuckDB versus SQLite?”

> Both are embedded, but SQLite is row-oriented and optimized for transactional
> workloads, while DuckDB is columnar and vectorized for analytical scans. My
> workload groups and aggregates many rows, so DuckDB fits.

### “DuckDB versus PostgreSQL?”

> PostgreSQL is better for concurrent OLTP and service-style access. DuckDB
> offers simpler local reproducibility and columnar OLAP. If concurrent users or
> multi-node scale became binding, I would move marts to a client-server or
> cloud analytical warehouse.

### “Can DuckDB handle production?”

> It can support real embedded analytical workloads, but this topology lacks
> high availability and concurrent service access. I chose it for the project's
> scale and reproducibility, not as a universal warehouse choice.

### “What happens if the warehouse is deleted?”

> It is derived state. dbt can rebuild it from immutable raw Parquet and seeds.
> Losing the only raw landing copy is the more serious unrecoverable event.
