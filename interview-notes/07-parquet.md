# 07 — Apache Parquet and the Landing Zone

## What Parquet is

Parquet is a binary, typed, columnar file format for analytics. Values from the
same column are stored together, improving compression and allowing an engine
to read only columns required by a query.

It is a format, not a database: plain Parquet alone does not provide
transactions, catalog management, concurrent updates, or row-level indexes.

## Why RideFlow uses it

- Efficient analytical scans and compression.
- Explicit Arrow schema and consistent types.
- Direct support in DuckDB, Power BI tooling, Spark, and cloud warehouses.
- Files decouple the continuous consumer from DuckDB's writer lock.
- Raw history can rebuild every downstream table.

## Partitioning

Paths use `topic=.../dt=.../hour=...`. Query engines can prune irrelevant
directories. RideFlow partitions by ingestion time to preserve append-only
arrival writes; event time remains the business grouping column.

Do not over-partition. A directory per trip would create huge metadata and
small-file overhead with little pruning benefit.

## Safe writing

The writer uses an explicit schema, creates a unique temporary file, completes
the Parquet write, and renames it. Readers never intentionally consume a
half-written `.parquet` file.

## Questions and answers

### “Parquet versus CSV?”

> CSV has no reliable embedded schema, requires parsing text, and reads whole
> rows even when a query needs few columns. Parquet is typed, compressed, and
> columnar. CSV remains useful for small human-editable reference data, which is
> why dbt seeds use it.

### “Parquet versus JSON?”

> JSON is excellent for flexible transport and debugging but repeats field
> names and is expensive to scan. I keep JSON payload semantics in Kafka and
> store them inside typed Parquet envelope columns for analytical landing.

### “Why not Iceberg or Delta?”

> They would add transactions, snapshots, schema evolution, and compaction
> metadata. At this local scale, their operational complexity is not justified.
> Small-file growth or concurrent table updates would be the trigger.

### “What is the small-file problem?”

> Thousands of tiny files increase listing, metadata, open, and planning costs.
> Batching reduces creation; later compaction can combine files while a manifest
> or table format safely publishes the optimized layout.
