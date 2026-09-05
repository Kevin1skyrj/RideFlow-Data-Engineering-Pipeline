# 09 — dbt

## What dbt is

dbt is a transformation framework: it compiles SQL/Jinja models, derives a DAG
from `ref()`, materialises relations, runs tests, loads small seeds, and emits
lineage/documentation artifacts. It works on data already available to the
warehouse or query engine.

## RideFlow layers

- Staging: source-aligned views, casting, naming, payload extraction, dedup.
- Intermediate: reusable trip and driver-session assembly logic.
- Marts: facts, dimensions, quarantine, and pipeline-quality tables.

## Materializations

- View: stored query; cheap storage, recomputed when queried.
- Table: stored result; faster repeated reads, needs rebuild/refresh.
- Incremental: table updated from a selected subset of input.
- Ephemeral: compiled into dependent SQL rather than stored independently.

RideFlow uses views near sources and tables/incrementals for consumer-facing
models.

## Incremental safety

Normal runs use a 48-hour lookback and `delete+insert`. The system re-selects
affected business keys and replaces their rows. Older changes require an
explicit backfill. Content hashes across reruns test idempotency.

## Tests and seeds

Generic tests cover reusable constraints; singular SQL tests encode specific
business invariants. Error severity blocks publication, while warnings retain
visibility for explicitly accepted conditions. Eleven CSV seeds version small
reference dimensions.

## Questions and answers

### “What does `ref()` do?”

> It resolves the physical relation name and creates a dependency edge. dbt can
> build models in the correct order and generate lineage without a separately
> maintained DAG.

### “Why `delete+insert` rather than append?”

> Late data, retries, and backfills revisit existing windows. Append would
> duplicate facts. Replacing the selected window makes repetition idempotent.

### “Why not full refresh every hour?”

> It is simple but scan and compute cost grows with all history. Incremental
> processing bounds routine work while full refresh remains a correctness and
> recovery tool.

### “What does a dbt test return?”

> A test query returns violating rows. Zero rows passes. Severity determines
> whether violations block downstream execution or produce a warning.

### “Does dbt orchestrate the whole platform?”

> It orders its transformation graph, but Airflow owns cross-tool scheduling,
> retries, backfills, concurrency, exports, and publication.
