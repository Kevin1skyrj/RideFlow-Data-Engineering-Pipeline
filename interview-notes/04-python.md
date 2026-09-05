# 04 — Python in RideFlow

## Where Python is used

Python implements synthetic generation, Kafka production/consumption,
validation, DLQ construction, Parquet writing, warehouse export, the static
dashboard generator, Airflow DAG code, and tests.

The repository is organized into packages rather than one script. Configuration,
domain generation, transport, validation, persistence, and presentation have
separate responsibilities, which makes pure logic testable without Kafka.

## Important language concepts

- Dataclasses represent structured configuration, events, and results.
- Type hints document boundaries and help static reasoning, but Python still
  validates types at runtime only when code explicitly checks them.
- Iterables let sinks process event streams without depending on one concrete
  container type.
- Context and exception handling convert operational failures into actionable
  errors while allowing unexpected bugs to surface.
- Dependency injection in tests supplies fake producers/writers rather than
  requiring live infrastructure for every unit test.
- `python -m package.module` executes modules with reliable package imports.

## Determinism

A seed controls pseudo-random generation. The same configuration and seed must
produce the same business events, which makes failures reproducible. Physical
metadata such as actual landing wall-clock time may differ and should not be
included when comparing business-content hashes.

## Testing levels

- Unit tests isolate generator, pricing, anomaly, validation, and sink logic.
- Contract tests validate schemas and compatibility.
- Integration tests exercise Parquet, DuckDB, dashboard, and Kafka boundaries.
- Pytest fixtures create isolated temporary state.

Ruff detects code-quality issues; Black enforces formatting. These run locally,
in pre-commit, and in GitHub Actions.

## Questions and answers

### “Why Python rather than Java?”

> Python has strong Kafka, JSON Schema, Arrow, dbt, Airflow, and analytical
> ecosystems and keeps transformation support concise. Java may offer stronger
> static typing and JVM throughput for a very high-volume consumer. I would
> profile the current bottleneck before rewriting; the recorded Python consumer
> exceeded the v1 throughput target.

### “Do type hints guarantee safety?”

> No. They are metadata used by developers and tooling unless runtime validation
> is added. External event bytes are validated with JSON Schema and explicit
> parsing because type hints cannot protect an I/O boundary.

### “Why separate pure generation from sinks?”

> The generator should describe events, while sinks decide where they go. That
> lets the same event stream target JSONL, Kafka, stdout, or a CI landing fixture
> and lets unit tests exercise generation without infrastructure.

### “How do you handle exceptions?”

> Expected operational states such as an unavailable broker receive a specific
> error and actionable message. Record-level validation failures become DLQ
> data. Unexpected programming errors should fail visibly rather than being
> caught by a broad exception and silently dropping records.
