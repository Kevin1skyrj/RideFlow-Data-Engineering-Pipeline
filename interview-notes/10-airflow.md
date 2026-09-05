# 10 — Apache Airflow

## What Airflow is

Airflow is a workflow orchestrator. It represents tasks and dependencies as a
DAG, schedules runs, tracks state, retries appropriate failures, exposes logs,
and accepts parameters for backfills. It does not itself transform data.

## RideFlow DAG

```text
check landing freshness → dbt deps → dbt seed → dbt run → dbt test
                                                        ├→ dbt docs
                                                        └→ export marts
                                                            → reconcile
                                                            → success marker
```

There are nine tasks. The exact graph is visible in the saved Airflow evidence.

## Important configuration

- Hourly schedule: aligns with analytical freshness requirements.
- `max_active_runs=1`: prevents concurrent DuckDB writers.
- `catchup=False`: avoids automatically creating a large historical backlog.
- Fixed start date: deterministic scheduling.
- DAG-run timeout shorter than the interval: a hung run does not overlap the
  next scheduled run indefinitely.
- Quality-test retries zero: deterministic data failures need investigation.
- Exponential backoff for transient tasks: avoids hammering a recovering service.

## Scheduler, DAG processor, triggerer, API server

- Scheduler decides which task instances are ready.
- DAG processor parses DAG files separately.
- Triggerer efficiently waits for deferrable asynchronous conditions.
- API server provides Airflow's UI/API surface.
- PostgreSQL stores Airflow metadata; it is not the analytical warehouse.

## Backfill

RideFlow accepts start/end bounds and optional full refresh in `dag_run.conf`.
Both bounds must be supplied together. The same DAG and model code are used for
normal and historical runs, reducing drift. `delete+insert` makes reruns safe.

## Interview answers

### “Why Airflow instead of cron?”

> Cron can start a command, but it does not model task dependencies, partial
> failure, per-task retry policy, historical parameters, concurrency, or
> operational state. Airflow also enforces the single-writer constraint through
> one active run.

### “Why not orchestrate the Kafka consumer?”

> The consumer is a long-running service, while an Airflow task should be a
> bounded unit of work. Airflow orchestrates the scheduled transformation and
> publication path; service supervision belongs to the runtime platform.

### “Why `catchup=False` if you support backfills?”

> Automatic catchup can generate hundreds of serial runs after downtime.
> Historical processing should be explicit and parameterized so the operator
> controls the range and full-refresh behavior.

### “Why is concurrency a correctness property?”

> DuckDB permits one writer. `max_active_runs=1` is therefore not just resource
> tuning; it prevents two dbt jobs from racing for the warehouse file and
> protects deterministic state.

### “What does a green DAG prove?”

> It proves every configured task completed for that run. Correctness also
> depends on what tasks assert, which is why reconciliation and dbt tests occur
> before the success marker. Green orchestration alone is not data proof.
