"""RideFlow hourly transformation DAG.

Airflow does three jobs here, and the third is the one that matters
architecturally:

  1. Schedule the transformation.
  2. Retry and alert, with policy graded by failure class.
  3. SERIALISE dbt runs. `max_active_runs=1` is what enforces the single-writer
     invariant the whole lakehouse rests on - DuckDB takes an exclusive write
     lock, and two concurrent dbt runs would collide.

Airflow deliberately does NOT orchestrate ingestion. The Kafka consumer is a
long-running service, not a scheduled task; this DAG *checks* that ingestion is
healthy and reacts to it, but never starts or stops it. Scheduling a continuous
stream consumer would be a category error.

Design rationale: docs/airflow_design.md
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# ── Paths (container-side) ───────────────────────────────────────────────────
PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/rideflow/transformation")
PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", PROJECT_DIR)
DBT_BIN = os.environ.get("DBT_BIN", "/home/airflow/dbt-venv/bin/dbt")
LANDING_ZONE = Path(os.environ.get("RIDEFLOW_LANDING_ZONE", "/opt/rideflow/data/raw"))
WAREHOUSE = Path(
    os.environ.get("RIDEFLOW_WAREHOUSE_PATH", "/opt/rideflow/data/warehouse/rideflow.duckdb")
)
MART_EXPORT = Path(os.environ.get("RIDEFLOW_MART_EXPORT", "/opt/rideflow/data/processed"))

DBT = f"cd {PROJECT_DIR} && {DBT_BIN}"
DBT_FLAGS = f"--profiles-dir {PROFILES_DIR} --project-dir {PROJECT_DIR}"

# ── Backfill, driven by DAG run configuration ────────────────────────────────
#
#   Trigger DAG w/ config:
#     {"backfill_start": "2026-07-15", "backfill_end": "2026-08-14"}
#     {"backfill_start": "...", "backfill_end": "...", "full_refresh": true}
#
# These are Airflow Jinja fragments rendered at TASK RUNTIME, not Python
# f-strings - so they are concatenated rather than interpolated, or the braces
# would be eaten by Python's own formatting.
#
# An empty conf renders to an empty string, so a normal scheduled run is
# completely unaffected. That is deliberate: the backfill path must not be a
# separate DAG that can drift out of sync with the one that actually runs
# hourly. One code path, two modes.
DBT_BACKFILL_VARS = (
    "{% if dag_run and dag_run.conf.get('backfill_start') %}"
    '--vars \'{"backfill_start": "{{ dag_run.conf[\'backfill_start\'] }}", '
    '"backfill_end": "{{ dag_run.conf[\'backfill_end\'] }}"}\''
    "{% endif %}"
)

# full_refresh rebuilds from scratch, discarding incremental state. Needed when
# a column is added or a model's logic changes in a way history must reflect.
DBT_FULL_REFRESH = "{% if dag_run and dag_run.conf.get('full_refresh') %}--full-refresh{% endif %}"

FRESHNESS_THRESHOLD_HOURS = 6

MARTS = [
    "fct_trips",
    "fct_payments",
    "fct_driver_sessions",
    "fct_trip_events",
    "fct_pipeline_quality",
    "dim_date",
    "dim_time",
]


# ─────────────────────────────────────────────────────────────────────────────
# Python callables
# ─────────────────────────────────────────────────────────────────────────────
def check_landing_zone_freshness(**context) -> dict:
    """Verify the landing zone has recent data.

    Classified as a WARNING, not a hard failure. At 3 a.m. there may genuinely
    be no new trips, and failing the DAG for correct quiet-hour behaviour would
    train people to ignore the alert - which is how a real outage gets missed.
    """
    import time

    if not LANDING_ZONE.exists():
        raise FileNotFoundError(
            f"Landing zone {LANDING_ZONE} does not exist. "
            "Is the volume mounted and has the consumer ever run?"
        )

    files = list(LANDING_ZONE.rglob("*.parquet"))
    if not files:
        print(f"WARNING: no parquet files under {LANDING_ZONE}")
        return {"files": 0, "stale": True}

    newest = max(f.stat().st_mtime for f in files)
    age_hours = (time.time() - newest) / 3600

    print(f"landing zone: {len(files)} files, newest is {age_hours:.1f}h old")
    if age_hours > FRESHNESS_THRESHOLD_HOURS:
        print(
            f"WARNING: newest file is {age_hours:.1f}h old, beyond the "
            f"{FRESHNESS_THRESHOLD_HOURS}h threshold. Is the consumer running?"
        )
    return {"files": len(files), "age_hours": round(age_hours, 2)}


def export_marts_to_parquet(**context) -> dict:
    """Export marts as Parquet for the serving layer.

    This is what keeps the analytical layer engine-neutral. Power BI Desktop is
    Windows-only and .pbix is a binary blob, so a reviewer on another platform
    would otherwise have no access at all (architecture.md section 0).

    Marts are TABLES, so exporting them carries no path dependency - unlike the
    staging views, whose baked-in landing-zone path is container-bound.
    """
    import duckdb

    MART_EXPORT.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(WAREHOUSE), read_only=True)
    exported = {}
    try:
        for mart in MARTS:
            target = MART_EXPORT / f"{mart}.parquet"
            connection.execute(
                f"COPY (SELECT * FROM main.{mart}) TO '{target.as_posix()}' "
                "(FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            rows = connection.sql(f"SELECT count(*) FROM main.{mart}").fetchone()[0]
            exported[mart] = rows
            print(f"exported {mart}: {rows} rows -> {target.name}")
    finally:
        connection.close()
    return exported


def reconciliation_check(**context) -> dict:
    """Cross-layer reconciliation (N1-N4), run AFTER the export.

    Deliberately positioned after export so a mismatch is caught before the run
    is declared successful, not after consumers have already read the files.
    """
    import duckdb

    connection = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        pattern = (LANDING_ZONE / "**" / "*.parquet").as_posix()
        landed, distinct = connection.sql(
            f"SELECT count(*), count(DISTINCT event_id) FROM read_parquet('{pattern}')"
        ).fetchone()

        staged = connection.sql(
            "SELECT (SELECT count(*) FROM main_staging.stg_trip_events) "
            "     + (SELECT count(*) FROM main_staging.stg_driver_presence)"
        ).fetchone()[0]

        trips = connection.sql("SELECT count(*) FROM main.fct_trips").fetchone()[0]
        quarantined = connection.sql(
            "SELECT count(*) FROM main.fct_trips WHERE is_quarantined"
        ).fetchone()[0]

        result = {
            "landed_rows": landed,
            "distinct_events": distinct,
            "duplicates_removed": landed - distinct,
            "staged_rows": staged,
            "trips": trips,
            "quarantined_trips": quarantined,
        }
        print(json.dumps(result, indent=2))

        # N2: staging must account for exactly the distinct landed events.
        if staged != distinct:
            raise ValueError(
                f"N2 RECONCILIATION FAILED: staging has {staged} rows but the "
                f"landing zone holds {distinct} distinct events "
                f"(difference {distinct - staged})"
            )
        return result
    finally:
        connection.close()


CRITICAL_TASKS = {"dbt_test", "reconciliation_check"}


def alert_on_failure(context) -> None:
    """Route a failure by severity.

    Severity is GRADED on purpose. Ungraded alerting is ignored alerting: if a
    docs-generation warning pages someone at 2 a.m., the next genuine financial
    alert gets muted (airflow_design.md 6.2).

      CRITICAL - dbt_test / reconciliation_check. Wrong numbers, or numbers that
                 do not agree across layers. Would page.
      HIGH     - any other task failure. Same-day attention.

    This writes a structured record rather than sending anything. Wiring a real
    channel is one function call away, but inventing a Slack webhook that does
    not exist would make the alerting look implemented when it is not.
    """
    task_instance = context.get("task_instance")
    task_id = getattr(task_instance, "task_id", "unknown")
    severity = "CRITICAL" if task_id in CRITICAL_TASKS else "HIGH"

    alert = {
        "severity": severity,
        "dag_id": context.get("dag").dag_id if context.get("dag") else "unknown",
        "task_id": task_id,
        "run_id": context.get("run_id"),
        "try_number": getattr(task_instance, "try_number", None),
        "exception": str(context.get("exception"))[:500],
        "raised_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "reason": (
            "data quality or reconciliation failure - published numbers may be wrong"
            if severity == "CRITICAL"
            else "pipeline task failure"
        ),
    }

    print(f"[{severity} ALERT] " + json.dumps(alert))

    # Append to a durable log so a failure is inspectable after the container
    # restarts and Airflow's own task logs have rotated away.
    try:
        MART_EXPORT.mkdir(parents=True, exist_ok=True)
        with (MART_EXPORT / "_ALERTS.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(alert) + "\n")
    except OSError as exc:  # never let alerting failure mask the real failure
        print(f"could not persist alert: {exc}")


def publish_success_marker(**context) -> dict:
    """Record that this run completed and published.

    The dashboard reads this to display an honest freshness indicator. Without
    it, Power BI cannot distinguish "current" from "stale", and a stale number
    read as current is worse than no number.
    """
    # Read context DEFENSIVELY.
    #
    # Airflow 3 omits `logical_date` entirely for a manually triggered run - it
    # is absent from the context dict, not present-and-None. Subscripting it
    # raises KeyError and fails the task after every upstream step has already
    # succeeded, which is a maximally annoying place to fall over.
    task_instance = context.get("ti") or context.get("task_instance")
    logical_date = context.get("logical_date")

    marker = MART_EXPORT / "_LAST_SUCCESSFUL_RUN.json"
    payload = {
        "dag_run_id": context.get("run_id"),
        "logical_date": logical_date.isoformat() if logical_date else None,
        "published_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "reconciliation": (
            task_instance.xcom_pull(task_ids="reconciliation_check") if task_instance else None
        ),
        "exported": (
            task_instance.xcom_pull(task_ids="export_marts_parquet") if task_instance else None
        ),
    }
    marker.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"published marker -> {marker}")
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    # An hour's failure must not block the next hour indefinitely.
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    # Exponential rather than fixed: transient failures often have a recovery
    # time proportional to their severity, and hammering a struggling broker
    # every 60s can prevent the recovery it is waiting for.
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    # Fires only after retries are exhausted, so a transient blip that recovers
    # on attempt 2 never raises an alert.
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="rideflow_transform_hourly",
    description="Landing zone -> dbt -> DuckDB marts -> Parquet export",
    # Fixed, never days_ago(): a moving start date makes runs non-reproducible
    # and backfill ranges ambiguous.
    start_date=datetime(2026, 3, 1),
    schedule="0 * * * *",
    # Prevents a storm on first deploy. With max_active_runs=1 those runs would
    # execute SERIALLY - a three-month gap becomes days of backlog while the
    # current hour goes unprocessed. Backfill is explicit instead.
    catchup=False,
    # ── THE most important setting in this file ──────────────────────────────
    # DuckDB takes an exclusive write lock. This is what guarantees only one
    # dbt process ever writes to it, which is the invariant the entire lakehouse
    # architecture depends on (architecture.md 1.1).
    max_active_runs=1,
    max_active_tasks=4,
    # Below the schedule interval, so a hung run is killed before the next is
    # due and max_active_runs=1 can never cause an unbounded queue.
    dagrun_timeout=timedelta(minutes=45),
    default_args=default_args,
    tags=["rideflow", "transform", "dbt"],
    doc_md=__doc__,
) as dag:

    freshness = PythonOperator(
        task_id="check_landing_zone_freshness",
        python_callable=check_landing_zone_freshness,
        retries=2,
        doc_md="Warns rather than fails: quiet hours are legitimate.",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT} deps {DBT_FLAGS}",
        retries=2,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT} seed {DBT_FLAGS}",
        retries=2,
        doc_md=(
            "Runs every cycle. Seeds are small, and reloading them guarantees "
            "the dimensions match the committed CSVs - a dimension that has "
            "silently drifted from its seed is very hard to detect otherwise."
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT} run {DBT_FLAGS} " + DBT_BACKFILL_VARS + " " + DBT_FULL_REFRESH,
        retries=3,
        doc_md=(
            "Honours `backfill_start` / `backfill_end` / `full_refresh` from the "
            "DAG run configuration. With no config it is an ordinary "
            "incremental run - one code path, two modes."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_FLAGS} " + DBT_BACKFILL_VARS,
        # ── ZERO retries, deliberately ──────────────────────────────────────
        # A data-quality failure is DETERMINISTIC: the same data tested again
        # fails again. Retrying burns the budget on a guaranteed failure and
        # delays the alert by the full backoff sequence, turning a 1-minute
        # notification into a 15-minute one for no benefit.
        retries=0,
        doc_md="THE QUALITY GATE. Failure here stops everything downstream.",
    )

    export_marts = PythonOperator(
        task_id="export_marts_parquet",
        python_callable=export_marts_to_parquet,
        retries=3,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT} docs generate {DBT_FLAGS}",
        retries=1,
        # Advisory: documentation failing must not block publication.
        trigger_rule="all_done",
    )

    reconcile = PythonOperator(
        task_id="reconciliation_check",
        python_callable=reconciliation_check,
        # Also deterministic - see dbt_test.
        retries=0,
    )

    publish = PythonOperator(
        task_id="publish_success_marker",
        python_callable=publish_success_marker,
        retries=2,
    )

    # dbt_test sits between run and export. That ordering IS the quality gate:
    # nothing untested can reach the serving layer.
    freshness >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test
    dbt_test >> export_marts >> reconcile >> publish
    dbt_test >> dbt_docs
