"""Export marts to Parquet for the serving layer.

Shared by the Airflow DAG and the host CLI so the two cannot diverge. An export
that behaved differently depending on who ran it would be a silent source of
"the dashboard disagrees with the warehouse".

Why export at all when the marts already live in DuckDB:

  * Power BI Desktop is Windows-only and `.pbix` is a binary blob. Parquet keeps
    the analytical layer engine-neutral, so a reviewer on any platform retains
    full access (architecture.md section 0).
  * Marts are TABLES, so exporting them carries no path dependency - unlike the
    staging views, whose baked-in landing-zone path is bound to whichever
    machine or container built them.

    python -m warehouse.export
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from warehouse.connection import ROOT, WAREHOUSE_PATH

MART_EXPORT = Path.cwd() / "data" / "processed"

MARTS = (
    "fct_trips",
    "fct_payments",
    "fct_driver_sessions",
    "fct_trip_events",
    "fct_pipeline_quality",
    "quarantined_trips",
    "dim_date",
    "dim_time",
)

REFERENCE = (
    "dim_city",
    "dim_zone",
    "dim_vehicle_type",
    "dim_ride_status",
    "dim_cancellation_reason",
    "dim_payment_method",
    "dim_payment_status",
    "dim_driver_status",
    "dim_customer_tier",
    "dim_weather",
    "dim_traffic_level",
)


def export_marts(
    warehouse_path: Path | None = None,
    target_dir: Path | None = None,
) -> dict[str, int]:
    """Write every mart and reference dimension as Parquet."""
    import duckdb

    source = (warehouse_path or WAREHOUSE_PATH).resolve()
    target = (target_dir or (ROOT / "data" / "processed")).resolve()
    target.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect(str(source), read_only=True)
    exported: dict[str, int] = {}
    try:
        connection.execute("LOAD icu; SET TimeZone='UTC';")
        for schema, tables in (("main", MARTS), ("main_reference", REFERENCE)):
            for table in tables:
                destination = target / f"{table}.parquet"
                connection.execute(
                    f"COPY (SELECT * FROM {schema}.{table}) "
                    f"TO '{destination.as_posix()}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
                )
                exported[table] = connection.sql(
                    f"SELECT count(*) FROM {schema}.{table}"
                ).fetchone()[0]
    finally:
        connection.close()
    return exported


FUTURE_TOLERANCE_HOURS = 0.5


def _freshness_status(age_hours: float | None) -> str:
    """Classify data age, including the case nobody remembers to handle.

    A NEGATIVE age means the newest record is timestamped in the future. The
    first version of this function had no branch for it, so `-7.98` fell
    through `age <= 2` and reported FRESH - the dashboard would have shown a
    green indicator for data that is not merely stale but incoherent.

    Future-dated data has two real causes, and both need surfacing rather than
    smoothing over:
      * producer clock skew, which is a genuine defect;
      * a simulated dataset whose window extends past wall clock, which is
        exactly what this project's generator produces.

    Half an hour of tolerance absorbs ordinary clock drift without hiding
    either case.
    """
    if age_hours is None:
        return "UNKNOWN"
    if age_hours < -FUTURE_TOLERANCE_HOURS:
        return "FUTURE_DATED"
    if age_hours <= 2:
        return "FRESH"
    if age_hours <= 6:
        return "STALE"
    return "VERY_STALE"


def write_freshness_marker(exported: dict[str, int], target_dir: Path | None = None) -> Path:
    """Record when the export happened, and what the data's own clock says.

    Two different timestamps, deliberately:

      exported_at    - when this export ran (wall clock)
      latest_trip_at - the newest event IN the data

    A dashboard must show the second. The first only proves the export process
    ran; if ingestion stalled three hours ago, the export still succeeds every
    hour and a freshness indicator based on it would report "fresh" while the
    numbers silently aged. That is worse than no indicator, because it is
    confidently wrong.
    """
    import duckdb

    target = (target_dir or (ROOT / "data" / "processed")).resolve()
    trips = target / "fct_trips.parquet"

    connection = duckdb.connect()
    try:
        connection.execute("LOAD icu; SET TimeZone='UTC';")
        latest, earliest, rows = connection.sql(
            f"SELECT max(requested_at), min(requested_at), count(*) "
            f"FROM read_parquet('{trips.as_posix()}') WHERE requested_at IS NOT NULL"
        ).fetchone()
    finally:
        connection.close()

    now = datetime.now(UTC)
    age_hours = (now - latest).total_seconds() / 3600 if latest else None

    marker = {
        "exported_at": now.isoformat().replace("+00:00", "Z"),
        "latest_trip_at": latest.isoformat() if latest else None,
        "earliest_trip_at": earliest.isoformat() if earliest else None,
        "data_age_hours": round(age_hours, 2) if age_hours is not None else None,
        # Thresholds the dashboard renders against. Encoded here so the report
        # and the pipeline cannot disagree about what "stale" means.
        "freshness_status": _freshness_status(age_hours),
        "trip_rows": rows,
        "exported_tables": exported,
    }

    path = target / "_FRESHNESS.json"
    path.write_text(json.dumps(marker, indent=2, default=str), encoding="utf-8")
    return path


def main() -> int:
    exported = export_marts()
    marker = write_freshness_marker(exported)

    for name, rows in sorted(exported.items()):
        print(f"  {name:28s} {rows:>7} rows")
    print(f"\nfreshness marker -> {marker}")
    print(marker.read_text(encoding="utf-8")[:400])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
