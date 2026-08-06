"""Warehouse connection management.

Two responsibilities:

1. Hand out READ-ONLY connections. dbt is the sole writer to the DuckDB file
   (docs/architecture.md 1.1); anything else opening it read-write would take
   the exclusive lock and block the next dbt run.

2. Produce the environment dbt needs, with an ABSOLUTE landing-zone path.
   dbt bakes that path into the staging view definitions, and DuckDB resolves
   relative paths against the caller's working directory - so a relative path
   makes the warehouse queryable only from transformation/ and broken from
   anywhere else, including Power BI.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]

LANDING_ZONE = Path(os.environ.get("RIDEFLOW_LANDING_ZONE") or ROOT / "data" / "raw").resolve()

WAREHOUSE_PATH = Path(
    os.environ.get("RIDEFLOW_WAREHOUSE_PATH") or ROOT / "data" / "warehouse" / "rideflow.duckdb"
).resolve()

DBT_PROJECT_DIR = ROOT / "transformation"

STAGING_SCHEMA = "main_staging"
REFERENCE_SCHEMA = "main_reference"


class WarehouseNotBuiltError(FileNotFoundError):
    """The DuckDB file does not exist yet.

    Distinct from a query error: 'the warehouse has not been built' and 'the
    query is wrong' need different responses, and collapsing them makes the
    first look like a bug in the caller's SQL.
    """


def dbt_env(
    *,
    landing_zone: Path | None = None,
    warehouse_path: Path | None = None,
) -> dict[str, str]:
    """Environment for invoking dbt, with absolute paths resolved.

    Always pass this to a dbt subprocess rather than relying on the relative
    fallback in dbt_project.yml.
    """
    env = dict(os.environ)
    env["RIDEFLOW_LANDING_ZONE"] = str((landing_zone or LANDING_ZONE).resolve())
    env["RIDEFLOW_WAREHOUSE_PATH"] = str((warehouse_path or WAREHOUSE_PATH).resolve())
    env["DBT_PROFILES_DIR"] = str(DBT_PROJECT_DIR)
    return env


@contextmanager
def read_only_connection(path: Path | None = None) -> Iterator[Any]:
    """A read-only DuckDB connection.

    Read-only is not a courtesy. A second read-write connection takes the
    exclusive lock and the next dbt run fails - which is exactly the collision
    the lakehouse architecture exists to prevent.
    """
    import duckdb

    target = (path or WAREHOUSE_PATH).resolve()
    if not target.exists():
        raise WarehouseNotBuiltError(
            f"No warehouse at {target}\n" "Build it with:  cd transformation && dbt seed && dbt run"
        )

    connection = duckdb.connect(str(target), read_only=True)
    try:
        yield connection
    finally:
        connection.close()


def table_counts(path: Path | None = None) -> dict[str, int]:
    """Row counts for every staging and reference table, for reconciliation."""
    with read_only_connection(path) as connection:
        tables = connection.sql("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('main_staging', 'main_reference')
            ORDER BY 1, 2
            """).fetchall()
        counts: dict[str, int] = {}
        for schema, name in tables:
            qualified = f"{schema}.{name}"
            counts[qualified] = connection.sql(
                f"SELECT count(*) FROM {qualified}"
            ).fetchone()[0]
        return counts


def landing_zone_counts(landing_zone: Path | None = None) -> dict[str, int]:
    """Row and distinct-event counts straight from the Parquet landing zone.

    Read independently of the warehouse so reconciliation compares two genuinely
    separate sources rather than the warehouse against itself.
    """
    import duckdb

    target = (landing_zone or LANDING_ZONE).resolve()
    pattern = str(target / "**" / "*.parquet").replace("\\", "/")
    connection = duckdb.connect()
    try:
        rows, distinct = connection.sql(
            f"SELECT count(*), count(DISTINCT event_id) FROM read_parquet('{pattern}')"
        ).fetchone()
        return {"rows": rows, "distinct_event_ids": distinct, "duplicates": rows - distinct}
    finally:
        connection.close()
