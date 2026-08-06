"""DuckDB warehouse access.

DuckDB is a single-writer embedded database, and that constraint shapes the
whole platform: dbt is the ONLY writer, and everything else - analytics,
dashboards, tests - connects read-only. See docs/architecture.md section 1.1.
"""

from warehouse.connection import (
    LANDING_ZONE,
    WAREHOUSE_PATH,
    dbt_env,
    read_only_connection,
)

__all__ = ["LANDING_ZONE", "WAREHOUSE_PATH", "dbt_env", "read_only_connection"]
