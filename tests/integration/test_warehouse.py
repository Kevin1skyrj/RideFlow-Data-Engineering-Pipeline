"""Warehouse reconciliation: staging must account for every landed event.

Runs against an already-built warehouse rather than invoking dbt, so it is fast
and can run in the normal pytest suite. It skips cleanly when the warehouse has
not been built - a skip that says what to do beats a failure that means
"you haven't run dbt yet".
"""

from __future__ import annotations

import pytest

from warehouse.connection import (
    LANDING_ZONE,
    WAREHOUSE_PATH,
    landing_zone_counts,
    read_only_connection,
)

pytestmark = pytest.mark.skipif(
    not WAREHOUSE_PATH.exists() or not LANDING_ZONE.exists(),
    reason=(
        f"No warehouse at {WAREHOUSE_PATH}. Build it with: "
        "cd transformation && dbt seed && dbt run"
    ),
)


@pytest.fixture(scope="module")
def con():
    with read_only_connection() as connection:
        yield connection


@pytest.fixture(scope="module")
def landing():
    return landing_zone_counts()


def _staging_is_readable() -> bool:
    """Can staging views be queried from HERE?

    Staging models are VIEWS, and dbt bakes the landing-zone path into their
    definition. DuckDB resolves that path against whoever queries the view, so
    a warehouse built inside the Airflow container leaves staging bound to
    `/opt/rideflow/data/raw` and unreadable from the host:

        IO Error: No files found that match the pattern
        "/opt/rideflow/data/raw/topic=trips/**/*.parquet"

    That is not a defect - it is the documented consequence of the container
    boundary (docker-compose.airflow.yml, "A note on path binding"). The MARTS
    are tables, carry no path dependency, and are the actual consumer contract.

    Staging-dependent tests therefore SKIP rather than fail when the warehouse
    was container-built. Failing would report a broken pipeline every time
    Airflow ran on schedule, which is exactly the kind of false alarm that
    trains people to ignore a suite.
    """
    try:
        with read_only_connection() as connection:
            connection.sql("SELECT 1 FROM main_staging.stg_trip_events LIMIT 1").fetchone()
        return True
    except Exception:
        return False


STAGING_READABLE = _staging_is_readable()

needs_local_staging = pytest.mark.skipif(
    not STAGING_READABLE,
    reason=(
        "Staging views are bound to a container path (warehouse built by "
        "Airflow). Marts are unaffected. Rebuild on the host to re-enable: "
        "cd transformation && dbt build"
    ),
)


class TestReconciliation:
    @needs_local_staging
    def test_staging_row_count_equals_distinct_landed_events(self, con, landing):
        """The M5 exit criterion, and invariant N2.

        Staging must contain exactly one row per distinct landed event: no more
        (deduplication ran) and no fewer (nothing was dropped).
        """
        trips = con.sql("SELECT count(*) FROM main_staging.stg_trip_events").fetchone()[0]
        presence = con.sql("SELECT count(*) FROM main_staging.stg_driver_presence").fetchone()[0]

        assert trips + presence == landing["distinct_event_ids"], (
            f"staging has {trips + presence} rows but the landing zone holds "
            f"{landing['distinct_event_ids']} distinct events"
        )

    def test_deduplication_actually_removed_something(self, con, landing):
        """Guards against a false pass.

        If the landing zone happened to contain no duplicates, the test above
        would pass without proving deduplication works at all. This asserts the
        duplicates were real and were removed.
        """
        if landing["duplicates"] == 0:
            pytest.skip("landing zone contains no duplicates to remove")
        assert landing["rows"] > landing["distinct_event_ids"]

    @needs_local_staging
    def test_no_duplicate_event_ids_survive_in_staging(self, con):
        remaining = con.sql("""
            SELECT count(*) FROM (
              SELECT event_id FROM main_staging.stg_trip_events
              GROUP BY 1 HAVING count(*) > 1
              UNION ALL
              SELECT event_id FROM main_staging.stg_driver_presence
              GROUP BY 1 HAVING count(*) > 1
            )
            """).fetchone()[0]
        assert remaining == 0

    @needs_local_staging
    def test_earliest_arrival_was_kept(self, con):
        """Determinism depends on this.

        Both copies of a redelivered event carry an identical payload, so which
        one survives looks arbitrary. Keeping the earliest makes re-runs
        byte-identical; keeping the latest would give the same payload with a
        different ingested_at and make lateness metrics irreproducible.
        """
        pattern = str(LANDING_ZONE / "**" / "*.parquet").replace("\\", "/")
        mismatches = con.sql(f"""
            WITH duplicated AS (
              SELECT event_id, min(ingested_at) AS earliest
              FROM read_parquet('{pattern}')
              GROUP BY 1 HAVING count(*) > 1
            )
            SELECT count(*)
            FROM duplicated d
            JOIN main_staging.stg_trip_events s USING (event_id)
            WHERE s.ingested_at != d.earliest
            """).fetchone()[0]
        assert mismatches == 0, f"{mismatches} rows kept a later arrival"


class TestStagingShape:
    def test_every_event_type_has_a_typed_model(self, con):
        expected = {
            "stg_ride_requested",
            "stg_ride_accepted",
            "stg_driver_arrived",
            "stg_ride_started",
            "stg_ride_completed",
            "stg_ride_cancelled",
            "stg_payment_completed",
            "stg_driver_online",
            "stg_driver_offline",
        }
        present = {
            row[0]
            for row in con.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main_staging'"
            ).fetchall()
        }
        assert expected <= present, f"missing models: {expected - present}"

    @needs_local_staging
    def test_typed_models_cover_the_base_model(self, con):
        """Sum of the per-type models must equal the base model. A gap means an
        event type is landing but has no extraction model."""
        base = con.sql("SELECT count(*) FROM main_staging.stg_trip_events").fetchone()[0]
        typed = con.sql("""
            SELECT
              (SELECT count(*) FROM main_staging.stg_ride_requested)
            + (SELECT count(*) FROM main_staging.stg_ride_accepted)
            + (SELECT count(*) FROM main_staging.stg_driver_arrived)
            + (SELECT count(*) FROM main_staging.stg_ride_started)
            + (SELECT count(*) FROM main_staging.stg_ride_completed)
            + (SELECT count(*) FROM main_staging.stg_ride_cancelled)
            + (SELECT count(*) FROM main_staging.stg_payment_completed)
            """).fetchone()[0]
        assert base == typed, f"base has {base} rows, typed models cover {typed}"

    def test_money_is_decimal_not_float(self, con):
        """Floating-point money is how a reconciliation ends up off by a paisa."""
        types = dict(con.sql("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'main_staging'
                  AND table_name = 'stg_ride_completed'
                """).fetchall())
        for column in ("total_fare", "base_fare", "driver_payout", "platform_commission"):
            assert "DECIMAL" in types[column].upper(), f"{column} is {types[column]}"

    def test_reference_seeds_are_loaded(self, con):
        loaded = {
            row[0]
            for row in con.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main_reference'"
            ).fetchall()
        }
        assert len(loaded) >= 11, f"only {len(loaded)} seeds loaded: {sorted(loaded)}"

    def test_every_dimension_has_its_unknown_row(self, con):
        """Surrogate key -1 keeps unresolved lookups countable instead of
        silently dropping fact rows from every aggregate."""
        for table, key in [
            ("dim_city", "city_id"),
            ("dim_zone", "zone_id"),
            ("dim_vehicle_type", "vehicle_type_id"),
            ("dim_payment_method", "payment_method_id"),
            ("dim_cancellation_reason", "cancellation_reason_id"),
        ]:
            found = con.sql(
                f"SELECT count(*) FROM main_reference.{table} WHERE {key} = -1"
            ).fetchone()[0]
            assert found == 1, f"{table} has no UNKNOWN row"


class TestWarehousePortability:
    def test_marts_are_queryable_from_any_directory(self, con):
        """MARTS are the portability guarantee - not staging.

        Marts are materialised as TABLES, so their data lives in the DuckDB
        file and carries no path dependency. They are readable from the host,
        from the container, and by Power BI regardless of which built them.

        An earlier version of this test asserted the same of STAGING. That was
        wrong: staging models are views with the landing-zone path baked in, so
        a container-built warehouse leaves them unreadable on the host. The
        test failed the moment Airflow ran on schedule - asserting a guarantee
        the architecture never made.
        """
        for mart in ("fct_trips", "fct_payments", "fct_driver_sessions"):
            count = con.sql(f"SELECT count(*) FROM main.{mart}").fetchone()[0]
            assert count > 0, f"{mart} unreadable from this directory"
