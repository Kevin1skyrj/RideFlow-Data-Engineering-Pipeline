"""M7: the quality gate, quarantine, and pipeline health metrics.

Runs against a built warehouse. Skips cleanly when there isn't one.
"""

from __future__ import annotations

import pytest

from warehouse.connection import WAREHOUSE_PATH, read_only_connection

pytestmark = pytest.mark.skipif(
    not WAREHOUSE_PATH.exists(),
    reason=(f"No warehouse at {WAREHOUSE_PATH}. Build it with: " "cd transformation && dbt build"),
)


@pytest.fixture(scope="module")
def con():
    with read_only_connection() as connection:
        connection.execute("LOAD icu; SET TimeZone='UTC';")
        yield connection


class TestQuarantine:
    def test_quarantined_trips_are_retained_not_deleted(self, con):
        """Quarantine, never discard.

        A quarantined trip stays in fct_trips so that when its missing event
        arrives late, the next run reassembles it and it leaves quarantine on
        its own. Deleting it would make recovery impossible - the evidence it
        existed would be gone.
        """
        quarantined = con.sql(
            "SELECT count(*) FROM main.fct_trips WHERE is_quarantined"
        ).fetchone()[0]
        listed = con.sql("SELECT count(*) FROM main.quarantined_trips").fetchone()[0]
        assert quarantined == listed
        assert quarantined > 0, "no quarantined trips - the anomaly injection may be off"

    def test_every_quarantined_trip_has_a_stated_reason(self, con):
        """A silent exclusion is indistinguishable from data loss."""
        unexplained = con.sql(
            "SELECT count(*) FROM main.quarantined_trips "
            "WHERE quarantine_reason IS NULL OR quarantine_reason = 'unknown'"
        ).fetchone()[0]
        assert unexplained == 0

    def test_quarantine_covers_orphans_and_invalid_sequences(self, con):
        orphans, invalid = con.sql("""
            SELECT
              sum(CASE WHEN NOT has_request_event THEN 1 ELSE 0 END),
              sum(CASE WHEN NOT is_sequence_valid THEN 1 ELSE 0 END)
            FROM main.fct_trips
            """).fetchone()
        assert orphans > 0, "no orphaned trips to quarantine"
        # Every orphan is also sequence-invalid: S1 requires a RideRequested.
        assert invalid >= orphans

    def test_business_metrics_can_exclude_quarantined_trips(self, con):
        """The flag has to actually partition the data, otherwise filtering on
        it is a no-op that looks like a safeguard."""
        clean, dirty = con.sql("""
            SELECT
              sum(CASE WHEN NOT is_quarantined THEN 1 ELSE 0 END),
              sum(CASE WHEN is_quarantined THEN 1 ELSE 0 END)
            FROM main.fct_trips
            """).fetchone()
        assert clean > 0 and dirty > 0


class TestPipelineQuality:
    def test_quality_model_counts_orphans_it_should_measure(self, con):
        """Regression guard.

        The first version grouped on requested_at and filtered out nulls - so
        orphaned trips, which have no requested_at, were silently excluded. It
        reported 0 orphans while 12 existed: a quality metric hiding the exact
        defect it exists to surface.
        """
        reported = con.sql(
            "SELECT coalesce(sum(orphaned_trips), 0) FROM main.fct_pipeline_quality"
        ).fetchone()[0]
        actual = con.sql(
            "SELECT count(*) FROM main.fct_trips WHERE NOT has_request_event"
        ).fetchone()[0]
        assert reported == actual, f"quality reports {reported} orphans, actual {actual}"

    def test_trip_totals_reconcile_with_the_fact_table(self, con):
        reported = con.sql(
            "SELECT coalesce(sum(trips), 0) FROM main.fct_pipeline_quality"
        ).fetchone()[0]
        actual = con.sql("SELECT count(*) FROM main.fct_trips").fetchone()[0]
        assert reported == actual

    def test_no_duplicate_events_remain_after_staging(self, con):
        remaining = con.sql(
            "SELECT coalesce(sum(duplicate_events_remaining), 0) " "FROM main.fct_pipeline_quality"
        ).fetchone()[0]
        assert remaining == 0, "deduplication left duplicates in staging"

    def test_unexplained_revenue_is_tracked(self, con):
        """Money collected for trips the warehouse cannot describe, because the
        RideCompleted was quarantined in the DLQ. Expected to be non-zero here;
        what matters is that it is measured rather than silently absorbed."""
        rows = con.sql(
            "SELECT count(*) FROM main.fct_pipeline_quality " "WHERE unexplained_revenue IS NULL"
        ).fetchone()[0]
        assert rows == 0, "unexplained_revenue must never be null"


class TestBusinessMartsAreProtected:
    def test_financial_invariants_hold_in_the_marts(self, con):
        """F1 and F2, asserted directly against the published marts rather than
        against staging - the layer a consumer actually reads."""
        f1 = con.sql("""
            SELECT count(*) FROM main.fct_trips
            WHERE total_fare IS NOT NULL
              AND abs((base_fare + distance_fare + time_fare + surge_amount
                       + airport_fee + toll_amount + booking_fee + tax_amount)
                      - total_fare) > 0.01
            """).fetchone()[0]
        assert f1 == 0, f"{f1} trips violate F1 in the mart"

        f2 = con.sql("""
            SELECT count(*) FROM main.fct_trips
            WHERE total_fare IS NOT NULL
              AND abs((driver_payout + platform_commission)
                      - (total_fare - tax_amount)) > 0.01
            """).fetchone()[0]
        assert f2 == 0, f"{f2} trips violate F2 in the mart"

    def test_no_future_dated_corruption_survives(self, con):
        """The chaos injection used year 2099. Nothing from it may remain in a
        business mart after cleanup."""
        for table, column in [("fct_trips", "requested_at"), ("fct_payments", "paid_at")]:
            leaked = con.sql(
                f"SELECT count(*) FROM main.{table} " f"WHERE {column} >= TIMESTAMPTZ '2099-01-01'"
            ).fetchone()[0]
            assert leaked == 0, f"{leaked} corrupt rows survive in {table}"

    def test_no_negative_money_in_the_marts(self, con):
        negatives = con.sql("""
            SELECT count(*) FROM main.fct_trips
            WHERE total_fare < 0 OR driver_payout < 0 OR platform_commission < 0
               OR tax_amount < 0 OR surge_amount < 0 OR cancellation_fee < 0
            """).fetchone()[0]
        assert negatives == 0
