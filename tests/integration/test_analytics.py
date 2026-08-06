"""M9: the serving layer - exported marts, metric queries, freshness contract.

Runs against the exported Parquet in data/processed/, which is what Power BI
actually reads. Testing the DuckDB marts instead would miss export bugs
entirely - and the export is where a nested type or a lost column would break
the dashboard.
"""

from __future__ import annotations

import json

import duckdb
import pytest

from warehouse.connection import ROOT

EXPORT_DIR = ROOT / "data" / "processed"
METRICS_DIR = ROOT / "analytics" / "metrics"

pytestmark = pytest.mark.skipif(
    not (EXPORT_DIR / "fct_trips.parquet").exists(),
    reason=f"No exported marts in {EXPORT_DIR}. Run: python -m warehouse.export",
)

REQUIRED_EXPORTS = [
    "fct_trips",
    "fct_payments",
    "fct_driver_sessions",
    "fct_trip_events",
    "fct_pipeline_quality",
    "quarantined_trips",
    "dim_date",
    "dim_time",
    "dim_zone",
    "dim_city",
    "dim_ride_status",
    "dim_cancellation_reason",
    "dim_vehicle_type",
    "dim_payment_method",
    "dim_customer_tier",
    "dim_weather",
    "dim_traffic_level",
]


@pytest.fixture(scope="module")
def con():
    connection = duckdb.connect()
    connection.execute("LOAD icu; SET TimeZone='UTC';")
    yield connection
    connection.close()


def _parquet(name: str) -> str:
    return (EXPORT_DIR / f"{name}.parquet").as_posix()


@pytest.fixture(scope="module")
def marker() -> dict:
    """The freshness contract the dashboard binds to.

    Module-scoped and defined at module level. A class-scoped fixture declared
    inside the test class tripped a pytest finalizer assertion, which surfaced
    as four ERRORs with no useful message - easy to misread as a broken test
    rather than a fixture-scope problem.
    """
    path = EXPORT_DIR / "_FRESHNESS.json"
    assert path.exists(), "freshness marker missing - the dashboard cannot show data age"
    return json.loads(path.read_text(encoding="utf-8"))


class TestExportCompleteness:
    @pytest.mark.parametrize("mart", REQUIRED_EXPORTS)
    def test_mart_is_exported_and_readable(self, con, mart):
        rows = con.sql(f"SELECT count(*) FROM read_parquet('{_parquet(mart)}')").fetchone()[0]
        assert rows >= 0

    @pytest.mark.parametrize("mart", REQUIRED_EXPORTS)
    def test_no_nested_types(self, con, mart):
        """Power BI's Parquet connector cannot load STRUCT/LIST/MAP columns.

        A nested column would import as an error rather than data, and the
        failure appears in the GUI rather than anywhere testable.
        """
        described = con.sql(f"DESCRIBE SELECT * FROM read_parquet('{_parquet(mart)}')").df()
        nested = [
            row.column_name
            for _, row in described.iterrows()
            if any(t in row.column_type.upper() for t in ("STRUCT", "LIST", "MAP", "UNION"))
        ]
        assert not nested, f"{mart} has nested columns Power BI cannot read: {nested}"

    def test_dim_time_is_complete(self, con):
        """1,440 rows - one per minute. A gap makes trips join to nothing and
        vanish from every time-of-day aggregate, silently."""
        rows = con.sql(f"SELECT count(*) FROM read_parquet('{_parquet('dim_time')}')").fetchone()[0]
        assert rows == 1440


class TestMetricQueries:
    """Every metric SQL file must execute and return rows.

    A query that no longer compiles after a schema change is a broken
    dashboard, and nothing else in the suite would notice.
    """

    @pytest.mark.parametrize(
        "metric",
        ["marketplace_health", "conversion_funnel", "pricing_effectiveness", "financial_truth"],
    )
    def test_metric_query_runs(self, con, metric):
        sql = (METRICS_DIR / f"{metric}.sql").read_text(encoding="utf-8")
        result = con.sql(sql).df()
        assert not result.empty, f"{metric} returned no rows"

    def test_funnel_is_monotonically_decreasing(self, con):
        """Each stage must contain a subset of the previous one. A stage with
        MORE trips than its predecessor means the funnel logic is wrong."""
        sql = (METRICS_DIR / "conversion_funnel.sql").read_text(encoding="utf-8")
        trips = con.sql(sql).df()["trips"].tolist()
        assert trips == sorted(trips, reverse=True), f"funnel not monotonic: {trips}"

    def test_completion_rate_is_plausible(self, con):
        rate = con.sql(f"""
            SELECT 100.0 * sum(CASE WHEN is_completed THEN 1 ELSE 0 END) / count(*)
            FROM read_parquet('{_parquet('fct_trips')}') WHERE NOT is_quarantined
            """).fetchone()[0]
        assert 75 <= rate <= 95, f"completion rate {rate:.1f}% is not plausible"

    def test_weighted_surge_is_computable_and_sane(self, con):
        """The revenue-weighted surge measure must work and be plausible.

        NOTE: an earlier version of this test asserted the weighted figure
        differs from AVERAGE(surge_multiplier) by more than a point. That was
        wrong. On this dataset they agree closely (19.36% vs 19.98%), because
        surge happens to RISE with fare size and the weighting nearly cancels.

        The two measures answer different questions and are not interchangeable
        - but a numeric gap between them is a property of the data, not an
        invariant, and asserting one made the test fail on correct data.
        """
        weighted, naive_x = con.sql(f"""
            SELECT
              100.0 * sum(surge_amount)
                    / nullif(sum(base_fare + distance_fare + time_fare), 0),
              avg(surge_multiplier)
            FROM read_parquet('{_parquet('fct_trips')}')
            WHERE NOT is_quarantined AND is_completed
            """).fetchone()

        assert weighted is not None and weighted >= 0
        assert naive_x >= 1.0, "surge multiplier below the contract floor of 1.00"
        assert weighted < 300, f"weighted surge {weighted:.1f}% is implausible"

    def test_surge_amount_reconciles_with_its_multiplier(self, con):
        """Invariant F5, asserted on the EXPORTED mart.

        This is the real guarantee behind the weighted measure: if
        surge_amount did not equal (base+distance+time) x (multiplier-1), the
        currency-based calculation would be quietly wrong and no DAX measure
        could detect it.
        """
        violations = con.sql(f"""
            SELECT count(*) FROM read_parquet('{_parquet('fct_trips')}')
            WHERE is_completed
              AND abs(surge_amount
                      - (base_fare + distance_fare + time_fare)
                        * (surge_multiplier - 1)) > 0.01
            """).fetchone()[0]
        assert violations == 0, f"{violations} trips violate F5 in the exported mart"


class TestFreshnessContract:
    def test_marker_has_the_required_fields(self, marker):
        for field in ("exported_at", "latest_trip_at", "data_age_hours", "freshness_status"):
            assert field in marker, f"missing {field}"

    def test_status_is_a_known_value(self, marker):
        assert marker["freshness_status"] in {
            "FRESH",
            "STALE",
            "VERY_STALE",
            "FUTURE_DATED",
            "UNKNOWN",
        }

    def test_age_is_measured_from_the_data_not_the_export(self, marker):
        """If ingestion stalls, the export still succeeds every hour. An
        indicator based on exported_at would show green while numbers aged -
        confidently wrong, which is worse than absent."""
        assert marker["latest_trip_at"] is not None
        assert marker["latest_trip_at"] != marker["exported_at"]

    def test_negative_age_is_not_reported_as_fresh(self, marker):
        """Regression guard.

        The first version had no branch for negative age, so data timestamped
        8 hours in the FUTURE fell through `age <= 2` and reported FRESH.
        """
        age = marker["data_age_hours"]
        if age is not None and age < -0.5:
            assert marker["freshness_status"] == "FUTURE_DATED", (
                f"age {age}h is in the future but status is " f"{marker['freshness_status']}"
            )


class TestQuarantineIsExcludable:
    def test_quarantined_trips_are_exported_for_inspection(self, con):
        """A silent exclusion is indistinguishable from data loss."""
        listed = con.sql(
            f"SELECT count(*) FROM read_parquet('{_parquet('quarantined_trips')}')"
        ).fetchone()[0]
        flagged = con.sql(
            f"SELECT count(*) FROM read_parquet('{_parquet('fct_trips')}') WHERE is_quarantined"
        ).fetchone()[0]
        assert listed == flagged

    def test_filtering_quarantine_changes_the_numbers(self, con):
        """If it didn't, the filter in every measure would be decoration."""
        total, clean = con.sql(f"""
            SELECT count(*),
                   sum(CASE WHEN NOT is_quarantined THEN 1 ELSE 0 END)
            FROM read_parquet('{_parquet('fct_trips')}')
            """).fetchone()
        assert clean < total
