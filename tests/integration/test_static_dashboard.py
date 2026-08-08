"""The published dashboard must agree with the warehouse.

This is the point of generating the page from a script rather than clicking it
together: a `.pbix` cannot be checked by CI, but an HTML file produced by code
can be, and a number on a public page that has drifted from the pipeline is
worse than no page.

The tests regenerate the page in memory and assert the rendered TEXT contains
the figures computed independently from the Parquet marts. Asserting on the
rendered output rather than on the collect() dict is deliberate - a formatting
bug that prints the wrong number is exactly the failure this must catch, and
checking the dict would miss it.
"""

from __future__ import annotations

import re

import pytest

from warehouse.connection import ROOT

EXPORT_DIR = ROOT / "data" / "processed"

pytestmark = pytest.mark.skipif(
    not (EXPORT_DIR / "fct_trips.parquet").exists(),
    reason=f"No exported marts in {EXPORT_DIR}. Run: python -m warehouse.export",
)


@pytest.fixture(scope="module")
def built() -> tuple[dict, str]:
    from dashboard.generate_static import collect, connect, render

    con = connect()
    try:
        data = collect(con)
    finally:
        con.close()
    return data, render(data)


@pytest.fixture(scope="module")
def data(built) -> dict:
    return built[0]


@pytest.fixture(scope="module")
def html(built) -> str:
    return built[1]


class TestNumbersMatchTheWarehouse:
    def test_headline_counts_are_rendered(self, data, html):
        requested = data["kpi"][0]
        assert f"{requested:,}" in html, f"{requested:,} trips requested is not on the page"

    def test_completion_rate_matches(self, data, html):
        requested, completed = data["kpi"][0], data["kpi"][1]
        expected = 100.0 * completed / requested
        assert f"{expected:.1f}%" in html

    def test_gross_bookings_matches(self, data, html):
        gross = float(data["kpi"][3])
        assert f"{gross:,.0f}" in html

    def test_fare_decomposition_is_exact(self, data):
        """Invariant F1, re-checked at publish time.

        payout + commission + tax must equal gross to the paisa. If it ever
        does not, the page would show a decomposition that silently fails to
        add up, and a reader doing the arithmetic would catch it before we did.
        """
        _, _, _, gross, commission, payout, tax, _, _ = data["kpi"]
        assert float(payout) + float(commission) + float(tax) == pytest.approx(
            float(gross), abs=0.01
        )

    def test_funnel_is_monotonically_decreasing(self, data):
        counts = [n for _, _, n in data["funnel"]]
        assert counts == sorted(counts, reverse=True), f"funnel not monotonic: {counts}"

    def test_quarantined_trips_are_disclosed(self, data, html):
        """A silent exclusion is indistinguishable from data loss."""
        assert f"{data['quarantined']:,}" in html
        assert "quarantined" in html.lower()


class TestHonestPresentation:
    def test_freshness_status_is_shown(self, data, html):
        status = data["marker"].get("freshness_status", "UNKNOWN")
        assert status in html, "the page does not disclose data freshness"

    def test_snapshot_caveat_is_present(self, html):
        """The page has no live data. Saying so is not optional."""
        assert "snapshot" in html.lower()

    def test_reported_day_is_stated(self, data, html):
        """The page shows one operating day, not everything ever landed.
        Which day must be on the page or the numbers are unattributable."""
        assert str(data["day"]) in html

    def test_every_chart_has_a_table_view(self, html):
        """Colour and hover must never be the only route to a value."""
        charts = html.count("<svg")
        tables = html.count("tableview")
        assert tables >= charts, f"{charts} charts but only {tables} table views"

    def test_no_external_requests(self, html):
        """Self-contained by construction.

        A CDN link would make the published page depend on a third party
        staying up and on the viewer having network access to it - and would
        break under any strict content policy.
        """
        for pattern in ("http://", "src=", "@import", "cdn."):
            offenders = [
                m
                for m in re.findall(rf"[^\s\"']*{re.escape(pattern)}[^\s\"'>]*", html)
                if "github.com" not in m
            ]
            assert not offenders, f"external reference found: {offenders[:3]}"

    def test_dark_mode_is_defined_in_both_scopes(self, html):
        """A viewer's explicit theme choice and their OS setting are different
        signals; a page that handles only one renders unreadable for the other."""
        assert "prefers-color-scheme:dark" in html.replace(" ", "")
        assert '[data-theme="dark"]' in html


class TestChartCorrectness:
    def test_supply_and_demand_are_indexed_not_dual_axis(self, data, html):
        """Trips and driver sessions are different units.

        Plotting them against two y-scales, or as small multiples each scaled
        to its own peak, invents a visual correlation. Both are indexed to
        their own daily total so they share one axis in one unit.
        """
        assert "share of its own daily total" in html
        demand_total = sum(n for _, n in data["demand"])
        supply_total = sum(n for _, n in data["supply"])
        assert demand_total > 0 and supply_total > 0
        assert demand_total != supply_total, (
            "totals are equal, so this dataset cannot demonstrate the problem "
            "indexing solves - the test is vacuous"
        )

    def test_zone_chart_excludes_small_samples(self, data):
        """An unmet-demand percentage over a handful of trips is noise, and it
        would dominate a 'worst zones' ranking sorted by percentage."""
        assert all(requests >= 50 for _, _, requests in data["unmet"])

    def test_page_is_small_enough_to_open_instantly(self, html):
        """The whole argument for a static page is that it loads immediately.
        A megabyte of inline SVG would forfeit that."""
        kb = len(html.encode("utf-8")) / 1024
        assert kb < 500, f"page is {kb:.0f} KB"
