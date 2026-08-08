"""Generate the public static dashboard from the exported Parquet marts.

    python -m dashboard.generate_static          -> site/index.html

── Why this exists alongside the Power BI report ───────────────────────────
`.pbix` is a proprietary binary: it cannot be diffed, reviewed, or tested, and
opening it needs Windows plus Power BI Desktop. architecture.md 0 accepts that
cost deliberately for the interactive report.

This page is the inverse trade. It is produced by a script that IS diffable,
from SQL already under test, and CI asserts that the numbers it publishes match
the warehouse (tests/integration/test_static_dashboard.py). It is the only
visualisation in this project whose output is verifiable.

── Why the numbers are baked in ────────────────────────────────────────────
Every figure is computed at BUILD time and written into the HTML as text. The
published page has no runtime data dependency: no database, no API, no Parquet
fetch, no CDN. That is what lets it be a few hundred KB of static files that
open instantly on a phone - and it is why `data/` can stay gitignored
(PROJECT_PLAN.md 9, invariant 1) instead of being committed to feed a live app.

The cost, stated plainly on the page itself: it is a SNAPSHOT, only as current
as the last build.

── Scope: one operating day ────────────────────────────────────────────────
The landing zone holds more than one generator run, and short test bursts sit
alongside full days. Mixing them produces a demand curve with an artificial
spike where a burst landed. The page reports the most recent date present, and
says which date that is.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from html import escape

import duckdb

from dashboard import charts
from warehouse.connection import ROOT

EXPORT_DIR = ROOT / "data" / "processed"
OUT_DIR = ROOT / "site"
OUT_FILE = OUT_DIR / "index.html"

# Status colours are reserved: never reused as a series colour, and always
# shipped with a label rather than carrying meaning through hue alone.
FRESHNESS_STATUS = {
    "FRESH": ("good", "Data is current"),
    "STALE": ("warning", "Data is ageing"),
    "VERY_STALE": ("critical", "Data is stale"),
    "FUTURE_DATED": ("serious", "Timestamped ahead of wall clock"),
    "UNKNOWN": ("warning", "Freshness could not be determined"),
}


def _parquet(name: str) -> str:
    return (EXPORT_DIR / f"{name}.parquet").as_posix()


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    # Same session settings as the warehouse. Without a pinned timezone the
    # hour-of-day figures silently follow the build machine's locale - the bug
    # that made every date calculation look correct on an IST laptop and wrong
    # in CI (profiles.yml).
    con.execute("LOAD icu; SET TimeZone='UTC';")
    return con


def collect(con: duckdb.DuckDBPyConnection) -> dict:
    """Every number the page shows, computed once."""
    trips = _parquet("fct_trips")
    dates = _parquet("dim_date")
    zones = _parquet("dim_zone")
    sessions = _parquet("fct_driver_sessions")

    day = con.sql(f"""
        SELECT max(d.full_date)
        FROM read_parquet('{trips}') t
        JOIN read_parquet('{dates}') d ON d.date_id = t.request_date_id
        WHERE NOT t.is_quarantined
        """).fetchone()[0]

    # Repeated in every query rather than factored into a helper, so that no
    # query can accidentally omit the quarantine filter. That omission is
    # exactly what overstated gross bookings by Rs 100,039 in the Power BI
    # measures before it was caught against these same marts.
    where = f"NOT t.is_quarantined AND d.full_date = DATE '{day}'"
    frm = f"""FROM read_parquet('{trips}') t
              JOIN read_parquet('{dates}') d ON d.date_id = t.request_date_id"""

    kpi = con.sql(f"""
        SELECT count(*)                                          AS requested,
               sum(CASE WHEN t.is_completed THEN 1 ELSE 0 END)   AS completed,
               sum(CASE WHEN NOT t.is_matched THEN 1 ELSE 0 END) AS unmatched,
               sum(t.total_fare)                                 AS gross,
               sum(t.platform_commission)                        AS commission,
               sum(t.driver_payout)                              AS payout,
               sum(t.tax_amount)                                 AS tax,
               sum(t.surge_amount)                               AS surge,
               sum(t.base_fare + t.distance_fare + t.time_fare)  AS presurge
        {frm} WHERE {where}
        """).fetchone()

    demand = con.sql(
        f"SELECT t.request_local_hour, count(*) {frm} WHERE {where} GROUP BY 1 ORDER BY 1"
    ).fetchall()

    supply = con.sql(f"""
        SELECT s.online_time_id // 100 AS hr, count(*)
        FROM read_parquet('{sessions}') s
        JOIN read_parquet('{dates}') d ON d.date_id = s.online_date_id
        WHERE d.full_date = DATE '{day}'
        GROUP BY 1 ORDER BY 1
        """).fetchall()

    funnel = con.sql(f"""
        WITH base AS (SELECT t.* {frm} WHERE {where})
        SELECT 'Requested' AS stage, 1 AS ord, count(*) AS n FROM base
        UNION ALL SELECT 'Matched',        2, count(*) FROM base WHERE funnel_stage_reached >= 2
        UNION ALL SELECT 'Driver arrived', 3, count(*) FROM base WHERE funnel_stage_reached >= 3
        UNION ALL SELECT 'Started',        4, count(*) FROM base WHERE funnel_stage_reached >= 4
        UNION ALL SELECT 'Completed',      5, count(*) FROM base WHERE funnel_stage_reached >= 5
        UNION ALL SELECT 'Paid',           6, count(*) FROM base WHERE funnel_stage_reached >= 6
        ORDER BY ord
        """).fetchall()

    # >= 50 requests, because an unmet-demand percentage over a handful of
    # trips is noise. marketplace_health.sql applies the same guard as an
    # INSUFFICIENT_DATA branch.
    unmet = con.sql(f"""
        SELECT z.zone_name,
               round(100.0 * sum(CASE WHEN NOT t.is_matched THEN 1 ELSE 0 END)
                     / nullif(count(*), 0), 1) AS pct,
               count(*) AS requests
        {frm}
        JOIN read_parquet('{zones}') z ON z.zone_id = t.pickup_zone_id
        WHERE {where}
        GROUP BY 1
        HAVING count(*) >= 50
        ORDER BY 2 DESC
        LIMIT 10
        """).fetchall()

    surge_by_hour = con.sql(f"""
        SELECT t.request_local_hour AS hr,
               round(100.0 * sum(t.surge_amount)
                     / nullif(sum(t.base_fare + t.distance_fare + t.time_fare), 0), 1) AS pct
        {frm} WHERE {where} AND t.is_completed
        GROUP BY 1 ORDER BY 1
        """).fetchall()

    quality = con.sql(f"""
        SELECT landed_date,
               trip_events_landed + presence_events_landed AS landed,
               duplicate_events_remaining, late_arrivals, clock_skewed_events,
               orphaned_trips, sequence_invalid_trips, unexplained_revenue
        FROM read_parquet('{_parquet('fct_pipeline_quality')}')
        ORDER BY landed_date
        """).fetchall()

    quarantined = con.sql(
        f"SELECT count(*) FROM read_parquet('{_parquet('quarantined_trips')}')"
    ).fetchone()[0]

    marker_path = EXPORT_DIR / "_FRESHNESS.json"
    marker = json.loads(marker_path.read_text(encoding="utf-8")) if marker_path.exists() else {}

    return {
        "day": day,
        "kpi": kpi,
        "demand": demand,
        "supply": supply,
        "funnel": funnel,
        "unmet": unmet,
        "surge_by_hour": surge_by_hour,
        "quality": quality,
        "quarantined": quarantined,
        "marker": marker,
        "generated_at": datetime.now(UTC),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Rendering
# ─────────────────────────────────────────────────────────────────────────────

CSS = """
*,*::before,*::after{box-sizing:border-box}
:root{
  color-scheme:light;
  --page:#f9f9f7; --surface:#fcfcfb;
  --ink:#0b0b0b; --ink-2:#52514e; --muted:#898781;
  --grid:#e1e0d9; --axis:#c3c2b7; --border:rgba(11,11,11,.10);
  --s1:#2a78d6; --s2:#eb6834;
  --good:#0ca30c; --warning:#fab219; --serious:#ec835a; --critical:#d03b3b;
}
@media (prefers-color-scheme:dark){
  :root:not([data-theme="light"]){
    color-scheme:dark;
    --page:#0d0d0d; --surface:#1a1a19;
    --ink:#fff; --ink-2:#c3c2b7; --muted:#898781;
    --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,.10);
    --s1:#3987e5; --s2:#d95926;
  }
}
:root[data-theme="dark"]{
  color-scheme:dark;
  --page:#0d0d0d; --surface:#1a1a19;
  --ink:#fff; --ink-2:#c3c2b7; --muted:#898781;
  --grid:#2c2c2a; --axis:#383835; --border:rgba(255,255,255,.10);
  --s1:#3987e5; --s2:#d95926;
}
body{margin:0;background:var(--page);color:var(--ink);
  font:15px/1.55 system-ui,-apple-system,"Segoe UI",sans-serif;}
.wrap{max-width:1080px;margin:0 auto;padding:32px 20px 64px}
h1{font-size:1.75rem;margin:0 0 4px;letter-spacing:-.02em}
h2{font-size:1.05rem;margin:0 0 2px;letter-spacing:-.01em}
.sub{color:var(--ink-2);margin:0 0 24px}
.note{color:var(--muted);font-size:.82rem;margin:6px 0 0}
a{color:var(--s1)}

.banner{display:flex;gap:12px;align-items:baseline;flex-wrap:wrap;
  background:var(--surface);border:1px solid var(--border);border-left-width:4px;
  border-radius:8px;padding:12px 16px;margin-bottom:24px}
.banner.good{border-left-color:var(--good)}
.banner.warning{border-left-color:var(--warning)}
.banner.serious{border-left-color:var(--serious)}
.banner.critical{border-left-color:var(--critical)}
.banner strong{font-size:.85rem;letter-spacing:.06em;text-transform:uppercase}
.banner span{color:var(--ink-2);font-size:.9rem}

.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:28px}
.tile{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px 16px}
.tile .v{font-size:1.7rem;font-weight:600;letter-spacing:-.02em;line-height:1.15}
.tile .k{color:var(--ink-2);font-size:.82rem;margin-top:2px}

.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:16px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:8px;
  padding:16px 18px 12px;margin-bottom:16px;min-width:0}
.card .cap{color:var(--ink-2);font-size:.85rem;margin:0 0 10px}

.chart{width:100%;height:auto;display:block;overflow:visible}
.bar.s1{fill:var(--s1)} .bar.s2{fill:var(--s2)}
.line{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}
.line.s1{stroke:var(--s1)} .line.s2{stroke:var(--s2)}
.val.lbl{font-size:11px;font-weight:600}
.val.lbl.s1{fill:var(--s1)} .val.lbl.s2{fill:var(--s2)}
.dot.s1{fill:var(--s1);stroke:var(--surface);stroke-width:2}
.grid{stroke:var(--grid);stroke-width:1}
.baseline{stroke:var(--axis);stroke-width:1}
.axis{fill:var(--muted);font-size:10px;font-variant-numeric:tabular-nums}
.cat{fill:var(--ink-2);font-size:11.5px}
.val{fill:var(--ink-2);font-size:11px;font-variant-numeric:tabular-nums}
.val.peak{fill:var(--ink);font-weight:600}
.hit{fill:transparent}
.mark:hover .bar{opacity:.78}
.legend{display:flex;gap:16px;margin:0 0 8px;padding:0;list-style:none;
  color:var(--ink-2);font-size:.82rem}
.legend li{display:flex;align-items:center;gap:6px}
.swatch{width:10px;height:10px;border-radius:2px;display:inline-block}
.swatch.s1{background:var(--s1)} .swatch.s2{background:var(--s2)}

.tableview{margin-top:10px}
.tableview summary{cursor:pointer;color:var(--ink-2);font-size:.82rem}
.scroll{overflow-x:auto;margin-top:8px}
table{border-collapse:collapse;width:100%;font-size:.82rem}
th,td{text-align:right;padding:5px 8px;border-bottom:1px solid var(--border);
  font-variant-numeric:tabular-nums;white-space:nowrap}
th:first-child,td:first-child{text-align:left;font-variant-numeric:normal}
th{color:var(--ink-2);font-weight:600}
footer{margin-top:40px;padding-top:20px;border-top:1px solid var(--border);
  color:var(--muted);font-size:.82rem}
.empty{color:var(--muted);font-size:.85rem}
"""


def _tile(value: str, key: str) -> str:
    return f'<div class="tile"><div class="v">{escape(value)}</div><div class="k">{escape(key)}</div></div>'


def render(d: dict) -> str:
    requested, completed, unmatched, gross, commission, payout, tax, surge, presurge = d["kpi"]
    completion = 100.0 * completed / requested if requested else 0.0
    unmet = 100.0 * unmatched / requested if requested else 0.0
    weighted_surge = 100.0 * float(surge) / float(presurge) if presurge else 0.0

    marker = d["marker"]
    status = marker.get("freshness_status", "UNKNOWN")
    tone, explain = FRESHNESS_STATUS.get(status, FRESHNESS_STATUS["UNKNOWN"])
    age = marker.get("data_age_hours")

    demand = [(str(h), n) for h, n in d["demand"]]
    supply = [(str(h), n) for h, n in d["supply"]]
    funnel = [(s, n) for s, _, n in d["funnel"]]
    surge_hours = [(str(h), float(p or 0)) for h, p in d["surge_by_hour"]]
    unmet_rows = [(z, float(p or 0)) for z, p, _ in d["unmet"]]

    fare_split = [
        ("Driver payout", float(payout)),
        ("Platform commission", float(commission)),
        ("Tax", float(tax)),
    ]

    parts: list[str] = [
        '<div class="wrap"><h1>RideFlow</h1>',
        '<p class="sub">Ride-hailing marketplace analytics &mdash; Kafka &rarr; Parquet '
        "lakehouse &rarr; dbt &rarr; DuckDB. Every figure below is generated from the "
        "warehouse and verified in CI.</p>",
        f'<div class="banner {tone}"><strong>{escape(status)}</strong>'
        f"<span>{escape(explain)}"
        + (f" &middot; data age {age:+.1f}h" if isinstance(age, int | float) else "")
        + f" &middot; reporting {escape(str(d['day']))}</span></div>",
        '<div class="tiles">',
        _tile(f"{requested:,}", "Trips requested"),
        _tile(f"{completion:.1f}%", "Completion rate"),
        _tile(f"{unmet:.1f}%", "Unmet demand"),
        _tile(f"₹{float(gross):,.0f}", "Gross bookings"),
        _tile(f"₹{float(commission):,.0f}", "Platform commission"),
        _tile(f"{d['quarantined']:,}", "Quarantined"),
        "</div>",
    ]

    # ── Demand vs supply: SMALL MULTIPLES, never a dual axis ────────────────
    # Indexed to each series' own daily total, so both sit on one axis in the
    # same unit. Raw counts cannot share a scale (trips and sessions are
    # different things) and a second y-axis would invent a correlation.
    supply_by_hour = dict(supply)
    hours = [str(h) for h in range(24)]
    demand_by_hour = dict(demand)
    demand_total = sum(demand_by_hour.values()) or 1
    supply_total = sum(supply_by_hour.values()) or 1
    demand_pct = [100.0 * demand_by_hour.get(h, 0) / demand_total for h in hours]
    supply_pct = [100.0 * supply_by_hour.get(h, 0) / supply_total for h in hours]

    peak_demand_hour = hours[demand_pct.index(max(demand_pct))]
    peak_supply_hour = hours[supply_pct.index(max(supply_pct))]

    parts.append(
        '<div class="card"><h2>When supply arrives, and when demand peaks</h2>'
        '<p class="cap">Each series as a share of its own daily total, so both sit '
        "on one axis in the same unit. Levels are not comparable &mdash; trips and "
        "sessions are different things &mdash; but timing is, and timing is the "
        "question.</p>"
        '<ul class="legend"><li><span class="swatch s1"></span>Trips requested</li>'
        '<li><span class="swatch s2"></span>Driver sessions started</li></ul>'
        + charts.indexed_lines(
            hours,
            [
                ("Demand", demand_pct, "1"),
                ("Supply", supply_pct, "2"),
            ],
        )
        + f'<p class="note">Supply peaks at {peak_supply_hour}:00; demand peaks at '
        f"{peak_demand_hour}:00. Drivers come online before the rush and drop off "
        f"during it &mdash; which is what the {unmet:.1f}% unmet demand is made of.</p>"
        + charts.table_view(
            ["Hour", "Trips requested", "% of day", "Sessions started", "% of day"],
            [
                [
                    f"{h}:00",
                    f"{demand_by_hour.get(h, 0):,}",
                    f"{demand_pct[i]:.1f}%",
                    f"{supply_by_hour.get(h, 0):,}",
                    f"{supply_pct[i]:.1f}%",
                ]
                for i, h in enumerate(hours)
            ],
            label="Show as table",
        )
        + "</div>"
    )

    parts.append('<div class="grid2">')

    parts.append(
        '<div class="card"><h2>Conversion funnel</h2>'
        '<p class="cap">Trips reaching each stage. Cumulative, so a trip counted at '
        "&ldquo;Started&rdquo; is also counted at every earlier stage.</p>"
        + charts.bar_h(funnel, label_width=110, width=440)
        + charts.table_view(
            ["Stage", "Trips", "% of requests"],
            [[s, f"{n:,}", f"{100.0 * n / funnel[0][1]:.1f}%"] for s, n in funnel],
            label="Show as table",
        )
        + "</div>"
    )

    parts.append(
        '<div class="card"><h2>Unmet demand by zone</h2>'
        '<p class="cap">Share of requests that never matched a driver. Zones with '
        "fewer than 50 requests excluded &mdash; a percentage over a handful of trips "
        "is noise.</p>"
        + charts.bar_h(unmet_rows, label_width=150, width=460, decimals=1, suffix="%")
        + charts.table_view(
            ["Zone", "Unmet demand", "Requests"],
            [[z, f"{p:.1f}%", f"{r:,}"] for z, p, r in d["unmet"]],
            label="Show as table",
        )
        + "</div>"
    )

    parts.append(
        '<div class="card"><h2>Surge through the day</h2>'
        '<p class="cap">Revenue-weighted: surge revenue &divide; pre-surge revenue. '
        "Not the average of <code>surge_multiplier</code>, which averages ratios and "
        "is dominated by cheap trips.</p>"
        + charts.line(surge_hours, decimals=1, suffix="%", label_every=3)
        + charts.table_view(
            ["Hour", "Weighted surge"],
            [[h, f"{p:.1f}%"] for h, p in surge_hours],
            label="Show as table",
        )
        + "</div>"
    )

    parts.append(
        '<div class="card"><h2>Where the fare goes</h2>'
        f'<p class="cap">Gross bookings of ₹{float(gross):,.0f} decompose exactly '
        "into these three. The identity is enforced as an error-severity dbt test, so "
        "the pipeline refuses to publish if it ever fails.</p>"
        + charts.bar_h(fare_split, label_width=150, width=460, decimals=0)
        + charts.table_view(
            ["Component", "Amount"],
            [[k, f"₹{v:,.2f}"] for k, v in fare_split] + [["Total", f"₹{float(gross):,.2f}"]],
            label="Show as table",
        )
        + "</div>"
    )

    parts.append("</div>")

    q = d["quality"]
    parts.append(
        '<div class="card"><h2>Pipeline health</h2>'
        '<p class="cap">Known defect counts, published rather than hidden. Duplicates '
        "arrive because Kafka guarantees at-least-once delivery; zero surviving is the "
        "two-pass deduplication working.</p>"
        + charts.table_view(
            [
                "Load date",
                "Events landed",
                "Duplicates left",
                "Late arrivals",
                "Clock-skewed",
                "Orphaned trips",
                "Sequence invalid",
                "Unexplained revenue",
            ],
            [
                [
                    str(r[0])[:10],
                    f"{r[1]:,}",
                    f"{int(r[2] or 0):,}",
                    f"{int(r[3] or 0):,}",
                    f"{int(r[4] or 0):,}",
                    f"{int(r[5] or 0):,}",
                    f"{int(r[6] or 0):,}",
                    f"₹{float(r[7] or 0):,.2f}",
                ]
                for r in q
            ],
            label="Show the full quality record",
        )
        + f'<p class="note">Weighted surge across the day: {weighted_surge:.2f}%. '
        f"{d['quarantined']:,} trips quarantined and excluded from every figure above; "
        "they are exported separately for inspection, because a silent exclusion is "
        "indistinguishable from data loss.</p></div>"
    )

    parts.append(
        "<footer><p><strong>This is a snapshot.</strong> Every number is computed at "
        "build time and written into the page, so it has no runtime data dependency "
        "&mdash; and is only as current as the last build. The freshness banner reports "
        "the age of the <em>data</em>, not of this file.</p>"
        f"<p>Generated {d['generated_at']:%Y-%m-%d %H:%M} UTC from "
        f"<code>data/processed/</code>. Source: "
        "<a href='https://github.com/Kevin1skyrj/RideFlow-Data-Engineering-Pipeline'>"
        "RideFlow-Data-Engineering-Pipeline</a>.</p></footer></div>"
    )

    body = "".join(parts)
    return (
        "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>RideFlow &mdash; marketplace analytics</title>"
        "<meta name='description' content='Ride-hailing data pipeline: Kafka, dbt, "
        "DuckDB, Airflow. Generated from the warehouse and verified in CI.'>"
        f"<style>{CSS}</style></head><body>{body}</body></html>"
    )


def main() -> int:
    con = connect()
    try:
        data = collect(con)
    finally:
        con.close()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(render(data), encoding="utf-8")

    size_kb = OUT_FILE.stat().st_size / 1024
    print(f"wrote {OUT_FILE} ({size_kb:.0f} KB) for {data['day']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
