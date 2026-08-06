# RideFlow — Power BI dashboard

**Everything except the `.pbix` is built and verified. The report itself has to be assembled in Power BI Desktop, which is a Windows GUI application.**

That is a real limitation, not an oversight: `.pbix` is a proprietary binary that cannot be generated from a script. What *is* automated is everything it depends on — the marts, the measure definitions, the freshness contract, and a compatibility check proving the files load.

---

## Status

| Piece | State |
|---|---|
| Parquet marts (19 files) | ✅ Exported, verified Power BI-readable |
| Metric definitions (SQL) | ✅ `analytics/metrics/` — 4 business questions |
| DAX measures + dbt equivalents | ✅ [`measures.md`](measures.md) |
| Freshness contract | ✅ `_FRESHNESS.json`, with `FUTURE_DATED` handling |
| Compatibility check | ✅ No nested types in any export |
| **`RideFlow.pbix`** | ⬜ **Yours to build** — steps below |

---

## Prerequisites

- **Power BI Desktop** — free, Microsoft Store or [powerbi.microsoft.com/desktop](https://powerbi.microsoft.com/desktop/). Windows only.
- Marts exported: `python -m warehouse.export`

---

## Step 1 — Load the marts

**Home → Get Data → Parquet**, then point at `data/processed/`.

Load these first:

```
fct_trips.parquet          16,415 rows   (95 columns - see step 2)
fct_payments.parquet       14,059 rows
fct_driver_sessions.parquet 11,368 rows
dim_date.parquet              203 rows
dim_time.parquet            1,440 rows
dim_zone.parquet               27 rows
dim_ride_status.parquet        11 rows
dim_cancellation_reason.parquet 16 rows
dim_vehicle_type.parquet        7 rows
dim_payment_method.parquet      7 rows
dim_weather.parquet             8 rows
dim_traffic_level.parquet       6 rows
dim_customer_tier.parquet       5 rows
```

**Skip `fct_trip_events.parquet` unless you need it.** It's 90,186 rows and — deliberately — contains events that failed business assertions, because it is the audit record of what *arrived*. Aggregating measures from it would include data the quality gate rejected.

## Step 2 — Hide the noise

`fct_trips` has 95 columns. Most are diagnostics, not report fields. In **Model view**, hide everything except:

- keys (`*_id`), lifecycle timestamps, fare components
- the flags used by measures: `is_completed`, `is_cancelled`, `is_matched`, `is_quarantined`, `is_surge_trip`
- `funnel_stage_reached`, `request_local_hour`

A field list nobody can navigate is how people end up inventing their own measures.

## Step 3 — Relationships

Power BI will auto-detect most. Verify each is **single-direction, many-to-one**, fact → dimension:

| From | To | On |
|---|---|---|
| `fct_trips` | `dim_date` | `request_date_id` → `date_id` |
| `fct_trips` | `dim_time` | `request_time_id` → `time_id` |
| `fct_trips` | `dim_zone` | `pickup_zone_id` → `zone_id` |
| `fct_trips` | `dim_ride_status` | `ride_status_id` → `ride_status_id` |
| `fct_trips` | `dim_vehicle_type` | `vehicle_type_id` → `vehicle_type_id` |
| `fct_trips` | `dim_cancellation_reason` | `cancellation_reason_id` → `cancellation_reason_id` |
| `fct_trips` | `dim_customer_tier` | `customer_tier_id` → `customer_tier_id` |
| `fct_trips` | `dim_weather` | `request_weather_id` → `weather_id` |
| `fct_payments` | `dim_date` | `paid_date_id` → `date_id` |
| `fct_payments` | `dim_time` | `paid_time_id` → `time_id` |
| `fct_payments` | `dim_payment_method` | `payment_method_id` → `payment_method_id` |
| `fct_driver_sessions` | `dim_date` | `online_date_id` → `date_id` |
| `fct_driver_sessions` | `dim_time` | `online_time_id` → `time_id` |
| `fct_driver_sessions` | `dim_zone` | `online_zone_id` → `zone_id` |
| `fct_driver_sessions` | `dim_vehicle_type` | `vehicle_type_id` → `vehicle_type_id` |

> ### No fact-to-fact relationship
> An earlier version of this table listed `fct_payments` → `fct_trips` on `trip_id`. **Delete that relationship if Power BI auto-detects it.**
>
> Payments are 1:1 with trips here, so Power BI creates a one-to-one relationship — and one-to-one relationships in Power BI always filter *bidirectionally*. That opens a second path from `dim_date` to `fct_payments` (direct, and via `fct_trips`), which is exactly the ambiguity the star schema exists to prevent.
>
> Both facts carry their own conformed keys (`city_id`, `paid_date_id`, `payment_method_id`), so comparing charged against collected is a **drill-across** — filter both facts through the shared dimensions, never join one to the other. That is the Kimball rule, and here Power BI enforces it for us.

**Role-playing dimensions.** `fct_trips` carries four zone keys (`pickup_`, `dropoff_`, `actual_dropoff_`, `driver_pickup_`), two weather keys and two traffic keys. Only **one** relationship per table pair can be active. Keep `pickup_zone_id` and `request_weather_id` active; leave the rest with no relationship at all rather than an inactive one, unless a specific visual needs `USERELATIONSHIP`. An inactive relationship nobody activates is a line on a diagram that does nothing.

**Expected blank row.** 86 quarantined trips have a null `request_date_id`, so Power BI will report blank values on the `fct_trips` → `dim_date` relationship. That is correct: those trips never emitted a valid `RideRequested`, which is *why* they are quarantined. Every measure filters `is_quarantined = FALSE`, so they never reach a number.

> ### The model view is a test of the dimensional model
> A correctly-conformed star loads **without a single warning**. If Power BI reports ambiguous relationships or refuses a join, the warehouse model is wrong — fix it in dbt, not with a workaround here. That diagnostic is a genuine reason to use a BI tool rather than a Python chart library.

**Turn off bidirectional filtering** wherever Power BI enables it. Ambiguous filter paths resolve in ways that are hard to predict and harder to explain to whoever asks why a number changed.

## Step 4 — Measures

Create them from [`measures.md`](measures.md), which lists every measure with its dbt/SQL equivalent.

> **The binding rule: no business logic in DAX.** Aggregate, divide, format — nothing else. A threshold or classification written here lives in a binary that cannot be diffed, reviewed or tested, and will silently drift from the pipeline. If you need one, add a column in dbt.

**Do not create `AVERAGE(surge_multiplier)`.** It averages ratios and is dominated by cheap trips. `measures.md` §4 explains why, with the numbers.

## Step 5 — Freshness indicator

**Get Data → JSON** → `data/processed/_FRESHNESS.json`, converted to a single-row table.

Put a card on **every page** bound to `Freshness Status`, conditionally formatted via `Freshness Colour`.

Bind it to `latest_trip_at`, **never** `exported_at`. If ingestion stalls, the export still succeeds hourly — an indicator on `exported_at` shows green while the numbers silently age. Confidently wrong is worse than absent.

Current data reports **`FUTURE_DATED`**: the generated dataset extends ~8 hours past wall clock. That is the indicator working, not a bug.

## Step 6 — Pages

Five pages, mapped to the four business questions plus pipeline health — layout and visuals in [`measures.md`](measures.md) §6.

## Step 7 — Save

Save as `dashboard/RideFlow.pbix`.

`.pbix` is a binary blob: no diffs, no code review, not testable in CI. That cost is accepted deliberately (`architecture.md` §0) and mitigated by keeping every definition here in reviewable Markdown and SQL.

---

## Verifying your report against the pipeline

The report is correct if it reproduces these. Any disagreement means a measure has drifted from its dbt definition.

```bash
python -c "
import duckdb; con = duckdb.connect(); con.execute(\"LOAD icu; SET TimeZone='UTC';\")
print(con.sql(open('analytics/metrics/conversion_funnel.sql').read()).df())
"
```

| Metric | Expected |
|---|---|
| Trips requested (non-quarantined) | **16,203** |
| Completion rate | **85.8%** |
| Biggest funnel drop | Requested → Matched (**1,196** trips) |
| Quarantined trips | **212** |
| Freshness status | `FUTURE_DATED` |

---

## If something looks wrong

| Symptom | Likely cause |
|---|---|
| Totals higher than expected | Missing `is_quarantined = FALSE`, or a fan trap from joining two facts |
| Surge looks tiny or huge | Used `AVERAGE(surge_multiplier)` instead of `Weighted Surge %` |
| Ambiguous relationship warning | Bidirectional filtering, or a second path between two tables |
| Hourly chart peaks at 02:00 | Used a UTC timestamp instead of `request_time_id` (already local) |
| Freshness always green | Bound to `exported_at` rather than `latest_trip_at` |
| Revenue ≠ money collected | Correct — `fct_trips` is charged, `fct_payments` is collected. See `financial_truth.sql` |
