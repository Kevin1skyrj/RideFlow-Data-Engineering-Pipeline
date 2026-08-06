# RideFlow — Power BI measures

**Every DAX measure, with the dbt/SQL definition it corresponds to.**

| Field | Value |
|---|---|
| Version | `1.0.0` |
| Milestone | M9 |
| Last updated | 2026-08-06 |

---

## 0. The binding rule

> **No business logic in DAX.**

This is the single discipline that decides whether the serving layer strengthens the architecture or undermines it (`architecture.md` §0). Power BI makes it trivially easy to write a measure that quietly becomes the definition of a business concept — and then that definition lives in a binary `.pbix` that cannot be diffed, reviewed, or tested.

So DAX here is allowed to do exactly three things:

1. **Aggregate** a pre-computed column (`SUM`, `COUNTROWS`, `AVERAGE`).
2. **Divide** two aggregates (`DIVIDE`).
3. **Format** for display.

Anything else — a rule, a threshold, a classification, a weighting — belongs in dbt. If a measure below has no "dbt equivalent" column filled in, that is a defect, not an exception.

**Why it matters concretely:** if the late-arrival threshold lived in DAX, the pipeline and the dashboard could disagree about what "late" means, and nobody would find out until two people compared numbers in a meeting.

---

## 1. Data model in Power BI

Load the exported Parquet from `data/processed/`. Relationships:

```
dim_date ──────┐
dim_time ──────┤
dim_zone ──────┼──< fct_trips >──── dim_vehicle_type
dim_city ──────┤         │          dim_ride_status
dim_weather ───┤         │          dim_customer_tier
dim_traffic ───┘         │          dim_cancellation_reason
                         │
                         └──< fct_payments >── dim_payment_method

dim_zone ──< fct_driver_sessions >── dim_driver_status
```

**All relationships single-direction, many-to-one, from fact to dimension.** Bidirectional filtering creates ambiguous paths that Power BI resolves in ways that are hard to predict and harder to explain — and the model view will warn about it.

> **The star schema is a test of itself.** If Power BI reports ambiguous relationships or refuses a join, the dimensional model is wrong. A correctly-conformed star loads without a single warning (`architecture.md` §7).

---

## 2. Base measures

| Measure | DAX | dbt / SQL equivalent | Notes |
|---|---|---|---|
| `Trips Requested` | `CALCULATE(COUNTROWS(fct_trips), fct_trips[is_quarantined] = FALSE)` | `count(*) from fct_trips where not is_quarantined` | **Every measure filters quarantine.** |
| `Trips Completed` | `CALCULATE([Trips Requested], fct_trips[is_completed] = TRUE)` | `... and is_completed` | |
| `Trips Cancelled` | `CALCULATE([Trips Requested], fct_trips[is_cancelled] = TRUE)` | `... and is_cancelled` | |
| `Trips Unmatched` | `CALCULATE([Trips Requested], fct_trips[is_matched] = FALSE)` | `... and not is_matched` | Demand that never found supply. |
| `Gross Bookings` | `SUM(fct_trips[total_fare])` | `sum(total_fare)` | Additive. |
| `Driver Payout` | `SUM(fct_trips[driver_payout])` | `sum(driver_payout)` | Additive. |
| `Platform Commission` | `SUM(fct_trips[platform_commission])` | `sum(platform_commission)` | Net revenue before gateway cost. |
| `Surge Revenue` | `SUM(fct_trips[surge_amount])` | `sum(surge_amount)` | **Currency, not a ratio.** See §4. |
| `Pre-Surge Revenue` | `SUM(fct_trips[base_fare]) + SUM(fct_trips[distance_fare]) + SUM(fct_trips[time_fare])` | same | Denominator for weighted surge. |
| `Amount Collected` | `SUM(fct_payments[amount_charged])` | `sum(amount_charged)` | Money received ≠ fare charged. |
| `Gateway Fees` | `SUM(fct_payments[gateway_fee_amount])` | `sum(gateway_fee_amount)` | Real margin leakage. |
| `Driver Sessions` | `COUNTROWS(fct_driver_sessions)` | `count(*)` | Supply side. |
| `Online Hours` | `DIVIDE(SUM(fct_driver_sessions[session_duration_sec]), 3600)` | `sum(session_duration_sec)/3600` | NULL for open sessions — excluded, never imputed. |

---

## 3. Ratio measures

Always `DIVIDE()`, never `/`. `DIVIDE` returns blank on a zero denominator; `/` returns `Infinity`, which renders as a number and looks like a real value.

| Measure | DAX | dbt / SQL equivalent |
|---|---|---|
| `Completion Rate %` | `DIVIDE([Trips Completed], [Trips Requested]) * 100` | `100.0 * completed / requested` |
| `Cancellation Rate %` | `DIVIDE([Trips Cancelled], [Trips Requested]) * 100` | `100.0 * cancelled / requested` |
| `Unmet Demand %` | `DIVIDE([Trips Unmatched], [Trips Requested]) * 100` | `analytics/metrics/marketplace_health.sql` |
| `Weighted Surge %` | `DIVIDE([Surge Revenue], [Pre-Surge Revenue]) * 100` | `analytics/metrics/pricing_effectiveness.sql` |
| `Net Platform Revenue` | `[Platform Commission] - [Gateway Fees] - SUM(fct_payments[discount_amount])` | `analytics/metrics/financial_truth.sql` |
| `Trips per Online Hour` | `DIVIDE([Trips Completed], [Online Hours])` | `fct_driver_sessions.trips_per_online_hour` |

---

## 4. ⚠️ The measure that must NOT be written

```dax
-- DO NOT CREATE THIS
Avg Surge = AVERAGE(fct_trips[surge_multiplier])
```

It averages **ratios**. A ₹50 trip at 3.0× and a ₹3,000 trip at 1.1× average to **2.05×** — a number describing no real state of the marketplace, dominated by cheap trips.

Use `Weighted Surge %` instead:

```dax
Weighted Surge % = DIVIDE([Surge Revenue], [Pre-Surge Revenue]) * 100
```

`surge_amount` is stored as **currency** in `fct_trips` precisely so the correct calculation is also the easy one. The model is designed so the right query is the obvious one — that is the strongest available defence against analytical error.

### What the current data actually shows

Measured, not assumed:

| Segment | Trips | Avg fare | Naive avg multiplier | Weighted surge % |
|---|---|---|---|---|
| **Overall** | 13,897 | — | **1.200×** | **19.36%** |
| Cheapest quartile | 3,475 | ₹201 | 1.097× | 7.99% |
| 2nd quartile | 3,474 | ₹353 | 1.150× | 11.65% |
| 3rd quartile | 3,474 | ₹548 | 1.224× | 17.07% |
| Most expensive | 3,474 | ₹1,182 | 1.329× | 24.98% |

**Overall, the two are close** — 19.36% weighted versus a 1.200× naive average (≈19.98%). That is a *property of this dataset*, not a reason to relax: surge here rises with fare size, so the weighting nearly cancels out.

The gap widens exactly when it matters — when cheap trips carry disproportionate surge, which is what a late-night scarcity event looks like. The naive average would then be dragged upward by many small fares and overstate revenue impact.

So the rule stands, but honestly stated: **the two measures answer different questions and are not interchangeable.** On this data they happen to agree closely; that is not something to rely on.

---

## 5. Freshness — the honest indicator

Read `data/processed/_FRESHNESS.json` as a single-row table.

| Measure | DAX | Notes |
|---|---|---|
| `Data Age Hours` | `MAX(freshness[data_age_hours])` | Age of the newest TRIP, not of the export. |
| `Freshness Status` | `MAX(freshness[freshness_status])` | `FRESH` / `STALE` / `VERY_STALE` / `FUTURE_DATED` / `UNKNOWN` |
| `Freshness Colour` | `SWITCH([Freshness Status], "FRESH", "#1E4A2E", "STALE", "#4A3A1E", "VERY_STALE", "#5F1E1E", "FUTURE_DATED", "#5F1E1E", "#3A3A3A")` | Formatting only — thresholds live in `warehouse/export.py`. |

**Two timestamps, and the dashboard must use the second:**

- `exported_at` — when the export ran.
- `latest_trip_at` — the newest event **in the data**.

If ingestion stalls, the export still succeeds every hour. An indicator based on `exported_at` would report "fresh" while the numbers silently aged — **confidently wrong, which is worse than no indicator at all.**

> `FUTURE_DATED` exists because the first version of this logic had no branch for negative age: data timestamped 8 hours in the future fell through `age <= 2` and reported **FRESH**. Future-dated data means either producer clock skew (a real defect) or a simulated dataset extending past wall clock. Both need surfacing.

---

## 6. Report pages

| Page | Answers | Key visuals |
|---|---|---|
| **Marketplace Health** | Where is unmet demand? | Zone map (bubble = requests, colour = `Unmet Demand %`), demand-vs-supply by hour, `diagnosis` breakdown |
| **Conversion Funnel** | Where do riders drop out? | Funnel visual by `funnel_stage_reached`, cancellation reasons by `reason_category`, drop-off by stage |
| **Pricing** | Is surge working? | `Weighted Surge %` by hour, surge vs `avg_match_sec` scatter, surge by weather |
| **Financial** | Reconciled revenue | Waterfall: gross → tax → payout → commission → gateway → net; charged vs collected |
| **Pipeline Health** | Is the data trustworthy? | `fct_pipeline_quality` trends, quarantine count, DLQ-driven `unexplained_revenue` |

**Every page carries the freshness indicator.** A dashboard that cannot tell you how old its numbers are should not be trusted with a decision.

---

## 7. Anti-patterns

| Don't | Why | Instead |
|---|---|---|
| `AVERAGE(surge_multiplier)` | Averages ratios | `Weighted Surge %` |
| Bidirectional relationships | Ambiguous filter paths | Single-direction, many-to-one |
| Business rules in DAX | Untestable, undiffable, diverges from dbt | Add a column in dbt |
| Aggregating `fct_trip_events` | Audit table; contains events that failed assertions | Aggregate `fct_trips` |
| Omitting the quarantine filter | Includes trips with unknown attributes | `is_quarantined = FALSE` |
| Joining `fct_trips` to `fct_driver_sessions` directly | **Fan trap** — measures inflate | Aggregate each, then join |
| `dim_rider[current_tier_id]` for history | Drags today's tier over past trips | `fct_trips[customer_tier_id]` (point-in-time) |
| Freshness from `exported_at` | Reports fresh while data ages | `latest_trip_at` |
