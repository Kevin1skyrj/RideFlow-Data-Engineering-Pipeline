# RideFlow — Reference Data

**Static lookup tables (conformed dimensions) used across every RideFlow mart.**

| Field | Value |
|---|---|
| Version | `1.0.0` |
| Status | **Frozen** — aligned to `docs/event_contract.md` v1.0.0 |
| Milestone | M1 |
| Last updated | 2026-08-05 |

> **Authority.** Every enum value in the event contract resolves to a row in one of these tables. If a code appears in an event but not here, it is an unknown value and is handled by the §2.3 unknown-enum rule of the event contract — bucketed to `UNKNOWN`, counted as a quality signal, never silently dropped.

---

## 0. Conventions

### 0.1 Key strategy

Every table carries **two** identifiers, and the distinction matters:

| Key | Purpose | Rule |
|---|---|---|
| **Surrogate key** (`*_id`, integer) | Primary key. Joins from facts. | Platform-assigned, never reused, never reassigned. Meaningless by design — it carries no business information that could become wrong. |
| **Natural key** (`*_code`, string) | What events carry. Human-readable. | Stable, uppercase `SNAKE_CASE`. Unique among active rows. |

Events carry the **code**; facts store the **id**. Staging resolves code → id. This costs one join and buys the ability to rename a display label without rewriting history.

### 0.2 The `UNKNOWN` row

**Every table has a row with surrogate key `-1` and code `UNKNOWN`.**

This is not defensive clutter. Without it, an unrecognised code forces a choice between dropping the fact row (data loss) and a null foreign key (breaks inner joins and silently excludes rows from every aggregate). The `-1` row makes unknown values *visible and countable* — the row survives, joins succeed, and a rising `UNKNOWN` count is an alertable quality signal.

### 0.3 Slowly Changing Dimensions

| Type | Applied to | Behaviour |
|---|---|---|
| **SCD Type 1** (overwrite) | Display labels, descriptions | History not preserved. A typo fix should not create a version. |
| **SCD Type 2** (versioned) | Attributes that affect metrics — `commission_pct`, `base_fare_multiplier`, `discount_pct` | New row with `valid_from` / `valid_to` / `is_current`. History preserved. |

**The rule:** if changing an attribute would change a historical metric, it is Type 2. A commission rate rising from 20% to 22% must not retroactively alter last quarter's reported net revenue.

### 0.4 Retirement, never deletion

Rows are **never deleted.** Retirement sets `is_active = false`. A deleted city breaks every historical fact that references it; a retired city keeps history intact while disappearing from operational pickers.

### 0.5 Common audit columns

Every table carries these; they are not repeated in each section:

| Column | Type | Nullable | Purpose |
|---|---|---|---|
| `is_active` | boolean | No | Available for new operational use |
| `valid_from` | timestamp UTC | No | SCD2 version start |
| `valid_to` | timestamp UTC | Yes | SCD2 version end; null = current |
| `is_current` | boolean | No | SCD2 current-version flag |
| `created_at` | timestamp UTC | No | Row insertion |
| `updated_at` | timestamp UTC | No | Last modification |

Type 1 tables carry these columns with a single permanently-current version, so the schema is uniform across all dimensions and a Type 1 table can be promoted to Type 2 without a structural migration.

---

## 1. `dim_city`

**Grain:** One row per operating city per version (SCD Type 2).

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `city_id` | integer | No | **PK.** Surrogate key. |
| `city_code` | varchar(8) | No | Natural key, IATA-style. |
| `city_name` | varchar(80) | No | Display name. |
| `country_code` | char(2) | No | ISO 3166-1 alpha-2. |
| `region` | varchar(40) | No | Operational grouping for regional roll-ups. |
| `timezone` | varchar(40) | No | IANA timezone. **Load-bearing** — see below. |
| `currency_code` | char(3) | No | ISO 4217. Every fare in this city is denominated here. |
| `center_lat` | decimal(9,6) | No | Geographic centroid. |
| `center_lon` | decimal(9,6) | No | Geographic centroid. |
| `bounding_box_wkt` | varchar(500) | No | Service area polygon. Validates that coordinates fall in-city. |
| `commission_pct` | decimal(5,2) | No | Platform commission. **SCD2-tracked.** |
| `base_fare` | decimal(10,2) | No | City base fare. **SCD2-tracked.** |
| `per_km_rate` | decimal(10,2) | No | Per-km rate. **SCD2-tracked.** |
| `per_min_rate` | decimal(10,2) | No | Per-minute rate. **SCD2-tracked.** |
| `airport_fee` | decimal(10,2) | No | Regulated airport levy. **SCD2-tracked.** |
| `tax_pct` | decimal(5,2) | No | Applicable tax rate. **SCD2-tracked.** |
| `max_surge_multiplier` | decimal(4,2) | No | Regulatory surge cap. Varies by jurisdiction. |
| `launch_date` | date | No | Operations start. |
| `is_active` | boolean | No | Currently operating. |

### Primary Key

`city_id` — surrogate. Business uniqueness on `(city_code, is_current)`.

### Allowed Values

| `city_id` | `city_code` | `city_name` | `country_code` | `timezone` | `currency_code` | Active |
|---|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | Unknown City | `XX` | `UTC` | `XXX` | No |
| `1` | `BLR` | Bengaluru | `IN` | `Asia/Kolkata` | `INR` | Yes |
| `2` | `DEL` | Delhi NCR | `IN` | `Asia/Kolkata` | `INR` | Yes |
| `3` | `BOM` | Mumbai | `IN` | `Asia/Kolkata` | `INR` | Yes |
| `4` | `HYD` | Hyderabad | `IN` | `Asia/Kolkata` | `INR` | Yes |
| `5` | `MAA` | Chennai | `IN` | `Asia/Kolkata` | `INR` | Yes |
| `6` | `PNQ` | Pune | `IN` | `Asia/Kolkata` | `INR` | Yes |

### Business Purpose

The top-level segmentation of the entire platform. Cities have independent pricing, independent regulation, independent unit economics, and independent supply pools. Almost no metric is meaningful when aggregated across cities without weighting.

**`timezone` is the most consequential column in this table.** All events are stored in UTC (event contract §3.1). "Morning peak" is a *local* concept: 08:00 IST is 02:30 UTC. Without joining to `timezone`, every hour-of-day analysis is wrong, and it is wrong in a way that looks plausible — peaks appear at odd hours and are rationalised rather than investigated. `dim_date` and `dim_time` therefore derive local time by joining here.

`bounding_box_wkt` supports the validation rule that a pickup coordinate must fall inside its city. A coordinate outside it is a GPS fault or a mis-assigned `city_id` — either way, a defect.

### Future Extensibility

- `parent_market_id` for multi-city markets (Delhi NCR is really several municipalities).
- `regulatory_regime_code` as regulation diverges by jurisdiction.
- `is_surge_permitted` — some jurisdictions ban surge pricing outright.
- Migrate `bounding_box_wkt` to a native geometry type when DuckDB's spatial extension is adopted.
- Split pricing into a dedicated `dim_pricing_config` if rates begin changing more often than city attributes — currently they do not, so the split would be premature.

---

## 2. `dim_vehicle_type`

**Grain:** One row per service tier per version (SCD Type 2 on pricing).

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `vehicle_type_id` | integer | No | **PK.** Surrogate key. |
| `vehicle_type_code` | varchar(20) | No | Natural key. Carried in events. |
| `display_name` | varchar(50) | No | Rider-facing name. |
| `description` | varchar(200) | No | Tier explanation. |
| `seat_capacity` | smallint | No | Maximum passengers. |
| `base_fare_multiplier` | decimal(4,2) | No | Multiplier on city base fare. **SCD2-tracked.** |
| `per_km_multiplier` | decimal(4,2) | No | Multiplier on city per-km rate. **SCD2-tracked.** |
| `is_shared` | boolean | No | Whether the vehicle is shared between bookings. |
| `is_premium` | boolean | No | Premium tier flag. |
| `requires_commercial_license` | boolean | No | Regulatory driver requirement. |
| `min_driver_rating` | decimal(3,2) | No | Rating floor to serve this tier. |
| `sort_order` | smallint | No | Display ordering. |
| `is_active` | boolean | No | Offered to riders. |

### Primary Key

`vehicle_type_id`. Business uniqueness on `(vehicle_type_code, is_current)`.

### Allowed Values

| `vehicle_type_id` | `vehicle_type_code` | `display_name` | Seats | `is_shared` | `is_premium` | Active |
|---|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | Unknown | `0` | No | No | No |
| `1` | `ECONOMY` | RideFlow Go | `4` | No | No | Yes |
| `2` | `PREMIUM` | RideFlow Premier | `4` | No | Yes | Yes |
| `3` | `XL` | RideFlow XL | `6` | No | No | Yes |
| `4` | `POOL` | RideFlow Pool | `4` | **Yes** | No | Yes |
| `5` | `AUTO` | RideFlow Auto | `3` | No | No | Yes |
| `6` | `BIKE` | RideFlow Bike | `1` | No | No | Yes |

### Business Purpose

Determines pricing, driver eligibility, and vehicle requirements. Tier mix is a primary revenue lever — a shift from `PREMIUM` to `ECONOMY` reduces revenue per trip even when trip volume is flat, and without this dimension that decline is invisible in headline numbers.

**`is_shared` carries a modelling consequence disproportionate to its size.** `POOL` breaks the one-trip-one-vehicle-occupancy assumption: two riders share a vehicle simultaneously, so driver utilisation, per-trip distance, and revenue attribution all behave differently. The v1 model treats each pool booking as an independent trip, which is a **known simplification** — it will over-count vehicle-hours for pooled trips. Correct pool modelling requires a separate `fct_pool_segments` grain and is deferred (`PROJECT_PLAN.md` §11).

`min_driver_rating` encodes why a high-surge premium request can go unmatched while economy drivers sit idle nearby — the eligible supply pool is narrower than the visible one.

### Future Extensibility

- `max_luggage_capacity`, `is_wheelchair_accessible`, `is_pet_friendly` for accessibility filtering.
- `is_electric` / `emission_class` for sustainability reporting, an increasingly common regulatory requirement.
- `vehicle_type_group` to roll tiers into families.
- Per-city availability (`bridge_city_vehicle_type`) — not every tier operates in every city, and the current model cannot express that.

---

## 3. `dim_ride_status`

**Grain:** One row per lifecycle status. SCD Type 1 — statuses are structural.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `ride_status_id` | integer | No | **PK.** Surrogate key. |
| `ride_status_code` | varchar(30) | No | Natural key. |
| `display_name` | varchar(50) | No | Human-readable label. |
| `description` | varchar(200) | No | What this status means. |
| `is_terminal` | boolean | No | No further transition possible. |
| `is_revenue_generating` | boolean | No | Whether fare revenue is recognised. |
| `is_successful` | boolean | No | Whether this counts as a completed marketplace transaction. |
| `funnel_stage` | smallint | No | Position in the conversion funnel, 1 = earliest. |
| `source_event_type` | varchar(30) | Yes | The event producing this status. Null when derived rather than emitted. |
| `sort_order` | smallint | No | Display ordering. |

### Primary Key

`ride_status_id`. Business uniqueness on `ride_status_code`.

### Allowed Values

| `id` | `ride_status_code` | Terminal | Revenue | Successful | Funnel | Source event |
|---|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | No | No | No | `0` | — |
| `1` | `REQUESTED` | No | No | No | `1` | `RideRequested` |
| `2` | `MATCHED` | No | No | No | `2` | `RideAccepted` |
| `3` | `DRIVER_ARRIVED` | No | No | No | `3` | `DriverArrived` |
| `4` | `STARTED` | No | No | No | `4` | `RideStarted` |
| `5` | `COMPLETED` | No | **Yes** | **Yes** | `5` | `RideCompleted` |
| `6` | `PAID` | **Yes** | **Yes** | **Yes** | `6` | `PaymentCompleted` |
| `7` | `CANCELLED_RIDER` | **Yes** | No | No | `99` | `RideCancelled` |
| `8` | `CANCELLED_DRIVER` | **Yes** | No | No | `99` | `RideCancelled` |
| `9` | `CANCELLED_SYSTEM` | **Yes** | No | No | `99` | `RideCancelled` |
| `10` | `EXPIRED` | **Yes** | No | No | `99` | **null — derived** |

### Business Purpose

The spine of the conversion funnel. `funnel_stage` allows drop-off between consecutive stages to be computed with a single ordering column rather than hard-coded status lists scattered across queries.

`is_revenue_generating` prevents the most common revenue-reporting error: counting cancellation fees as trip revenue. A cancelled trip may produce a fee, but it is not a completed transaction, and conflating the two overstates both trip count and average fare.

**`EXPIRED` has a null `source_event_type`, and that null is deliberate.** No event produces it. It is inferred from a `RideRequested` with no successor past a timeout — which makes it indistinguishable from a trip whose later events were lost. This is Gap 1 in the event contract (§0.1). Every mart exposing `EXPIRED` must label it as derived, because reporting an inference with the same confidence as an observation is how a pipeline misleads people who trust it.

Statuses 7–9 all share `funnel_stage = 99` because cancellation is an exit from the funnel, not a position within it. The stage a trip *reached* before cancelling is carried separately, on `RideCancelled.cancelled_at_status`.

### Future Extensibility

- `RIDER_NO_SHOW` as a status distinct from a cancellation.
- `IN_DISPUTE`, `REFUNDED`, `CHARGED_BACK` for post-trip financial states.
- `SCHEDULED` for advance bookings, which would insert a stage before `REQUESTED`.
- Retire `EXPIRED`'s derived status once `RideExpired` ships in contract v1.1, backfilling `source_event_type`.

---

## 4. `dim_cancellation_reason`

**Grain:** One row per cancellation reason. SCD Type 1.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `cancellation_reason_id` | integer | No | **PK.** Surrogate key. |
| `cancellation_reason_code` | varchar(40) | No | Natural key. Carried in events. |
| `display_name` | varchar(80) | No | Human-readable label. |
| `description` | varchar(250) | No | Full explanation. |
| `cancelled_by_party` | varchar(10) | No | `RIDER`, `DRIVER`, `SYSTEM`. |
| `reason_category` | varchar(30) | No | Grouping: `SUPPLY`, `DEMAND`, `OPERATIONAL`, `TECHNICAL`, `SAFETY`. |
| `is_fee_applicable` | boolean | No | Whether a fee may be charged. |
| `is_driver_fault` | boolean | No | Counts against driver quality score. |
| `is_rider_fault` | boolean | No | Counts against rider reliability score. |
| `is_platform_fault` | boolean | No | Indicates a platform failure — the actionable engineering signal. |
| `affects_acceptance_rate` | boolean | No | Whether it reduces the driver's acceptance rate. |
| `sort_order` | smallint | No | Display ordering. |

### Primary Key

`cancellation_reason_id`. Business uniqueness on `cancellation_reason_code`.

### Allowed Values

| `id` | `cancellation_reason_code` | Party | Category | Fee | Driver fault | Rider fault | Platform fault |
|---|---|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | `SYSTEM` | `OPERATIONAL` | No | No | No | No |
| `1` | `RIDER_CHANGED_MIND` | `RIDER` | `DEMAND` | **Yes** | No | **Yes** | No |
| `2` | `DRIVER_ETA_TOO_LONG` | `RIDER` | `SUPPLY` | **Yes** | No | No | No |
| `3` | `RIDER_FOUND_ALTERNATIVE` | `RIDER` | `DEMAND` | **Yes** | No | **Yes** | No |
| `4` | `WRONG_PICKUP_LOCATION` | `RIDER` | `OPERATIONAL` | No | No | **Yes** | No |
| `5` | `RIDER_UNRESPONSIVE` | `DRIVER` | `DEMAND` | **Yes** | No | **Yes** | No |
| `6` | `PRICE_TOO_HIGH` | `RIDER` | `DEMAND` | No | No | No | No |
| `7` | `DRIVER_VEHICLE_ISSUE` | `DRIVER` | `SUPPLY` | No | **Yes** | No | No |
| `8` | `DRIVER_TOO_FAR` | `DRIVER` | `SUPPLY` | No | **Yes** | No | No |
| `9` | `DRIVER_EMERGENCY` | `DRIVER` | `SAFETY` | No | No | No | No |
| `10` | `DRIVER_DECLINED_DESTINATION` | `DRIVER` | `SUPPLY` | No | **Yes** | No | No |
| `11` | `NO_DRIVER_AVAILABLE` | `SYSTEM` | `SUPPLY` | No | No | No | **Yes** |
| `12` | `PAYMENT_METHOD_INVALID` | `SYSTEM` | `TECHNICAL` | No | No | **Yes** | No |
| `13` | `SAFETY_CONCERN` | `SYSTEM` | `SAFETY` | No | No | No | No |
| `14` | `DUPLICATE_REQUEST` | `SYSTEM` | `TECHNICAL` | No | No | No | **Yes** |
| `15` | `SYSTEM_ERROR` | `SYSTEM` | `TECHNICAL` | No | No | No | **Yes** |

### Business Purpose

Cancellation reasons are the highest-value diagnostic signal in the marketplace, because each category implies a different fix and they are routinely confused at the headline level.

| Category | What it means | Who acts |
|---|---|---|
| `SUPPLY` | Not enough drivers, or wrong drivers | Supply/incentives team |
| `DEMAND` | Rider changed their mind | Product/pricing team |
| `TECHNICAL` | The platform failed | Engineering |
| `SAFETY` | Escalation required | Trust & safety |
| `OPERATIONAL` | Process or data issue | Operations |

A cancellation rate reported as one number is close to useless. `DRIVER_ETA_TOO_LONG` rising means the supply pool is too thin; `PRICE_TOO_HIGH` rising means surge is over-aggressive. **These have opposite remedies** — the first calls for more surge to attract supply, the second for less. A single blended metric would point in no direction at all.

The three fault columns are separate rather than one `fault_party` column deliberately: faults are not mutually exclusive. `PAYMENT_METHOD_INVALID` is a rider-attributable fault that may also indicate a platform validation gap, and a single column would force an arbitrary choice that loses information.

`is_fee_applicable` states whether a fee *may* be charged; the event's `is_fee_charged` records whether one *was*. The gap between them is the waiver rate — a real and separately monitorable business metric.

### Future Extensibility

- `fee_amount_override` per reason, and per city as regulation diverges.
- `requires_manual_review` for safety escalation routing.
- `sla_response_minutes` for reasons requiring an operational response.
- Sub-reasons (`cancellation_reason_detail_id`), if the current 15 prove too coarse.

---

## 5. `dim_payment_method`

**Grain:** One row per payment method. SCD Type 1.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `payment_method_id` | integer | No | **PK.** Surrogate key. |
| `payment_method_code` | varchar(20) | No | Natural key. Carried in events. |
| `display_name` | varchar(50) | No | Rider-facing label. |
| `method_category` | varchar(20) | No | `CASH`, `CARD`, `DIGITAL`, `ACCOUNT`. |
| `is_cash` | boolean | No | Settled in physical currency. |
| `is_prepaid` | boolean | No | Funds captured before the trip. |
| `requires_gateway` | boolean | No | Involves an external processor. |
| `settlement_delay_hours` | smallint | No | Hours until funds reach the platform. |
| `gateway_fee_pct` | decimal(5,2) | No | Processor fee. **Affects net revenue.** |
| `supports_tip` | boolean | No | Whether in-app tipping is possible. |
| `supports_refund` | boolean | No | Whether automated refund is possible. |
| `failure_rate_baseline_pct` | decimal(5,2) | No | Expected failure rate — the alerting baseline. |
| `is_active` | boolean | No | Offered to riders. |

### Primary Key

`payment_method_id`. Business uniqueness on `payment_method_code`.

### Allowed Values

| `id` | `payment_method_code` | `display_name` | Category | Cash | Gateway | Settlement hrs | Gateway fee % |
|---|---|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | Unknown | `UNKNOWN` | No | No | `0` | `0.00` |
| `1` | `CASH` | Cash | `CASH` | **Yes** | No | `0` | `0.00` |
| `2` | `CARD` | Credit / Debit Card | `CARD` | No | **Yes** | `48` | `2.00` |
| `3` | `UPI` | UPI | `DIGITAL` | No | **Yes** | `24` | `0.30` |
| `4` | `WALLET` | RideFlow Wallet | `DIGITAL` | No | No | `0` | `0.00` |
| `5` | `CORPORATE` | Corporate Account | `ACCOUNT` | No | No | `720` | `0.00` |
| `6` | `NETBANKING` | Net Banking | `DIGITAL` | No | **Yes** | `48` | `1.20` |

### Business Purpose

Payment method drives cash flow, cost, and risk — three things that are invisible in trip counts.

**`settlement_delay_hours` is the working-capital column.** `CASH` settles instantly but the driver holds the money, so the platform is owed its commission. `CORPORATE` settles at 30 days, meaning revenue is recognised long before cash arrives. A business reading only revenue and not settlement timing can be profitable and insolvent simultaneously.

**`gateway_fee_pct` is real margin leakage.** At scale, the 1.7-point spread between `CARD` (2.00%) and `UPI` (0.30%) is a material profitability lever, and it never appears in fare data. Payment-mix shift is a genuine driver of net revenue that trip-level analysis cannot see.

`failure_rate_baseline_pct` exists so that payment failure alerting is *relative*. Card failures at 3% are normal; UPI failures at 3% are an incident. A single absolute threshold would either miss the incident or fire constantly.

`is_cash` matters for reconciliation: cash trips never produce a gateway reference, so §7.6-N3 reconciliation must exclude them or it will fail on correct data — a common cause of false alarms in finance pipelines.

### Future Extensibility

- Per-city availability — UPI is India-specific and does not generalise.
- `min_amount` / `max_amount` limits per method.
- Card network sub-types (Visa / Mastercard / Amex) with distinct fee structures.
- `is_split_payment_supported` for fare splitting.
- BNPL and crypto methods, if ever offered.

---

## 6. `dim_payment_status`

**Grain:** One row per payment state. SCD Type 1.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `payment_status_id` | integer | No | **PK.** Surrogate key. |
| `payment_status_code` | varchar(20) | No | Natural key. |
| `display_name` | varchar(50) | No | Human-readable label. |
| `description` | varchar(200) | No | State explanation. |
| `is_terminal` | boolean | No | No further transition possible. |
| `is_successful` | boolean | No | Funds successfully collected. |
| `is_revenue_recognised` | boolean | No | Whether revenue may be booked. |
| `requires_action` | boolean | No | Needs operational intervention. |
| `sort_order` | smallint | No | Display ordering. |

### Primary Key

`payment_status_id`. Business uniqueness on `payment_status_code`.

### Allowed Values

| `id` | `payment_status_code` | Terminal | Successful | Revenue recognised | Requires action |
|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | No | No | No | **Yes** |
| `1` | `PENDING` | No | No | No | No |
| `2` | `AUTHORIZED` | No | No | No | No |
| `3` | `SUCCEEDED` | **Yes** | **Yes** | **Yes** | No |
| `4` | `FAILED` | **Yes** | No | No | **Yes** |
| `5` | `REFUNDED` | **Yes** | No | No | No |
| `6` | `PARTIALLY_REFUNDED` | **Yes** | No | **Yes** | No |
| `7` | `CHARGEBACK` | **Yes** | No | No | **Yes** |
| `8` | `DISPUTED` | No | No | **Yes** | **Yes** |

### Business Purpose

Separates *fare charged* from *money collected* — routinely conflated, and the conflation is expensive. A completed trip with a failed payment produces revenue in the trip mart and nothing in the bank.

**This table is deliberately broader than v1.0.0 of the event contract uses.** `PaymentCompleted` constrains `payment_status` to `SUCCEEDED` (event contract §5.7), so in v1.0.0 only rows `3` and `-1` will ever appear in fact data. The remaining rows exist because:

1. The dimension must be complete before `PaymentFailed` ships in contract v1.1 — adding statuses later means backfilling.
2. Refunds and chargebacks are already known future events.
3. A dimension defined only by what today's events emit will need restructuring the moment a new event type ships.

**This is a deliberate, documented gap between the dimension and current fact coverage, not an oversight.** Any dashboard filtering on `payment_status` in v1.0.0 will correctly find only `SUCCEEDED`, and that should not be mistaken for a 100% payment success rate — failures are simply not observable yet (event contract §0.1 Gap 2).

`is_revenue_recognised` is `true` for `PARTIALLY_REFUNDED` and `DISPUTED` because accounting recognises revenue until a dispute is resolved against you. It is `false` for `AUTHORIZED` because authorisation is a hold, not a collection — treating it as revenue would overstate income and is a well-known accounting error.

### Future Extensibility

- `RETRY_SCHEDULED` for automated retry flows.
- `SETTLEMENT_PENDING` distinguishing collection from settlement.
- Sub-statuses for failure reasons (currently on the payment event, not the dimension).
- `WRITTEN_OFF` for terminally uncollectable amounts.

---

## 7. `dim_driver_status`

**Grain:** One row per driver operational state. SCD Type 1.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `driver_status_id` | integer | No | **PK.** Surrogate key. |
| `driver_status_code` | varchar(20) | No | Natural key. |
| `display_name` | varchar(50) | No | Human-readable label. |
| `description` | varchar(200) | No | State explanation. |
| `is_available_for_dispatch` | boolean | No | Eligible to receive offers. |
| `is_online` | boolean | No | Counts toward online supply hours. |
| `counts_toward_utilisation` | boolean | No | Included in the utilisation denominator. |
| `is_earning` | boolean | No | Actively earning on a trip. |
| `sort_order` | smallint | No | Display ordering. |

### Primary Key

`driver_status_id`. Business uniqueness on `driver_status_code`.

### Allowed Values

| `id` | `driver_status_code` | Available | Online | In utilisation | Earning |
|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | No | No | No | No |
| `1` | `AVAILABLE` | **Yes** | **Yes** | **Yes** | No |
| `2` | `EN_ROUTE` | No | **Yes** | **Yes** | No |
| `3` | `ON_TRIP` | No | **Yes** | **Yes** | **Yes** |
| `4` | `BREAK` | No | **Yes** | No | No |
| `5` | `OFFLINE` | No | No | No | No |
| `6` | `SUSPENDED` | No | No | No | No |

### Business Purpose

Supply-side accounting. The three boolean columns look similar and are not, and the distinctions decide whether utilisation is measured correctly.

**`is_online` vs `counts_toward_utilisation`.** `BREAK` is online but excluded from utilisation. Including break time in the denominator would understate utilisation and make an efficient supply pool look idle.

**`is_available_for_dispatch` vs `is_online`.** `EN_ROUTE` and `ON_TRIP` are online but unavailable. This is the difference between supply that exists and supply that can be dispatched — the distinction at the heart of marketplace health. A zone can be full of online drivers and still have unmet demand if every one of them is mid-trip.

**`is_earning` isolates paid time.** `EN_ROUTE` is unpaid deadhead. The ratio of `ON_TRIP` to `EN_ROUTE` time is driver earning efficiency, the metric that most directly predicts driver churn.

Note that `DriverOnline` (event contract §5.8) captures status only at session start, so intra-session transitions are **not** currently observable. Utilisation in v1.0.0 must therefore be derived from trip overlap against session windows rather than from status transitions. This is a real limitation and is recorded below.

### Future Extensibility

- A `DriverStatusChanged` event enabling true intra-session status tracking — the direct fix for the limitation above.
- `PENDING_VERIFICATION` and `DOCUMENT_EXPIRED` for compliance states.
- `is_accepting_pool_only` for tier-restricted availability.
- Reason codes for `SUSPENDED`.

---

## 8. `dim_customer_tier`

**Grain:** One row per loyalty tier per version (SCD Type 2 on benefits).

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `customer_tier_id` | integer | No | **PK.** Surrogate key. |
| `customer_tier_code` | varchar(20) | No | Natural key. Carried in events. |
| `display_name` | varchar(50) | No | Rider-facing label. |
| `min_lifetime_trips` | integer | No | Trips required to qualify. |
| `min_lifetime_spend` | decimal(12,2) | No | Spend required to qualify. |
| `discount_pct` | decimal(5,2) | No | Standing discount. **SCD2-tracked.** |
| `has_priority_matching` | boolean | No | Preferential dispatch. |
| `max_free_cancellations_monthly` | smallint | No | Waived cancellation fees per month. |
| `has_premium_support` | boolean | No | Priority support queue. |
| `tier_rank` | smallint | No | Ordinal rank, 1 = lowest. |
| `is_active` | boolean | No | Currently offered. |

### Primary Key

`customer_tier_id`. Business uniqueness on `(customer_tier_code, is_current)`.

### Allowed Values

| `id` | `customer_tier_code` | `display_name` | Min trips | Discount % | Priority match | Free cancels | Rank |
|---|---|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | Unknown | `0` | `0.00` | No | `0` | `0` |
| `1` | `BASIC` | Basic | `0` | `0.00` | No | `1` | `1` |
| `2` | `SILVER` | Silver | `25` | `2.50` | No | `2` | `2` |
| `3` | `GOLD` | Gold | `100` | `5.00` | **Yes** | `4` | `3` |
| `4` | `PLATINUM` | Platinum | `300` | `8.00` | **Yes** | `8` | `4` |

### Business Purpose

Segments riders by value and quantifies the cost of retention. `discount_pct` is a direct, measurable margin cost, and the analytical question the tier programme must answer is whether the retention it buys exceeds the margin it spends.

**`has_priority_matching` is a supply allocation decision with a measurable cost, and it is easy to miss.** Preferring a `PLATINUM` rider means some `BASIC` rider waits longer. That trade-off only becomes visible when matching time is segmented by tier — otherwise the programme appears free.

**Tier is captured on the event as a point-in-time snapshot** (`RideRequested.customer_tier`), not joined from a current rider dimension. A rider who was `SILVER` in March and `GOLD` in August must show as `SILVER` on their March trips. Joining to current tier would retroactively rewrite history and inflate the apparent performance of higher tiers — a subtle and very common analytical error.

`max_free_cancellations_monthly` explains why `is_fee_applicable` (§4) can be true while `is_fee_charged` is false: the rider had a waiver remaining. Without this column that gap looks like a billing defect.

### Future Extensibility

- Tier-specific surge caps.
- `points_multiplier` for a points-based programme.
- Time-boxed tiers with expiry, requiring a rider-tier bridge table with validity ranges.
- Corporate tiers for B2B accounts.
- `min_lifetime_spend` currency handling for multi-currency operation — the current single-currency assumption will not survive international expansion.

---

## 9. `dim_weather`

**Grain:** One row per weather condition. SCD Type 1.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `weather_id` | integer | No | **PK.** Surrogate key. |
| `weather_code` | varchar(20) | No | Natural key. Carried in events. |
| `display_name` | varchar(50) | No | Human-readable label. |
| `description` | varchar(200) | No | Condition explanation. |
| `severity_level` | smallint | No | 0 = benign … 4 = extreme. |
| `demand_impact_factor` | decimal(4,2) | No | Expected demand multiplier. |
| `supply_impact_factor` | decimal(4,2) | No | Expected supply multiplier. |
| `avg_speed_reduction_pct` | decimal(5,2) | No | Expected speed reduction. |
| `is_adverse` | boolean | No | Adverse-condition flag. |
| `triggers_safety_protocol` | boolean | No | Whether operational safety measures apply. |

### Primary Key

`weather_id`. Business uniqueness on `weather_code`.

### Allowed Values

| `id` | `weather_code` | `display_name` | Severity | Demand × | Supply × | Speed −% | Adverse |
|---|---|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | Unknown | `0` | `1.00` | `1.00` | `0.00` | No |
| `1` | `CLEAR` | Clear | `0` | `1.00` | `1.00` | `0.00` | No |
| `2` | `CLOUDY` | Cloudy | `0` | `1.02` | `1.00` | `0.00` | No |
| `3` | `RAIN_LIGHT` | Light Rain | `1` | `1.25` | `0.92` | `8.00` | **Yes** |
| `4` | `RAIN_HEAVY` | Heavy Rain | `2` | `1.60` | `0.75` | `22.00` | **Yes** |
| `5` | `THUNDERSTORM` | Thunderstorm | `3` | `1.45` | `0.55` | `30.00` | **Yes** |
| `6` | `FOG` | Fog | `2` | `1.10` | `0.85` | `25.00` | **Yes** |
| `7` | `EXTREME_HEAT` | Extreme Heat | `2` | `1.30` | `0.88` | `3.00` | **Yes** |

### Business Purpose

Weather is the single largest *exogenous* driver of marketplace imbalance, and the most valuable thing this table encodes is that **it moves demand and supply in opposite directions simultaneously.**

Heavy rain raises demand 60% while reducing supply 25%. The resulting imbalance is roughly 2.1× — far more severe than either figure alone suggests. Without `supply_impact_factor` as a separate column, a rain-driven supply collapse looks like a dispatch algorithm failure, and the engineering team investigates a bug that does not exist.

These factors serve two distinct purposes:

1. **Generator input (M2):** producing realistic correlated demand and supply patterns rather than independent noise.
2. **Analytical baseline (M6+):** separating weather-explained variance from genuine operational anomalies. A 40% demand spike during a thunderstorm is expected; the same spike on a clear day is worth investigating.

`THUNDERSTORM` shows *lower* demand impact than `RAIN_HEAVY` (1.45 vs 1.60) — deliberately, because severe storms suppress trip-taking entirely rather than shifting it to ride-hailing. The relationship between severity and demand is not monotonic, and a model assuming it is would mispredict exactly the conditions that matter most.

**Note on data lineage:** `weather_code` is captured on the event at request time, not joined from an external weather service. This makes it a point-in-time observation and keeps the pipeline self-contained, at the cost of granularity — one code per trip, not per minute per zone.

### Future Extensibility

- Continuous measurements (`temperature_c`, `precipitation_mm`, `wind_speed_kmh`) alongside the categorical code.
- Per-city impact factors — monsoon rain in Mumbai is not light rain in Bengaluru, and shared factors are a real oversimplification.
- Time-of-day interaction: rain at rush hour is materially worse than rain at midnight.
- A separate `fct_weather_observations` table at zone-hour grain, joined rather than denormalised.

---

## 10. `dim_traffic_level`

**Grain:** One row per traffic condition. SCD Type 1.

### Columns

| Column | Type | Nullable | Description |
|---|---|---|---|
| `traffic_level_id` | integer | No | **PK.** Surrogate key. |
| `traffic_level_code` | varchar(20) | No | Natural key. Carried in events. |
| `display_name` | varchar(50) | No | Human-readable label. |
| `description` | varchar(200) | No | Condition explanation. |
| `severity_rank` | smallint | No | 1 = free-flowing … 5 = gridlock. |
| `speed_factor` | decimal(4,2) | No | Fraction of free-flow speed achieved. |
| `eta_multiplier` | decimal(4,2) | No | ETA adjustment. |
| `avg_speed_kmh` | decimal(5,2) | No | Representative average speed. |
| `is_congested` | boolean | No | Congestion flag. |

### Primary Key

`traffic_level_id`. Business uniqueness on `traffic_level_code`.

### Allowed Values

| `id` | `traffic_level_code` | `display_name` | Severity | Speed factor | ETA × | Avg km/h | Congested |
|---|---|---|---|---|---|---|---|
| `-1` | `UNKNOWN` | Unknown | `0` | `1.00` | `1.00` | `0.00` | No |
| `1` | `FREE_FLOW` | Free Flowing | `1` | `1.00` | `1.00` | `45.00` | No |
| `2` | `LIGHT` | Light Traffic | `2` | `0.85` | `1.15` | `38.00` | No |
| `3` | `MODERATE` | Moderate Traffic | `3` | `0.65` | `1.50` | `29.00` | **Yes** |
| `4` | `HEAVY` | Heavy Traffic | `4` | `0.45` | `2.10` | `20.00` | **Yes** |
| `5` | `GRIDLOCK` | Gridlock | `5` | `0.25` | `3.60` | `11.00` | **Yes** |

### Business Purpose

Traffic is the primary explanation of variance between estimated and actual trip duration, and it has a **direct and frequently misread financial consequence.**

RideFlow's fare model charges for both distance and time (event contract §5.5). In heavy traffic the same route takes longer, so `time_fare` rises while `distance_fare` is unchanged. Revenue per trip goes up — but driver earnings *per hour* go down, because the driver completes fewer trips. Traffic simultaneously increases revenue per trip and decreases marketplace throughput.

Analysed without this dimension, a congested week looks like healthy revenue growth while driver satisfaction quietly deteriorates. This is one of the clearest cases where a metric moving in the "right" direction is a warning sign.

`eta_multiplier` and `speed_factor` are related but not reciprocal (`HEAVY`: 0.45 speed factor, 2.10 ETA multiplier, where the reciprocal would be 2.22). The gap absorbs stop-start acceleration losses and junction delays that a pure speed ratio does not capture. Keeping both explicit is more honest than deriving one from the other and pretending the relationship is clean.

Like `weather_code`, traffic is captured on the event as a point-in-time observation. `RideRequested` and `RideCompleted` each carry it, so a trip that begins in `MODERATE` and ends in `GRIDLOCK` is detectable — a genuinely useful signal for ETA model evaluation.

### Future Extensibility

- Continuous congestion index rather than five buckets — the current granularity is coarse.
- Per-zone and per-corridor traffic rather than per-trip.
- Time-series traffic sampled during the trip, not only at its endpoints.
- Incident flags (accident, road closure, event-driven congestion).
- Per-city `avg_speed_kmh` calibration — 45 km/h free-flow is not realistic for every city.

---

## 11. Cross-Table Notes

### 11.1 Which tables the events reference

| Event | References |
|---|---|
| `RideRequested` | city, vehicle type, payment method, customer tier, weather, traffic |
| `RideAccepted` | city, vehicle type |
| `DriverArrived` | city |
| `RideStarted` | city |
| `RideCompleted` | city, traffic, weather, payment method |
| `RideCancelled` | city, cancellation reason, ride status |
| `PaymentCompleted` | city, payment method, payment status |
| `DriverOnline` | city, vehicle type, driver status |
| `DriverOffline` | city |

### 11.2 Reference tables with no direct event

`dim_ride_status` is referenced by `RideCancelled.cancelled_at_status`, but a trip's *current* status is derived from the event sequence (event contract §6.2) rather than emitted. `dim_payment_status` is only partially exercised in v1.0.0 (§6).

### 11.3 Two tables not defined here

`dim_zone` and `dim_date`/`dim_time` are referenced throughout but are **not reference data in the same sense**:

- **`dim_zone`** is city-specific, large (hundreds of rows per city), and geographic. It is a generated dimension with its own boundary definitions, deferred to M2 alongside the generator that needs it.
- **`dim_date` / `dim_time`** are algorithmically generated calendar dimensions, not curated lookups. They must derive local time via `dim_city.timezone` (§1) — the point at which the timezone column stops being metadata and starts being load-bearing.

### 11.4 Seeding

All tables in this document are loaded as **dbt seeds** — CSV files version-controlled in `transformation/seeds/`. This is the correct mechanism because the data is small, slow-changing, and human-curated. A change to a seed goes through code review like any other change, which is exactly the control that reference data warrants.

**Seed files are the implementation of this document, not a second source of truth.** Where a seed and this document disagree, this document is correct and the seed is a defect. CI validates the seeds against the allowed-values tables above.
