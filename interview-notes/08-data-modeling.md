# 08 — Dimensional Data Modeling

## Begin with grain

Grain states exactly what one row represents. Every column and join must agree
with it. If grain is unclear, aggregations eventually double count.

RideFlow examples:

- `fct_trips`: one row per trip.
- `fct_trip_events`: one row per accepted event.
- `fct_payments`: one row per payment event.
- `fct_driver_sessions`: one row per online session.
- marketplace health: one row per zone and hour.

## Fact and dimension tables

A fact table records measurable business processes and foreign keys. A
dimension describes the who, what, where, and when used to filter facts.

A star schema places facts in the center and denormalised dimensions around
them. It is easier and faster for analytics than highly normalized 3NF because
common questions require fewer joins and consistent dimensions.

## Why cancellations belong in `fct_trips`

The trip fact represents a request lifecycle, not only revenue. Removing
cancelled requests would inflate completion rate and destroy funnel analysis.
Amounts may be null when they are not applicable; null must not be converted to
zero because “not observed/not applicable” differs from a measured zero.

## Keys

- Natural/business key: meaningful source identifier such as trip ID.
- Surrogate key: warehouse-controlled identifier for a dimension row.
- Foreign key: fact reference to a dimension.
- Unknown member `-1`: preserves the fact when a dimension mapping is missing,
  allowing unknown-rate monitoring instead of losing rows through inner joins.
- Degenerate dimension: business identifier stored directly on a fact, such as
  trip ID, when no separate descriptive dimension is useful.

## Additivity

- Additive: can sum across all relevant dimensions, such as fare.
- Semi-additive: additive across some dimensions but not time, such as an
  account balance snapshot.
- Non-additive: ratios and averages, such as completion rate.

Never average averages without their denominators. Compute completion rate as
completed trips divided by requested trips at the selected filter context.

Average surge should generally be weighted by trips, not averaged over already
aggregated zone averages.

## Fan-out and fan traps

Joining two fact tables directly can multiply rows. RideFlow once joined demand
and supply at zone only even though each table's grain was zone-hour. The fix
was to aggregate both and join on the complete `zone + hour` grain.

For facts with different grains, aggregate each to shared dimensions first,
then join the aggregates—often called drill-across.

## Date and time dimensions

RideFlow stores separate date and minute/time dimensions. Business keys use
Bengaluru local time. Bucketing UTC directly would place the morning peak near
02:00 instead of 08:00 IST and create a plausible-looking but wrong dashboard.

## SCD concepts

- Type 1 overwrites an attribute and keeps no history.
- Type 2 inserts a new dimension row with effective dates and preserves history.

Use Type 2 when analysis must reflect the attribute as it was at event time.
RideFlow avoids inventing full rider/driver SCD history when the source contract
does not supply reliable attribute-change events; some attributes are captured
on the fact instead.

## Interview answers

### “Why star schema rather than one wide table?”

> A wide table duplicates descriptive attributes, makes governance harder, and
> mixes grains. The star schema gives each process an explicit fact grain and
> reuses conformed dimensions, while still remaining BI-friendly.

### “Why is `fct_payments` separate?”

> Payment events and trips do not necessarily have identical cardinality or
> timing. A separate fact preserves payment grain and prevents forcing payment
> state into a one-row trip model. Reconciliation joins or aggregates at trip
> level when required.

### “What is a conformed dimension?”

> A dimension with consistent keys and meaning across multiple facts. Using the
> same zone, date, time, and vehicle definitions lets users compare trips,
> payments, and sessions without conflicting filters.

### “Explain your fan-out bug.”

> Demand and supply were both zone-hour datasets, but I joined on zone only.
> Every demand hour matched every supply hour and inflated rows. I corrected the
> grain to zone plus hour and added reconciliation tests. It taught me to state
> grain before writing joins.

### “Why use local date keys?”

> Business operations interpret peaks and daily totals in the city's timezone.
> Events remain stored in UTC, but dimensional keys are derived with explicit
> `Asia/Kolkata` conversion so results do not depend on machine timezone.
