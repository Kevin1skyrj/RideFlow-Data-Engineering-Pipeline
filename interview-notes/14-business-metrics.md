# 14 — RideFlow Business Metrics

## Conversion funnel

The denominator begins with all clean ride requests. Later stages count trips
that reached matched, arrived, started, completed, and paid. Cancellations stay
in the fact because removing them would inflate conversion.

Two percentages answer different questions:

- overall conversion: stage / all requests;
- step conversion: current stage / previous stage.

## Financial truth

Gross bookings sum clean trip fare. Platform commission and driver payout must
reconcile with the defined fare components. Money uses decimal types rather
than binary floating-point to avoid rounding drift.

Quarantined trips are excluded from business totals but remain auditable.

## Marketplace health

Supply and demand are compared at `zone + hour` grain. Demand alone does not
show shortage; supply alone does not show unmet need. Joining at zone only
causes hour-to-hour fan-out.

## Pricing effectiveness

Surge is interpreted with demand, supply, and conversion—not only revenue.
Higher surge may increase fare but suppress demand. Correlation is not causal
proof because high demand itself causes surge.

## Freshness

The dashboard should show the latest event/export time. A correct old number can
still be operationally useless. `FUTURE_DATED` or stale statuses must be shown,
not silently presented as live.

## Questions and answers

### “What did the funnel reveal?”

> Of 16,203 clean requests, 13,897 completed, or 85.77%. The largest absolute
> loss was at matching—1,196 requests—which points toward a supply/matching
> problem rather than later ride execution. It is synthetic evidence, not a
> claim about a real business.

### “Can you say surge caused higher revenue?”

> No. Surge is triggered by marketplace imbalance, so demand and supply
> confound the relationship. The dashboard describes association. Causal impact
> would require an experiment or a defensible causal design.

### “Why use decimal for money?”

> Binary floats cannot represent many decimal fractions exactly, so repeated
> financial arithmetic can drift. Fixed-precision decimal supports cent-level
> reconciliation.

### “Why exclude quarantine everywhere?”

> Governed business measures require trustworthy lifecycle and financial state.
> I centralize the filter and separately report quarantine counts/reasons so
> exclusions remain visible rather than becoming silent data loss.
