# 09 — Data Quality and Reliability

## Quality dimensions

- Completeness: required values and events exist.
- Uniqueness: business identifiers are not duplicated.
- Validity: values satisfy formats, ranges, and contracts.
- Consistency: related columns/tables agree.
- Referential integrity: fact keys resolve to dimensions.
- Timeliness: data is recent enough for its use.
- Accuracy: data represents reality; synthetic data can test consistency but
  cannot prove real-world accuracy.

## Layers of protection

1. JSON Schema rejects structurally unusable events.
2. DLQ preserves rejects with reason and raw evidence.
3. Staging deduplicates stable event IDs.
4. dbt generic tests enforce structural warehouse rules.
5. Singular tests enforce lifecycle and financial invariants.
6. Quarantine separates questionable trips from business metrics.
7. Reconciliation compares counts and money across layers.
8. Freshness prevents stale data from being treated as current.

## Deduplication

Batch-local dedup reduces unnecessary writes, but it cannot see duplicates from
another batch or consumer restart. Warehouse deduplication is the correctness
layer because it sees complete history. RideFlow keeps the earliest arrival for
one `event_id`, making reruns deterministic.

Do not deduplicate by entire-row equality: metadata such as landing time can
differ even when the business event is the same.

## Idempotency and retry

Retries are safe only if repeating work does not double count or corrupt state.
Stable event IDs, deterministic tie-breaking, and `delete+insert` windows make
RideFlow replay-safe. A retry policy cannot compensate for non-idempotent logic.

## Reconciliation

Reconciliation checks conservation rules, for example:

```text
polled = landed + rejected + batch duplicates
distinct landed events = staged events
fare components = total fare
fare = driver payout + platform commission + other defined components
```

A pipeline can finish successfully while dropping rows. Reconciliation detects
silent wrongness that process monitoring cannot.

## Quarantine

Schema-valid events can still create an impossible trip. RideFlow keeps the row
and reason in quarantine so late corrective events can repair it later.
Business measures explicitly exclude quarantined trips. The atomic audit fact
continues to preserve accepted events.

## Error versus warning

An error means publishing could give a materially wrong answer, such as broken
financial reconciliation. A warning records a known or monitorable condition
without necessarily blocking all output. Severity is a business decision, not
only a technical one.

## Failure evidence

The forced-kill test produced 17,880 distinct valid Kafka events, recovered all
17,880 in Parquet, and observed 13 duplicate rows from an uncommitted batch.
Those duplicates were then removed in staging. This proves the chosen loss
versus duplicate trade-off under that tested scenario.

## Interview answers

### “How do you guarantee no double-counted revenue?”

> Stable event IDs are deduplicated across the full landing history, trip facts
> have one-row grain, incremental windows replace rather than append, and dbt
> asserts payment/fare and payout splits. I also hash marts across reruns to
> test idempotency instead of relying only on design intent.

### “How do you know no data was lost?”

> I compare source Kafka distinct event IDs with Parquet distinct IDs after a
> forced consumer kill and restart. The test recovered all 17,880 distinct valid
> events. Count reconciliation is stronger evidence than saying offsets are
> committed after writes.

### “Why not delete bad rows?”

> Deletion destroys evidence and can prevent self-repair when a late missing
> event arrives. I quarantine the assembled trip, expose the reason, and exclude
> it from governed business measures while keeping the audit trail.

### “Would you retry a data-quality failure?”

> Not automatically when it is deterministic. The same invalid data produces
> the same result, so retries delay diagnosis. Airflow gives dbt quality tests
> zero retries; transient infrastructure tasks can retry with backoff.

### “Tests passed—does that prove accuracy?”

> It proves the encoded contracts and invariants for the tested data. It does
> not prove synthetic data matches Bengaluru reality or that no missing rule
> exists. Accuracy requires source validation and business-owner agreement.
