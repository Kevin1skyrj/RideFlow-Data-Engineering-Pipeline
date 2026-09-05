# 06 — Python Ingestion Consumer

## Purpose

The consumer is the boundary between Kafka transport and durable analytical
storage. It polls records, validates raw bytes, sends invalid messages to DLQ,
batches valid events, writes Parquet, and only then commits offsets.

It does not calculate completion rate, revenue, or trip status. Those require
cross-event context and belong in dbt.

## Processing sequence

```text
poll batch
  → validate each raw message
  → send rejects to DLQ
  → remove duplicates within this batch
  → write valid rows to temporary Parquet files
  → atomically publish files
  → commit Kafka offsets
```

The batch flushes when either its row threshold or 60-second age is reached.
This balances throughput with bounded landing latency.

## Failure reasoning

- Commit before persistence: crash can lose data permanently.
- Persist before commit: crash can cause redelivery and duplicates.
- RideFlow chooses persist first because stable IDs make duplicates removable.
- A poison message is committed only after its DLQ record is safely produced,
  preventing one invalid record from blocking a partition forever.

## Two deduplication layers

Batch-local deduplication is an efficiency optimization. It cannot detect a
duplicate from an earlier batch or restart. dbt staging performs the global
correctness dedup across landed history.

## Questions and answers

### “Why Python for the consumer?”

> It integrates well with Kafka, JSON Schema, and Arrow/Parquet and keeps this
> portfolio project accessible. At much higher throughput I would profile CPU,
> serialization, and I/O before deciding whether concurrency or a JVM/Go
> implementation is justified.

### “How does backpressure appear?”

> Kafka retains unread records while consumer lag grows. The consumer processes
> bounded batches; lag is the signal that it is losing ground. I would first
> tune batching, then add consumers within the partition count.

### “What if Parquet writing fails?”

> Offsets remain uncommitted, so Kafka can redeliver. Temporary files are not
> published as valid Parquet. The next attempt may duplicate already published
> rows if failure occurs around commit, which staging handles.

### “Why not validate business sequence in the consumer?”

> A consumer sees arrivals, not necessarily the complete trip. Rejecting a
> `RideCompleted` because `RideStarted` has not arrived yet would discard valid
> out-of-order data. Sequence validation happens after history is assembled.
