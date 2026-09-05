# 05 — Apache Kafka

## Mental model

Kafka is a durable, ordered log divided into topics and partitions. Producers
append records. Consumers read using offsets and can replay retained history.
Unlike a traditional task queue, reading does not delete the record.

## Core vocabulary

- **Broker:** Kafka server storing partitions.
- **Topic:** named stream of related records.
- **Partition:** ordered append-only shard of a topic.
- **Offset:** a record's position inside one partition.
- **Producer:** publishes records.
- **Consumer:** reads records.
- **Consumer group:** consumers that divide a topic's partitions.
- **Retention:** how long Kafka keeps records.
- **Lag:** newest available offset minus consumer progress.
- **KRaft:** Kafka's built-in metadata quorum, replacing ZooKeeper.

## RideFlow topology

| Topic family | Partitions | Key | Reason |
|---|---:|---|---|
| Trip events | 12 | trip ID | Preserve per-trip partition order and scale evenly |
| Driver presence | 6 | driver ID | Preserve per-driver order at lower volume |
| Each DLQ | 3 | source key | Isolate invalid records for investigation |

Twelve has many divisors—1, 2, 3, 4, 6, 12—so several consumer counts divide
work evenly. More partitions also cost broker resources and cannot be decreased.

## Producer durability settings

- `acks=all`: wait for all in-sync replicas to acknowledge.
- idempotent producer: suppress duplicate writes created by producer retries.
- bounded retries and delivery timeout: tolerate transient failure but eventually
  return a clear error.

Producer idempotence does not remove duplicates caused by consumer redelivery.
Those are separate failure boundaries.

## Consumer groups

Within one group, a partition is assigned to only one consumer at a time. A
12-partition topic can use at most 12 actively consuming workers in that group;
additional consumers remain idle. A second group reads independently, which is
useful when two applications need the same events.

## Offset commit sequence

```text
poll → validate → DLQ invalid / batch valid → write Parquet → commit offsets
```

Commit before write risks permanent loss. Write before commit risks duplicates
after a crash. RideFlow chooses the second because duplicates can be removed;
missing events cannot be reconstructed after retention expires.

## Rebalance

A rebalance redistributes partitions when group membership changes. Consumers
must finish or safely abandon in-flight work before losing ownership. Long
processing can exceed polling limits and trigger repeated rebalances, so work is
bounded and commits are explicit.

## Retention and lag

RideFlow retains valid topics for seven days, matching the maximum accepted
lateness. DLQs retain longer for investigation. Growing consumer lag is the
most urgent Kafka metric: if lag exceeds retention, unread data is deleted and
cannot be recovered from Kafka.

## High-probability questions

### “Why Kafka instead of RabbitMQ?”

> I needed a replayable event log, not only task delivery. Kafka retains events
> after consumption, provides per-partition ordering and consumer groups, and
> lets me rebuild downstream state. RabbitMQ is strong for work queues and
> routing, but replayable history is not its central abstraction.

### “What ordering does Kafka guarantee?”

> Only within one partition. There is no global topic order. I key trip events
> by trip ID so one trip stays in one partition, and presence events by driver
> ID. I still resolve causal order downstream because arrival order can differ
> from event time.

### “What happens on crash after write but before commit?”

> Kafka redelivers the uncommitted batch. Raw Parquet may contain duplicate
> event IDs, and dbt keeps the earliest landed copy. The forced-kill test showed
> zero missing distinct events and 13 expected duplicate rows.

### “Is your Kafka highly available?”

> No. The local topology has one broker and replication factor 1, so it proves
> configuration and delivery semantics rather than availability. Production
> would require at least three brokers, replicated partitions, durable disks,
> monitoring, security, and failure-zone planning.

### “Why disable topic auto-creation?”

> A typo should fail immediately. Auto-created topics inherit defaults, which
> could silently create one partition instead of the deliberate 12/6 design and
> weaken scale and ordering assumptions.

### “Exactly once?”

> Kafka transactions can provide exactly-once behavior within Kafka workflows,
> but Parquet is not part of a Kafka transaction. RideFlow therefore implements
> at-least-once delivery plus deterministic downstream deduplication.

## Follow-up traps

- Increasing partitions can change key-to-partition mapping and therefore the
  cross-time ordering expectation for a key.
- `acks=all` with replication factor 1 cannot survive broker loss; settings do
  not create replicas.
- A null key distributes records without entity ordering.
- Committing a DLQ'd record is correct after the DLQ write succeeds; otherwise
  one poison message can block the partition forever.
