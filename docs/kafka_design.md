# RideFlow — Kafka Design

**Topics, partitioning, delivery semantics, and dead-letter handling.**

| Field | Value |
|---|---|
| Version | `1.0.0` |
| Status | Approved for implementation |
| Milestone | M3 |
| Last updated | 2026-08-06 |
| Deployment mode | **KRaft** (no ZooKeeper) |

---

## 1. Topics

| Topic | Partitions | Replication | Retention | Cleanup | Key |
|---|---|---|---|---|---|
| `rideflow.trips.events.v1` | **12** | 1 | **7 days** | `delete` | `trip_id` |
| `rideflow.drivers.presence.v1` | **6** | 1 | 7 days | `delete` | `driver_id` |
| `rideflow.trips.events.dlq.v1` | 3 | 1 | **30 days** | `delete` | `event_id` |
| `rideflow.drivers.presence.dlq.v1` | 3 | 1 | 30 days | `delete` | `event_id` |

### 1.1 Naming: `rideflow.<domain>.<stream>.v<major>`

Only the **major** version appears in the name. Minor and patch changes are backward-compatible by definition (`event_contract.md` §2.2) and must not force a new topic — doing so would make every additive field change a migration.

### 1.2 Why two production topics, not one

This is the most consequential topic decision, and it was not obvious when `PROJECT_PLAN.md` was written.

Trip events must be partitioned by `trip_id` to guarantee per-trip ordering. Presence events must be partitioned by `driver_id` to guarantee per-driver ordering. **A topic has exactly one partitioning strategy, so a shared topic would lose one of the two orderings.**

The failure mode that would follow is nasty: a driver appearing offline while a trip is in progress, intermittently, depending on which partition each event happened to land on. That is very hard to diagnose after the fact — the data looks plausible and the bug is not reproducible on demand.

Two topics, two consumer paths, two fact tables (`event_contract.md` §3.3).

### 1.3 Retention: 7 days, chosen not defaulted

Retention is set to exactly match invariant **T4** — events more than 7 days late are treated as corruption rather than late data (`event_contract.md` §7.2).

The alignment is deliberate. If retention were shorter than the lateness tolerance, a genuinely-late event could be unrecoverable while still being considered valid. If it were much longer, storage would be spent retaining events the pipeline has already declared corrupt.

**Retention is also the disaster-recovery window.** If the landing zone is lost, the only recovery is re-consuming from Kafka — and that is possible for 7 days, then never again. The landing zone remains the sole unrecoverable component (`architecture.md` §11.2).

**DLQ retention is 30 days** — longer, because rejected messages need human investigation and a bad weekend should not silently expire the evidence.

### 1.4 Replication factor 1

Single broker, so replication is 1. **This means a broker disk failure loses all unconsumed data.**

Acceptable here: this is a local single-node development platform, and the generator can regenerate events. In production this would be `replication.factor=3` with `min.insync.replicas=2`. Stating this plainly matters — a replication factor of 1 with no acknowledgement of why is a red flag; with the reasoning, it is a scoped decision.

---

## 2. Partitioning

### 2.1 Why 12 partitions

12 is chosen for its divisors: **1, 2, 3, 4, 6, 12**.

Kafka assigns whole partitions to consumers. If a consumer group has more consumers than partitions, the extras sit idle. If the partition count does not divide evenly by the consumer count, load is unbalanced.

With 12, the group can scale to 1, 2, 3, 4, 6, or 12 consumers with **perfectly even distribution every time**. A count like 10 would leave 3, 4, 6, 7, 8, 9, and 11 unbalanced.

Partition count can be **increased** later but never decreased — and increasing it breaks key-to-partition mapping for existing keys, so trips in flight would lose ordering. Over-provisioning slightly at the start is far cheaper than repartitioning later.

Presence topic gets 6 (divisors 1, 2, 3, 6) — lower volume, same principle.

### 2.2 Why `trip_id` is the key

| Property | Consequence |
|---|---|
| All events for a trip land on one partition | Per-trip ordering is guaranteed |
| High cardinality | Even distribution across partitions |
| Present on every trip event | No null-key fallback to round-robin |
| Immutable for the trip's life | A trip never migrates partitions mid-lifecycle |

**Why not `city_id`?** Six cities across twelve partitions means six partitions idle and severe skew — Bengaluru would carry a disproportionate share, and that one partition becomes the throughput ceiling for the entire topic.

**Why not `rider_id`?** It would order a *rider's* events, which no downstream consumer needs. Trip assembly is per-trip.

**Why not null (round-robin)?** Perfect balance, zero ordering. Every trip's events would scatter across all twelve partitions and arrive in arbitrary order.

### 2.3 What ordering actually guarantees

**Guaranteed:** events for one `trip_id` are delivered in the order the producer sent them.

**Not guaranteed:**
- Ordering across different trips
- Ordering across partitions
- That producer send order matches real-world event order

The third point matters most. A mobile device buffering events during a signal loss sends them late and possibly out of sequence — Kafka faithfully preserves the order it received, which is not the order things happened.

**This is why ordering is never relied on for correctness.** Trip assembly uses `causation_id` and `event_timestamp` against the fully assembled trip (`etl_design.md` §6.1). Partition ordering is a useful property, not a foundation.

---

## 3. Consumer Groups

| Group | Topic | Consumers (v1) | Purpose |
|---|---|---|---|
| `rideflow-trip-ingestor` | `rideflow.trips.events.v1` | 1 | Land trip events to Parquet |
| `rideflow-presence-ingestor` | `rideflow.drivers.presence.v1` | 1 | Land presence events to Parquet |

### 3.1 Why separate groups

Separate groups give **independent offsets, independent failure, and independent scaling**. A crash in trip ingestion does not stall presence ingestion, and the higher-volume trip stream can scale to more consumers without over-provisioning the presence stream.

### 3.2 Configuration

| Setting | Value | Reason |
|---|---|---|
| `enable.auto.commit` | **`false`** | **The most important consumer setting.** Auto-commit commits on a timer regardless of processing success — a crash between auto-commit and a durable write loses data silently. |
| `auto.offset.reset` | `earliest` | On a new group or lost offset, start from the beginning. `latest` would silently skip unprocessed history. |
| `max.poll.records` | 500 | Bounds batch memory |
| `max.poll.interval.ms` | 300000 | 5 min. Must exceed worst-case batch processing time, or the consumer is evicted mid-work and its batch is redelivered. |
| `session.timeout.ms` | 45000 | Failure detection latency |
| `heartbeat.interval.ms` | 3000 | ~1/3 of session timeout |
| `partition.assignment.strategy` | `CooperativeSticky` | Incremental rebalancing — only affected partitions move, rather than stopping the world. |

### 3.3 Rebalancing

A rebalance triggers on consumer join, leave, or timeout. With `CooperativeSticky`, unaffected partitions keep processing.

**On revocation, the consumer must flush its in-flight batch and commit before releasing the partition.** Failing to do so is not a correctness bug — the events are redelivered to the new owner — but it wastes work and inflates the duplicate rate, which then masks genuine duplicate signals.

---

## 4. Message Format

### 4.1 v1: JSON

Envelope + payload per `event_contract.md` §3. JSON chosen for v1 because it is human-readable in `kafka-ui`, requires no schema registry, and keeps M3 focused on Kafka mechanics.

**Its weaknesses are real and acknowledged:** no schema enforcement at the broker, 3–5× larger than binary formats, and no compile-time contract between producer and consumer.

### 4.2 v2 path: Avro + Schema Registry

Deferred, not dismissed. Avro would move contract enforcement from the consumer to the broker, making an incompatible producer fail at publish time rather than at consume time.

**The trigger is a second producer.** With one producer, the consumer-side validation in `etl_design.md` §2 is sufficient. With two, the coordination cost of keeping them in sync exceeds the cost of running a registry.

### 4.3 Headers

| Header | Purpose |
|---|---|
| `event_type` | Routing without deserialising the body |
| `event_version` | Version-aware handling before parse |
| `content_type` | `application/json` |
| `producer_service` | Attribution |

Headers duplicate envelope fields deliberately: a consumer can route or filter on them **without paying deserialisation cost**, which matters when most messages are being skipped.

### 4.4 Compression

`compression.type=snappy` at the producer. Snappy over gzip because it is far faster to compress and decompress at a modest ratio cost — the right trade for a high-throughput streaming path where CPU is the scarcer resource.

---

## 5. Delivery Semantics

### 5.1 The three guarantees

| Guarantee | Mechanism | Consequence |
|---|---|---|
| **At-most-once** | Commit offset *before* processing | Crash between commit and write = **silent data loss** |
| **At-least-once** | Commit offset *after* durable write | Crash before commit = **duplicates**, never loss |
| **Exactly-once** | Kafka transactions across read-process-write | Only within Kafka; does not extend to an external filesystem |

### 5.2 What RideFlow uses, and why

**At-least-once delivery + idempotent downstream processing = effectively-once semantics.**

```
poll → validate → dedupe → write Parquet → fsync → COMMIT OFFSET
                                                    ↑
                                    crash before here ⟹ redelivery
```

**Why not Kafka's exactly-once (EOS)?** Kafka's transactional guarantee covers Kafka-to-Kafka read-process-write. RideFlow's sink is **the filesystem**, which is not a transactional participant. There is no atomic "write Parquet and commit offset" operation, so EOS cannot span the boundary. Claiming exactly-once here would be claiming a guarantee the architecture does not provide.

**Why at-least-once rather than at-most-once?** Duplicates are removable; loss is not. Deduplication in staging (`etl_design.md` §3) is deterministic and cheap. Data lost to an early commit is gone permanently and — worse — invisibly.

**This is the pragmatic industry-standard answer**, and articulating *why* exactly-once is not achievable here is a stronger signal than claiming it is.

### 5.3 Producer configuration

| Setting | Value | Reason |
|---|---|---|
| `acks` | **`all`** | Wait for all in-sync replicas. `acks=1` risks loss on leader failure. |
| `enable.idempotence` | **`true`** | Prevents *producer-side* duplicates from internal retries. Distinct from consumer dedup — this eliminates one duplicate source at origin. |
| `retries` | `10` | Survive transient broker unavailability |
| `max.in.flight.requests.per.connection` | `5` | Maximum that preserves ordering when idempotence is enabled |
| `linger.ms` | `10` | Small batching delay, large throughput gain |
| `delivery.timeout.ms` | `120000` | Total time before permanent failure is declared |

`enable.idempotence=true` with `max.in.flight > 1` is safe **only** because idempotence adds sequence numbers that let the broker reorder correctly. Without idempotence, in-flight > 1 plus retries silently breaks ordering.

---

## 6. Dead Letter Queue

### 6.1 What goes to the DLQ

Only **unprocessable** messages — those that cannot be parsed or validated (`etl_design.md` §2.1). An unknown enum value does **not** go to the DLQ; it is new information and is landed normally.

### 6.2 DLQ message structure

The original message is preserved byte-for-byte, wrapped with diagnostic context:

```json
{
  "rejection_reason": "SCHEMA_VIOLATION",
  "rejection_detail": "payload.surge_multiplier: 0.8 below minimum 1.0",
  "rejected_at": "2026-03-17T02:42:11.204Z",
  "consumer_group": "rideflow-trip-ingestor",
  "source_topic": "rideflow.trips.events.v1",
  "source_partition": 7,
  "source_offset": 148392,
  "original_key": "1a4f8c22-9e5b-4c07-8d3a-2f6b1e904d77",
  "original_value_base64": "eyJldmVudF9pZCI6...",
  "original_headers": { "event_type": "RideRequested" }
}
```

**`original_value_base64` preserves raw bytes**, not a parsed representation. For a `MALFORMED_JSON` rejection there *is* no parsed representation — and re-serialising a partially-parsed message would destroy the evidence needed to diagnose the producer bug.

Partition and offset make the message locatable in the source topic for replay within the retention window.

### 6.3 Rejection reasons

| Reason | Cause | Typical fix |
|---|---|---|
| `MALFORMED_JSON` | Unparseable bytes | Producer serialisation bug |
| `MISSING_ENVELOPE_FIELD` | Required envelope field absent | Producer version mismatch |
| `UNKNOWN_EVENT_TYPE` | `event_type` not in the nine | New event type not yet deployed to the consumer |
| `INVALID_VERSION` | `event_version` not semver | Producer defect |
| `SCHEMA_VIOLATION` | Payload fails its schema | Contract breach |
| `INVALID_TIMESTAMP` | Unparseable timestamp | Producer defect |
| `TIMESTAMP_OUT_OF_BOUNDS` | Violates T3/T4 | Clock skew or genuinely ancient data |

### 6.4 Offset handling

**The offset is always committed for a DLQ'd message.**

Not committing creates a **poison-pill loop**: the consumer retries the same unparseable message forever and the partition never advances. One malformed byte would halt an entire partition indefinitely.

Nothing is lost — the message is quarantined in the DLQ with full context, not discarded.

### 6.5 DLQ as a monitored signal

**DLQ depth is a first-class alertable metric**, not a place messages go to be forgotten.

| Condition | Meaning | Action |
|---|---|---|
| Depth 0 | Healthy | — |
| Slow steady growth | A producer edge case | Investigate within a day |
| Sudden spike | Producer deployment broke the contract | **Page immediately** |
| One reason dominating | Systematic defect | Root-cause that reason |

A DLQ nobody watches is just a slower way to lose data.

### 6.6 Replay

After fixing the producer, DLQ messages can be replayed by decoding `original_value_base64` and republishing to the source topic. Because deduplication is keyed on `event_id`, replaying a message that was **also** processed successfully is harmless — the duplicate is removed in staging.

This is the payoff of making dedup a correctness guarantee rather than an optimisation: replay becomes safe by default.

---

## 7. Topic Configuration

| Setting | Trip / presence topics | Reason |
|---|---|---|
| `cleanup.policy` | `delete` | Time-based expiry. **Not `compact`** — compaction keeps only the latest value per key, which would discard every event of a trip except the last. Catastrophic for an event log. |
| `retention.ms` | `604800000` (7 days) | Matches T4 (§1.3) |
| `min.insync.replicas` | `1` | Single broker |
| `max.message.bytes` | `1048576` (1 MB) | Events are ~1–2 KB; this is a wide safety margin |
| `compression.type` | `producer` | Preserve producer compression rather than recompressing at the broker |

**`cleanup.policy=compact` would be actively wrong here** and is worth naming explicitly. Compaction is for changelog topics where only current state matters. An event log needs every event; compacting `rideflow.trips.events.v1` by `trip_id` would retain one event per trip and silently destroy the lifecycle.

---

## 8. Monitoring

| Metric | Threshold | Meaning |
|---|---|---|
| **Consumer lag** | > 10,000 or growing 5 min | Consumer cannot keep up — the primary health signal |
| **DLQ depth** | > 0 sustained; any spike | Contract violations |
| **Rebalance frequency** | > 1/hour | Instability, often a `max.poll.interval` problem |
| **Broker disk usage** | > 80% | Retention or volume issue |
| **Produce error rate** | > 0.1% | Broker or network trouble |
| **End-to-end latency** | > 5 min p99 | Freshness at risk |

**Consumer lag is the single most important metric.** Steady lag means throughput is matched. *Growing* lag means the consumer is losing ground, and the time to act is before it exhausts retention — because once lag exceeds retention, data is lost permanently and no amount of scaling recovers it.

---

## 9. Local deployment

**KRaft mode — no ZooKeeper.** Fewer moving parts, one less container, and the direction Kafka itself has taken. ZooKeeper would add operational complexity with no benefit at single-broker scale.

Services: one broker, plus `kafka-ui` for topic inspection during development. Topics are created by an init container with explicit partition counts rather than relying on auto-creation — **auto-created topics get default partition counts**, which would silently give 1 partition instead of 12 and cap scale-out at a single consumer.

---

## 10. Design decisions worth defending

| Decision | Reasoning |
|---|---|
| **Two topics, not one** | One partitioning strategy per topic; trip and presence events need different keys |
| **12 partitions** | Divisible by 1, 2, 3, 4, 6, 12 — even scale-out at every consumer count |
| **Key by `trip_id`** | Per-trip ordering with even distribution; `city_id` would skew badly |
| **7-day retention** | Deliberately matched to invariant T4 |
| **`enable.auto.commit=false`** | Auto-commit permits silent loss on crash |
| **Commit after durable write** | At-least-once; duplicates are removable, loss is not |
| **Not Kafka EOS** | Transactions do not span Kafka → filesystem; claiming otherwise would be false |
| **`cleanup.policy=delete`** | Compaction would destroy the event log |
| **DLQ offset always committed** | Otherwise one bad message halts a partition forever |
| **Raw bytes in DLQ** | Malformed messages have no valid parsed form |

---

## 11. Related documents

| Document | Covers |
|---|---|
| `docs/event_contract.md` | Message schemas, envelope, invariants |
| `docs/etl_design.md` | Validation, deduplication, landing |
| `docs/architecture.md` | Why Kafka, failure recovery, scaling |
| `docs/testing_strategy.md` | Consumer restart and chaos tests |
