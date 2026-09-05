# 13 — Scaling, Security, Observability, and Production

## Production-readiness language

Say: “It demonstrates production patterns and tested failure semantics.”
Do not say: “It is production ready.” A local single-broker platform has no
high availability, managed secrets, external alerts, or disaster-recovery SLA.

## Scale by measured bottleneck

1. Too many Parquet files: increase batches and compact files.
2. Consumer lag grows: add consumers up to partition count; then reconsider partitions.
3. dbt full refresh exceeds memory/time: optimize models and partition pruning.
4. DuckDB hits concurrent-user or single-node limits: move marts to a cloud warehouse.
5. Transformation exceeds one-machine compute: then consider distributed engines.

Do not introduce Spark or Kubernetes merely because the interviewer says
“scale.” State the metric and threshold that would justify them.

## Production Kafka changes

- At least three brokers across failure zones.
- Replication factor commonly 3 with suitable minimum in-sync replicas.
- TLS in transit, SASL authentication, topic ACLs, secret rotation.
- Capacity planning for throughput, retention, partitions, and disk.
- Consumer-lag, under-replicated-partition, and broker-health alerts.
- Backup/recovery strategy appropriate to the business.

## Storage changes

Use durable object storage instead of one laptop filesystem. At growing scale,
an open table format such as Iceberg or Delta can add transactions, snapshots,
schema evolution, compaction metadata, and time travel around Parquet files.

## Security

RideFlow uses synthetic data and excludes names, phones, and payment account
details. Production still requires least privilege, encryption, secrets
management, audit logs, data retention/deletion policies, network isolation,
dependency scanning, and access controls at raw and mart layers.

Synthetic data reduces privacy risk but does not automatically make the
infrastructure secure.

## Observability

Three categories:

- Infrastructure: broker health, disk, CPU, memory, container restarts.
- Pipeline: throughput, consumer lag, batch duration, task failures, retries.
- Data: freshness, row counts, rejection rate/reasons, duplicates, unknown keys,
  quarantined rate, and financial reconciliation.

Logs explain individual events; metrics reveal trends; traces connect work
across services. Data observability additionally watches the content and shape
of datasets.

## Cost

The local project has no realistic cloud cost model. In production, major cost
drivers would include Kafka retention/throughput, object-storage requests,
warehouse compute and scans, orchestration, BI licensing, network egress, and
operational staff time. Partition pruning and incremental models reduce compute
and scanned bytes.

## Interview answers

### “How would you scale to 100×?”

> I would first identify the binding metric. I expect small-file overhead, then
> consumer lag, then single-node warehouse resources. I would compact and tune
> batches, scale the consumer group within the existing 12 partitions, and move
> to a concurrent cloud warehouse only when DuckDB becomes the measured limit.
> The immutable Parquet boundary and isolated dbt logic contain that migration.

### “What is your disaster-recovery plan?”

> In the portfolio version, DuckDB is derived and can be rebuilt from raw
> Parquet, but raw storage itself is local and therefore a single point of loss.
> Production would require versioned replicated object storage, infrastructure
> backups, tested restore procedures, explicit RPO/RTO, and retained Kafka for
> short-term replay.

### “How would you secure Kafka?”

> TLS, SASL authentication, service-specific ACLs, secret rotation, private
> networking, hardened images, and audit monitoring. Producers should only
> write their topics; consumers should only read their assigned topics and
> write controlled DLQs.

### “What would you alert on first?”

> Growing consumer lag because crossing retention can cause permanent source
> loss, then failed quality/reconciliation gates, stale exports, DLQ spikes by
> rejection reason, under-replicated partitions, disk pressure, and repeated
> Airflow failures.

### “Where is the single point of failure?”

> The local broker, local raw storage, and single machine. DuckDB is
> reconstructable; losing the only raw Parquet copy is the critical data-loss
> event.
