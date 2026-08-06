# RideFlow — Docker runbook

Streaming infrastructure for M3. Design rationale lives in
[`../docs/kafka_design.md`](../docs/kafka_design.md); this file is operational.

---

## Prerequisites

**Docker Desktop must be running**, not merely installed. These are different states, and the failure mode is confusing:

```
error during connect: ... open //./pipe/docker_engine:
The system cannot find the file specified.
```

That message means the daemon is not started. Check with:

```bash
docker info --format '{{.ServerVersion}}'
```

---

## Start

```bash
# From the repository root
docker compose -f docker/docker-compose.yml up -d

# Watch the broker become healthy (start_period is 20s)
docker compose -f docker/docker-compose.yml ps

# Confirm topics were provisioned
docker compose -f docker/docker-compose.yml logs topic-init
```

With the optional web UI (see the caveat in §Services):

```bash
docker compose -f docker/docker-compose.yml --profile ui up -d
# http://localhost:8081
```

## Stop

```bash
docker compose -f docker/docker-compose.yml down          # keeps the log
docker compose -f docker/docker-compose.yml down -v       # DELETES the log
```

> `-v` removes the `rideflow-kafka-data` volume. Everything not yet consumed is gone. Use it to reset to a clean state, never to "restart".

---

## Services

| Service | Port | Purpose |
|---|---|---|
| `broker` | `29092` (host) / `9092` (internal) | Kafka in KRaft mode |
| `topic-init` | — | Creates topics, then exits. Exit code 0 is success. |
| `kafka-ui` | `8081` | **Optional**, behind the `ui` profile |

### Two listeners, and why it matters

`29092` is the **host-facing** listener. The generator and consumer run on the host during development and must use `localhost:29092`. Anything running *inside* the Compose network uses `broker:9092` — `localhost` there would resolve to the container itself.

Using the wrong one produces a connection that appears to succeed and then times out on metadata, which is a genuinely confusing failure.

### The UI is optional on purpose

`provectuslabs/kafka-ui` last released `v0.7.2` in 2023 and may lag the Kafka 4.x protocol. It sits behind a profile so that if it misbehaves it cannot block the core stack. Topic inspection never depends on it:

```bash
MSYS_NO_PATHCONV=1 docker exec rideflow-broker \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

> ### ⚠️ Git Bash on Windows: `MSYS_NO_PATHCONV=1` is required
>
> Git Bash rewrites arguments that look like Unix paths into Windows paths before the process sees them. Without the prefix, `/opt/kafka/bin/...` is silently rewritten to `C:/Program Files/Git/opt/kafka/bin/...` inside the container and you get:
>
> ```
> exec: "C:/Program Files/Git/opt/kafka/bin/kafka-topics.sh": no such file or directory
> ```
>
> This is easy to misread as "the container is broken". It isn't — the path was mangled before Docker ever saw it. PowerShell and CMD are unaffected; drop the prefix there.

---

## Topics

| Topic | Partitions | Key | Retention |
|---|---|---|---|
| `rideflow.trips.events.v1` | 12 | `trip_id` | 7 days |
| `rideflow.drivers.presence.v1` | 6 | `driver_id` | 7 days |
| `rideflow.trips.events.dlq.v1` | 3 | `event_id` | 30 days |
| `rideflow.drivers.presence.dlq.v1` | 3 | `event_id` | 30 days |

**Auto-creation is disabled.** An auto-created topic silently gets the broker default partition count, which would cap consumer scale-out at one consumer and quietly break the ordering guarantee. `topic-init` is the only thing that creates topics.

**Partition counts can be increased but never decreased** — and increasing them changes key-to-partition mapping for keys already in flight, so trips mid-lifecycle would lose per-trip ordering. Over-provisioning slightly is the cheap direction to err in.

---

## Verify end to end

```bash
# 1. Produce a short window
python -m event_generator.main --duration 600 --trips-per-hour 300 --sink kafka

# 2. Confirm messages landed (sum the per-partition offsets)
#    NOTE: kafka.tools.GetOffsetShell was REMOVED in Kafka 4.x.
#    The dedicated script below is the replacement.
MSYS_NO_PATHCONV=1 docker exec rideflow-broker \
  /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
  --topic rideflow.trips.events.v1

# 3. Read a few back
MSYS_NO_PATHCONV=1 docker exec rideflow-broker \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic rideflow.trips.events.v1 --from-beginning --max-messages 3

# 4. Run the integration suite (skips automatically without a broker)
pytest tests/integration -v
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `open //./pipe/docker_engine` | Docker Desktop not started | Start Docker Desktop |
| `exec: "C:/Program Files/Git/opt/kafka/..."` | Git Bash path mangling | Prefix with `MSYS_NO_PATHCONV=1` |
| `ClassNotFoundException: kafka.tools.GetOffsetShell` | Removed in Kafka 4.x | Use `kafka-get-offsets.sh` |
| `Kafka unavailable: no broker at localhost:29092` | Broker down, or wrong listener | `docker compose ... ps`; use `29092` from the host |
| Integration tests all skip | No broker reachable | Expected — start the stack first |
| `topic-init` exited non-zero | Broker was not healthy in time | `docker compose ... logs topic-init`, then re-run `up` |
| Topics exist with 1 partition | Created by auto-creation before this config | `down -v` and start again |
| Broker healthy but produce times out | Advertised listener mismatch | Confirm `KAFKA_ADVERTISED_LISTENERS` matches how you connect |
| Port 29092 already in use | Another broker running | `docker ps`; stop it, or change the host port mapping |

---

## Resetting

```bash
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d
```

This is safe for RideFlow specifically: the landing zone is the system of record, and the generator can reproduce any window byte-identically from its seed. Neither would be true of a production cluster.
