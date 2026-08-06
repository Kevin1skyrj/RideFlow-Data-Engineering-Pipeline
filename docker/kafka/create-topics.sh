#!/usr/bin/env bash
# Provision RideFlow topics with EXPLICIT partition counts.
#
# Auto-creation is disabled on the broker, so this script is the only thing that
# creates topics. That is deliberate: an auto-created topic silently gets the
# broker default partition count, and partition count can be increased later but
# never decreased - and increasing it breaks key-to-partition mapping for keys
# already in flight, so trips mid-lifecycle would lose their ordering guarantee.
#
# Topic design and the reasoning behind each number: docs/kafka_design.md.

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVER:-broker:9092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

# 7 days, deliberately matched to invariant T4: events more than 7 days late are
# treated as corruption rather than late data. Retention shorter than the
# lateness tolerance would make a genuinely-late event unrecoverable while still
# considered valid. This is also the disaster-recovery window if the landing
# zone is lost.
RETENTION_MS=$((7 * 24 * 60 * 60 * 1000))

# DLQ holds rejected messages for human investigation. 30 days, because a bad
# weekend should not silently expire the evidence.
DLQ_RETENTION_MS=$((30 * 24 * 60 * 60 * 1000))

create_topic() {
  local name="$1" partitions="$2" retention="$3" description="$4"

  if $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | grep -qx "$name"; then
    echo "  = $name already exists (leaving untouched)"
    return 0
  fi

  $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP" \
    --create \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config "retention.ms=$retention" \
    --config "cleanup.policy=delete" \
    --config "compression.type=producer" \
    --config "max.message.bytes=1048576" \
    >/dev/null

  echo "  + $name  (${partitions}p)  ${description}"
}

echo "Waiting for broker at ${BOOTSTRAP} ..."
for _ in $(seq 1 30); do
  if $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Provisioning RideFlow topics:"

# 12 partitions: divisible by 1, 2, 3, 4, 6 and 12, so the consumer group scales
# to any of those counts with perfectly even partition distribution. A count like
# 10 leaves most consumer counts unbalanced, with some consumers idle.
create_topic "rideflow.trips.events.v1" 12 "$RETENTION_MS" \
  "trip lifecycle, keyed by trip_id"

# Separate topic, not a shared one. Presence events must be keyed by driver_id
# for per-driver ordering while trip events need trip_id. A topic has exactly one
# partitioning strategy, so sharing would silently lose one of the two orderings.
create_topic "rideflow.drivers.presence.v1" 6 "$RETENTION_MS" \
  "driver sessions, keyed by driver_id"

create_topic "rideflow.trips.events.dlq.v1" 3 "$DLQ_RETENTION_MS" \
  "rejected trip events + reason"

create_topic "rideflow.drivers.presence.dlq.v1" 3 "$DLQ_RETENTION_MS" \
  "rejected presence events + reason"

echo
echo "Topics now present:"
$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP" --list | sed 's/^/  /'
echo
echo "Partition detail:"
$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP" --describe \
  | grep -E "^Topic:" | sed 's/^/  /'
echo
echo "Topic provisioning complete."
