"""The ingestion consumer loop.

The whole design reduces to one ordering rule:

    poll -> validate -> dedupe -> write Parquet -> fsync -> COMMIT OFFSET

Everything else follows from it. Committing before the write makes delivery
at-most-once and permits silent loss on crash. Committing after makes it
at-least-once, so a crash produces duplicates - which staging removes
deterministically. Losing data is unrecoverable; duplicating it is not.
"""

from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass, field
from typing import Any

from ingestion.config import DLQ_FOR, FAMILY_FOR, ConsumerConfig
from ingestion.dlq import DlqProducer, build_dlq_message
from ingestion.validation import Rejection, Validator, ValidEvent
from ingestion.writer import ParquetLandingZoneWriter

log = logging.getLogger("rideflow.ingestion")


@dataclass
class IngestionStats:
    polled: int = 0
    landed: int = 0
    rejected: int = 0
    duplicates_dropped: int = 0
    batches_flushed: int = 0
    files_written: int = 0
    rejections_by_reason: dict[str, int] = field(default_factory=dict)
    started_at: float = field(default_factory=time.monotonic)

    first_message_at: float | None = None
    last_message_at: float | None = None
    """Bracket the period when messages were actually flowing.

    Throughput must be measured over this window, not over total wall clock.
    Wall clock includes idle polling and shutdown timeouts, which drag the rate
    down and would understate real capability - the first measurement of this
    consumer reported 297 ev/s for work it did at roughly 1,300 ev/s, purely
    because a 12-second idle timeout was counted as processing time.
    """

    def record_rejection(self, reason: str) -> None:
        self.rejected += 1
        self.rejections_by_reason[reason] = self.rejections_by_reason.get(reason, 0) + 1

    def mark_message(self) -> None:
        now = time.monotonic()
        if self.first_message_at is None:
            self.first_message_at = now
        self.last_message_at = now

    @property
    def elapsed_sec(self) -> float:
        """Total wall clock, including idle."""
        return max(time.monotonic() - self.started_at, 1e-9)

    @property
    def processing_sec(self) -> float:
        """Time from first to last message - the throughput denominator."""
        if self.first_message_at is None or self.last_message_at is None:
            return 0.0
        return max(self.last_message_at - self.first_message_at, 1e-9)

    @property
    def events_per_sec(self) -> float:
        if self.processing_sec <= 1e-9:
            return 0.0
        return self.polled / self.processing_sec

    def reconciles(self) -> bool:
        """Invariant N1: everything polled was either landed, rejected, or
        dropped as a duplicate. Nothing may simply vanish."""
        return self.polled == self.landed + self.rejected + self.duplicates_dropped

    def as_dict(self) -> dict[str, Any]:
        return {
            "polled": self.polled,
            "landed": self.landed,
            "rejected": self.rejected,
            "duplicates_dropped": self.duplicates_dropped,
            "batches_flushed": self.batches_flushed,
            "files_written": self.files_written,
            "rejections_by_reason": dict(sorted(self.rejections_by_reason.items())),
            "wall_clock_sec": round(self.elapsed_sec, 2),
            "processing_sec": round(self.processing_sec, 2),
            "events_per_sec": round(self.events_per_sec, 1),
            "reconciles": self.reconciles(),
        }


@dataclass
class Batch:
    """Accumulates work until a flush trigger fires."""

    valid: list[tuple[ValidEvent, str, str, int, int]] = field(default_factory=list)
    dlq: list[tuple[str, bytes, bytes]] = field(default_factory=list)
    seen_event_ids: set[str] = field(default_factory=set)
    opened_at: float = field(default_factory=time.monotonic)
    consumed: int = 0

    def age_sec(self) -> float:
        return time.monotonic() - self.opened_at

    def is_empty(self) -> bool:
        return self.consumed == 0

    def reset(self) -> None:
        self.valid.clear()
        self.dlq.clear()
        self.seen_event_ids.clear()
        self.opened_at = time.monotonic()
        self.consumed = 0


class IngestionConsumer:
    def __init__(
        self,
        config: ConsumerConfig | None = None,
        *,
        consumer: Any = None,
        dlq: DlqProducer | None = None,
        writer: ParquetLandingZoneWriter | None = None,
    ) -> None:
        self.config = config or ConsumerConfig.from_env()
        self.validator = Validator()
        self.writer = writer or ParquetLandingZoneWriter(
            self.config.landing_zone, compression=self.config.compression
        )
        self.dlq = dlq or DlqProducer(self.config.bootstrap_servers)
        self.stats = IngestionStats()
        self.batch = Batch()
        self._consumer = consumer
        self._running = False

    # ---- lifecycle --------------------------------------------------------

    def _build_consumer(self) -> Any:
        from confluent_kafka import Consumer

        consumer = Consumer(self.config.to_librdkafka())
        consumer.subscribe(list(self.config.topics))
        return consumer

    def install_signal_handlers(self) -> None:
        """Stop at the next loop boundary rather than mid-batch.

        A clean stop flushes and commits; SIGKILL does not, and that is exactly
        the crash case the restart test exercises deliberately.
        """

        def _stop(signum, _frame):
            log.info("signal %s received, finishing current batch", signum)
            self._running = False

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, _stop)

    # ---- message handling -------------------------------------------------

    def handle(self, message: Any) -> None:
        """Validate one message into the current batch."""
        self.batch.consumed += 1
        self.stats.polled += 1
        self.stats.mark_message()

        raw = message.value()
        topic = message.topic()
        event, rejection = self.validator.validate(raw or b"")

        if rejection is not None:
            self._reject(message, rejection, raw or b"")
            return

        assert event is not None

        # Deduplication pass 1: within this batch only.
        #
        # This is an OPTIMISATION, not a correctness guarantee. It cannot see a
        # duplicate that arrives in a later batch, hours later after a network
        # partition, or during a full replay. The authoritative pass is the
        # window function in dbt staging, which is recomputed from the whole
        # immutable landing zone every run (etl_design.md 3.1).
        if event.event_id in self.batch.seen_event_ids:
            self.stats.duplicates_dropped += 1
            return
        self.batch.seen_event_ids.add(event.event_id)

        self.batch.valid.append(
            (
                event,
                FAMILY_FOR.get(topic, "unknown"),
                topic,
                message.partition(),
                message.offset(),
            )
        )

    def _reject(self, message: Any, rejection: Rejection, raw: bytes) -> None:
        topic = message.topic()
        dlq_topic = DLQ_FOR.get(topic)
        self.stats.record_rejection(str(rejection.reason))

        if dlq_topic is None:  # pragma: no cover - unreachable with known topics
            log.error("no DLQ configured for topic %s; message dropped", topic)
            return

        payload = build_dlq_message(
            rejection=rejection,
            raw=raw,
            consumer_group=self.config.group_id,
            source_topic=topic,
            source_partition=message.partition(),
            source_offset=message.offset(),
            key=message.key(),
            headers=message.headers(),
        )
        self.batch.dlq.append((dlq_topic, message.key() or b"", payload))

    # ---- flushing ---------------------------------------------------------

    def should_flush(self) -> bool:
        if self.batch.is_empty():
            return False
        return (
            len(self.batch.valid) >= self.config.batch_max_events
            or self.batch.age_sec() >= self.config.batch_max_seconds
        )

    def flush(self) -> None:
        """Write, publish rejections, THEN commit.

        The order is the entire safety property. If the process dies before the
        commit, every message in this batch is redelivered - producing
        duplicates, never a gap.
        """
        if self.batch.is_empty():
            return

        result = self.writer.write(self.batch.valid)
        self.stats.landed += result.rows_written
        self.stats.files_written += result.files_written

        for topic, key, payload in self.batch.dlq:
            self.dlq.send(topic, key=key, value=payload)
        if self.batch.dlq:
            self.dlq.flush(30)

        # Only now is it safe to acknowledge these messages.
        if self._consumer is not None:
            self._consumer.commit(asynchronous=False)

        self.stats.batches_flushed += 1
        log.info(
            "flushed batch: %d landed in %d file(s), %d rejected",
            result.rows_written,
            result.files_written,
            len(self.batch.dlq),
        )
        self.batch.reset()

    # ---- main loop --------------------------------------------------------

    def run(
        self, *, max_messages: int | None = None, idle_timeout: float | None = None
    ) -> IngestionStats:
        if self._consumer is None:
            self._consumer = self._build_consumer()
        self._running = True
        idle_since: float | None = None

        try:
            while self._running:
                message = self._consumer.poll(1.0)

                if message is None:
                    # A time-triggered flush must still happen when the stream
                    # is quiet, or the last partial batch would sit unwritten
                    # indefinitely.
                    if self.should_flush():
                        self.flush()
                    idle_since = idle_since or time.monotonic()
                    if idle_timeout and time.monotonic() - idle_since >= idle_timeout:
                        break
                    continue

                idle_since = None

                if message.error():
                    log.warning("consumer error: %s", message.error())
                    continue

                self.handle(message)

                if self.should_flush():
                    self.flush()

                if max_messages and self.stats.polled >= max_messages:
                    break
        finally:
            # Flush whatever is in hand before releasing the partitions, so a
            # graceful stop never leaves work uncommitted.
            self.flush()
            if self._consumer is not None:
                self._consumer.close()

        return self.stats
