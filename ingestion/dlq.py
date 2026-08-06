"""Dead letter queue producer.

A rejected message is quarantined with enough context to diagnose and replay it,
never discarded. Structure follows docs/kafka_design.md section 6.2.

The raw bytes are preserved base64-encoded rather than re-serialised. For a
MALFORMED_JSON rejection there IS no parsed form, and re-serialising a partially
parsed message would destroy the evidence needed to find the producer bug.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ingestion.validation import Rejection


@dataclass
class DlqReport:
    produced: int = 0
    failed: int = 0

    def as_dict(self) -> dict[str, int]:
        return {"produced": self.produced, "failed": self.failed}


def build_dlq_message(
    *,
    rejection: Rejection,
    raw: bytes,
    consumer_group: str,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    key: bytes | None,
    headers: list[tuple[str, bytes]] | None,
) -> bytes:
    """Wrap a rejected message with diagnostic context."""
    document: dict[str, Any] = {
        "rejection_reason": str(rejection.reason),
        "rejection_detail": rejection.detail,
        "rejected_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "consumer_group": consumer_group,
        "source_topic": source_topic,
        # Partition and offset make the message locatable in the source topic
        # for replay, for as long as retention holds.
        "source_partition": source_partition,
        "source_offset": source_offset,
        "original_key": key.decode("utf-8", errors="replace") if key else None,
        "original_value_base64": base64.b64encode(raw).decode("ascii"),
        "original_headers": {
            name: value.decode("utf-8", errors="replace") for name, value in (headers or [])
        },
    }
    return json.dumps(document, separators=(",", ":")).encode("utf-8")


class DlqProducer:
    """Publishes rejected messages. A no-op producer can be injected for tests."""

    def __init__(self, bootstrap_servers: str, *, producer: Any = None) -> None:
        self.report = DlqReport()
        self._producer = producer or self._build(bootstrap_servers)

    @staticmethod
    def _build(bootstrap_servers: str) -> Any:
        from confluent_kafka import Producer

        return Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "rideflow-ingestion-dlq",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "snappy",
            }
        )

    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is None:
            self.report.produced += 1
        else:
            self.report.failed += 1

    def send(self, topic: str, *, key: bytes, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    topic=topic, key=key, value=value, on_delivery=self._on_delivery
                )
                return
            except BufferError:
                self._producer.poll(0.5)

    def flush(self, timeout: float = 30.0) -> int:
        return self._producer.flush(timeout)
