"""Output sinks for generated events.

M2 writes newline-delimited JSON. M3 replaces this with a Kafka producer; the
`Sink` protocol exists so that swap does not touch the generator.

Events are emitted in `ingested_at` order - the order a consumer would actually
observe them. That is what makes an injected out-of-order arrival visible in the
file rather than tidied away by writing in trip order.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Protocol

from event_generator.anomalies import TAG_MALFORMED_JSON
from event_generator.envelope import Event


class Sink(Protocol):
    def write(self, events: Iterable[Event]) -> int: ...


def _serialise(event: Event) -> str:
    line = json.dumps(event.to_dict(), separators=(",", ":"), ensure_ascii=False)
    if TAG_MALFORMED_JSON in event.anomaly_tags:
        # Truncate mid-token to produce genuinely unparseable bytes. A
        # schema-violating event is still valid JSON; this variant exercises the
        # MALFORMED_JSON rejection path, where there is no parsed form at all.
        return line[: max(len(line) // 2, 12)]
    return line


class JsonlFileSink:
    """Single newline-delimited JSON file."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def write(self, events: Iterable[Event]) -> int:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        count = 0
        with self.path.open("w", encoding="utf-8", newline="\n") as fh:
            for event in events:
                fh.write(_serialise(event))
                fh.write("\n")
                count += 1
        return count


class PartitionedJsonlSink:
    """Hive-style dt=/hour= partitions keyed on ingested_at.

    Partitioning on ingestion time rather than event time is deliberate and
    matches the landing-zone layout in etl_design.md section 4.1: arrival time
    is monotonic, so partitions are append-only, whereas a late event would
    otherwise have to be written into an already-closed event-time partition.
    """

    def __init__(self, root: Path) -> None:
        self.root = root

    def write(self, events: Iterable[Event]) -> int:
        handles: dict[Path, object] = {}
        count = 0
        try:
            for event in events:
                stamp = event.ingested_at
                directory = (
                    self.root / f"dt={stamp.strftime('%Y-%m-%d')}" / f"hour={stamp.strftime('%H')}"
                )
                path = directory / "events.jsonl"
                if path not in handles:
                    directory.mkdir(parents=True, exist_ok=True)
                    handles[path] = path.open("a", encoding="utf-8", newline="\n")
                handle = handles[path]
                handle.write(_serialise(event))
                handle.write("\n")
                count += 1
        finally:
            for handle in handles.values():
                handle.close()
        return count


class StdoutSink:
    def write(self, events: Iterable[Event]) -> int:
        count = 0
        for event in events:
            sys.stdout.write(_serialise(event))
            sys.stdout.write("\n")
            count += 1
        return count
