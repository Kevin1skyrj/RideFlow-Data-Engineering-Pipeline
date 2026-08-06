"""Loads the event contract's JSON Schemas.

The schemas are parsed out of the fenced ```json blocks in
docs/event_contract.md rather than duplicated into .json files.

This is a deliberate trade-off:

  + There is exactly ONE copy of every schema, so a schema in the docs that
    disagrees with a schema in code cannot exist. Drift is impossible rather
    than merely detectable.
  - The markdown file must ship wherever the consumer runs. It is a few hundred
    KB and lives in the repo, so containerising it is a COPY line - but it is a
    real deployment constraint and is called out here rather than discovered.

Parsing happens once at startup and is cached; it is not on the hot path.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "docs" / "event_contract.md"

_FENCE = re.compile(r"```json\s*\n(.*?)\n```", re.DOTALL)
_ID = re.compile(r"/schemas/([A-Za-z]+)/")

ENVELOPE_FIELDS = (
    "event_id",
    "event_type",
    "event_version",
    "event_timestamp",
    "ingested_at",
    "partition_key",
    "correlation_id",
    "causation_id",
    "producer_service",
    "producer_version",
    "environment",
    "payload",
)

REQUIRED_ENVELOPE_FIELDS = tuple(f for f in ENVELOPE_FIELDS if f != "causation_id")
"""causation_id is nullable but must still be PRESENT - null is meaningful
(the event starts a chain), absent is a contract violation."""

SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


@lru_cache(maxsize=1)
def load_schemas(path: str | None = None) -> dict[str, dict]:
    """Schema name -> parsed JSON Schema, keyed by the `$id` segment."""
    target = Path(path) if path else CONTRACT_PATH
    if not target.exists():
        raise FileNotFoundError(
            f"Event contract not found: {target}\n"
            "The consumer validates against docs/event_contract.md; it must be present."
        )

    schemas: dict[str, dict] = {}
    for block in _FENCE.findall(target.read_text(encoding="utf-8")):
        stripped = block.strip()
        if '"$schema"' not in stripped or '"$id"' not in stripped:
            continue  # sample payloads and DLQ examples, not schemas
        parsed = json.loads(stripped)
        match = _ID.search(parsed.get("$id", ""))
        if match:
            schemas[match.group(1)] = parsed

    if not schemas:  # pragma: no cover
        raise AssertionError(f"No schemas extracted from {target}")
    return schemas


@lru_cache(maxsize=1)
def known_event_types() -> frozenset[str]:
    """The closed set of event types the contract defines.

    An unrecognised event_type is the ONE enum that goes to the DLQ: the
    consumer cannot infer the shape of a payload it has never seen. Every other
    unknown enum value is accepted as new information (event_contract.md 2.3).
    """
    envelope = load_schemas()["envelope"]
    return frozenset(envelope["properties"]["event_type"]["enum"])
