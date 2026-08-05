"""Extracts JSON Schemas directly from docs/event_contract.md.

The schemas are NOT duplicated into .json files. They are parsed out of the
fenced code blocks in the contract at test time, which makes the markdown
document literally executable and eliminates an entire class of drift: a schema
in the doc that disagrees with a schema in code cannot exist, because there is
only one copy.

If the contract is edited so a schema no longer parses, these tests fail - which
is the correct outcome. The contract is the source of truth (event_contract.md,
"Authority").
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "docs" / "event_contract.md"

_FENCE = re.compile(r"```json\s*\n(.*?)\n```", re.DOTALL)
_ID = re.compile(r"/schemas/([A-Za-z]+)/")


@lru_cache(maxsize=1)
def load_schemas() -> dict[str, dict]:
    """Map schema name -> parsed JSON Schema.

    Keys are the segment of `$id` after /schemas/, e.g. "RideRequested",
    plus "envelope".
    """
    if not CONTRACT_PATH.exists():
        raise FileNotFoundError(f"Event contract not found: {CONTRACT_PATH}")

    text = CONTRACT_PATH.read_text(encoding="utf-8")
    schemas: dict[str, dict] = {}

    for block in _FENCE.findall(text):
        stripped = block.strip()
        if '"$schema"' not in stripped or '"$id"' not in stripped:
            continue  # sample payloads and DLQ examples, not schemas
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:  # pragma: no cover
            raise AssertionError(
                f"A ```json block in {CONTRACT_PATH.name} is not valid JSON: {exc}"
            ) from exc

        match = _ID.search(parsed.get("$id", ""))
        if match:
            schemas[match.group(1)] = parsed

    if not schemas:  # pragma: no cover
        raise AssertionError(f"No schemas extracted from {CONTRACT_PATH}")
    return schemas


def envelope_schema() -> dict:
    return load_schemas()["envelope"]


def payload_schema(event_type: str) -> dict:
    schemas = load_schemas()
    if event_type not in schemas:
        raise KeyError(f"No schema for event type {event_type!r}. " f"Extracted: {sorted(schemas)}")
    return schemas[event_type]
