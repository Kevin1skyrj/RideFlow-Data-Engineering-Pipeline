"""Schema access for tests.

Delegates to `ingestion.contract`, which is what the consumer itself uses. The
tests must validate against the SAME loader as production code - two loaders
could disagree, and then a passing test would prove nothing about what the
consumer actually accepts.
"""

from __future__ import annotations

from ingestion.contract import CONTRACT_PATH, load_schemas

__all__ = ["CONTRACT_PATH", "envelope_schema", "load_schemas", "payload_schema"]


def envelope_schema() -> dict:
    return load_schemas()["envelope"]


def payload_schema(event_type: str) -> dict:
    schemas = load_schemas()
    if event_type not in schemas:
        raise KeyError(f"No schema for event type {event_type!r}. Extracted: {sorted(schemas)}")
    return schemas[event_type]
