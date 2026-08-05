"""Validates generated events against the schemas in docs/event_contract.md."""

from __future__ import annotations

import re
from collections import Counter

import pytest
from jsonschema import Draft202012Validator

from event_generator.envelope import CONTRACT_EVENT_TYPES
from tests.contract.schema_loader import envelope_schema, load_schemas, payload_schema

pytestmark = pytest.mark.contract

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


def test_contract_defines_a_schema_for_every_event_type():
    """A schema missing from the contract is a contract defect, not a test gap."""
    extracted = load_schemas()
    missing = [t for t in CONTRACT_EVENT_TYPES if t not in extracted]
    assert not missing, f"event_contract.md has no schema for: {missing}"
    assert "envelope" in extracted


def test_all_schemas_are_themselves_valid_json_schema():
    for _name, schema in load_schemas().items():
        Draft202012Validator.check_schema(schema)


def test_every_clean_event_validates(clean_result):
    """With anomalies disabled, 100% of events must satisfy the contract.

    Any failure here is a generator bug: the business logic produced data its
    own contract forbids.
    """
    envelope_validator = Draft202012Validator(envelope_schema())
    payload_validators = {
        name: Draft202012Validator(payload_schema(name)) for name in CONTRACT_EVENT_TYPES
    }

    failures = []
    for event in clean_result.events:
        raw = event.to_dict()
        for error in envelope_validator.iter_errors(raw):
            failures.append(f"{event.event_type} envelope: {error.message}")
        for error in payload_validators[event.event_type].iter_errors(raw["payload"]):
            failures.append(f"{event.event_type} payload {list(error.path)}: {error.message}")

    assert not failures, "Contract violations:\n" + "\n".join(failures[:25])


def test_all_nine_event_types_are_produced(clean_result):
    """A generator that never emits DriverOffline would pass every other test."""
    seen = Counter(e.event_type for e in clean_result.events)
    missing = [t for t in CONTRACT_EVENT_TYPES if seen[t] == 0]
    assert not missing, f"Event types never generated: {missing}"


def test_identifier_and_timestamp_formats(clean_result):
    """jsonschema skips `format` unless extra validators are installed, so the
    formats the contract depends on are asserted explicitly rather than assumed."""
    for event in clean_result.events:
        raw = event.to_dict()
        assert UUID_RE.match(raw["event_id"]), raw["event_id"]
        assert UUID_RE.match(raw["correlation_id"]), raw["correlation_id"]
        assert TS_RE.match(raw["event_timestamp"]), raw["event_timestamp"]
        assert TS_RE.match(raw["ingested_at"]), raw["ingested_at"]
        assert SEMVER_RE.match(raw["event_version"])
        if raw["causation_id"] is not None:
            assert UUID_RE.match(raw["causation_id"])


def test_partition_and_correlation_keys(clean_result):
    """Invariants I2 and I3 from event_contract.md section 7.1."""
    for event in clean_result.events:
        payload = event.payload
        if "trip_id" in payload:
            assert event.partition_key == payload["trip_id"], "I2 (trip)"
            assert event.correlation_id == payload["trip_id"], "I3 (trip)"
        else:
            assert event.partition_key == payload["driver_id"], "I2 (presence)"
            assert event.correlation_id == payload["session_id"], "I3 (presence)"


def test_causation_resolves_within_the_same_correlation(clean_result):
    """Invariant C1: a causation_id must reference a real event in the same chain."""
    by_id = {e.event_id: e for e in clean_result.events}
    for event in clean_result.events:
        if event.causation_id is None:
            continue
        parent = by_id.get(event.causation_id)
        assert parent is not None, f"dangling causation on {event.event_type}"
        assert parent.correlation_id == event.correlation_id, "C1 cross-correlation"


def test_ingestion_never_precedes_the_event(clean_result):
    """Invariant T1. Clock skew is an injected anomaly, so a clean stream must
    have no negative lateness at all."""
    for event in clean_result.events:
        assert event.lateness_sec >= 0, f"{event.event_type} lateness={event.lateness_sec}"


def test_events_are_emitted_in_arrival_order(result):
    """The landing zone sees arrival order, so that is what the sink writes."""
    stamps = [e.ingested_at for e in result.events]
    assert stamps == sorted(stamps)
