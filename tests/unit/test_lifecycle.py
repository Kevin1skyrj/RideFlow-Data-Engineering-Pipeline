"""Trip lifecycle: sequence rules S1-S8 and temporal invariants T2.

These run against the anomaly-free stream. A clean chain must be legal by
construction; if it is not, the business logic is producing data its own
contract forbids, and no amount of downstream handling can fix that.
"""

from __future__ import annotations

from decimal import Decimal

TERMINAL = {"RideCancelled", "PaymentCompleted"}


def test_every_trip_starts_with_ride_requested(trips_by_id):
    """Rule S1."""
    for trip_id, chain in trips_by_id.items():
        assert chain[0].event_type == "RideRequested", f"{trip_id}: {[e.event_type for e in chain]}"


def test_completed_requires_started(trips_by_id):
    """Rule S2."""
    for trip_id, chain in trips_by_id.items():
        types = [e.event_type for e in chain]
        if "RideCompleted" in types:
            assert "RideStarted" in types, trip_id
            assert types.index("RideStarted") < types.index("RideCompleted"), trip_id


def test_payment_requires_completion(trips_by_id):
    """Rule S3."""
    for trip_id, chain in trips_by_id.items():
        types = [e.event_type for e in chain]
        if "PaymentCompleted" in types:
            assert "RideCompleted" in types, trip_id


def test_completed_and_cancelled_are_mutually_exclusive(trips_by_id):
    """Rule S4 - the rule that makes the maximum event count 6, not 7."""
    for trip_id, chain in trips_by_id.items():
        types = {e.event_type for e in chain}
        assert not ("RideCompleted" in types and "RideCancelled" in types), trip_id


def test_cancellation_is_terminal(trips_by_id):
    """Rule S5."""
    for trip_id, chain in trips_by_id.items():
        types = [e.event_type for e in chain]
        if "RideCancelled" in types:
            assert types.index("RideCancelled") == len(types) - 1, trip_id


def test_arrival_and_start_require_acceptance(trips_by_id):
    """Rule S6."""
    for trip_id, chain in trips_by_id.items():
        types = [e.event_type for e in chain]
        for dependent in ("DriverArrived", "RideStarted"):
            if dependent in types:
                assert "RideAccepted" in types, trip_id
                assert types.index("RideAccepted") < types.index(dependent), trip_id


def test_driver_id_is_constant_after_acceptance(trips_by_id):
    """Rule S7."""
    for trip_id, chain in trips_by_id.items():
        drivers = {e.payload["driver_id"] for e in chain if e.payload.get("driver_id") is not None}
        assert len(drivers) <= 1, f"{trip_id} had multiple drivers: {drivers}"


def test_driver_id_is_null_only_where_permitted(trips_by_id):
    """Invariant R2: null driver_id is meaningful absence, not missing data."""
    for trip_id, chain in trips_by_id.items():
        for event in chain:
            if event.event_type in {"RideRequested"}:
                continue
            if event.event_type == "RideCancelled":
                if event.payload["driver_id"] is None:
                    assert event.payload["cancelled_at_status"] == "REQUESTED", trip_id
                continue
            assert event.payload.get("driver_id") is not None, f"{trip_id} {event.event_type}"


def test_lifecycle_timestamps_are_monotonic(trips_by_id):
    """Invariant T2."""
    order = [
        "RideRequested",
        "RideAccepted",
        "DriverArrived",
        "RideStarted",
        "RideCompleted",
        "PaymentCompleted",
    ]
    field = {
        "RideRequested": "requested_at",
        "RideAccepted": "accepted_at",
        "DriverArrived": "arrived_at",
        "RideStarted": "started_at",
        "RideCompleted": "completed_at",
        "PaymentCompleted": "paid_at",
    }
    for trip_id, chain in trips_by_id.items():
        stamps = [
            e.payload[field[e.event_type]]
            for e in sorted(
                chain, key=lambda e: order.index(e.event_type) if e.event_type in order else 99
            )
            if e.event_type in field
        ]
        assert stamps == sorted(stamps), f"{trip_id} timestamps out of order"


def test_stored_durations_match_their_own_timestamps(trips_by_id):
    """A stored derived value that contradicts its inputs is worse than not
    storing it - it creates two sources of truth that disagree."""
    for trip_id, chain in trips_by_id.items():
        by_type = {e.event_type: e.payload for e in chain}

        if "RideAccepted" in by_type and "RideRequested" in by_type:
            delta = (
                by_type["RideAccepted"]["accepted_at"] - by_type["RideRequested"]["requested_at"]
            ).total_seconds()
            assert abs(delta - by_type["RideAccepted"]["matching_duration_sec"]) < 1, trip_id

        if "DriverArrived" in by_type:
            arrived = by_type["DriverArrived"]
            delta = (arrived["arrived_at"] - by_type["RideAccepted"]["accepted_at"]).total_seconds()
            assert abs(delta - arrived["actual_pickup_duration_sec"]) < 1, trip_id
            expected_delay = (
                arrived["actual_pickup_duration_sec"] - by_type["RideAccepted"]["eta_to_pickup_sec"]
            )
            assert arrived["arrival_delay_sec"] == expected_delay, trip_id
            assert arrived["is_late_arrival"] == (arrived["arrival_delay_sec"] > 180), trip_id

        if "RideCompleted" in by_type:
            delta = (
                by_type["RideCompleted"]["completed_at"] - by_type["RideStarted"]["started_at"]
            ).total_seconds()
            assert abs(delta - by_type["RideCompleted"]["duration_sec"]) < 1, trip_id


def test_surge_is_identical_in_request_and_completion(trips_by_id):
    """Invariant F7: proof the rider was charged the rate they were quoted."""
    for trip_id, chain in trips_by_id.items():
        by_type = {e.event_type: e.payload for e in chain}
        if "RideCompleted" in by_type:
            assert (
                by_type["RideRequested"]["surge_multiplier"]
                == by_type["RideCompleted"]["surge_multiplier"]
            ), trip_id


def test_payment_amount_reconciles(trips_by_id):
    """Invariant F3."""
    for trip_id, chain in trips_by_id.items():
        by_type = {e.event_type: e.payload for e in chain}
        if "PaymentCompleted" not in by_type:
            continue
        pay = by_type["PaymentCompleted"]
        expected = pay["trip_fare"] + pay["tip_amount"] - pay["discount_amount"]
        assert abs(expected - pay["amount_charged"]) <= Decimal("0.01"), trip_id
        assert pay["amount_charged"] > 0, trip_id
        assert pay["trip_fare"] == by_type["RideCompleted"]["total_fare"], trip_id


def test_payment_failure_reason_tracks_attempt_number(trips_by_id):
    for chain in trips_by_id.values():
        for event in chain:
            if event.event_type != "PaymentCompleted":
                continue
            has_reason = event.payload["previous_attempt_failure_reason"] is not None
            assert has_reason == (event.payload["attempt_number"] > 1)


def test_promo_code_and_discount_agree(trips_by_id):
    for chain in trips_by_id.values():
        for event in chain:
            if event.event_type != "PaymentCompleted":
                continue
            has_promo = event.payload["promo_code"] is not None
            assert has_promo == (event.payload["discount_amount"] > 0)


def test_driver_sessions_pair_correctly(clean_result):
    """Invariant R3, and the open-session rule from event_contract.md 5.9."""
    online = {}
    offline = {}
    for event in clean_result.events:
        if event.event_type == "DriverOnline":
            online[event.payload["session_id"]] = event
        elif event.event_type == "DriverOffline":
            offline[event.payload["session_id"]] = event

    for session_id, event in offline.items():
        assert session_id in online, f"orphaned DriverOffline {session_id}"
        assert event.payload["offline_at"] > event.payload["online_at"]
        expected = (event.payload["offline_at"] - event.payload["online_at"]).total_seconds()
        assert abs(expected - event.payload["session_duration_sec"]) < 1

    open_sessions = set(online) - set(offline)
    assert open_sessions, "open sessions must occur - they are legitimate, not an error"


def test_a_realistic_share_of_trips_complete(clean_result):
    """Guards the supply model.

    An undersupplied marketplace silently inflates cancellations and depresses
    completions, which makes every funnel metric wrong in the same direction.
    Real ride-hailing completion sits around 85-90%.
    """
    rate = clean_result.trips_completed / clean_result.trips_requested
    assert 0.78 <= rate <= 0.95, f"completion rate {rate:.2%} is not plausible"


def test_cancellations_occur_at_every_stage(clean_result):
    """A funnel with drop-off at only one stage cannot answer the conversion
    question the warehouse exists for."""
    stages = {
        e.payload["cancelled_at_status"]
        for e in clean_result.events
        if e.event_type == "RideCancelled"
    }
    assert {"REQUESTED", "MATCHED"} <= stages, f"only saw {stages}"
