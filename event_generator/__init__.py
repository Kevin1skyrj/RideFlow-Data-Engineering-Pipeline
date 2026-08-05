"""RideFlow synthetic event generator.

Produces complete, causally-consistent trip lifecycles and driver presence
sessions conforming to docs/event_contract.md v1.0.0.

Distributions are driven by analytics/calibration/calibration_params.json.
Every parameter there is labelled `tlc_calibrated` or `hand_tuned` - see
docs/data_strategy.md for what that distinction means and why it matters.
"""

__version__ = "0.1.0"
