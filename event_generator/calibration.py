"""Calibration parameters and seeded sampling helpers.

All randomness in the generator flows through the `Sampler` here, which wraps a
single seeded `random.Random`. Nothing calls the module-level `random` functions
and nothing calls `uuid.uuid4()` - neither is seedable, and either would break
the determinism guarantee in testing_strategy.md section 2.1.
"""

from __future__ import annotations

import json
import math
import random
import uuid
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from event_generator.config import CALIBRATION_PATH


class Calibration:
    """Read-only accessor over calibration_params.json."""

    def __init__(self, params: Mapping[str, Any]) -> None:
        self._params = params

    @classmethod
    def load(cls, path: Path | None = None) -> Calibration:
        target = path or CALIBRATION_PATH
        if not target.exists():
            raise FileNotFoundError(f"Calibration params missing: {target}")
        with target.open(encoding="utf-8") as fh:
            return cls(json.load(fh))

    def __getitem__(self, key: str) -> Any:
        return self._params[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._params.get(key, default)

    def sources(self) -> dict[str, str]:
        """Every top-level block mapped to its provenance label.

        Used by the calibration-provenance test: a block whose source is
        unlabelled would let 'calibrated against real data' silently cover
        parameters that were invented (data_strategy.md section 7).
        """
        return {
            key: value["source"]
            for key, value in self._params.items()
            if isinstance(value, dict) and "source" in value
        }


class Sampler:
    """Seeded sampling. One instance per generation run."""

    def __init__(self, seed: int) -> None:
        self._rng = random.Random(seed)

    # ---- primitives -------------------------------------------------------

    def uuid(self) -> str:
        """Deterministic UUIDv4-shaped identifier.

        uuid.uuid4() draws from os.urandom and cannot be seeded, so it is never
        used here. This derives the 128 bits from the seeded RNG instead.
        """
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def random(self) -> float:
        return self._rng.random()

    def chance(self, probability: float) -> bool:
        return self._rng.random() < probability

    def uniform(self, low: float, high: float) -> float:
        return self._rng.uniform(low, high)

    def randint(self, low: int, high: int) -> int:
        return self._rng.randint(low, high)

    def choice(self, seq: Sequence[Any]) -> Any:
        return self._rng.choice(seq)

    def weighted_choice(self, weights: Mapping[str, float]) -> str:
        """Pick a key by weight. Keys are sorted first so the draw does not
        depend on dict insertion order."""
        keys = sorted(weights)
        values = [weights[k] for k in keys]
        return self._rng.choices(keys, weights=values, k=1)[0]

    def weighted_index(self, weights: Sequence[float]) -> int:
        return self._rng.choices(range(len(weights)), weights=list(weights), k=1)[0]

    def weighted_pick(self, items: Sequence[Any], weights: Sequence[float]) -> Any:
        return self._rng.choices(list(items), weights=list(weights), k=1)[0]

    def shuffled(self, items: Iterable[Any]) -> list[Any]:
        out = list(items)
        self._rng.shuffle(out)
        return out

    # ---- distributions ----------------------------------------------------

    def lognormal(self, mu: float, sigma: float, lo: float, hi: float) -> float:
        """Log-normal draw clamped to [lo, hi].

        Clamping rather than rejection-sampling keeps the number of RNG draws
        per call fixed at one, which keeps the whole run reproducible even if a
        bound is later widened.
        """
        return min(max(math.exp(self._rng.gauss(mu, sigma)), lo), hi)

    def normal(self, mean: float, stddev: float, lo: float, hi: float) -> float:
        return min(max(self._rng.gauss(mean, stddev), lo), hi)

    def from_spec(self, spec: Mapping[str, Any]) -> float:
        """Sample from a {'distribution': ...} block in calibration_params."""
        kind = spec.get("distribution", "uniform")
        if kind == "lognormal":
            return self.lognormal(spec["mu"], spec["sigma"], spec["min"], spec["max"])
        if kind == "normal":
            return self.normal(spec["mean"], spec["stddev"], spec["min"], spec["max"])
        if kind == "uniform":
            return self.uniform(spec["min"], spec["max"])
        raise ValueError(f"Unsupported distribution: {kind}")

    def int_from_spec(self, spec: Mapping[str, Any]) -> int:
        return round(self.from_spec(spec))
