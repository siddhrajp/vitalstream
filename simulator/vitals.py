"""Simulated physiology.

Each metric follows a mean-reverting random walk: every step it takes a small random jump, then gets
pulled part of the way back toward the patient's personal baseline. That gives values that drift
smoothly and plausibly (heart rate wanders 68 -> 71 -> 70 -> 74 ...) instead of jumping at random.
"""

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class MetricModel:
    baseline: float   # population-typical resting value
    spread: float     # how far one patient's own baseline may sit from it (std dev)
    noise: float      # size of each random step (std dev)
    reversion: float  # fraction of the gap to the baseline closed per step, 0..1
    low: float        # physiological clamp
    high: float
    decimals: int


MODELS = {
    "HEART_RATE":   MetricModel(baseline=72,   spread=8,   noise=2.0,  reversion=0.10, low=35, high=190, decimals=0),
    "SPO2":         MetricModel(baseline=97.5, spread=1,   noise=0.4,  reversion=0.20, low=80, high=100, decimals=1),
    "BP_SYSTOLIC":  MetricModel(baseline=120,  spread=10,  noise=3.0,  reversion=0.10, low=80, high=210, decimals=0),
    "BP_DIASTOLIC": MetricModel(baseline=78,   spread=6,   noise=2.0,  reversion=0.10, low=45, high=130, decimals=0),
    "TEMPERATURE":  MetricModel(baseline=36.8, spread=0.3, noise=0.05, reversion=0.10, low=34, high=42,  decimals=1),
    "GLUCOSE":      MetricModel(baseline=100,  spread=12,  noise=4.0,  reversion=0.05, low=40, high=350, decimals=0),
}

# What each kind of device measures (names match api-service's DeviceType enum).
METRICS_BY_DEVICE_TYPE = {
    "HEART_RATE_MONITOR": ["HEART_RATE"],
    "PULSE_OXIMETER": ["SPO2", "HEART_RATE"],
    "BLOOD_PRESSURE_CUFF": ["BP_SYSTOLIC", "BP_DIASTOLIC"],
    "THERMOMETER": ["TEMPERATURE"],
    "GLUCOSE_METER": ["GLUCOSE"],
}


class DeviceSignal:
    """The evolving readings of one device attached to one patient."""

    def __init__(self, device_type: str, rng: random.Random):
        self.rng = rng
        self.metrics = METRICS_BY_DEVICE_TYPE[device_type]
        # Each patient gets a personal baseline, e.g. one patient rests at 64 bpm, another at 81.
        self.baselines = {m: rng.gauss(MODELS[m].baseline, MODELS[m].spread) for m in self.metrics}
        self.current = dict(self.baselines)

    def next(self) -> list[tuple[str, float]]:
        """Advance one step and return [(metric, value), ...] for everything this device measures."""
        readings = []
        for metric in self.metrics:
            model = MODELS[metric]
            value = self.current[metric]
            value += model.reversion * (self.baselines[metric] - value) + self.rng.gauss(0, model.noise)
            value = min(max(value, model.low), model.high)
            self.current[metric] = value
            readings.append((metric, round(value, model.decimals)))
        return readings
