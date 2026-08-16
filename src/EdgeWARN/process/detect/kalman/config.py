"""Typed Kalman tracking configuration read from ``config/kalman.yaml``.

Every value here has exactly one base default and it lives in the YAML. The
dataclasses declare no field defaults and the loader is never given a fallback,
so a missing file or key is a startup error rather than a silent substitution of
a second copy of the number.

The YAML's ``filter_internals``, ``confidence`` and ``assignment_costs`` sections
are sibling top-level sections but are modelled here as nested members of the
config object whose consumer reads them, so a caller threads one object rather
than four.

``default_*_config`` are memoized because the module-level singletons they
replace were evaluated at import time; deferring the read keeps YAML I/O out of
import order while still parsing once per process.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional

from common.config import loader as config_loader

_CONFIG_NAME = "kalman"


def _document(config_dir: Optional[str]) -> Any:
    return config_loader.load_config(_CONFIG_NAME, config_dir=config_dir)


@dataclass(frozen=True)
class FilterInternalsConfig:
    """Numerical policy for the filter's initial state and matrix conditioning."""

    initial_position_uncertainty_km: float
    innovation_covariance_regularization: float
    singular_retry_regularization: float
    initial_velocity_variance: float
    initial_acceleration_variance: float

    @classmethod
    def from_section(cls, data: Any) -> "FilterInternalsConfig":
        return cls(
            initial_position_uncertainty_km=data["initial_position_uncertainty_km"],
            innovation_covariance_regularization=data["innovation_covariance_regularization"],
            singular_retry_regularization=data["singular_retry_regularization"],
            initial_velocity_variance=data["initial_velocity_variance"],
            initial_acceleration_variance=data["initial_acceleration_variance"],
        )


@dataclass(frozen=True)
class ConfidenceConfig:
    """Confidence decay shape and the display bands over the resulting score.

    ``high_boundary``/``medium_boundary`` label a score for operators and are
    deliberately separate keys from ``TrackingConfig.confidence_decay_factor``
    and ``confidence_threshold`` even though they currently hold the same
    numbers: one pair drives termination, the other only names a band.
    """

    time_penalty_weight: float
    motion_factor_variance_denominator: float
    factor_floor: float
    position_decay_onset_std: float
    position_decay_scale: float
    high_boundary: float
    medium_boundary: float

    @classmethod
    def from_section(cls, data: Any) -> "ConfidenceConfig":
        return cls(
            time_penalty_weight=data["time_penalty_weight"],
            motion_factor_variance_denominator=data["motion_factor_variance_denominator"],
            factor_floor=data["factor_floor"],
            position_decay_onset_std=data["position_decay_onset_std"],
            position_decay_scale=data["position_decay_scale"],
            high_boundary=data["high_boundary"],
            medium_boundary=data["medium_boundary"],
        )


@dataclass(frozen=True)
class AssignmentCostsConfig:
    """Deadbands and caps applied to the individual assignment cost terms."""

    default_dt_seconds: float
    predicted_speed_deadband_ms: float
    implied_speed_deadband_ms: float
    reflectivity_diff_cap: float
    size_ratio_log2_divisor: float
    size_cost_cap: float

    @classmethod
    def from_section(cls, data: Any) -> "AssignmentCostsConfig":
        return cls(
            default_dt_seconds=data["default_dt_seconds"],
            predicted_speed_deadband_ms=data["predicted_speed_deadband_ms"],
            implied_speed_deadband_ms=data["implied_speed_deadband_ms"],
            reflectivity_diff_cap=data["reflectivity_diff_cap"],
            size_ratio_log2_divisor=data["size_ratio_log2_divisor"],
            size_cost_cap=data["size_cost_cap"],
        )


@dataclass(frozen=True)
class KalmanConfig:
    """Process and measurement noise for the storm-cell Kalman filter."""

    process_noise_position: float
    process_noise_velocity: float
    process_noise_acceleration: float
    measurement_noise_position: float
    internals: FilterInternalsConfig

    @classmethod
    def from_yaml(cls, *, config_dir: Optional[str] = None) -> "KalmanConfig":
        document = _document(config_dir)
        kalman_filter = document["kalman_filter"]
        process_noise = kalman_filter["process_noise"]
        measurement_noise = kalman_filter["measurement_noise"]
        return cls(
            process_noise_position=process_noise["position"],
            process_noise_velocity=process_noise["velocity"],
            process_noise_acceleration=process_noise["acceleration"],
            measurement_noise_position=measurement_noise["position"],
            internals=FilterInternalsConfig.from_section(document["filter_internals"]),
        )


@dataclass(frozen=True)
class TrackingConfig:
    """Prediction-mode limits and the confidence policy applied within them."""

    max_prediction_time_minutes: float
    reacquisition_radius_km: float
    confidence_threshold: float
    confidence_decay_factor: float
    confidence: ConfidenceConfig

    @classmethod
    def from_yaml(cls, *, config_dir: Optional[str] = None) -> "TrackingConfig":
        document = _document(config_dir)
        tracking = document["tracking"]
        return cls(
            max_prediction_time_minutes=tracking["max_prediction_time_minutes"],
            reacquisition_radius_km=tracking["reacquisition_radius_km"],
            confidence_threshold=tracking["confidence_threshold"],
            confidence_decay_factor=tracking["confidence_decay_factor"],
            confidence=ConfidenceConfig.from_section(document["confidence"]),
        )


@dataclass(frozen=True)
class AssignmentConfig:
    """Track-to-detection assignment gating, weights and algorithm selection."""

    prefilter_radius_km: float
    gating_threshold: float
    min_gating_radius_km: float
    weight_position: float
    weight_velocity: float
    weight_shape: float
    method: str
    costs: AssignmentCostsConfig

    @classmethod
    def from_yaml(cls, *, config_dir: Optional[str] = None) -> "AssignmentConfig":
        document = _document(config_dir)
        assignment = document["assignment"]
        weights = assignment["weights"]
        return cls(
            prefilter_radius_km=assignment["prefilter_radius_km"],
            gating_threshold=assignment["gating_threshold"],
            min_gating_radius_km=assignment["min_gating_radius_km"],
            weight_position=weights["position"],
            weight_velocity=weights["velocity_direction"],
            weight_shape=weights["size_similarity"],
            method=assignment["method"],
            costs=AssignmentCostsConfig.from_section(document["assignment_costs"]),
        )


@lru_cache(maxsize=None)
def default_kalman_config(config_dir: Optional[str] = None) -> KalmanConfig:
    return KalmanConfig.from_yaml(config_dir=config_dir)


@lru_cache(maxsize=None)
def default_tracking_config(config_dir: Optional[str] = None) -> TrackingConfig:
    return TrackingConfig.from_yaml(config_dir=config_dir)


@lru_cache(maxsize=None)
def default_assignment_config(config_dir: Optional[str] = None) -> AssignmentConfig:
    return AssignmentConfig.from_yaml(config_dir=config_dir)


def reset_cache() -> None:
    """Clear memoized configs. Intended for tests, alongside loader.reset_cache."""
    default_kalman_config.cache_clear()
    default_tracking_config.cache_clear()
    default_assignment_config.cache_clear()
