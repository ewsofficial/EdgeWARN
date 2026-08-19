"""Typed detection configuration read from ``config/detection.yaml``.

``DetectionConfig`` is built once at an entrypoint and threaded explicitly down
through the detection pipeline. That is what lets the three CLI-overridable
thresholds keep CLI > env > YAML precedence: the entrypoint has already resolved
them against ``argparse``, and every function below the entrypoint takes the
resolved object rather than re-deriving a default of its own.

``section()`` is the accessor for leaf modules (morphology, the cell saver, the
alert matcher) that have no config parameter to thread. It is memoized because
``load_config`` re-resolves the config root on every call, and these modules read
config from inside per-cell loops.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Optional

from common.config import loader as config_loader

_CONFIG_NAME = "detection"


@lru_cache(maxsize=None)
def section(name: str, config_dir: Optional[str] = None) -> Any:
    """Frozen view of one top-level section of ``detection.yaml``."""
    return config_loader.load_config(_CONFIG_NAME, config_dir=config_dir)[name]


def reset_cache() -> None:
    """Clear memoized sections. Intended for tests, alongside loader.reset_cache."""
    section.cache_clear()


@dataclass(frozen=True)
class GateMapperConfig:
    """Gate expansion and contour tracing policy.

    ``baseline_refl_floor`` and the ``dynamic_*``/``max_refl_clamp`` values are
    the terms of the per-cell threshold curve
    ``max(where(m < switch, low, high), min(m, clamp) - drop_offset)``. They are
    configurable only because the code below now reads every term; changing one
    in isolation still shifts the whole curve.
    """

    baseline_refl_floor: float
    dynamic_switch_max_refl: float
    dynamic_min_threshold_low: float
    dynamic_min_threshold_high: float
    max_refl_clamp: float
    crop_pad_px: int
    reject_clusters_at_or_below_gates: int
    contour_downsample: int
    contour_keep_all_below_points: int
    contour_coarse_below_points: int
    contour_keep_all_step: int
    contour_coarse_step: int
    contour_level: float
    coordinate_decimals: int

    @classmethod
    def from_section(cls, data: Any) -> "GateMapperConfig":
        dynamic = data["dynamic_min_threshold"]
        adaptive = data["contour_downsample_adaptive"]
        return cls(
            baseline_refl_floor=data["baseline_refl_floor"],
            dynamic_switch_max_refl=dynamic["switch_max_refl"],
            dynamic_min_threshold_low=dynamic["low"],
            dynamic_min_threshold_high=dynamic["high"],
            max_refl_clamp=data["max_refl_clamp"],
            crop_pad_px=data["crop_pad_px"],
            reject_clusters_at_or_below_gates=data["reject_clusters_at_or_below_gates"],
            contour_downsample=data["contour_downsample"],
            contour_keep_all_below_points=adaptive["keep_all_below_points"],
            contour_coarse_below_points=adaptive["coarse_below_points"],
            contour_keep_all_step=adaptive["keep_all_step"],
            contour_coarse_step=adaptive["coarse_step"],
            contour_level=data["contour_level"],
            coordinate_decimals=data["coordinate_decimals"],
        )


@dataclass(frozen=True)
class DetectionConfig:
    """Detection settings for one pipeline run.

    Frozen and scalar-only so it can cross a ``multiprocessing`` spawn boundary
    without the child re-reading, and re-validating, the YAML.
    """

    refl_threshold: float
    min_seed_percentage: float
    drop_offset: float
    stormcell_cleanup_max_age_minutes: int
    fallback_dt_seconds: float
    tracking_disabled_mode: str
    tracking_disabled_prediction_count: int
    tracking_disabled_event_type: str
    dataset_load_max_workers: int
    gatemapper: GateMapperConfig

    @classmethod
    def from_yaml(
        cls,
        *,
        config_dir: Optional[str] = None,
        refl_threshold: Optional[float] = None,
        min_seed_percentage: Optional[float] = None,
        drop_offset: Optional[float] = None,
    ) -> "DetectionConfig":
        """Load from YAML, letting already-resolved CLI values win.

        ``None`` means "the operator did not supply this", matching the sentinel
        convention in ``common.config.overlay``.
        """
        document = config_loader.load_config(_CONFIG_NAME, config_dir=config_dir)
        detection = document["detection"]
        disabled = detection["tracking_disabled_defaults"]
        return cls(
            refl_threshold=(
                detection["refl_threshold"] if refl_threshold is None else float(refl_threshold)
            ),
            min_seed_percentage=(
                detection["min_seed_percentage"]
                if min_seed_percentage is None
                else float(min_seed_percentage)
            ),
            drop_offset=(
                detection["drop_offset"] if drop_offset is None else float(drop_offset)
            ),
            stormcell_cleanup_max_age_minutes=detection["stormcell_cleanup_max_age_minutes"],
            fallback_dt_seconds=detection["fallback_dt_seconds"],
            tracking_disabled_mode=disabled["tracking_mode"],
            tracking_disabled_prediction_count=disabled["prediction_count"],
            tracking_disabled_event_type=disabled["event_type"],
            dataset_load_max_workers=detection["dataset_load_max_workers"],
            gatemapper=GateMapperConfig.from_section(document["gatemapper"]),
        )
