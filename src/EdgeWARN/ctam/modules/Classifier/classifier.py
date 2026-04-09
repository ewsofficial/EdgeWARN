from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from scipy import ndimage

import util.file as fs
from util.grib_loader import load_grib_fast

from ...interface import AnalysisModule
from ...util.json import CTAMJsonManager


REGION_DBZ = 40.0
CORE_DBZ = 45.0
MIN_CORE_PIXELS = 12
MIN_ELONGATION_RATIO = 2.5
MIN_AXIS_COVERAGE = 0.65
MAX_STRAIGHT_RESIDUAL = 0.18
MAX_BOW_RESIDUAL = 0.22
MIN_BOW_DEVIATION = 0.03
MAX_BOW_DEVIATION = 0.35


@dataclass
class LinearObjectMetrics:
    parent_region_id: int
    pixel_count: int
    elongation_ratio: float
    orientation_deg: float
    axis_coverage: float
    straight_residual: float
    bow_residual: float
    bow_deviation: float
    is_straight: bool
    is_bowed: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "parent_region_id": self.parent_region_id,
            "pixel_count": self.pixel_count,
            "elongation_ratio": round(self.elongation_ratio, 3),
            "orientation_deg": round(self.orientation_deg, 2),
            "axis_coverage": round(self.axis_coverage, 3),
            "straight_residual": round(self.straight_residual, 3),
            "bow_residual": round(self.bow_residual, 3),
            "bow_deviation": round(self.bow_deviation, 3),
            "is_straight": self.is_straight,
            "is_bowed": self.is_bowed,
        }


class ClassifierModule(AnalysisModule):
    """CTAM storm-mode classifier with linear convective mode support."""

    def __init__(self) -> None:
        self._scan_cache: Dict[str, Dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return "Classifier"

    def run(
        self,
        storm_entry: Dict[str, Any],
        environment: Optional[Dict[str, Any]] = None,
        history_cache: Optional[Any] = None,
    ) -> None:
        storm_entry.setdefault("modules", {})

        try:
            scan_key = self._scan_key(storm_entry)
            scan_state = self._scan_cache.get(scan_key)
            if scan_state is None:
                scan_state = self._build_scan_state(storm_entry, scan_key)
                self._scan_cache = {scan_key: scan_state}

            classification = self._classify_entry(storm_entry, scan_state)
            storm_entry["classification"] = classification
            storm_entry["modules"][self.name] = {"classification": classification}
        except Exception as exc:
            storm_entry["classification"] = None
            storm_entry["modules"][self.name] = {"classification": None, "error": str(exc)}

    def _classify_entry(self, storm_entry: Dict[str, Any], scan_state: Dict[str, Any]) -> Optional[str]:
        if scan_state["status"] != "success":
            return None

        centroid = storm_entry.get("centroid")
        if not centroid or len(centroid) < 2:
            return None

        row, col = self._nearest_grid_index(scan_state["latitude"], scan_state["longitude"], centroid)
        region_id = int(scan_state["region_labels"][row, col])

        if region_id <= 0:
            return None

        if region_id not in scan_state["occupied_region_ids"]:
            return None

        core_id = int(scan_state["core_labels"][row, col])
        metrics = scan_state["valid_objects"].get(core_id)
        if metrics is None:
            return None

        return "LINEAR"

    def _build_scan_state(self, storm_entry: Dict[str, Any], scan_key: str) -> Dict[str, Any]:
        latest_files = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)
        if not latest_files:
            return {"status": "skipped", "reason": "missing_composite_reflectivity"}

        composite_path = latest_files[-1]
        ds = load_grib_fast(composite_path)
        data_var = "unknown" if "unknown" in ds.data_vars else next(iter(ds.data_vars), None)
        if data_var is None:
            return {"status": "skipped", "reason": "missing_reflectivity_variable"}

        reflectivity = np.asarray(ds[data_var].values, dtype=np.float32)
        if reflectivity.ndim != 2:
            return {"status": "skipped", "reason": "invalid_reflectivity_shape"}

        latitude = np.asarray(ds["latitude"].values)
        longitude = np.asarray(ds["longitude"].values)
        region_mask = np.isfinite(reflectivity) & (reflectivity >= REGION_DBZ)
        region_labels, _ = ndimage.label(region_mask, structure=np.ones((3, 3), dtype=int))

        cells = self._load_scan_cells(storm_entry)
        occupied_region_ids = self._occupied_region_ids(cells, latitude, longitude, region_labels)
        retained_region_mask = np.isin(region_labels, list(occupied_region_ids)) if occupied_region_ids else np.zeros_like(region_labels, dtype=bool)

        core_mask = np.isfinite(reflectivity) & (reflectivity >= CORE_DBZ) & retained_region_mask
        core_labels, core_count = ndimage.label(core_mask, structure=np.ones((3, 3), dtype=int))

        valid_objects: Dict[int, LinearObjectMetrics] = {}
        valid_core_mask = np.zeros_like(core_mask, dtype=bool)

        for core_id in range(1, core_count + 1):
            coords = np.argwhere(core_labels == core_id)
            if coords.shape[0] < MIN_CORE_PIXELS:
                continue

            parent_region_ids = region_labels[core_labels == core_id]
            parent_region_ids = parent_region_ids[parent_region_ids > 0]
            if parent_region_ids.size == 0:
                continue
            parent_region_id = int(np.bincount(parent_region_ids).argmax())
            metrics = self._evaluate_linear_object(coords, parent_region_id)
            if metrics is None:
                continue

            valid_objects[core_id] = metrics
            valid_core_mask[core_labels == core_id] = True

        return {
            "status": "success",
            "reason": None,
            "scan_key": scan_key,
            "composite_path": str(composite_path),
            "latitude": latitude,
            "longitude": longitude,
            "region_labels": region_labels,
            "core_labels": core_labels,
            "occupied_region_ids": occupied_region_ids,
            "valid_objects": valid_objects,
            "valid_core_mask": valid_core_mask,
        }

    def _load_scan_cells(self, storm_entry: Dict[str, Any]) -> List[Dict[str, Any]]:
        timestamp = storm_entry.get("timestamp")
        if not timestamp:
            return [storm_entry]

        scan_identifier = self._timestamp_to_scan_identifier(timestamp)
        if scan_identifier is None:
            return [storm_entry]

        payload = CTAMJsonManager.load_json(scan_identifier)
        if isinstance(payload, dict):
            features = payload.get("features")
            if isinstance(features, list) and features:
                return features

        return [storm_entry]

    @staticmethod
    def _timestamp_to_scan_identifier(timestamp: str) -> Optional[str]:
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).strftime("%Y%m%d-%H%M%S")
        except Exception:
            return None

    def _occupied_region_ids(
        self,
        cells: List[Dict[str, Any]],
        latitude: np.ndarray,
        longitude: np.ndarray,
        region_labels: np.ndarray,
    ) -> set[int]:
        occupied_region_ids: set[int] = set()
        for cell in cells:
            centroid = cell.get("centroid")
            if not centroid or len(centroid) < 2:
                continue
            row, col = self._nearest_grid_index(latitude, longitude, centroid)
            region_id = int(region_labels[row, col])
            if region_id > 0:
                occupied_region_ids.add(region_id)
        return occupied_region_ids

    @staticmethod
    def _nearest_grid_index(latitude: np.ndarray, longitude: np.ndarray, centroid: List[float]) -> Tuple[int, int]:
        lat = float(centroid[0])
        lon = float(centroid[1])
        if longitude.ndim == 1:
            lon_mod = lon % 360.0
            if np.nanmax(longitude) <= 180.0 and lon_mod > 180.0:
                lon_mod -= 360.0
            row = int(np.argmin(np.abs(latitude - lat)))
            col = int(np.argmin(np.abs(longitude - lon_mod)))
            return row, col

        lon_grid = np.where(longitude > 180.0, longitude - 360.0, longitude)
        lon_mod = lon if np.nanmax(lon_grid) <= 180.0 else lon % 360.0
        dist = np.square(latitude - lat) + np.square(lon_grid - lon_mod)
        row, col = np.unravel_index(int(np.argmin(dist)), dist.shape)
        return int(row), int(col)

    def _evaluate_linear_object(self, coords: np.ndarray, parent_region_id: int) -> Optional[LinearObjectMetrics]:
        elongation_ratio, orientation_deg, principal_axis, centered = self._pca_metrics(coords)
        if elongation_ratio < MIN_ELONGATION_RATIO:
            return None

        axis_projection = centered @ principal_axis
        ortho_axis = np.array([-principal_axis[1], principal_axis[0]])
        cross_projection = centered @ ortho_axis

        axis_coverage = self._axis_coverage(axis_projection)
        straight_residual = self._normalized_straight_residual(cross_projection, axis_projection)
        bow_residual, bow_deviation = self._normalized_bow_metrics(axis_projection, cross_projection)

        is_straight = axis_coverage >= MIN_AXIS_COVERAGE and straight_residual <= MAX_STRAIGHT_RESIDUAL
        is_bowed = axis_coverage >= MIN_AXIS_COVERAGE and bow_residual <= MAX_BOW_RESIDUAL and MIN_BOW_DEVIATION <= bow_deviation <= MAX_BOW_DEVIATION
        if not (is_straight or is_bowed):
            return None

        return LinearObjectMetrics(
            parent_region_id=parent_region_id,
            pixel_count=int(coords.shape[0]),
            elongation_ratio=float(elongation_ratio),
            orientation_deg=float(orientation_deg),
            axis_coverage=float(axis_coverage),
            straight_residual=float(straight_residual),
            bow_residual=float(bow_residual),
            bow_deviation=float(bow_deviation),
            is_straight=bool(is_straight),
            is_bowed=bool(is_bowed),
        )

    @staticmethod
    def _pca_metrics(coords: np.ndarray) -> Tuple[float, float, np.ndarray, np.ndarray]:
        points = coords.astype(np.float64)
        centered = points - points.mean(axis=0)
        cov = np.cov(centered, rowvar=False)
        eigvals, eigvecs = np.linalg.eigh(cov)
        order = np.argsort(eigvals)[::-1]
        eigvals = eigvals[order]
        eigvecs = eigvecs[:, order]

        lambda1 = float(max(eigvals[0], 0.0))
        lambda2 = float(max(eigvals[1], 1.0e-6))
        ratio = lambda1 / lambda2
        principal_axis = eigvecs[:, 0]
        orientation_deg = float(np.degrees(np.arctan2(principal_axis[0], principal_axis[1])) % 180.0)
        return ratio, orientation_deg, principal_axis, centered

    @staticmethod
    def _axis_coverage(axis_projection: np.ndarray) -> float:
        if axis_projection.size == 0:
            return 0.0
        span = float(np.max(axis_projection) - np.min(axis_projection))
        if span <= 1.0e-6:
            return 0.0
        bin_count = max(5, min(12, int(np.sqrt(axis_projection.size)) + 1))
        hist, _ = np.histogram(axis_projection, bins=bin_count)
        return float(np.count_nonzero(hist) / hist.size)

    @staticmethod
    def _normalized_straight_residual(cross_projection: np.ndarray, axis_projection: np.ndarray) -> float:
        span = float(np.max(axis_projection) - np.min(axis_projection))
        if span <= 1.0e-6:
            return float("inf")
        return float(np.sqrt(np.mean(np.square(cross_projection))) / span)

    @staticmethod
    def _normalized_bow_metrics(axis_projection: np.ndarray, cross_projection: np.ndarray) -> Tuple[float, float]:
        span = float(np.max(axis_projection) - np.min(axis_projection))
        if axis_projection.size < 3 or span <= 1.0e-6:
            return float("inf"), 0.0

        fit_axis = axis_projection / span
        coeffs = np.polyfit(fit_axis, cross_projection / span, deg=2)
        fitted = np.polyval(coeffs, fit_axis)
        residual = float(np.sqrt(np.mean(np.square((cross_projection / span) - fitted))))
        bow_deviation = float(np.max(fitted) - np.min(fitted))
        return residual, bow_deviation

    def _scan_key(self, storm_entry: Dict[str, Any]) -> str:
        timestamp = storm_entry.get("timestamp")
        if timestamp:
            return timestamp
        cell_id = storm_entry.get("id", "unknown")
        return f"cell:{cell_id}"
