"""
FLOHAR (FLOod HAzaRds) CTAM Grid Module

GridAnalysisModule implementation that loads MRMS FLASH GRIB files,
computes composite threat scores, extracts flood threat regions,
and produces GeoJSON output with alert integration.
"""

import json
import numpy as np
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from EdgeWARN.core.ctam.interface import GridAnalysisModule
from EdgeWARN.core.alerts.schema import AlertPayload

from .engine import compute_threat_grid
from .regions import extract_regions
from . import config as cfg

import util.file as fs


class FLOHARModule(GridAnalysisModule):
    """
    FLOHAR — FLOod HAzaRds detection module.

    Grid-based flash flood detection that operates on MRMS FLASH products
    to identify flood threat regions. Runs independently of storm cells
    via the CTAM Grid Module architecture.
    """

    @property
    def name(self) -> str:
        return "FLOHAR"

    def run(self) -> Dict[str, Any]:
        """
        Run flood hazard detection on FLASH GRIB files.

        Returns:
            Dict with:
                - 'features': GeoJSON FeatureCollection
                - 'metadata': Processing metadata
                - 'timestamp': Analysis timestamp (ISO 8601)
        """
        timestamp = datetime.utcnow()

        # Load FLASH GRIB files
        grids = self._load_grids()
        if grids is None:
            print("[FLOHAR] No valid FLASH grids available — skipping.")
            return {
                "features": {"type": "FeatureCollection", "features": []},
                "metadata": {"error": "Failed to load grids", "region_count": 0},
                "timestamp": timestamp.isoformat(),
            }

        # Remove coordinates so they aren't passed to the core engine
        lat_coords = grids.pop("latitude")
        lon_coords = grids.pop("longitude")

        # Compute threat scores (grids dict is consumed destructively to save memory)
        threat_grid, rainfall_grid, hydro_grid, ffg_grid = compute_threat_grid(grids)


        # Extract regions
        pillar_grids = {
            "rainfall": rainfall_grid,
            "hydro": hydro_grid,
            "ffg": ffg_grid,
        }

        regions = extract_regions(
            threat_grid,
            lat_coords,
            lon_coords,
            pillar_grids,
            threshold=cfg.THREAT_THRESHOLD,
            min_area_km2=cfg.MIN_REGION_AREA_KM2,
            max_regions=cfg.MAX_REGIONS,
            simplify_tolerance=cfg.POLYGON_SIMPLIFY_TOLERANCE,
        )

        # Convert to GeoJSON FeatureCollection
        features = [self._region_to_feature(r, timestamp) for r in regions]
        feature_collection = {
            "type": "FeatureCollection",
            "features": features,
        }

        # Save to disk
        self._save_geojson(feature_collection, timestamp)

        max_score = max((r["peak_score"] for r in regions), default=0)
        print(f"[FLOHAR] Detected {len(regions)} region(s), max score: {max_score}")

        return {
            "features": feature_collection,
            "metadata": {
                "region_count": len(regions),
                "max_threat_score": max_score,
                "grid_shape": list(threat_grid.shape),
            },
            "timestamp": timestamp.isoformat(),
        }

    # ── Alert generation ────────────────────────────────────────────

    def alerts(self, features: List[Dict[str, Any]]) -> Optional[List[AlertPayload]]:
        """
        Generate alert payloads for regions above advisory threshold.

        Alert priority:
            - Emergency (score >= 75): High priority — "Flash Flood Warning"
            - Warning (score 50–74):   Medium priority — "Flash Flood Watch"
            - Advisory (score 25–49):  No alert (monitor only)

        Args:
            features: List of GeoJSON feature dicts from run() output.

        Returns:
            List of AlertPayload objects, or None if no alerts.
        """
        alerts = []
        now = datetime.utcnow()
        expiry = now + timedelta(minutes=30)

        for feature in features:
            props = feature.get("properties", {})
            severity = props.get("severity", "none")
            peak_score = props.get("peak_score", 0)
            region_id = props.get("region_id", 0)

            # Extract polygon geometry
            geom = feature.get("geometry", {})
            coords = geom.get("coordinates", [[]])
            # Convert [lon, lat] → (lat, lon) tuples for AlertPayload
            polygon = [(c[1], c[0]) for c in coords[0]] if coords and coords[0] else []

            if severity == "emergency":
                alerts.append(AlertPayload(
                    alert_type="flash_flood",
                    source="FLOHAR",
                    cell_id=f"flohar_region_{region_id}",
                    geometry=polygon,
                    effective_time=now,
                    expiry_time=expiry,
                    severity="Extreme",
                    threats={
                        "event": "Flash Flood Warning",
                        "peak_score": peak_score,
                        "area_km2": props.get("area_km2"),
                        "pillar_peaks": props.get("pillar_peaks"),
                    },
                ))
            elif severity == "warning":
                alerts.append(AlertPayload(
                    alert_type="flash_flood",
                    source="FLOHAR",
                    cell_id=f"flohar_region_{region_id}",
                    geometry=polygon,
                    effective_time=now,
                    expiry_time=expiry,
                    severity="Moderate",
                    threats={
                        "event": "Flash Flood Watch",
                        "peak_score": peak_score,
                        "area_km2": props.get("area_km2"),
                        "pillar_peaks": props.get("pillar_peaks"),
                    },
                ))

        return alerts if alerts else None

    # ── Grid loading ────────────────────────────────────────────────

    def _load_grids(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Load all required FLASH GRIB files and extract 2D arrays.

        Uses ThreadPoolExecutor to load all 8 GRIB files concurrently,
        since the bottleneck is I/O (disk reads + eccodes parsing).

        Returns:
            Dict mapping grid key → 2D numpy array, plus 'latitude'
            and 'longitude' 1D coordinate arrays.
            Returns None if any required grid is missing.
        """
        from util.grib_loader import load_grib_fast

        # Build list of (grid_key, filepath) pairs
        load_tasks: List[Tuple[str, str]] = []
        for grid_key, dir_attr in cfg.GRID_DIR_MAP.items():
            directory = getattr(fs, dir_attr, None)
            if directory is None:
                print(f"[FLOHAR] Directory attribute '{dir_attr}' not found in fs.")
                return None

            files = fs.latest_files(directory, 1)
            if not files:
                print(f"[FLOHAR] No files found in {directory} for '{grid_key}'.")
                return None

            load_tasks.append((grid_key, files[0]))

        # Load all GRIB files in parallel
        def _load_one(task: Tuple[str, str]) -> Tuple[str, Any]:
            grid_key, filepath = task
            try:
                ds = load_grib_fast(filepath)
                var_names = list(ds.data_vars)
                if not var_names:
                    return (grid_key, None)
                data_array = ds[var_names[0]]
                return (grid_key, data_array)
            except Exception as e:
                print(f"[FLOHAR] Failed to load '{grid_key}' from {filepath}: {e}")
                return (grid_key, None)

        # Limit max_workers to 2 to prevent excessive concurrent eccodes float64 allocations
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(_load_one, load_tasks))

        # Assemble grids dict
        grids = {}
        ref_shape = None

        for grid_key, data_array in results:
            if data_array is None:
                print(f"[FLOHAR] No data for '{grid_key}'.")
                return None

            arr_2d = data_array.values

            if ref_shape is None:
                ref_shape = arr_2d.shape
                grids["latitude"] = data_array.coords["latitude"].values
                grids["longitude"] = data_array.coords["longitude"].values
            else:
                if arr_2d.shape != ref_shape:
                    print(
                        f"[FLOHAR] Grid shape mismatch for '{grid_key}': "
                        f"expected {ref_shape}, got {arr_2d.shape}."
                    )
                    return None

            grids[grid_key] = arr_2d

        return grids

    # ── Output helpers ──────────────────────────────────────────────

    @staticmethod
    def _region_to_feature(region: Dict, timestamp: datetime) -> Dict[str, Any]:
        """Convert a region dict to a GeoJSON Feature."""
        return {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [region["geometry"]],
            },
            "properties": {
                "region_id": region["region_id"],
                "peak_score": region["peak_score"],
                "mean_score": region["mean_score"],
                "severity": region["severity"],
                "area_km2": region["area_km2"],
                "centroid": region["centroid"],
                "pillar_peaks": region["pillar_peaks"],
                "timestamp": timestamp.isoformat(),
            },
        }

    @staticmethod
    def _save_geojson(feature_collection: Dict, timestamp: datetime) -> None:
        """Save a GeoJSON FeatureCollection to the FlashFlood output directory."""
        output_dir = fs.FLASH_FLOOD_DIR
        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"flohar_{timestamp.strftime('%Y%m%d_%H%M%S')}.json"
        filepath = output_dir / filename

        with open(filepath, "w") as f:
            json.dump(feature_collection, f, separators=(",", ":"))

        print(f"[FLOHAR] Saved GeoJSON to {filepath}")
