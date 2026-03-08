"""
FLOHAR (FLOod HAzaRds) CTAM Grid Module

GridAnalysisModule implementation that loads MRMS FLASH GRIB files,
computes composite threat scores, extracts flood threat regions,
and produces GeoJSON output with alert integration.
"""

import json
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional

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

        # Compute threat scores
        threat_grid, rainfall_grid, hydro_grid, ffg_grid = compute_threat_grid(
            grids["ari_max"],
            grids["ari_30m"],
            grids["ari_01h"],
            grids["crest_streamflow"],
            grids["hp_streamflow"],
            grids["soil_sat"],
            grids["ffg_ratio"],
            grids["rqi"],
        )

        # Extract regions
        pillar_grids = {
            "rainfall": rainfall_grid,
            "hydro": hydro_grid,
            "ffg": ffg_grid,
        }

        regions = extract_regions(
            threat_grid,
            grids["latitude"],
            grids["longitude"],
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

        Uses util.file.latest_files() to find the most recent GRIB
        in each product directory, then util.grib_loader.load_grib_fast()
        for fast eccodes-based loading.

        Returns:
            Dict mapping grid key → 2D numpy array, plus 'latitude'
            and 'longitude' 1D coordinate arrays.
            Returns None if any required grid is missing.
        """
        from util.grib_loader import load_grib_fast

        grids = {}
        ref_shape = None

        for grid_key, dir_attr in cfg.GRID_DIR_MAP.items():
            directory = getattr(fs, dir_attr, None)
            if directory is None:
                print(f"[FLOHAR] Directory attribute '{dir_attr}' not found in fs.")
                return None

            files = fs.latest_files(directory, 1)
            if not files:
                print(f"[FLOHAR] No files found in {directory} for '{grid_key}'.")
                return None

            try:
                ds = load_grib_fast(files[0])
                # Extract the first (and usually only) data variable
                var_names = list(ds.data_vars)
                if not var_names:
                    print(f"[FLOHAR] No data variables in {files[0]}.")
                    return None

                data_array = ds[var_names[0]]
                arr_2d = data_array.values

                # Store lat/lon from first grid
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

            except Exception as e:
                print(f"[FLOHAR] Failed to load '{grid_key}' from {files[0]}: {e}")
                return None

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
