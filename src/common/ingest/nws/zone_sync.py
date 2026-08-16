"""Synchronize NWS zone assets from authoritative NWS zone/UGC data."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests
from shapely.geometry import Polygon, MultiPolygon, shape

from common.config import loader as config_loader
from common.config import overlay
from util.release import format_user_agent


ZONE_TYPES: Tuple[str, ...] = ("forecast", "fire", "public", "county", "marine")


def _normalize_ring(coords: Sequence[Any], precision: int) -> List[List[float]]:
    ring: List[List[float]] = []
    for point in coords:
        if not isinstance(point, (list, tuple)) or len(point) < 2:
            continue
        try:
            ring.append([round(float(point[0]), precision), round(float(point[1]), precision)])
        except Exception:
            continue

    if len(ring) < 3:
        return []

    if ring[0] != ring[-1]:
        ring.append(ring[0])

    if len(ring) < 4:
        return []

    return ring


def _rings_valid(rings: Sequence[Sequence[Sequence[float]]]) -> bool:
    for ring in rings:
        try:
            polygon = Polygon(ring)
        except Exception:
            return False
        if polygon.is_empty or not polygon.is_valid:
            return False
    return True


def geometry_to_rings(geometry: Dict[str, Any], precision: int = 5) -> List[List[List[float]]]:
    """Convert GeoJSON Polygon/MultiPolygon to asset ring format."""
    if not geometry or not isinstance(geometry, dict):
        return []

    try:
        geom_obj = shape(geometry)
    except Exception:
        return []

    if geom_obj.is_empty:
        return []

    if not geom_obj.is_valid:
        try:
            geom_obj = geom_obj.buffer(0)
        except Exception:
            return []
        if geom_obj.is_empty or not geom_obj.is_valid:
            return []

    if geom_obj.geom_type not in {"Polygon", "MultiPolygon"}:
        return []

    if geom_obj.geom_type == "Polygon":
        geometries = [geom_obj]
    else:
        multipolygon = geom_obj
        assert isinstance(multipolygon, MultiPolygon)
        geometries = list(multipolygon.geoms)

    for active_precision in range(precision, 8):
        rings: List[List[List[float]]] = []
        for polygon in geometries:
            ring = _normalize_ring(list(polygon.exterior.coords), precision=active_precision)
            if ring:
                rings.append(ring)

        if rings and _rings_valid(rings):
            return rings

    return []


def merge_zone_entries(existing: List[Dict[str, Any]], additions: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Merge missing zone entries into an existing state zone list."""
    merged: Dict[str, Dict[str, Any]] = {}
    for row in existing:
        code = row.get("code")
        polygon = row.get("Polygon")
        if isinstance(code, str) and isinstance(polygon, list):
            merged[code] = {"code": code, "Polygon": polygon}

    for row in additions:
        code = row.get("code")
        polygon = row.get("Polygon")
        if not isinstance(code, str) or not isinstance(polygon, list):
            continue
        if code not in merged:
            merged[code] = {"code": code, "Polygon": polygon}

    return [merged[code] for code in sorted(merged.keys())]


@dataclass
class SyncReport:
    total_catalog_codes: int
    existing_asset_codes: int
    missing_codes: int
    fetched_with_geometry: int
    fetched_missing_geometry: int
    fetch_errors: int
    appended_codes: int
    unresolved_codes: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_catalog_codes": self.total_catalog_codes,
            "existing_asset_codes": self.existing_asset_codes,
            "missing_codes": self.missing_codes,
            "fetched_with_geometry": self.fetched_with_geometry,
            "fetched_missing_geometry": self.fetched_missing_geometry,
            "fetch_errors": self.fetch_errors,
            "appended_codes": self.appended_codes,
            "unresolved_codes": self.unresolved_codes,
        }


class NWSZoneSync:
    def __init__(
        self,
        assets_dir: Path,
        zone_types: Sequence[str] = ZONE_TYPES,
        timeout_seconds: int = 30,
        max_retries: int = 3,
        max_workers: int = 16,
        pause_seconds: float = 0.05,
        user_agent: str | None = None,
        show_progress: bool = True,
    ) -> None:
        self.assets_dir = Path(assets_dir)
        self.zone_types = tuple(zone_types)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.max_workers = max_workers
        self.pause_seconds = pause_seconds
        self.show_progress = show_progress
        self.headers = {
            "User-Agent": user_agent or format_user_agent(),
            "Accept": "application/geo+json",
        }
        self._thread_local = threading.local()

    def _get_thread_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(self.headers)
            self._thread_local.session = session
        return session

    def _request_json(self, url: str) -> Dict[str, Any]:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self._get_thread_session().get(url, timeout=self.timeout_seconds)
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                last_error = exc
                if attempt < self.max_retries:
                    time.sleep(0.25 * attempt)
        if last_error is None:
            raise RuntimeError(f"Failed to fetch {url}")
        raise last_error

    def load_existing_assets(self) -> Tuple[Set[str], Dict[str, List[Dict[str, Any]]]]:
        all_codes: Set[str] = set()
        state_rows: Dict[str, List[Dict[str, Any]]] = {}

        if not self.assets_dir.exists():
            return all_codes, state_rows

        for state_dir in sorted(self.assets_dir.iterdir()):
            if not state_dir.is_dir():
                continue
            state = state_dir.name
            zones_path = state_dir / "zones.json"
            if not zones_path.exists():
                state_rows[state] = []
                continue

            try:
                rows = json.loads(zones_path.read_text(encoding="utf-8"))
            except Exception:
                rows = []

            clean_rows: List[Dict[str, Any]] = []
            for row in rows:
                code = row.get("code") if isinstance(row, dict) else None
                polygon = row.get("Polygon") if isinstance(row, dict) else None
                if isinstance(code, str) and isinstance(polygon, list):
                    clean_rows.append({"code": code, "Polygon": polygon})
                    all_codes.add(code)

            state_rows[state] = clean_rows

        return all_codes, state_rows

    def fetch_zone_catalog(self) -> Dict[str, str]:
        """Return catalog map of code -> preferred zone type."""
        catalog: Dict[str, str] = {}
        for zone_type in self.zone_types:
            data = self._request_json(f"https://api.weather.gov/zones/{zone_type}")
            for feature in data.get("features", []):
                props = feature.get("properties") or {}
                code = props.get("id")
                if isinstance(code, str) and code not in catalog:
                    catalog[code] = zone_type
        return catalog

    def fetch_zone_geometry(self, zone_type: str, code: str) -> List[List[List[float]]]:
        detail = self._request_json(f"https://api.weather.gov/zones/{zone_type}/{code}")
        geometry = detail.get("geometry")
        return geometry_to_rings(geometry)

    def _fetch_missing_zone(self, code: str, zone_type: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
        # Paced here, in the worker, so it throttles api.weather.gov. Pausing in
        # the collection loop instead only delays bookkeeping after every future
        # has already been submitted.
        #
        # Scaled by the worker count so `pause_seconds` keeps its single-threaded
        # meaning: N workers each waiting `pause * N` yields 1/pause requests per
        # second in aggregate, not N/pause.
        if self.pause_seconds > 0:
            time.sleep(self.pause_seconds * max(1, self.max_workers))

        try:
            rings = self.fetch_zone_geometry(zone_type, code)
        except Exception:
            return code, None, "error"

        if not rings:
            return code, None, "missing_geometry"

        return code, {"code": code, "Polygon": rings}, "ok"

    @staticmethod
    def _print_progress(
        done: int,
        total: int,
        ok_count: int,
        missing_geom_count: int,
        error_count: int,
        started_at: float,
    ) -> None:
        elapsed = max(time.time() - started_at, 0.001)
        pct = (done / total) * 100 if total else 100.0
        rate = done / elapsed
        remaining = max(total - done, 0)
        eta = (remaining / rate) if rate > 0 else 0.0
        message = (
            f"\r[zone-sync] {done}/{total} ({pct:5.1f}%) "
            f"ok={ok_count} no-geom={missing_geom_count} errors={error_count} "
            f"rate={rate:0.1f}/s eta={eta:0.1f}s"
        )
        print(message, end="", flush=True)

    def sync(self, dry_run: bool = True) -> SyncReport:
        existing_codes, state_rows = self.load_existing_assets()
        catalog = self.fetch_zone_catalog()
        missing_codes = sorted(set(catalog.keys()) - existing_codes)

        additions_by_state: Dict[str, List[Dict[str, Any]]] = {}
        unresolved_codes: List[str] = []
        fetched_with_geometry = 0
        fetched_missing_geometry = 0
        fetch_errors = 0

        workers = max(1, self.max_workers)
        total_missing = len(missing_codes)

        if total_missing > 0:
            started_at = time.time()
            done = 0
            if self.show_progress:
                self._print_progress(done, total_missing, fetched_with_geometry, fetched_missing_geometry, fetch_errors, started_at)

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {}
                for code in missing_codes:
                    zone_type = catalog.get(code)
                    if not zone_type:
                        unresolved_codes.append(code)
                        done += 1
                        if self.show_progress:
                            self._print_progress(done, total_missing, fetched_with_geometry, fetched_missing_geometry, fetch_errors, started_at)
                        continue

                    future = executor.submit(self._fetch_missing_zone, code, zone_type)
                    futures[future] = code

                for future in as_completed(futures):
                    code, entry, status = future.result()
                    done += 1

                    if status == "ok" and entry is not None:
                        state = code[:2]
                        additions_by_state.setdefault(state, []).append(entry)
                        fetched_with_geometry += 1
                    elif status == "missing_geometry":
                        fetched_missing_geometry += 1
                        unresolved_codes.append(code)
                    else:
                        fetch_errors += 1
                        unresolved_codes.append(code)

                    if self.show_progress:
                        self._print_progress(done, total_missing, fetched_with_geometry, fetched_missing_geometry, fetch_errors, started_at)

            if self.show_progress:
                print("", flush=True)

        appended_codes = 0
        if not dry_run:
            for state, additions in sorted(additions_by_state.items()):
                state_dir = self.assets_dir / state
                state_dir.mkdir(parents=True, exist_ok=True)
                zones_path = state_dir / "zones.json"

                existing_rows = state_rows.get(state, [])
                merged = merge_zone_entries(existing_rows, additions)

                before_codes = {row["code"] for row in existing_rows}
                after_codes = {row["code"] for row in merged}
                appended_codes += len(after_codes - before_codes)

                zones_path.write_text(json.dumps(merged), encoding="utf-8")

        return SyncReport(
            total_catalog_codes=len(catalog),
            existing_asset_codes=len(existing_codes),
            missing_codes=len(missing_codes),
            fetched_with_geometry=fetched_with_geometry,
            fetched_missing_geometry=fetched_missing_geometry,
            fetch_errors=fetch_errors,
            appended_codes=appended_codes,
            unresolved_codes=sorted(set(unresolved_codes)),
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync assets/nws_zones from NWS zone/UGC APIs")
    parser.add_argument(
        "--assets-dir",
        type=Path,
        default=None,
        help="Path to assets/nws_zones directory (default: from nws.yaml)",
    )
    parser.add_argument(
        "--zone-types",
        nargs="+",
        default=None,
        help="Zone types to query (default: from nws.yaml)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help="HTTP timeout in seconds (default: from nws.yaml)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help="HTTP retry attempts (default: from nws.yaml)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Concurrent workers for zone detail fetches (default: from nws.yaml)",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=None,
        help="Pause between zone-detail requests (default: from nws.yaml)",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Show progress updates (default: from nws.yaml; --no-progress disables)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write updates to assets (default is dry run)",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        help="Optional path to write sync report JSON",
    )
    parser.add_argument(
        "--config-dir",
        type=str,
        default=None,
        help="Override the config/ directory (else EDGEWARN_CONFIG_DIR or repo root)",
    )
    return parser.parse_args()


def _resolve_zone_sync_args(args: argparse.Namespace) -> argparse.Namespace:
    zone_sync_cfg = config_loader.load_config("nws", config_dir=args.config_dir)["zone_sync"]

    assets_dir_yaml = config_loader.repo_root(args.config_dir) / zone_sync_cfg["assets_dir"]
    args.assets_dir = overlay.resolve(args.assets_dir, yaml_value=assets_dir_yaml)
    args.zone_types = overlay.resolve(args.zone_types, yaml_value=list(zone_sync_cfg["zone_types"]))
    args.timeout_seconds = overlay.resolve(args.timeout_seconds, yaml_value=zone_sync_cfg["timeout_seconds"])
    args.max_retries = overlay.resolve(args.max_retries, yaml_value=zone_sync_cfg["max_retries"])
    args.max_workers = overlay.resolve(args.max_workers, yaml_value=zone_sync_cfg["max_workers"])
    args.pause_seconds = overlay.resolve(args.pause_seconds, yaml_value=zone_sync_cfg["pause_seconds"])
    args.progress = overlay.resolve(args.progress, yaml_value=zone_sync_cfg["progress"])
    return args


def main() -> int:
    args = _resolve_zone_sync_args(_parse_args())
    syncer = NWSZoneSync(
        assets_dir=args.assets_dir,
        zone_types=args.zone_types,
        timeout_seconds=args.timeout_seconds,
        max_retries=args.max_retries,
        max_workers=args.max_workers,
        pause_seconds=args.pause_seconds,
        show_progress=args.progress,
    )
    report = syncer.sync(dry_run=not args.apply)
    report_json = json.dumps(report.to_dict(), indent=2)
    if args.report_path:
        args.report_path.write_text(report_json, encoding="utf-8")
    print(report_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
