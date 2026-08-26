"""Benchmark realtime NEXRAD Level-II parse/export latency.

Runs the NEXRAD realtime ingestion pipeline in a child process for a
configured wall-clock duration, polls the per-elevation and site manifests
under the runtime base directory, and persists one JSON result file per radar
site.

Only Level-II ingest through parse/export is measured.  EWMRS NEXRAD
rendering, API indexing, and client delivery latency are excluded.

Usage:
    PYTHONPATH=src python tests/benchmarks/benchmark_nexrad_pipeline_latency.py \
        --duration-seconds 1800 --site KTLH --base-dir /path/to/runtime

    # all sites
    PYTHONPATH=src python tests/benchmarks/benchmark_nexrad_pipeline_latency.py \
        --duration-seconds 14400 --site all --base-dir /path/to/runtime

Output:
    <base-dir>/data/benchmarks/nexrad-latency/<SITE>.json
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from common.ingest.nexrad.grouping import INGEST_READINESS_ELEVATION_IDS

ALL_SITES_MARKER = "all"

SCHEMA_VERSION = 1
PERCENTILE_METHOD = "linear_interpolation"
DEFAULT_OUTPUT_SUBDIR = Path("benchmarks") / "nexrad-latency"
DEFAULT_POLL_INTERVAL_SECONDS = 0.25

WRAPPER_SCRIPT = """
import json
import sys
sys.path.insert(0, sys.argv[1])
from common.ingest.nexrad.pipeline import run_realtime_ingestion_pipeline
config = json.loads(sys.argv[2])
try:
    run_realtime_ingestion_pipeline(
        sites=config.get("sites"),
        base_dir=config.get("base_dir"),
        scan_interval_seconds=config.get("scan_interval_seconds", 20),
        completion_interval_seconds=config.get("completion_interval_seconds", 10),
        max_candidate_volumes_per_site=config.get("max_candidate_volumes_per_site", 3),
    )
except KeyboardInterrupt:
    pass
"""

_REFERENCE_TIMESTAMP_RE = re.compile(r"^(\d{4})(\d{2})(\d{2})-(\d{2})(\d{2})(\d{2})$")


def utc_timestamp_ms() -> str:
    """Current UTC time as ISO-8601 with milliseconds, e.g. ``2026-08-13T17:42:31.482Z``."""
    return _format_iso_ms(datetime.now(timezone.utc))


def _format_iso_ms(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_iso_utc(value) -> datetime | None:
    """Parse an ISO-8601 UTC timestamp, preserving fractional seconds."""
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_reference_timestamp(value) -> datetime | None:
    """Parse a NEXRAD ``YYYYMMDD-HHMMSS`` reference timestamp as UTC."""
    if not value:
        return None
    text = str(value).strip()
    match = _REFERENCE_TIMESTAMP_RE.match(text)
    if match:
        try:
            year, month, day, hour, minute, second = (int(part) for part in match.groups())
            return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
        except ValueError:
            return None
    return _parse_iso_utc(text)


def _load_json_defensive(path: Path) -> dict | None:
    """Read a JSON object defensively; return None for incomplete/invalid reads."""
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _linear_interp_percentile(ordered: list[float], percentile: float) -> float:
    if len(ordered) == 1:
        return ordered[0]
    rank = (percentile / 100.0) * (len(ordered) - 1)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return ordered[lower]
    fraction = rank - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def _percentile_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"min": None, "p50": None, "p95": None, "max": None}
    ordered = sorted(values)
    return {
        "min": round(ordered[0], 3),
        "p50": round(_linear_interp_percentile(ordered, 50), 3),
        "p95": round(_linear_interp_percentile(ordered, 95), 3),
        "max": round(ordered[-1], 3),
    }


def _build_summary(elevations: list[dict], volumes: list[dict]) -> dict:
    elevation_latencies = [item["latency_seconds"] for item in elevations if item.get("latency_seconds") is not None]
    volume_latencies = [item["latency_seconds"] for item in volumes if item.get("latency_seconds") is not None]
    return {
        "elevation_count": len(elevations),
        "volume_count": len(volumes),
        "excluded_elevation_count": sum(1 for item in elevations if item.get("latency_seconds") is None),
        "excluded_volume_count": sum(1 for item in volumes if item.get("latency_seconds") is None),
        "elevation_latency_seconds": _percentile_summary(elevation_latencies),
        "volume_latency_seconds": _percentile_summary(volume_latencies),
    }


class LatencyCollector:
    """Poll per-elevation and site manifests and accumulate latency records."""

    def __init__(
        self,
        base_dir,
        *,
        sites=None,
        started_at: datetime | None = None,
        readiness_elevations=None,
    ):
        self.base_dir = Path(base_dir)
        self.started_at = started_at or datetime.now(timezone.utc)
        self.readiness_elevations = list(readiness_elevations) if readiness_elevations is not None else list(INGEST_READINESS_ELEVATION_IDS)
        self._sites = {str(site).upper() for site in (sites or [])}
        self._elevations: dict[str, list[dict]] = {}
        self._volumes: dict[str, list[dict]] = {}
        self._exclusions: dict[str, list[dict]] = {}
        self._elevation_seen: set[tuple] = set()
        self._volume_seen: set[tuple] = set()

    @property
    def nexrad_root(self) -> Path:
        return self.base_dir / "data" / "NEXRAD_Level2"

    @staticmethod
    def _is_site_name(name: str) -> bool:
        return len(name) == 4 and name.startswith("K") and name.isalnum()

    def poll_once(self) -> None:
        root = self.nexrad_root
        if not root.is_dir():
            return
        for site_dir in sorted(root.iterdir()):
            if not site_dir.is_dir():
                continue
            site = site_dir.name.upper()
            if not self._is_site_name(site):
                continue
            self._poll_site(site, site_dir)

    def _poll_site(self, site: str, site_dir: Path) -> None:
        site = str(site).upper()
        self._sites.add(site)
        for manifest_file in sorted(site_dir.glob("*/*.json")):
            if manifest_file.name == "manifest.json":
                continue
            self._collect_elevation(site, manifest_file)
        site_manifest = site_dir / "manifest.json"
        if site_manifest.exists():
            self._collect_site_manifest(site, site_manifest)

    def _latency_and_reason(self, finish_value, reference_value) -> tuple[float | None, str | None]:
        finish = _parse_iso_utc(finish_value)
        if finish is None:
            return None, "missing_or_invalid_parse_finished_at"
        if finish < self.started_at:
            return None, "stale"
        reference = _parse_reference_timestamp(reference_value)
        if reference is None:
            return None, "missing_or_invalid_reference_timestamp"
        if reference < self.started_at:
            return None, "reference_before_benchmark"
        latency = (finish - reference).total_seconds()
        if latency < 0:
            return None, "reference_after_parse_finish"
        return latency, None

    def _collect_elevation(self, site: str, path: Path) -> None:
        payload = _load_json_defensive(path)
        if payload is None:
            return
        site = str(payload.get("site") or site).upper()
        volume_id = payload.get("volume_id")
        elevation = payload.get("elevation")
        if not volume_id or elevation is None:
            return
        elevation_timestamp = payload.get("elevation_timestamp")
        parse_finished_at = payload.get("parse_finished_at")
        ingest_status = payload.get("ingest_status")
        if ingest_status not in (None, "success"):
            self._exclusions.setdefault(site, []).append(
                {
                    "kind": "elevation",
                    "volume_id": str(volume_id),
                    "elevation": str(elevation),
                    "ingest_status": ingest_status,
                    "exclusion_reason": "ingest_not_successful",
                }
            )
            return
        latency, reason = self._latency_and_reason(parse_finished_at, elevation_timestamp)
        if reason in ("stale", "missing_or_invalid_parse_finished_at"):
            return
        record = {
            "volume_id": str(volume_id),
            "elevation": str(elevation),
            "elevation_timestamp": elevation_timestamp,
            "parse_finished_at": parse_finished_at,
            "latency_seconds": latency,
        }
        if reason:
            record["exclusion_reason"] = reason
        key = (site, str(volume_id), str(elevation), str(elevation_timestamp), str(parse_finished_at))
        if key in self._elevation_seen:
            return
        self._elevation_seen.add(key)
        self._elevations.setdefault(site, []).append(record)
        if latency is None:
            self._exclusions.setdefault(site, []).append({"kind": "elevation", **record})

    def _collect_site_manifest(self, site: str, path: Path) -> None:
        payload = _load_json_defensive(path)
        if payload is None:
            return
        site = str(payload.get("site") or site).upper()
        for volume in payload.get("volumes") or []:
            if not isinstance(volume, dict):
                continue
            volume_id = volume.get("volume_id")
            if not volume_id:
                continue
            volume_timestamp = volume.get("volume_timestamp") or volume.get("scan_timestamp")
            parse_finished_at = volume.get("volume_parse_finished_at")
            latency, reason = self._latency_and_reason(parse_finished_at, volume_timestamp)
            if reason in ("stale", "missing_or_invalid_parse_finished_at"):
                continue
            record = {
                "volume_id": str(volume_id),
                "volume_timestamp": volume_timestamp,
                "volume_parse_finished_at": parse_finished_at,
                "latency_seconds": latency,
                "readiness_elevations": list(self.readiness_elevations),
            }
            if reason:
                record["exclusion_reason"] = reason
            key = (site, str(volume_id), str(parse_finished_at))
            if key in self._volume_seen:
                continue
            self._volume_seen.add(key)
            self._volumes.setdefault(site, []).append(record)
            if latency is None:
                self._exclusions.setdefault(site, []).append({"kind": "volume", **record})

    def results_by_site(self) -> dict[str, dict]:
        results: dict[str, dict] = {}
        for site in sorted(self._sites):
            elevations = self._elevations.get(site, [])
            volumes = self._volumes.get(site, [])
            results[site] = {
                "elevations": elevations,
                "volumes": volumes,
                "exclusions": self._exclusions.get(site, []),
                "summary": _build_summary(elevations, volumes),
            }
        return results


class PipelineProcess:
    """Own a child pipeline process and drain its output in a background thread."""

    def __init__(self, argv, *, cwd=None, tail_size=200):
        self.argv = list(argv)
        self._cwd = cwd
        self._proc: subprocess.Popen | None = None
        self._reader: threading.Thread | None = None
        self._tail: deque[str] = deque(maxlen=tail_size)
        self.signal_used: str | None = None
        self.terminated_by_benchmark = False

    @property
    def is_running(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    @property
    def returncode(self) -> int | None:
        return self._proc.returncode if self._proc is not None else None

    def tail_lines(self) -> list[str]:
        return list(self._tail)

    def start(self) -> None:
        self._proc = subprocess.Popen(
            self.argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=self._cwd,
        )
        self._reader = threading.Thread(target=self._read_loop, daemon=True, name="nexrad-latency-pipeline-reader")
        self._reader.start()

    def _read_loop(self) -> None:
        if self._proc is None or self._proc.stdout is None:
            return
        try:
            for line in self._proc.stdout:
                self._tail.append(line.rstrip("\n"))
        except Exception:
            pass

    def stop_gracefully(self, *, grace_seconds=10.0, term_seconds=5.0, kill_seconds=5.0) -> dict:
        """Send SIGINT first, then SIGTERM then SIGKILL only if needed."""
        if self._proc is None or self._proc.poll() is not None:
            return self._exit_info()
        self.terminated_by_benchmark = True
        self.signal_used = "SIGINT"
        self._proc.send_signal(signal.SIGINT)
        try:
            self._proc.wait(timeout=max(0.0, float(grace_seconds)))
        except subprocess.TimeoutExpired:
            self.signal_used = "SIGTERM"
            self._proc.send_signal(signal.SIGTERM)
            try:
                self._proc.wait(timeout=max(0.0, float(term_seconds)))
            except subprocess.TimeoutExpired:
                self.signal_used = "SIGKILL"
                self._proc.kill()
                self._proc.wait(timeout=max(0.0, float(kill_seconds)))
        finally:
            self._join_reader()
        return self._exit_info()

    def _join_reader(self, timeout=2.0) -> None:
        reader = self._reader
        if reader is not None:
            self._reader = None
            reader.join(timeout=timeout)

    def _exit_info(self) -> dict:
        return {
            "signal": self.signal_used,
            "return_code": self._proc.returncode if self._proc is not None else None,
        }


@dataclass
class BenchmarkConfig:
    duration_seconds: float
    sites: list[str] | None
    base_dir: str
    output_dir: str | None = None
    scan_interval_seconds: float = 20.0
    completion_interval_seconds: float = 10.0
    max_candidate_volumes_per_site: int = 3
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS
    checkpoint_interval_seconds: float = 60.0
    append: bool = False
    grace_seconds: float = 10.0
    term_seconds: float = 5.0
    kill_seconds: float = 5.0


def _src_root() -> str:
    return str(Path(__file__).resolve().parents[2] / "src")


def _build_pipeline_argv(config: BenchmarkConfig) -> list[str]:
    pipeline_config = {
        "base_dir": str(config.base_dir),
        "scan_interval_seconds": config.scan_interval_seconds,
        "completion_interval_seconds": config.completion_interval_seconds,
        "max_candidate_volumes_per_site": config.max_candidate_volumes_per_site,
    }
    if config.sites is not None:
        pipeline_config["sites"] = list(config.sites)
    return [sys.executable, "-c", WRAPPER_SCRIPT, _src_root(), json.dumps(pipeline_config)]


def _persist_site_result(path: Path, doc: dict, *, append: bool = False) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if append and path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            existing = None
        if isinstance(existing, list):
            runs = existing + [doc]
        elif isinstance(existing, dict):
            runs = [existing, doc]
        else:
            runs = [doc]
        payload = runs
    else:
        payload = doc
    temp_path = path.with_name(path.name + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temp_path, path)


def _build_site_doc(
    config: BenchmarkConfig,
    site: str,
    site_results: dict,
    *,
    started_at: datetime,
    finished_at: datetime,
    exit_info: dict | None,
    tail_lines: list[str],
    interrupted: bool = False,
    checkpointed: bool = False,
) -> dict:
    doc = {
        "schema_version": SCHEMA_VERSION,
        "percentile_method": PERCENTILE_METHOD,
        "site": site,
        "benchmark": {
            "started_at": _format_iso_ms(started_at),
            "finished_at": _format_iso_ms(finished_at),
            "requested_duration_seconds": config.duration_seconds,
            "actual_duration_seconds": round((finished_at - started_at).total_seconds(), 3),
            "pipeline_exit": exit_info,
        },
        "configuration": {
            "base_dir": str(Path(config.base_dir)),
            "sites": None if config.sites is None else sorted(config.sites),
            "scan_interval_seconds": config.scan_interval_seconds,
            "completion_interval_seconds": config.completion_interval_seconds,
            "max_candidate_volumes_per_site": config.max_candidate_volumes_per_site,
            "poll_interval_seconds": config.poll_interval_seconds,
        },
        "elevations": site_results["elevations"],
        "volumes": site_results["volumes"],
        "summary": site_results["summary"],
        "exclusions": site_results["exclusions"],
        "pipeline_output_tail": tail_lines,
    }
    if interrupted:
        doc["benchmark"]["interrupted"] = True
    if checkpointed:
        doc["checkpointed"] = True
    return doc


def _checkpoint_results(
    config: BenchmarkConfig,
    collector: LatencyCollector,
    *,
    started_at: datetime,
    output_dir: Path,
) -> None:
    now = datetime.now(timezone.utc)
    results = collector.results_by_site()
    for site in sorted(results):
        doc = _build_site_doc(
            config,
            site,
            results[site],
            started_at=started_at,
            finished_at=now,
            exit_info=None,
            tail_lines=[],
            checkpointed=True,
        )
        _persist_site_result(output_dir / f"{site}.json", doc)
    for site in sorted(results):
        summary = results[site]["summary"]
        print(
            f"[checkpoint {_format_iso_ms(now)}] {site}: elevations={summary['elevation_count']} "
            f"volumes={summary['volume_count']} "
            f"(excluded_elevations={summary['excluded_elevation_count']}, "
            f"excluded_volumes={summary['excluded_volume_count']})",
            flush=True,
        )


def run_benchmark(config: BenchmarkConfig, *, pipeline=None) -> list[dict]:
    """Run the latency benchmark for the configured duration and persist results."""
    started_at = datetime.now(timezone.utc)
    output_dir = Path(config.output_dir) if config.output_dir else Path(config.base_dir) / "data" / DEFAULT_OUTPUT_SUBDIR
    collector = LatencyCollector(
        base_dir=config.base_dir,
        sites=config.sites,
        started_at=started_at,
    )
    if pipeline is None:
        pipeline = PipelineProcess(_build_pipeline_argv(config), cwd=_src_root())
    print(
        f"benchmark started: sites={config.sites or 'all'} duration={config.duration_seconds}s "
        f"base_dir={config.base_dir} output_dir={output_dir}",
        flush=True,
    )
    pipeline.start()

    interrupted = False
    deadline = time.monotonic() + max(0.0, config.duration_seconds)
    last_checkpoint = time.monotonic()
    checkpoint_interval = max(0.0, getattr(config, "checkpoint_interval_seconds", 0.0))
    try:
        while time.monotonic() < deadline:
            if not pipeline.is_running:
                break
            collector.poll_once()
            if (
                checkpoint_interval > 0
                and not config.append
                and time.monotonic() - last_checkpoint >= checkpoint_interval
            ):
                last_checkpoint = time.monotonic()
                _checkpoint_results(config, collector, started_at=started_at, output_dir=output_dir)
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(config.poll_interval_seconds, remaining))
    except KeyboardInterrupt:
        interrupted = True
    finally:
        collector.poll_once()
        exit_info = pipeline.stop_gracefully(
            grace_seconds=config.grace_seconds,
            term_seconds=config.term_seconds,
            kill_seconds=config.kill_seconds,
        )

    finished_at = datetime.now(timezone.utc)

    docs: list[dict] = []
    results = collector.results_by_site()
    for site in sorted(results):
        doc = _build_site_doc(
            config,
            site,
            results[site],
            started_at=started_at,
            finished_at=finished_at,
            exit_info=exit_info,
            tail_lines=pipeline.tail_lines(),
            interrupted=interrupted,
        )
        docs.append(doc)
        _persist_site_result(output_dir / f"{site}.json", doc, append=config.append)
    return docs


def _default_base_dir() -> str:
    return os.environ.get("EDGEWARN_BASE_DIR") or str(Path.home() / "EdgeWARN_input")


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark realtime NEXRAD Level-II parse/export latency.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--duration-seconds", type=float, required=True, help="positive wall-clock duration of the run")
    parser.add_argument("--site", action="append", help=f"radar site (repeatable; use {ALL_SITES_MARKER!r} or omit for all sites)")
    parser.add_argument("--base-dir", default=_default_base_dir(), help="runtime base directory")
    parser.add_argument("--output-dir", default=None, help="per-site result directory (default: <base-dir>/data/benchmarks/nexrad-latency)")
    parser.add_argument("--scan-interval-seconds", type=float, default=20.0)
    parser.add_argument("--completion-interval-seconds", type=float, default=10.0)
    parser.add_argument("--max-candidate-volumes-per-site", type=int, default=3)
    parser.add_argument("--poll-interval-seconds", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS, help="manifest observation cadence")
    parser.add_argument(
        "--checkpoint-interval-seconds",
        type=float,
        default=60.0,
        help="how often to checkpoint per-site JSON during the run (0 disables)",
    )
    parser.add_argument("--append", action="store_true", help="retain prior runs in the per-site output file")
    return parser


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    if args.duration_seconds <= 0:
        parser.error("--duration-seconds must be positive")
    sites = None
    if args.site:
        sites = None if ALL_SITES_MARKER in {str(site).lower() for site in args.site} else list(args.site)
    config = BenchmarkConfig(
        duration_seconds=args.duration_seconds,
        sites=sites,
        base_dir=args.base_dir,
        output_dir=args.output_dir,
        scan_interval_seconds=args.scan_interval_seconds,
        completion_interval_seconds=args.completion_interval_seconds,
        max_candidate_volumes_per_site=args.max_candidate_volumes_per_site,
        poll_interval_seconds=args.poll_interval_seconds,
        checkpoint_interval_seconds=args.checkpoint_interval_seconds,
        append=args.append,
    )
    docs = run_benchmark(config)
    for doc in docs:
        summary = doc["summary"]
        print(
            f"{doc['site']}: elevations={summary['elevation_count']} "
            f"volumes={summary['volume_count']} "
            f"(excluded_elevations={summary['excluded_elevation_count']}, "
            f"excluded_volumes={summary['excluded_volume_count']})"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
