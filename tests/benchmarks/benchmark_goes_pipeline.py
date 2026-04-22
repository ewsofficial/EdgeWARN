#!/usr/bin/env python3
"""Benchmark the full GOES render pipeline with per-step timing and memory stats.

This benchmark runs in two modes:
1) Detailed single-process pass for per-layer/per-step timings and memory.
2) End-to-end GOES pipeline pass using the normal parallel executor.

Run with:
    PYTHONPATH=src python tests/benchmarks/benchmark_goes_pipeline.py

Optional:
    PYTHONPATH=src python tests/benchmarks/benchmark_goes_pipeline.py \
        --sample-interval 0.05 \
        --output /tmp/goes_pipeline_benchmark.json
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import tempfile
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import psutil

import EWMRS.pipeline as ewmrs_pipeline
import EWMRS.render.goes_rgb as goes_rgb
import EWMRS.render.goes_transform as goes_transform
import EWMRS.render.render as render_module
from EWMRS.pipeline import _render_layer
from EWMRS.render.config import get_goes_file_list


@dataclass
class StepRun:
    layer_name: str
    step_name: str
    start_s: float
    end_s: float
    duration_s: float
    start_mem_mb: float
    end_mem_mb: float
    mean_mem_mb: float
    peak_mem_mb: float


class MemoryStepSampler:
    def __init__(self, sample_interval_s: float, include_children: bool = False):
        self.sample_interval_s = sample_interval_s
        self.include_children = include_children
        self._process = psutil.Process(os.getpid())

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

        self._next_token = 1
        self._active: dict[int, dict[str, Any]] = {}
        self.completed: list[StepRun] = []
        self.pipeline_samples_mb: list[float] = []

    def _total_rss_mb(self) -> float:
        total = 0
        try:
            total += self._process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0

        if self.include_children:
            try:
                for child in self._process.children(recursive=True):
                    try:
                        total += child.memory_info().rss
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        return total / (1024.0 * 1024.0)

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._sample_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None

    def _sample_loop(self) -> None:
        while not self._stop_event.is_set():
            mem_mb = self._total_rss_mb()
            with self._lock:
                self.pipeline_samples_mb.append(mem_mb)
                for entry in self._active.values():
                    entry["samples_mb"].append(mem_mb)
            self._stop_event.wait(self.sample_interval_s)

    def enter(self, layer_name: str, step_name: str) -> int:
        start_s = time.perf_counter()
        start_mb = self._total_rss_mb()
        with self._lock:
            token = self._next_token
            self._next_token += 1
            self._active[token] = {
                "layer_name": layer_name,
                "step_name": step_name,
                "start_s": start_s,
                "start_mb": start_mb,
                "samples_mb": [start_mb],
            }
        return token

    def exit(self, token: int) -> None:
        end_s = time.perf_counter()
        end_mb = self._total_rss_mb()

        with self._lock:
            entry = self._active.pop(token, None)
            if entry is None:
                return
            samples = entry["samples_mb"]
            samples.append(end_mb)

        duration_s = end_s - float(entry["start_s"])
        mean_mem_mb = float(sum(samples) / len(samples)) if samples else end_mb
        peak_mem_mb = float(max(samples)) if samples else end_mb

        self.completed.append(
            StepRun(
                layer_name=str(entry["layer_name"]),
                step_name=str(entry["step_name"]),
                start_s=float(entry["start_s"]),
                end_s=end_s,
                duration_s=duration_s,
                start_mem_mb=float(entry["start_mb"]),
                end_mem_mb=end_mb,
                mean_mem_mb=mean_mem_mb,
                peak_mem_mb=peak_mem_mb,
            )
        )


_CURRENT_LAYER = threading.local()


def _set_current_layer(layer_name: str | None) -> None:
    _CURRENT_LAYER.name = layer_name


def _get_current_layer() -> str | None:
    return getattr(_CURRENT_LAYER, "name", None)


def _channel_from_layer_config(layer_config: Any) -> str:
    if isinstance(layer_config, dict):
        value = layer_config.get("channel_id")
        if value:
            return str(value)
    return "unknown"


def _avg(values: list[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


@contextmanager
def _instrument_goes_steps(sampler: MemoryStepSampler):
    patches: list[tuple[Any, str, Any]] = []

    def patch(obj: Any, attr: str, step_name_builder: Callable[..., str]) -> None:
        original = getattr(obj, attr)

        @functools.wraps(original)
        def wrapped(*args, **kwargs):
            layer_name = _get_current_layer()
            if layer_name is None:
                return original(*args, **kwargs)

            step_name = step_name_builder(*args, **kwargs)
            token = sampler.enter(layer_name, step_name)
            try:
                return original(*args, **kwargs)
            finally:
                sampler.exit(token)

        setattr(obj, attr, wrapped)
        patches.append((obj, attr, original))

    patch(ewmrs_pipeline, "_latest_source_file", lambda *_args, **_kwargs: "latest_source_file")
    patch(goes_transform, "extract_goes_timestamp_iso", lambda *_args, **_kwargs: "extract_goes_timestamp")
    patch(
        goes_transform,
        "load_goes_abi_render_dataset",
        lambda *_args, **kwargs: f"load_goes_dataset[{_channel_from_layer_config(kwargs.get('layer_config') or (_args[1] if len(_args) > 1 else None))}]",
    )
    patch(goes_transform, "reproject_goes_abi_to_web_mercator", lambda *_args, **_kwargs: "reproject_goes_dataset")

    patch(
        goes_rgb,
        "prepare_goes_rgb_render",
        lambda *_args, **kwargs: f"prepare_goes_rgb[{(kwargs.get('layer_config') or (_args[0] if _args else {})).get('recipe_key', 'unknown')}]",
    )
    patch(goes_rgb, "prepare_goes_rgb_batch", lambda *_args, **_kwargs: "prepare_goes_rgb_batch")
    patch(
        goes_rgb,
        "compose_goes_rgb",
        lambda *_args, **_kwargs: f"compose_goes_rgb[{(_args[0] if _args else {}).get('recipe_key', 'unknown')}]",
    )
    patch(goes_rgb, "compose_goes_rgb_batch", lambda *_args, **_kwargs: "compose_goes_rgb_batch")
    patch(
        goes_transform,
        "load_reproject_goes_abi_render_array",
        lambda *_args, **kwargs: f"rgb_prepare_channel[{_channel_from_layer_config(kwargs.get('layer_config') or (_args[1] if len(_args) > 1 else None))}]",
    )
    patch(goes_rgb, "build_goes_rgb_shared_registry", lambda *_args, **_kwargs: "rgb_prepare_shared_registry")
    patch(goes_rgb, "_reference_lon_lat_coords", lambda *_args, **_kwargs: "rgb_reference_lon_lat_coords")
    patch(goes_rgb, "_load_goes_ir_colormap", lambda *_args, **_kwargs: "rgb_load_ir_colormap")
    patch(goes_rgb, "compute_goes_rgb_product", lambda *_args, **_kwargs: "rgb_compute_product")
    patch(goes_rgb, "_rgb_to_rgba", lambda *_args, **_kwargs: "rgb_to_rgba")

    patch(render_module.GUILayerRenderer, "convert_to_png", lambda *_args, **_kwargs: "convert_to_png")
    patch(render_module.GUIRGBAWriter, "save_rgba", lambda *_args, **_kwargs: "save_rgba")

    try:
        yield
    finally:
        for obj, attr, original in reversed(patches):
            setattr(obj, attr, original)


def _build_temp_goes_layers(out_root: Path) -> list[dict[str, Any]]:
    temp_layers: list[dict[str, Any]] = []
    for layer in get_goes_file_list():
        layer_copy = dict(layer)
        layer_copy["outdir"] = out_root / layer["name"]
        temp_layers.append(layer_copy)
    return temp_layers


def _summarize_step_runs(step_runs: list[StepRun]) -> dict[str, Any]:
    by_layer: dict[str, list[StepRun]] = defaultdict(list)
    for run in step_runs:
        by_layer[run.layer_name].append(run)

    summary: dict[str, Any] = {}
    for layer_name, runs in sorted(by_layer.items()):
        by_step: dict[str, list[StepRun]] = defaultdict(list)
        for run in runs:
            by_step[run.step_name].append(run)

        step_stats = []
        for step_name, step_calls in by_step.items():
            durations = [call.duration_s for call in step_calls]
            mean_mems = [call.mean_mem_mb for call in step_calls]
            peaks = [call.peak_mem_mb for call in step_calls]
            deltas = [call.end_mem_mb - call.start_mem_mb for call in step_calls]

            step_stats.append(
                {
                    "step": step_name,
                    "calls": len(step_calls),
                    "total_duration_s": sum(durations),
                    "avg_duration_s": _avg(durations),
                    "max_duration_s": max(durations),
                    "avg_memory_mb": _avg(mean_mems),
                    "peak_memory_mb": max(peaks),
                    "avg_memory_delta_mb": _avg(deltas),
                }
            )

        step_stats.sort(key=lambda item: item["total_duration_s"], reverse=True)
        summary[layer_name] = {
            "steps": step_stats,
        }

    return summary


def run_detailed_single_process_benchmark(
    layers: list[dict[str, Any]],
    sample_interval_s: float,
) -> dict[str, Any]:
    sampler = MemoryStepSampler(sample_interval_s=sample_interval_s, include_children=False)
    layer_status: dict[str, dict[str, Any]] = {}

    sampler.start()
    start_s = time.perf_counter()
    try:
        with _instrument_goes_steps(sampler):
            for layer in layers:
                layer_name = str(layer["name"])
                _set_current_layer(layer_name)

                layer_token = sampler.enter(layer_name, "layer_total")
                layer_start = time.perf_counter()
                error_message = None
                rendered_output = None

                try:
                    _, rendered_output = _render_layer(layer)
                except Exception as exc:  # pragma: no cover - defensive benchmark harness
                    error_message = str(exc)
                finally:
                    layer_elapsed = time.perf_counter() - layer_start
                    sampler.exit(layer_token)
                    _set_current_layer(None)

                output_count = len(rendered_output) if rendered_output else 0
                layer_status[layer_name] = {
                    "duration_s": layer_elapsed,
                    "success": bool(rendered_output),
                    "output_count": output_count,
                    "error": error_message,
                }
    finally:
        total_s = time.perf_counter() - start_s
        sampler.stop()
        _set_current_layer(None)

    summary = _summarize_step_runs(sampler.completed)

    for layer_name, status in layer_status.items():
        layer_summary = summary.setdefault(layer_name, {"steps": []})
        layer_summary["layer"] = {
            "duration_s": status["duration_s"],
            "success": status["success"],
            "output_count": status["output_count"],
            "error": status["error"],
        }

    avg_mem = _avg(sampler.pipeline_samples_mb)
    peak_mem = max(sampler.pipeline_samples_mb) if sampler.pipeline_samples_mb else 0.0

    return {
        "mode": "detailed_single_process",
        "total_duration_s": total_s,
        "avg_memory_mb": avg_mem,
        "peak_memory_mb": peak_mem,
        "layer_count": len(layers),
        "layers": summary,
    }


def run_parallel_pipeline_benchmark(
    layers: list[dict[str, Any]],
    sample_interval_s: float,
) -> dict[str, Any]:
    sampler = MemoryStepSampler(sample_interval_s=sample_interval_s, include_children=True)

    original_get_goes_file_list = ewmrs_pipeline.get_goes_file_list
    original_cleanup_old_gui_files = ewmrs_pipeline.cleanup_old_gui_files

    ewmrs_pipeline.get_goes_file_list = lambda: layers
    ewmrs_pipeline.cleanup_old_gui_files = lambda max_age_minutes=120: None

    sampler.start()
    start_s = time.perf_counter()
    results = {}
    try:
        results = ewmrs_pipeline.run_goes_render_pipeline(datetime.now(timezone.utc), max_entries=10)
    finally:
        total_s = time.perf_counter() - start_s
        sampler.stop()
        ewmrs_pipeline.get_goes_file_list = original_get_goes_file_list
        ewmrs_pipeline.cleanup_old_gui_files = original_cleanup_old_gui_files

    avg_mem = _avg(sampler.pipeline_samples_mb)
    peak_mem = max(sampler.pipeline_samples_mb) if sampler.pipeline_samples_mb else 0.0
    successful_layers = sum(1 for rendered in results.values() if rendered)

    return {
        "mode": "parallel_pipeline",
        "total_duration_s": total_s,
        "avg_memory_mb": avg_mem,
        "peak_memory_mb": peak_mem,
        "layer_count": len(layers),
        "successful_layers": successful_layers,
        "failed_layers": len(layers) - successful_layers,
    }


def _print_console_summary(report: dict[str, Any]) -> None:
    print("=" * 90)
    print("GOES RENDER PIPELINE BENCHMARK")
    print("=" * 90)

    detailed = report["detailed_single_process"]
    print("\nDetailed Per-Step Pass (single process)")
    print(f"  Total Duration: {detailed['total_duration_s']:.2f}s")
    print(f"  Avg Memory:     {detailed['avg_memory_mb']:.1f} MB")
    print(f"  Peak Memory:    {detailed['peak_memory_mb']:.1f} MB")
    print(f"  Layers:         {detailed['layer_count']}")

    print("\nPer-layer totals")
    print(f"{'Layer':<46} {'Time (s)':>10} {'Avg MB':>10} {'Peak MB':>10} {'Status':>10}")
    print("-" * 90)
    for layer_name, layer_info in sorted(detailed["layers"].items()):
        layer_meta = layer_info.get("layer", {})
        steps = layer_info.get("steps", [])
        avg_mem = _avg([s["avg_memory_mb"] for s in steps])
        peak_mem = max((s["peak_memory_mb"] for s in steps), default=0.0)
        status = "ok" if layer_meta.get("success") else "skip/fail"
        print(
            f"{layer_name:<46} {layer_meta.get('duration_s', 0.0):>10.2f} "
            f"{avg_mem:>10.1f} {peak_mem:>10.1f} {status:>10}"
        )

    parallel = report["parallel_pipeline"]
    print("\nEnd-to-End GOES Pipeline (parallel executor)")
    print(f"  Total Duration:   {parallel['total_duration_s']:.2f}s")
    print(f"  Avg Memory:       {parallel['avg_memory_mb']:.1f} MB")
    print(f"  Peak Memory:      {parallel['peak_memory_mb']:.1f} MB")
    print(f"  Successful Layers:{parallel['successful_layers']}/{parallel['layer_count']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark GOES rendering pipeline")
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=0.1,
        help="Memory sampling interval in seconds (default: 0.1)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional JSON output path",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="goes_render_benchmark_") as tmp_dir:
        out_root = Path(tmp_dir)
        detailed_layers = _build_temp_goes_layers(out_root / "detailed")
        parallel_layers = _build_temp_goes_layers(out_root / "parallel")

        detailed = run_detailed_single_process_benchmark(detailed_layers, args.sample_interval)
        parallel = run_parallel_pipeline_benchmark(parallel_layers, args.sample_interval)

    report = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "sample_interval_s": args.sample_interval,
        "detailed_single_process": detailed,
        "parallel_pipeline": parallel,
    }

    _print_console_summary(report)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2))
        print(f"\nWrote benchmark JSON: {args.output}")
    else:
        print("\nJSON report")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
