"""Utilities for sampling server resource usage for the monitor UI."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional

import psutil

try:  # GPU metrics are optional
    import GPUtil  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    GPUtil = None


@dataclass
class MetricSnapshot:
    """Container for a single sample of server metrics."""

    timestamp: float
    total_cpu: float
    python_cpu: float
    total_mem_percent: float
    python_mem_bytes: int
    python_mem_percent: float
    gpu_util_percent: Optional[float]
    gpu_mem_percent: Optional[float]
    net_sent_per_s: float
    net_recv_per_s: float


class MetricsSampler:
    """Samples system metrics with emphasis on Python processes."""

    def __init__(self) -> None:
        self._last_net = psutil.net_io_counters()
        # Prime CPU counters so the first real sample has non-zero cpu_percent values.
        self._prime_process_cpu()
        self._last_timestamp = time.time()

    def _prime_process_cpu(self) -> None:
        for proc in psutil.process_iter(["name", "cmdline"]):
            if self._is_python_process(proc):
                try:
                    proc.cpu_percent(interval=None)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

    @staticmethod
    def _is_python_process(proc: psutil.Process) -> bool:
        name = (proc.info.get("name") or "").lower()
        if "python" in name:
            return True
        cmdline = proc.info.get("cmdline") or []
        return any("python" in (part or "").lower() for part in cmdline)

    def _sample_python_processes(self) -> tuple[float, int]:
        cpu = 0.0
        mem_bytes = 0
        for proc in psutil.process_iter([
            "name",
            "cmdline",
            "cpu_percent",
            "memory_info",
        ]):
            if not self._is_python_process(proc):
                continue
            try:
                cpu += proc.cpu_percent(interval=None)
                mem_info = proc.memory_info()
                mem_bytes += mem_info.rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        return cpu, mem_bytes

    def _sample_gpu(self) -> tuple[Optional[float], Optional[float]]:
        if GPUtil is None:
            return None, None
        try:
            gpus = GPUtil.getGPUs()
        except Exception:
            return None, None
        if not gpus:
            return None, None
        gpu = gpus[0]
        util = float(gpu.load) * 100.0
        mem = float(gpu.memoryUtil) * 100.0
        return util, mem

    def sample(self) -> MetricSnapshot:
        """Collect a new metric snapshot."""

        total_cpu = psutil.cpu_percent(interval=None)
        python_cpu, python_mem_bytes = self._sample_python_processes()
        total_mem_percent = psutil.virtual_memory().percent
        python_mem_percent = 0.0
        if psutil.virtual_memory().total:
            python_mem_percent = (python_mem_bytes / psutil.virtual_memory().total) * 100

        gpu_util, gpu_mem = self._sample_gpu()

        current_net = psutil.net_io_counters()
        now = time.time()
        delta = max(now - self._last_timestamp, 1e-6)
        sent_per_s = (current_net.bytes_sent - self._last_net.bytes_sent) / delta
        recv_per_s = (current_net.bytes_recv - self._last_net.bytes_recv) / delta

        self._last_net = current_net
        self._last_timestamp = now

        return MetricSnapshot(
            timestamp=now,
            total_cpu=total_cpu,
            python_cpu=python_cpu,
            total_mem_percent=total_mem_percent,
            python_mem_bytes=python_mem_bytes,
            python_mem_percent=python_mem_percent,
            gpu_util_percent=gpu_util,
            gpu_mem_percent=gpu_mem,
            net_sent_per_s=sent_per_s,
            net_recv_per_s=recv_per_s,
        )
