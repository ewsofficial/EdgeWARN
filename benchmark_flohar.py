"""
FLOHAR Benchmark — Execution Time & Memory Usage

Measures compute_threat_grid and extract_regions on simulated CONUS
MRMS FLASH grids (3500×7000 = 24.5M pixels).

Safety:
    - 120 second hard timeout (SIGALRM)
    - 4 GB memory limit (resource.setrlimit RLIMIT_AS)
    - tracemalloc for peak memory tracking
"""

import os
import sys
import time
import signal
import resource
import tracemalloc
import numpy as np

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from EdgeWARN.core.ctam.modules.FLOHAR.engine import compute_threat_grid
from EdgeWARN.core.ctam.modules.FLOHAR.regions import extract_regions

# ── Safety limits ───────────────────────────────────────────────────
TIMEOUT_SECONDS = 120
MEMORY_LIMIT_GB = 6

def _timeout_handler(signum, frame):
    print(f"\n[BENCHMARK] TIMEOUT after {TIMEOUT_SECONDS}s — aborting.")
    sys.exit(1)

def _set_memory_limit(gb: float):
    """Set virtual memory limit (Linux only)."""
    limit_bytes = int(gb * 1024**3)
    try:
        resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
        print(f"[BENCHMARK] Memory limit set to {gb} GB")
    except Exception as e:
        print(f"[BENCHMARK] Warning: could not set memory limit: {e}")


def run_benchmark():
    # Set safety limits
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(TIMEOUT_SECONDS)
    _set_memory_limit(MEMORY_LIMIT_GB)
    print(f"[BENCHMARK] Timeout: {TIMEOUT_SECONDS}s | Memory cap: {MEMORY_LIMIT_GB} GB\n")

    # ── Generate simulated CONUS MRMS FLASH grids ───────────────────
    shape = (3500, 7000)
    print(f"[BENCHMARK] Generating {shape[0]}×{shape[1]} simulated FLASH grids...")
    t_gen_start = time.perf_counter()

    # Generate grids one at a time using a helper to avoid holding
    # both float64 (from np.random) and float32 copies simultaneously
    np.random.seed(42)
    def _gen(lo, hi):
        return np.random.uniform(lo, hi, size=shape).astype(np.float32)

    ari_max = _gen(0, 50)
    ari_30m = _gen(0, 50)
    ari_01h = _gen(0, 50)
    crest   = _gen(0, 5)
    hp      = _gen(0, 5)
    soil    = _gen(0, 1)
    ffg     = _gen(0, 2)
    rqi     = _gen(0, 1)

    # Inject hotspots to ensure regions
    for _ in range(200):
        r = np.random.randint(0, shape[0] - 50)
        c = np.random.randint(0, shape[1] - 50)
        ari_max[r:r+50, c:c+50] += 100
        crest[r:r+50, c:c+50]   += 10
        ffg[r:r+50, c:c+50]     += 3

    lat = np.linspace(55.0, 20.0, num=shape[0], dtype=np.float64)
    lon = np.linspace(-130.0, -60.0, num=shape[1], dtype=np.float64)

    t_gen = time.perf_counter() - t_gen_start
    input_mem_mb = 8 * (shape[0] * shape[1] * 4) / 1e6  # 8 grids × float32
    print(f"[BENCHMARK] Grid generation: {t_gen:.2f}s | Input memory: {input_mem_mb:.0f} MB\n")

    # ── Benchmark: compute_threat_grid ──────────────────────────────
    print("[BENCHMARK] === SCORING ENGINE ===")
    tracemalloc.start()
    t0 = time.perf_counter()

    threat, r_grid, h_grid, f_grid = compute_threat_grid(
        ari_max, ari_30m, ari_01h, crest, hp, soil, ffg, rqi
    )

    t1 = time.perf_counter()
    _, eng_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"  Time:       {t1 - t0:.3f}s")
    print(f"  Peak mem:   {eng_peak / 1e6:.1f} MB")
    print(f"  Grid dtype: {threat.dtype} (threat) / {r_grid.dtype} (pillars)")
    print(f"  Max threat: {threat.max()} | Min threat: {threat.min()}")
    print()

    # Free input grids (like the real module does)
    del ari_max, ari_30m, ari_01h, crest, hp, soil, ffg, rqi

    # ── Benchmark: extract_regions ──────────────────────────────────
    print("[BENCHMARK] === REGION EXTRACTION ===")
    pillars = {"rainfall": r_grid, "hydro": h_grid, "ffg": f_grid}

    tracemalloc.start()
    t2 = time.perf_counter()

    regions = extract_regions(
        threat_grid=threat,
        lat_coords=lat,
        lon_coords=lon,
        pillar_grids=pillars,
        threshold=25,
        min_area_km2=4.0,
    )

    t3 = time.perf_counter()
    _, reg_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"  Time:       {t3 - t2:.3f}s")
    print(f"  Peak mem:   {reg_peak / 1e6:.1f} MB")
    print(f"  Regions:    {len(regions)}")
    if regions:
        severities = {}
        for r in regions:
            sev = r["severity"]
            severities[sev] = severities.get(sev, 0) + 1
        print(f"  Severities: {severities}")
        print(f"  Largest:    {regions[0]['area_km2']:.1f} km² (score {regions[0]['peak_score']})")
    print()

    # ── Summary ─────────────────────────────────────────────────────
    total_time = (t1 - t0) + (t3 - t2)
    peak_mem = max(eng_peak, reg_peak)
    print("=" * 52)
    print(f"  TOTAL TIME:     {total_time:.3f}s")
    print(f"  OVERALL PEAK:   {peak_mem / 1e6:.1f} MB")
    print(f"  ENGINE:         {t1 - t0:.3f}s / {eng_peak / 1e6:.1f} MB")
    print(f"  REGIONS:        {t3 - t2:.3f}s / {reg_peak / 1e6:.1f} MB")
    print("=" * 52)

    # Cancel the alarm
    signal.alarm(0)


if __name__ == "__main__":
    run_benchmark()
