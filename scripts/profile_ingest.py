
import sys
import os
import asyncio
import time
import shutil
import statistics
import logging
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.getcwd(), 'src'))

import util.file as fs
# Initialize filesystem to a BENCHMARK dir
BENCHMARK_DIR = Path("EdgeWARN_benchmark")

# Monkey patch clean_old_files to measure its duration and blocking nature
# We want to measure the BASELINE first, so we keep the blocking behavior for now.
# But we will add instrumentation to it.

original_clean = fs.clean_old_files
original_clean_age = fs.clean_files_by_age

t_glob_start = 0

def mocked_clean_old_files(*args, **kwargs):
    t0 = time.time()
    # print(f"[{t0 - t_glob_start:.4f}s] BLOCKING START: fs.clean_old_files called")
    res = original_clean(*args, **kwargs)
    t1 = time.time()
    # print(f"[{t1 - t_glob_start:.4f}s] BLOCKING END: fs.clean_old_files finished (Duration: {t1-t0:.4f}s)")
    return res

def mocked_clean_files_by_age(*args, **kwargs):
    t0 = time.time()
    # print(f"[{t0 - t_glob_start:.4f}s] BLOCKING START: fs.clean_files_by_age called")
    res = original_clean_age(*args, **kwargs)
    t1 = time.time()
    # print(f"[{t1 - t_glob_start:.4f}s] BLOCKING END: fs.clean_files_by_age finished (Duration: {t1-t0:.4f}s)")
    return res

fs.clean_old_files = mocked_clean_old_files
fs.clean_files_by_age = mocked_clean_files_by_age

# Import ingestion modules
import EdgeWARN.core.ingest.mrms.main as mrms_ingest
import EdgeWARN.core.ingest.synoptic.main as rap_ingest
import EdgeWARN.core.ingest.nws.main as nws_ingest
import EdgeWARN.core.ingest.metar as metar_ingest

from util.io import IOManager, TimestampedOutput

# Configure logger to capture our metrics
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("Benchmark")

class BenchmarkHarness:
    def __init__(self, iterations=5, warm_up=True):
        self.iterations = iterations
        self.warm_up = warm_up
        self.results = {
            "MRMS": [],
            "RAP": [],
            "NWS": [],
            "METAR": [],
            "TOTAL": []
        }
        self.setup_filesystem()

    def setup_filesystem(self):
        if BENCHMARK_DIR.exists():
            shutil.rmtree(BENCHMARK_DIR)
        fs.initialize_filesystem(str(BENCHMARK_DIR))
        print(f"Initialized benchmark filesystem at {BENCHMARK_DIR}")

    def populate_dummy_files(self):
        print("Populating dummy files to simulate dirty state...")
        dirs_to_populate = [
            fs.MRMS_ECHOTOP18_DIR, fs.MRMS_ECHOTOP30_DIR, fs.MRMS_FLASH_DIR,
            fs.MRMS_MESH_DIR, fs.MRMS_RAIN_DIR, fs.MRMS_NLDN_DIR,
            fs.MRMS_PRECIPRATE_DIR, fs.MRMS_QPE_DIR, fs.MRMS_AZSHEARLOW_DIR,
            fs.MRMS_NWS_DIR, fs.METAR_DIR
        ]
        count = 0
        for d in dirs_to_populate:
            d.mkdir(parents=True, exist_ok=True)
            for i in range(50): # 50 files per dir
                (d / f"dummy_{uuid.uuid4()}.txt").touch()
                count += 1
        print(f"Created {count} dummy files.")

    async def run_cycle(self, cycle_id):
        global t_glob_start
        dt = datetime.now(timezone.utc)
        print(f"--- Cycle {cycle_id} Start ---")
        
        # Reset per-cycle metrics
        cycle_metrics = {}
        
        t_glob_start = time.time()
        start_time = time.time()

        async def timed_task(name, coro):
            t0 = time.time()
            try:
                await coro
            except Exception as e:
                print(f"{name} failed: {e}")
            duration = (time.time() - t0) * 1000 # ms
            cycle_metrics[name] = duration
            return duration

        # Run items
        await asyncio.gather(
            timed_task("MRMS", mrms_ingest.download_all_files_async(dt)),
            timed_task("RAP", rap_ingest.download_rap_async(dt)),
            timed_task("NWS", nws_ingest.download_alerts_async(dt)),
            timed_task("METAR", metar_ingest.ingest_metars_async()),
            return_exceptions=True
        )

        total_duration = (time.time() - start_time) * 1000
        cycle_metrics["TOTAL"] = total_duration
        
        print(f"--- Cycle {cycle_id} End: {total_duration:.2f}ms ---")
        return cycle_metrics

    async def run(self):
        if self.warm_up:
            print("Running warm-up cycle...")
            self.populate_dummy_files()
            await self.run_cycle("WARMUP")
        
        for i in range(self.iterations):
            # Re-populate files to ensure consistent "cleanup" load if we want to measure that
            self.populate_dummy_files()
            
            metrics = await self.run_cycle(i+1)
            for k, v in metrics.items():
                self.results[k].append(v)
            
            # Small cool-down
            await asyncio.sleep(1)

        self.report()

    def report(self):
        print("\n=== Benchmark Results ===")
        print(f"Iterations: {self.iterations}")
        print(f"{'Source':<10} | {'p50 (ms)':<10} | {'p95 (ms)':<10} | {'p99 (ms)':<10} | {'Mean (ms)':<10}")
        print("-" * 60)
        
        for source, values in self.results.items():
            if not values: continue
            sorted_v = sorted(values)
            n = len(sorted_v)
            p50 = sorted_v[int(n * 0.5)]
            p95 = sorted_v[int(n * 0.95)] if n > 1 else sorted_v[0]
            p99 = sorted_v[int(n * 0.99)] if n > 1 else sorted_v[0]
            mean = statistics.mean(values)
            
            print(f"{source:<10} | {p50:<10.2f} | {p95:<10.2f} | {p99:<10.2f} | {mean:<10.2f}")

if __name__ == "__main__":
    harness = BenchmarkHarness(iterations=20) # 20 iterations for solid baseline
    asyncio.run(harness.run())
