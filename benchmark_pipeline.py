import sys
import os
sys.path.append(os.path.join(os.getcwd(), "src"))

import run
from datetime import datetime, timezone
import tracemalloc
import time

class MockQueue:
    def put(self, item):
        pass

def benchmark():
    print("Starting FULL Pipeline Benchmark...")
    
    dt = datetime.now(timezone.utc)
    mock_queue = MockQueue()
    
    original_stdout = sys.stdout
    original_stderr = sys.stderr

    tracemalloc.start()
    start_time = time.time()
    
    try:
        run.pipeline(mock_queue, dt)
    except Exception as e:
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        print(f"Pipeline Crashed: {e}")
    finally:
        sys.stdout = original_stdout
        sys.stderr = original_stderr

    end_time = time.time()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    duration = end_time - start_time
    peak_mb = peak / 1024 / 1024
    
    print("\n" + "="*40)
    print("       BENCHMARK RESULTS       ")
    print("="*40)
    print(f"Total Duration:     {duration:.2f} seconds")
    print(f"Peak Memory Usage:  {peak_mb:.2f} MiB")
    print("="*40)

if __name__ == "__main__":
    benchmark()
