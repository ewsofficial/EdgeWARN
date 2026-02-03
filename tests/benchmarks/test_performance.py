"""
Performance benchmark tests for critical pipeline components.
Measures execution time, memory usage, and CPU usage.

Run with: pytest tests/benchmarks/test_performance.py -v -s
"""
import pytest
import time
import psutil
import os
import gc
from pathlib import Path


def get_process_metrics():
    """Get current process memory and CPU metrics."""
    process = psutil.Process(os.getpid())
    return {
        "memory_mb": process.memory_info().rss / 1024 / 1024,
        "cpu_percent": process.cpu_percent(interval=0.1)
    }


class PerformanceResult:
    """Container for benchmark results."""
    def __init__(self, name: str):
        self.name = name
        self.start_time = None
        self.end_time = None
        self.start_memory_mb = None
        self.end_memory_mb = None
        self.peak_memory_mb = None
        self.cpu_samples = []
    
    def start(self):
        gc.collect()
        metrics = get_process_metrics()
        self.start_time = time.time()
        self.start_memory_mb = metrics["memory_mb"]
        self.peak_memory_mb = self.start_memory_mb
    
    def sample(self):
        metrics = get_process_metrics()
        self.cpu_samples.append(metrics["cpu_percent"])
        if metrics["memory_mb"] > self.peak_memory_mb:
            self.peak_memory_mb = metrics["memory_mb"]
    
    def stop(self):
        self.end_time = time.time()
        metrics = get_process_metrics()
        self.end_memory_mb = metrics["memory_mb"]
        if metrics["memory_mb"] > self.peak_memory_mb:
            self.peak_memory_mb = metrics["memory_mb"]
    
    @property
    def duration_s(self):
        return self.end_time - self.start_time
    
    @property
    def memory_delta_mb(self):
        return self.end_memory_mb - self.start_memory_mb
    
    @property
    def avg_cpu_percent(self):
        return sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
    
    def report(self):
        return (
            f"\n{'='*50}\n"
            f"BENCHMARK: {self.name}\n"
            f"{'='*50}\n"
            f"Duration:      {self.duration_s:.2f}s\n"
            f"Memory Start:  {self.start_memory_mb:.1f} MB\n"
            f"Memory Peak:   {self.peak_memory_mb:.1f} MB\n"
            f"Memory Delta:  {self.memory_delta_mb:+.1f} MB\n"
            f"Avg CPU:       {self.avg_cpu_percent:.1f}%\n"
            f"{'='*50}"
        )


class TestGribLoaderPerformance:
    """Benchmark tests for the custom GRIB loader."""
    
    @pytest.fixture
    def sample_grib_path(self):
        """Get a sample GRIB file for testing."""
        import util.file as fs
        try:
            files = fs.latest_files(fs.MRMS_AZSHEARLOW_DIR, 1)
            return files[-1] if files else None
        except Exception:
            return None
    
    def test_fast_loader_execution_time(self, sample_grib_path):
        """Test that fast GRIB loader completes within time threshold."""
        if sample_grib_path is None:
            pytest.skip("No sample GRIB file available")
        
        from util.grib_loader import load_grib_fast
        
        result = PerformanceResult("Fast GRIB Loader")
        result.start()
        
        ds = load_grib_fast(sample_grib_path)
        
        result.stop()
        print(result.report())
        
        # Assert performance constraints
        assert result.duration_s < 5.0, f"Fast loader took {result.duration_s:.2f}s, expected < 5s"
        assert ds is not None
    
    def test_fast_loader_memory_usage(self, sample_grib_path):
        """Test that fast GRIB loader has reasonable memory footprint."""
        if sample_grib_path is None:
            pytest.skip("No sample GRIB file available")
        
        from util.grib_loader import load_grib_fast
        
        result = PerformanceResult("Fast GRIB Loader Memory")
        result.start()
        
        ds = load_grib_fast(sample_grib_path)
        
        result.stop()
        print(result.report())
        
        # MRMS AzShear is ~7000x14000 * 8 bytes = ~784 MB
        # Allow some overhead
        assert result.peak_memory_mb < result.start_memory_mb + 1500, \
            f"Memory usage too high: +{result.memory_delta_mb:.1f} MB"


class TestIntegrationPerformance:
    """Benchmark tests for the integration pipeline."""
    
    @pytest.fixture
    def sample_stormcells_path(self):
        """Get a sample storm cells JSON for testing."""
        import util.file as fs
        try:
            files = fs.latest_files(fs.STORMCELL_DIR, 1)
            return files[-1] if files else None
        except Exception:
            return None
    
    def test_integration_execution_time(self, sample_stormcells_path):
        """Test that integration completes within time threshold."""
        if sample_stormcells_path is None:
            pytest.skip("No sample storm cells file available")
        
        from EdgeWARN.core.process.integrate import main as integration
        
        result = PerformanceResult("Integration Pipeline")
        result.start()
        
        # Sample CPU during execution
        import threading
        stop_sampling = threading.Event()
        
        def sample_loop():
            while not stop_sampling.is_set():
                result.sample()
                time.sleep(0.5)
        
        sampler = threading.Thread(target=sample_loop, daemon=True)
        sampler.start()
        
        integration.main(sample_stormcells_path)
        
        stop_sampling.set()
        sampler.join(timeout=1)
        result.stop()
        
        print(result.report())
        
        # Assert performance constraints
        # With optimization, integration should complete in < 15s
        assert result.duration_s < 15.0, \
            f"Integration took {result.duration_s:.2f}s, expected < 15s"


class TestRAPIntegrationPerformance:
    """Benchmark tests for RAP data integration."""
    
    @pytest.fixture
    def sample_rap_path(self):
        """Get a sample RAP file for testing."""
        import util.file as fs
        try:
            files = fs.latest_files(fs.RAP_DIR, 1)
            return files[-1] if files else None
        except Exception:
            return None
    
    def test_rap_loading_time(self, sample_rap_path):
        """Test that RAP loading completes within time threshold."""
        if sample_rap_path is None:
            pytest.skip("No sample RAP file available")
        
        import cfgrib
        
        result = PerformanceResult("RAP Dataset Loading")
        result.start()
        
        datasets = cfgrib.open_datasets(sample_rap_path)
        
        result.stop()
        print(result.report())
        
        # RAP loading should be fast with open_datasets
        assert result.duration_s < 5.0, \
            f"RAP loading took {result.duration_s:.2f}s, expected < 5s"
        assert len(datasets) > 0
        
        # Cleanup
        for ds in datasets:
            ds.close()
