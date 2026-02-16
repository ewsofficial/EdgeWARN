
import time
import threading
from collections import OrderedDict


class TimingTracker:
    """
    Thread-safe singleton for tracking execution timings across the pipeline.
    
    Uses a reentrant lock to allow nested calls from the same thread.
    Each process gets its own instance due to process isolation (multiprocessing),
    but threads within a process share the instance safely.
    """
    _instance = None
    _lock = threading.RLock()  # Class-level lock for singleton creation

    def __new__(cls):
        # Double-checked locking pattern for thread-safe singleton
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super(TimingTracker, cls).__new__(cls)
                    instance._initialized = False
                    cls._instance = instance
        return cls._instance

    def __init__(self):
        # Guard against re-initialization
        if self._initialized:
            return
        
        with self._lock:
            if self._initialized:
                return
            
            self._instance_lock = threading.RLock()  # Instance-level lock for operations
            self.timings = OrderedDict()
            self.active_timers = {}
            self._initialized = True

    def start(self, name):
        """Start a timer with the given name. Thread-safe."""
        with self._instance_lock:
            self.active_timers[name] = time.time()

    def stop(self, name):
        """Stop the timer with the given name and record the duration. Thread-safe."""
        with self._instance_lock:
            if name in self.active_timers:
                start_time = self.active_timers.pop(name)
                duration = time.time() - start_time
                self.timings[name] = duration
            else:
                # Use thread-safe warning
                import warnings
                warnings.warn(f"Timer '{name}' stopped but not started.", RuntimeWarning)

    def get_timings(self):
        """Return a copy of timings dict. Thread-safe."""
        with self._instance_lock:
            return dict(self.timings)

    def get_active_timers(self):
        """Return a copy of active timers. Thread-safe."""
        with self._instance_lock:
            return dict(self.active_timers)

    def reset(self):
        """Clear all timings and active timers. Thread-safe."""
        with self._instance_lock:
            self.timings = OrderedDict()
            self.active_timers = {}

    def print_summary(self):
        """Print a formatted summary of all timings. Thread-safe."""
        with self._instance_lock:
            print("\n" + "="*50)
            print(f"{'Component':<35} | {'Time (s)':<10}")
            print("-" * 50)
            for name, duration in self.timings.items():
                print(f"{name:<35} | {duration:.4f}")
            print("="*50 + "\n")

    def __enter__(self):
        """Context manager support for the tracker itself."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        pass


class TimerContext:
    """
    Context manager for timing code blocks with automatic cleanup.
    
    Usage:
        with TimerContext("operation_name"):
            # code to time
    
    Or with the tracker's timer method:
        with perf_tracker.timer("operation_name"):
            # code to time
    """
    
    __slots__ = ('_tracker', '_name', '_start_time')
    
    def __init__(self, tracker, name):
        self._tracker = tracker
        self._name = name
        self._start_time = None
    
    def __enter__(self):
        self._tracker.start(self._name)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Always stop the timer, even if exception occurred
        self._tracker.stop(self._name)
        return False  # Don't suppress exceptions


# Add timer method to TimingTracker for context manager usage
def _timer_method(self, name):
    """Create a context manager for timing a code block."""
    return TimerContext(self, name)

# Monkey-patch the method onto TimingTracker
TimingTracker.timer = _timer_method

# Global singleton instance
tracker = TimingTracker()
