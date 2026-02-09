
import time
from collections import OrderedDict

class TimingTracker:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TimingTracker, cls).__new__(cls)
            cls._instance.timings = OrderedDict()
            cls._instance.active_timers = {}
        return cls._instance

    def start(self, name):
        """Start a timer with the given name."""
        self.active_timers[name] = time.time()

    def stop(self, name):
        """Stop the timer with the given name and record the duration."""
        if name in self.active_timers:
            start_time = self.active_timers.pop(name)
            duration = time.time() - start_time
            self.timings[name] = duration
        else:
            print(f"Warning: Timer '{name}' stopped but not started.")

    def get_timings(self):
        return self.timings

    def reset(self):
        self.timings = OrderedDict()
        self.active_timers = {}

    def print_summary(self):
        print("\n" + "="*50)
        print(f"{'Component':<35} | {'Time (s)':<10}")
        print("-" * 50)
        total_time = 0
        for name, duration in self.timings.items():
            print(f"{name:<35} | {duration:.4f}")
            # Only add to total if it's a top-level component (heuristic: no ' - ' separator or specific names)
            # Actually, let's just sum everything for now, or maybe the user can interpret the hierarchy.
            # A simple sum of all recorded times might double count if nested. 
            # For this simple implementation, we just list them.
            
        print("="*50 + "\n")

tracker = TimingTracker()
