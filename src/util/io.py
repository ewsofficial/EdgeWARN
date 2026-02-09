from datetime import datetime, timezone
import argparse
import sys

class TimestampedOutput:
    def __init__(self, stream):
        self.stream = stream

    def write(self, message):
        if message.strip():  # skip empty lines
            timestamp = datetime.now(timezone.utc).isoformat()
            self.stream.write(f"[{timestamp}] {message}")
        else:
            self.stream.write(message)

    def flush(self):
        self.stream.flush()

class QueueWriter:
    def __init__(self, queue):
        self.queue = queue

    def write(self, message):
        if message.strip():
            timestamp = datetime.now(timezone.utc).isoformat()
            self.queue.put(f"[{timestamp}] {message}")

    def flush(self):
        pass

class IOManager:
    def __init__(self, header):
        self.header = header
    
    def get_args(self):
        """Parse and validate EdgeWARN command-line arguments."""
        parser = argparse.ArgumentParser(description="EdgeWARN modifier specification")

        parser.add_argument(
            "--lat_limits",
            type=float,
            nargs=2,
            metavar=("LAT_MIN", "LAT_MAX"),
            default=[20, 55],
            help="Latitude limits for processing (default: 20 55)"
        )
        parser.add_argument(
            "--lon_limits",
            type=float,
            nargs=2,
            metavar=("LON_MIN", "LON_MAX"),
            default=[230, 300],
            help="Longitude limits for processing (default: 230 300)"
        )
        parser.add_argument(
            "--nogui",
            action="store_true",
            help="Disable the GUI and print output to console"
        )
        parser.add_argument(
            "--base_dir",
            type=str,
            default=None,
            help="Custom base directory for input/output data"
        )
        parser.add_argument(
            "--profile",
            action="store_true",
            help="Enable performance profiling"
        )

        args = parser.parse_args()

        # ===== Validate values =====
        if len(args.lat_limits) != 2 or len(args.lon_limits) != 2:
            print("ERROR: Latitude and longitude limits must each have exactly 2 numeric values.")
            sys.exit(1)

        if args.lat_limits == [0, 0] or args.lon_limits == [0, 0]:
            print("ERROR: lat_limits or lon_limits not specified! They must be two numeric values each.")
            sys.exit(1)

        # ===== Convert longitude from -180:180 to 0:360 =====
        args.lon_limits = [lon % 360 for lon in args.lon_limits]

        return args
    
    def write_info(self, msg):
        print(f"{self.header} INFO: {msg}")
        return

    def write_debug(self, msg):
        print(f"{self.header} DEBUG: {msg}")
        return

    def write_warning(self, msg):
        print(f"{self.header} WARN: {msg}")
        return

    def write_error(self, msg):
        print(f"{self.header} ERROR: {msg}")
        return

class PerformanceTimer:
    """Context manager for measuring execution time of blocks."""
    def __init__(self, io_manager, operation, trace_id=None, threshold_ms=0):
        self.io_manager = io_manager
        self.operation = operation
        self.trace_id = trace_id or "NO_TRACE"
        self.threshold_ms = threshold_ms
        self.start_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        if duration_ms >= self.threshold_ms:
            self.io_manager.write_perf(f"[{self.trace_id}] {self.operation}: {duration_ms:.2f}ms")

import time
# Add write_perf to IOManager
setattr(IOManager, "write_perf", lambda self, msg: print(f"{self.header} [PERF] {msg}"))