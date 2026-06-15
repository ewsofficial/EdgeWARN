from datetime import datetime, timezone
import argparse
import sys
import time


class TimestampedOutput:
    def __init__(self, stream):
        self.stream = stream

    def write(self, message):
        if message.strip():
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
            self.queue.put(f"[{timestamp}] {message.rstrip()}")

    def flush(self):
        pass


class IOManager:
    def __init__(self, header, *, include_timestamps=False):
        self.header = header
        self.include_timestamps = include_timestamps

    def _timestamp_prefix(self):
        if not self.include_timestamps:
            return ""
        return f"[{datetime.now(timezone.utc).isoformat()}] "

    def _write(self, level, msg):
        print(f"{self._timestamp_prefix()}{self.header} {level} {msg}")

    @staticmethod
    def get_base_dir_arg():
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--base_dir", "--base-dir", dest="base_dir", type=str, default=None)
        args, _ = parser.parse_known_args()
        return args.base_dir

    def get_args(self):
        parser = argparse.ArgumentParser(description="EdgeWARN modifier specification")
        parser.add_argument("--lat_limits", type=float, nargs=2, metavar=("LAT_MIN", "LAT_MAX"), default=[20, 55], help="Latitude limits for processing (default: 20 55)")
        parser.add_argument("--lon_limits", type=float, nargs=2, metavar=("LON_MIN", "LON_MAX"), default=[230, 300], help="Longitude limits for processing (default: 230 300)")
        parser.add_argument("--base_dir", "--base-dir", dest="base_dir", type=str, default=None, help="Custom base directory for input/output data")
        parser.add_argument("--profile", action="store_true", help="Enable performance profiling")
        parser.add_argument("--disable-ctam", action="store_true", help="Skip CTAM module execution during integration")
        parser.add_argument("--disable-tracking", action="store_true", help="Skip lineage detection and Kalman tracking in storm cell detection")
        parser.add_argument("--disable-ewmrs", action="store_true", help="Disable EWMRS workers and rendering pipeline")
        parser.add_argument("--disable-nws", action="store_true", help="Disable background NWS alert ingestion")
        parser.add_argument("--disable-metar", action="store_true", help="Disable background METAR ingestion")
        parser.add_argument("--disable-goes", action="store_true", help="Disable GOES ingest, GLM ingest, and GOES rendering components")
        parser.add_argument("--disable-polygon-expansion", action="store_true", help="Use original ProbSevere polygons directly and skip radar gate mapping plus watershed expansion")
        parser.add_argument("--refl-threshold", type=float, default=37.5, help="Override the baseline reflectivity threshold used by storm cell detection (default: 37.5)")
        parser.add_argument("--min-seed-percentage", type=float, default=0.001, help="Override the minimum polygon seed coverage ratio used during gate expansion (default: 0.001)")
        parser.add_argument("--drop-offset", type=float, default=10.0, help="Override the dynamic reflectivity drop offset used during gate expansion (default: 10.0)")
        args = parser.parse_args()

        if len(args.lat_limits) != 2 or len(args.lon_limits) != 2:
            print("ERROR: Latitude and longitude limits must each have exactly 2 numeric values.")
            sys.exit(1)
        if args.lat_limits == [0, 0] or args.lon_limits == [0, 0]:
            print("ERROR: lat_limits or lon_limits not specified! They must be two numeric values each.")
            sys.exit(1)
        if args.min_seed_percentage < 0:
            print("ERROR: --min-seed-percentage must be non-negative.")
            sys.exit(1)

        args.lon_limits = [lon % 360 for lon in args.lon_limits]
        return args

    def write_info(self, msg):
        self._write("INFO:", msg)

    def write_debug(self, msg):
        self._write("DEBUG:", msg)

    def write_warning(self, msg):
        self._write("WARN:", msg)

    def write_error(self, msg):
        self._write("ERROR:", msg)

    def write_perf(self, msg):
        self._write("[PERF]", msg)


class PerformanceTimer:
    def __init__(self, io_manager, operation, trace_id=None, threshold_ms=0):
        self.io_manager = io_manager
        self.operation = operation
        self.trace_id = trace_id or "NO_TRACE"
        self.threshold_ms = threshold_ms
        self.start_time = 0.0

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.time() - self.start_time) * 1000
        if duration_ms >= self.threshold_ms:
            self.io_manager.write_perf(f"[{self.trace_id}] {self.operation}: {duration_ms:.2f}ms")
