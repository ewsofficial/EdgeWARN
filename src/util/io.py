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
            default=[0, 0],
            help="Latitude limits for processing"
        )
        parser.add_argument(
            "--lon_limits",
            type=float,
            nargs=2,
            metavar=("LON_MIN", "LON_MAX"),
            default=[0, 0],
            help="Longitude limits for processing"
        )

        args = parser.parse_args()

        # ===== Validation =====
        if not args.lat_limits or not args.lon_limits or len(args.lat_limits) != 2 or len(args.lon_limits) != 2:
            print("ERROR: Latitude and longitude limits must both be provided as two numeric values each.")
            print("Example: --lat_limits 33.5 35.7 --lon_limits 280.7 284.6")
            sys.exit(1)

        if args.lat_limits == [0, 0] or args.lon_limits == [0, 0]:
            print("ERROR: lat_limits or lon_limits not specified! They must be two numeric values each.")
            sys.exit(1)

        # ===== Convert longitude from -180:180 to 0:360 if needed =====
        args.lon_limits = [lon % 360 for lon in args.lon_limits]

        return args
        
    def write_debug(self, msg):
        print(f"{self.header} DEBUG: {msg}")
        return

    def write_warning(self, msg):
        print(f"{self.header} WARN: {msg}")
        return

    def write_error(self, msg):
        print(f"{self.header} ERROR: {msg}")
        return