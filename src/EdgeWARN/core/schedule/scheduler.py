import datetime
import time
from pathlib import Path
from EdgeWARN.core.ingest.download import FileFinder
from EdgeWARN.core.ingest.config import base_dir, check_modifiers
from util.io import IOManager

io_manager = IOManager("[DataIngestion]")


class MRMSUpdateChecker:
    """Checks MRMS sources for new files and finds the latest common timestamps."""

    def __init__(self, max_time=datetime.timedelta(hours=6), max_entries=10, verbose=False):
        self.max_time = max_time
        self.max_entries = max_entries
        self.verbose = verbose

    def has_update(self, modifier_tuple, reference_dt=None):
        """Check if a specific MRMS modifier has a new file."""
        modifier, outdir = modifier_tuple
        if reference_dt is None:
            reference_dt = datetime.datetime.now(datetime.timezone.utc)

        finder = FileFinder(reference_dt, base_dir, self.max_time, self.max_entries, io_manager)

        try:
            files_with_timestamps = finder.lookup_files(modifier, verbose=False)
            if not files_with_timestamps:
                if self.verbose:
                    print(f"[{modifier}] No remote files found")
                return False

            _, latest_source_time = max(files_with_timestamps, key=lambda x: x[1])
            local_files = list(Path(outdir).glob("*.gz")) + list(Path(outdir).glob("*.grib2"))

            if not local_files:
                if self.verbose:
                    print(f"[{modifier}] No local files found")
                return True

            local_times = []
            for f in local_files:
                ts = finder.extract_timestamp_from_filename(f.name)
                if ts:
                    local_times.append(ts)

            if not local_times:
                if self.verbose:
                    print(f"[{modifier}] Could not extract timestamps from local files")
                return True

            latest_local_time = max(local_times)
            if self.verbose:
                print(f"[{modifier}] Remote: {latest_source_time}, Local: {latest_local_time}")
            return latest_source_time > latest_local_time

        except Exception as e:
            print(f"[MRMSUpdateChecker] Error checking {modifier}: {e}")
            return False

    def all_sources_available(self, modifiers):
        """Check all MRMS modifiers for new data availability."""
        all_new = True
        for modifier in modifiers:
            if self.has_update(modifier):
                print(f"[{modifier[0]}] New file available")
            else:
                print(f"[{modifier[0]}] No new file")
                all_new = False
        return all_new

    def latest_common_minute_1h(self, modifiers, reference_dt=None):
        """
        Find the latest common timestamp (to the minute) across all modifiers in the past hour.
        """
        if reference_dt is None:
            reference_dt = datetime.datetime.now(datetime.timezone.utc)

        max_time = datetime.timedelta(hours=1)
        modifier_times = []

        for modifier, _ in modifiers:
            finder = FileFinder(reference_dt, base_dir, max_time, 10, io_manager)
            files_with_timestamps = finder.lookup_files(modifier, verbose=False)
            if not files_with_timestamps:
                if self.verbose:
                    print(f"[{modifier}] No remote files found in the last hour")
                continue

            ts_rounded = [ts.replace(second=0, microsecond=0) for _, ts in files_with_timestamps]
            modifier_times.append(set(ts_rounded))

        if not modifier_times:
            if self.verbose:
                print("[Scheduler] No files found in any modifier within the last hour")
            return None

        common_minutes = set.intersection(*modifier_times)
        if not common_minutes:
            if self.verbose:
                print("[Scheduler] No common timestamps across all modifiers in the last hour")
            return None

        latest_common = max(common_minutes)
        if self.verbose:
            print(f"[Scheduler] Latest common timestamp within 1h: {latest_common}")
        return latest_common


if __name__ == "__main__":
    checker = MRMSUpdateChecker(verbose=True)

    print("\n=== MRMS Common Timestamp Monitor ===")
    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        print(f"\nCurrent time: {now}")
        common_ts = checker.latest_common_minute_1h(check_modifiers)
        if common_ts:
            print(f"[Result] Latest common timestamp: {common_ts}")
        else:
            print("[Result] Could not determine a common timestamp in last hour")
        time.sleep(15)
