import datetime
import time
from pathlib import Path
from EdgeWARN.core.ingest.mrms.s3_sync import FileFinder
from EdgeWARN.core.ingest.mrms.utils import extract_timestamp
from EdgeWARN.core.ingest.mrms.parse import parse_mrms_bucket_path
from EdgeWARN.core.ingest.mrms.config import bucket, check_modifiers, mrms_modifiers
from util.io import IOManager

io_manager = IOManager("[DataIngestion]")


class MRMSUpdateChecker:
    """Checks MRMS sources for new files and finds the latest common timestamps."""

    def __init__(self, max_entries=10, verbose=False):
        self.max_entries = max_entries
        self.verbose = verbose

    def has_update(self, modifier_tuple, reference_dt=None):
        """Check if a specific MRMS modifier has a new file."""
        region, modifier, outdir = modifier_tuple
        if reference_dt is None:
            reference_dt = datetime.datetime.now(datetime.timezone.utc)

        finder = FileFinder(reference_dt, bucket, self.max_entries, io_manager)
        try:
            bucket_path = parse_mrms_bucket_path(reference_dt, region, modifier)
            files_with_timestamps = finder.lookup_files(bucket_path, verbose=False)
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
                ts = extract_timestamp(f.name)
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
        for modifier_tuple in modifiers:
            if self.has_update(modifier_tuple):
                print(f"[{modifier_tuple[1]}] New file available")
            else:
                print(f"[{modifier_tuple[1]}] No new file")
                all_new = False
        return all_new

    def latest_common_minute_1h(self, modifiers, reference_dt=None):
        """
        Find the latest common timestamp (to the minute) across all modifiers.
        FIXED: Now properly handles timezone-aware vs UTC conflicts.
        """
        if reference_dt is None:
            reference_dt = datetime.datetime.now(datetime.timezone.utc)



        modifier_times = []

        for region, modifier, _ in modifiers:
            finder = FileFinder(reference_dt, bucket, 20, io_manager)
            bucket_path = parse_mrms_bucket_path(reference_dt, region, modifier)
            files_with_timestamps = finder.lookup_files(bucket_path, verbose=False)
            if not files_with_timestamps:
                if self.verbose:
                    print(f"[{modifier}] No remote files found")
                continue

            # Process timestamps: ensure all are timezone-aware UTC
            processed_timestamps = []
            for s3_path, ts in files_with_timestamps:
                # Ensure timestamp is timezone-aware UTC
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=datetime.timezone.utc)
                elif ts.tzinfo != datetime.timezone.utc:
                    # Convert to UTC if in different timezone
                    ts = ts.astimezone(datetime.timezone.utc)
                
                ts_rounded = ts.replace(second=0, microsecond=0)
                processed_timestamps.append(ts_rounded)
                


            modifier_times.append(set(processed_timestamps))

        if not modifier_times:
            if self.verbose:
                print("[Scheduler] No files found in any modifier")
            return None

        common_minutes = set.intersection(*modifier_times)
        if not common_minutes:
            if self.verbose:
                print("[Scheduler] No common timestamps across all modifiers")

                return None

        latest_common = max(common_minutes)
        if self.verbose:
            print(f"[Scheduler] Latest common timestamp: {latest_common}")
        return latest_common