import datetime
import time
from pathlib import Path
from EdgeWARN.core.ingest.s3_sync import FileFinder
from EdgeWARN.core.ingest.utils import extract_timestamp
from EdgeWARN.core.ingest.parse import parse_mrms_bucket_path
from EdgeWARN.core.ingest.config import bucket, check_modifiers, mrms_modifiers
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

        # Convert timezone-aware UTC to naive UTC for consistent comparison with S3 timestamps
        if reference_dt.tzinfo is not None:
            reference_dt = reference_dt.replace(tzinfo=None)
        
        if self.verbose:
            print(f"[Scheduler] Reference time: {reference_dt} (tzinfo: {reference_dt.tzinfo})")
            print(f"[Scheduler] Using modifiers: {[m[1] for m in modifiers if m[1] is not None]}")  # Show modifier names

        modifier_times = []

        for region, modifier, _ in modifiers:
            finder = FileFinder(reference_dt, bucket, 20, io_manager)
            bucket_path = parse_mrms_bucket_path(reference_dt, region, modifier)
            files_with_timestamps = finder.lookup_files(bucket_path, verbose=False)
            if not files_with_timestamps:
                if self.verbose:
                    print(f"[{modifier}] No remote files found")
                continue

            # Debug timezone information for all files
            if self.verbose:
                print(f"[{modifier}] DEBUG: Found {len(files_with_timestamps)} files")
                for i, (s3_path, ts) in enumerate(files_with_timestamps[:5]):  # Show first 5
                    print(f"[{modifier}] DEBUG: File {i+1}: {ts} (tzinfo: {ts.tzinfo}) - {s3_path}")
                if len(files_with_timestamps) > 5:
                    print(f"[{modifier}] DEBUG: ... and {len(files_with_timestamps) - 5} more files")

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
                
                if self.verbose and i < 3:  # Show first 3 processed
                    print(f"[{modifier}] PROCESSED: {ts_rounded} (from {ts})")

            modifier_times.append(set(processed_timestamps))

        if not modifier_times:
            if self.verbose:
                print("[Scheduler] No files found in any modifier")
            return None

        common_minutes = set.intersection(*modifier_times)
        if not common_minutes:
            if self.verbose:
                print("[Scheduler] No common timestamps across all modifiers")
                # Debug: show what's in each modifier
                for i, times in enumerate(modifier_times):
                    print(f"[Scheduler] Modifier {i} timestamps: {sorted(list(times))[-5:]}")  # Show last 5
                return None

        latest_common = max(common_minutes)
        if self.verbose:
            print(f"[Scheduler] Latest common timestamp: {latest_common}")
        return latest_common