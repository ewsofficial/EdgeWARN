
import datetime
import time
import concurrent.futures
from pathlib import Path
from EWMRS.ingest.mrms.s3_sync import FileFinder
from EWMRS.ingest.mrms.utils import extract_timestamp
from EWMRS.ingest.mrms.parse import parse_mrms_bucket_path
from EWMRS.ingest.mrms.config import bucket
from EWMRS.ingest.mrms.timestamp_utils import round_to_nearest_even_minute
from EWMRS.util.io import IOManager

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

    def _get_modifier_times(self, modifier_tuple, reference_dt):
        """Helper to fetch timestamps for a single modifier."""
        region, modifier, _ = modifier_tuple
        finder = FileFinder(reference_dt, bucket, 20, io_manager)
        bucket_path = parse_mrms_bucket_path(reference_dt, region, modifier)
        try:
            files_with_timestamps = finder.lookup_files(bucket_path, verbose=False)
        except Exception as e:
            if self.verbose:
                 print(f"[{modifier}] Error looking up files: {e}")
            return set()

        if not files_with_timestamps:
            if self.verbose:
                print(f"[{modifier}] No remote files found")
            return set()

        processed_timestamps = []
        for s3_path, ts in files_with_timestamps:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=datetime.timezone.utc)
            elif ts.tzinfo != datetime.timezone.utc:
                ts = ts.astimezone(datetime.timezone.utc)
            
            ts_rounded = round_to_nearest_even_minute(ts)
            processed_timestamps.append(ts_rounded)
            
        return set(processed_timestamps)


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

        # Parallelize checks using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Map returns an iterator in the order of the inputs
            results = executor.map(lambda m: self._get_modifier_times(m, reference_dt), modifiers)
            
            for res in results:
                if res:
                    modifier_times.append(res)

        if not modifier_times:
            if self.verbose:
                print("[Scheduler] No files found in any modifier")
            return self.check_https_fallback(modifiers, reference_dt)

        common_minutes = set.intersection(*modifier_times)
        if not common_minutes:
            if self.verbose:
                print("[Scheduler] No common timestamps across all modifiers")

            return self.check_https_fallback(modifiers, reference_dt)

        latest_common = max(common_minutes)
        if self.verbose:
            print(f"[Scheduler] Latest common timestamp: {latest_common}")
        return latest_common
    
    def check_https_fallback(self, modifiers, reference_dt):
        """
        Try to find the common timestamp using HTTPS fallback logic.
        """
        from EWMRS.ingest.mrms.https_client import HttpsFileFinder

        if reference_dt is None:
            reference_dt = datetime.datetime.now(datetime.timezone.utc)
            
        print("[Scheduler] Attempting HTTPS Fallback for timestamps...")
        
        modifier_times = []
        
        # Checking serially for now as this is a fallback
        for modifier_tuple in modifiers:
            region, modifier, _ = modifier_tuple
            
            try:
                finder = HttpsFileFinder(reference_dt, io_manager)
                # find_files_sync returns URLs
                urls = finder.find_files_sync(region, modifier)
                
                timestamps = set()
                for url in urls:
                    # extract_timestamp works on the filename part, and url behaves like a path
                    ts = extract_timestamp(url.split('/')[-1])
                    if ts:
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=datetime.timezone.utc)
                        timestamps.add(round_to_nearest_even_minute(ts))
                
                if timestamps:
                    modifier_times.append(timestamps)
                else:
                    if self.verbose:
                         print(f"[{modifier}] No files found via HTTPS")
            except Exception as e:
                print(f"[Scheduler] HTTPS Check Error for {modifier}: {e}")
        
        if not modifier_times:
            return None

        common_minutes = set.intersection(*modifier_times)
        if not common_minutes:
            if self.verbose:
                print("[Scheduler] HTTPS: No common timestamps across all modifiers")
            return None

        latest_common = max(common_minutes)
        print(f"[Scheduler] HTTPS Fallback found latest common: {latest_common}")
        return latest_common