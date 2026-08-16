import datetime
import time
import concurrent.futures
from pathlib import Path
from EdgeWARN.ingest.mrms.s3_sync import FileFinder
from EdgeWARN.ingest.mrms.utils import extract_timestamp
from EdgeWARN.ingest.mrms.parse import parse_mrms_bucket_path
from EdgeWARN.ingest.mrms.config import mrms_bucket
from EdgeWARN.ingest.mrms.timestamp_utils import round_to_nearest_even_minute
from EdgeWARN.schedule.config import (
    modifier_lookup_max_entries,
    mrms_update_checker_max_entries,
    s3_lookback_hours,
    slow_check_log_threshold_ms,
)
from util.io import IOManager, PerformanceTimer
import uuid

io_manager = IOManager("[DataIngestion]")


class MRMSUpdateChecker:
    """Checks MRMS sources for new files and finds the latest common timestamps."""

    def __init__(self, max_entries=None, verbose=False):
        self.max_entries = (
            mrms_update_checker_max_entries() if max_entries is None else max_entries
        )
        self.verbose = verbose
        # Shared S3 client for all checks
        import boto3
        from botocore import UNSIGNED
        from botocore.client import Config
        self.s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    def has_update(self, modifier_tuple, reference_dt=None):
        """Check if a specific MRMS modifier has a new file."""
        region, modifier, outdir = modifier_tuple
        if reference_dt is None:
            reference_dt = datetime.datetime.now(datetime.timezone.utc)

        # Pass shared client
        finder = FileFinder(reference_dt, mrms_bucket(), self.max_entries, io_manager, client=self.s3_client)
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



    def _get_modifier_times(
        self,
        modifier_tuple,
        reference_dt,
        trace_id=None,
        last_processed=None,
        s3_bucket=None,
        max_entries=None,
    ):
        """Helper to fetch timestamps for a single modifier.

        ``s3_bucket`` and ``max_entries`` are resolved by the caller because this
        runs once per modifier inside a thread pool. Reading them here meant one
        catalog stat per modifier per tick, issued concurrently from every worker
        thread against a config cache that is not synchronized.
        """
        region, modifier, _ = modifier_tuple
        if s3_bucket is None:
            s3_bucket = mrms_bucket()
        if max_entries is None:
            max_entries = modifier_lookup_max_entries()
        # Pass shared client
        finder = FileFinder(
            reference_dt,
            s3_bucket,
            max_entries,
            io_manager,
            client=self.s3_client,
        )
        bucket_path = parse_mrms_bucket_path(reference_dt, region, modifier)
        try:
            # We can't use PerformanceTimer here easily because it's synchronous + threaded map
            # But we can log manually if verbose
            t0 = time.time()
            
            # Optimization: StartAfter to skip previous history
            # If last_processed is provided, start after that timestamp
            start_after = None
            
            if last_processed:
                # Use last_processed timestamp converted to filename format
                # This is much more efficient than looking back 2 hours
                if modifier:
                    # Standard MRMS: MRMS_{modifier}_{YYYYMMDD}-{HH}
                    # We need to construct a key that is alphabetically just after the last processed file
                    # Ideally we'd know the exact filename, but constructing a prefix based 
                    # on last_processed is a good heuristic.
                    # Start matching from last_processed
                    start_after = f"{bucket_path}MRMS_{modifier}_{last_processed.strftime('%Y%m%d-%H%M')}"
                else:
                    # ProbSevere: MRMS_PROBSEVERE_{YYYYMMDD}_{HH}
                    start_after = f"{bucket_path}MRMS_PROBSEVERE_{last_processed.strftime('%Y%m%d_%H%M')}"
            else:
                 # Standardfallback: skip to the configured lookback
                 from datetime import timedelta
                 sa_dt = reference_dt - timedelta(hours=s3_lookback_hours())
                 if modifier:
                    start_after = f"{bucket_path}MRMS_{modifier}_{sa_dt.strftime('%Y%m%d-%H')}"
                 else:
                    start_after = f"{bucket_path}MRMS_PROBSEVERE_{sa_dt.strftime('%Y%m%d_%H')}"

            files_with_timestamps = finder.lookup_files(bucket_path, verbose=False, start_after=start_after)
            dt = (time.time() - t0) * 1000
            if dt > slow_check_log_threshold_ms():
                print(f"[PERF] [{trace_id}] Scheduler check for {modifier}: {dt:.2f}ms")
                
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

    def latest_common_minute_1h(self, modifiers, reference_dt=None, last_processed=None):
        """
        Find the latest common timestamp (to the minute) across all modifiers.
        FIXED: Now properly handles timezone-aware vs UTC conflicts.
        """
        if reference_dt is None:
            reference_dt = datetime.datetime.now(datetime.timezone.utc)

        trace_id = f"SCHED-{uuid.uuid4().hex[:8]}"

        modifier_times = []

        # Parallelize checks using ThreadPoolExecutor
        t0 = time.time()
        s3_bucket = mrms_bucket()
        max_entries = modifier_lookup_max_entries()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Map returns an iterator in the order of the inputs
            # Pass last_processed to _get_modifier_times
            results = executor.map(
                lambda m: self._get_modifier_times(
                    m,
                    reference_dt,
                    trace_id,
                    last_processed,
                    s3_bucket=s3_bucket,
                    max_entries=max_entries,
                ),
                modifiers,
            )
            
            modifier_times.extend(results)
        
        duration = (time.time() - t0) * 1000
        print(f"[PERF] [{trace_id}] Scheduler Check Total: {duration:.2f}ms")

        if not modifier_times or len(modifier_times) != len(modifiers):
            if self.verbose:
                print("[Scheduler] No files found in any modifier")
            return self.check_https_fallback(modifiers, reference_dt)

        if any(not timestamps for timestamps in modifier_times):
            if self.verbose:
                print("[Scheduler] At least one required modifier has no timestamps")
            return self.check_https_fallback(modifiers, reference_dt)

        common_minutes = set.intersection(*modifier_times)
        if not common_minutes:
            if self.verbose:
                print("[Scheduler] No common timestamps across all modifiers")

            return self.check_https_fallback(modifiers, reference_dt)

        latest_common = max(common_minutes)
        if self.verbose and latest_common != last_processed:
            print(f"[Scheduler] Latest common timestamp updated: {latest_common}")
        return latest_common
    
    def check_https_fallback(self, modifiers, reference_dt):
        """
        Try to find the common timestamp using HTTPS fallback logic.
        Now runs concurrently.
        """
        from EdgeWARN.ingest.mrms.https_client import HttpsFileFinder

        if reference_dt is None:
            reference_dt = datetime.datetime.now(datetime.timezone.utc)
            
        print("[Scheduler] Attempting HTTPS Fallback for timestamps (Parallel)...")
        
        modifier_times = []
        
        def check_single_https(modifier_tuple):
            region, modifier, _ = modifier_tuple
            try:
                # Re-instantiate per thread or use thread-safe if HttpsFileFinder is safe
                # HttpsFileFinder seems lightweight
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
                
                return timestamps
                    
            except Exception as e:
                print(f"[Scheduler] HTTPS Check Error for {modifier}: {e}")
                return None

        # Execute in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(check_single_https, modifiers)
            
            modifier_times.extend(results)
        
        if (
            not modifier_times
            or len(modifier_times) != len(modifiers)
            or any(not timestamps for timestamps in modifier_times)
        ):
            return None

        common_minutes = set.intersection(*modifier_times)
        if not common_minutes:
            if self.verbose:
                print("[Scheduler] HTTPS: No common timestamps across all modifiers")
            return None

        latest_common = max(common_minutes)
        print(f"[Scheduler] HTTPS Fallback found latest common: {latest_common}")
        return latest_common
