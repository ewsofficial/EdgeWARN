import datetime
from EdgeWARN.DataIngestion.download import FileFinder
from EdgeWARN.DataIngestion.config import base_dir, check_modifiers

#################################
### EdgeWARN Scheduler v1.2.0 ###
### Author: Yuchen Wei        ###
#################################

class SchedulerUtils:
    """
    Utility class to check synchronized MRMS data availability across modifiers.
    """
    def __init__(self, base_url, mrms_modifiers, max_time, max_entries, exclude_modifiers=None):
        """
        Args:
            base_url (str): Base directory or URL for MRMS files
            mrms_modifiers (list): List of (modifier, description) tuples
            max_time (datetime.timedelta): Maximum lookback window for data
            max_entries (int): Max number of files to query
            exclude_modifiers (list[str]): Optional list of modifiers to skip
        """
        self.base_url = base_url
        self.mrms_modifiers = mrms_modifiers
        self.max_time = max_time
        self.max_entries = max_entries
        self.exclude_modifiers = exclude_modifiers or []

    def check_latest_common_timestamp(self, current_time):
        """
        Finds the latest timestamp minute (UTC) that exists in *all* included modifiers.

        Returns:
            datetime.datetime (UTC): latest minute with full coverage
            None: if no common minute exists
        """
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=datetime.timezone.utc)

        timestamp_sets = {}
        for modifier, _ in self.mrms_modifiers:
            if modifier in self.exclude_modifiers:
                print(f"[Scheduler] ⚙️ Skipping excluded modifier: {modifier}")
                continue

            finder = FileFinder(current_time, self.base_url, self.max_time, self.max_entries)
            files = finder.lookup_files(modifier)
            if not files:
                print(f"[Scheduler] ❌ No files found for {modifier}")
                return None

            # Round all timestamps to minute precision
            minutes = {ts.replace(second=0, microsecond=0) for _, ts in files}
            timestamp_sets[modifier] = minutes

        if not timestamp_sets:
            print("[Scheduler] ⚠️ No active modifiers left after exclusions.")
            return None

        # Intersect all timestamp sets to find common minutes
        common_minutes = set.intersection(*timestamp_sets.values())
        if not common_minutes:
            print("[Scheduler] ⚠️ No common timestamps across all modifiers.")
            return None

        # Return the latest common timestamp
        latest_common = max(common_minutes)
        print(f"[Scheduler] ✅ Latest common timestamp minute: {latest_common.isoformat()}")
        return latest_common


if __name__ == "__main__":
    scheduler = SchedulerUtils(
        base_url=base_dir,
        mrms_modifiers=check_modifiers,
        max_time=datetime.timedelta(hours=6),
        max_entries=10,
        exclude_modifiers=["MergedReflectivityQC", "GaugeCorrQPE01H"]  # Example exclusions
    )
    import time

    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        common_time = scheduler.check_latest_common_timestamp(now)

        if common_time:
            print(f"\n[Result] ✅ Latest synchronized dataset minute: {common_time}")
        else:
            print("\n[Result] ❌ No synchronized dataset minute found.")
        time.sleep(6)
