import re
from datetime import datetime, timezone


def load_last_processed_from_stormcells(stormcell_dir):
    try:
        if stormcell_dir.exists():
            files = sorted(stormcell_dir.glob("stormcells_*.json"))
            if files:
                latest_file = files[-1]
                match = re.search(r"stormcells_(\d{8}-\d{6})\.json", latest_file.name)
                if match:
                    ts_str = match.group(1)
                    dt_exact = datetime.strptime(ts_str, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
                    last_processed = dt_exact.replace(second=0, microsecond=0)
                    return last_processed, f"[Scheduler] Initialized last_processed from {latest_file}: {last_processed}"
                return None, f"[Scheduler] Could not parse timestamp from {latest_file}"
            return None, f"[Scheduler] No previous stormcell data found in {stormcell_dir}. Starting fresh."
        return None, f"[Scheduler] {stormcell_dir} does not exist. Starting fresh."
    except Exception as exc:
        return None, f"[Scheduler] Failed to initialize last_processed: {exc}"
