import datetime
from pathlib import Path
import requests, re
from datetime import timedelta

# ---------- MRMS ProbSevere ----------
def download_latest_mrms_probsevere_flexible(dt, outdir: Path, max_lookback_minutes=60):
    outdir.mkdir(parents=True, exist_ok=True)
    base_url = "https://mrms.ncep.noaa.gov/ProbSevere/PROBSEVERE/"
    attempt_dt = dt.replace(second=0, microsecond=0)

    for _ in range((max_lookback_minutes // 10) + 1):
        rounded_minute = (attempt_dt.minute // 10) * 10
        attempt_dt_rounded = attempt_dt.replace(minute=rounded_minute)
        date_str = attempt_dt_rounded.strftime("%Y%m%d")
        hhmm_str = attempt_dt_rounded.strftime("%H%M")

        print(f"[ProbSevere] Searching for datetime: {date_str} {hhmm_str}xx")
        try:
            r = requests.get(base_url, timeout=20)
            r.raise_for_status()
            matches = sorted(re.findall(rf"MRMS_PROBSEVERE_{date_str}_{hhmm_str}\d{{2}}\.json", r.text))
            if matches:
                filename = matches[-1]
                outpath = outdir / filename
                if not outpath.exists():
                    print(f"[ProbSevere] Downloading: {filename}")
                    file_r = requests.get(f"{base_url}{filename}", timeout=30)
                    file_r.raise_for_status()
                    with open(outpath, "wb") as f:
                        f.write(file_r.content)
                    print(f"[ProbSevere] Download complete: {filename}")
                else:
                    print(f"[ProbSevere] Already exists: {filename}")

                return outpath
            else:
                attempt_dt -= timedelta(minutes=10)
        except Exception as e:
            print(f"[ProbSevere] Error: {e}")
            break

    print("[ProbSevere] No file found in time window.")
    return None