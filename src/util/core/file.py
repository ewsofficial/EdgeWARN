from pathlib import Path
import platform
from datetime import datetime

# ---------- PATH CONFIG ----------
BASE_DIR = Path("C:/input_data") if platform.system() == "Windows" else Path("data_ingest")
MRMS_3D_DIR = BASE_DIR / "nexrad_merged"
MRMS_LOWREFL_DIR = BASE_DIR / "refl_at_lowest"
MRMS_LTNG_DIR = BASE_DIR / "mrms_lightning"
MRMS_NLDN_DIR = BASE_DIR / "mrms_nldn"
MRMS_ECHOTOP18_DIR = BASE_DIR / "mrms_echotop18"
MRMS_ECHOTOP30_DIR = BASE_DIR / "mrms_echotop30"
MRMS_QPE15_DIR = BASE_DIR / "mrms_qpe15"
MRMS_PRECIPRATE_DIR = BASE_DIR / "mrms_preciprate"
MRMS_PROBSEVERE_DIR = BASE_DIR / "mrms_probsevere"
MRMS_RADAR_DIR = BASE_DIR / "nexrad_merged"
MRMS_FLASH_DIR = BASE_DIR / "mrms_flash"
MRMS_VIL_DIR = BASE_DIR / "mrms_vil_density"
MRMS_VII_DIR = BASE_DIR / "mrms_vii"
MRMS_ROTATIONT_DIR = BASE_DIR / "mrms_rotationtrack"
MRMS_RHOHV_DIR = BASE_DIR / "mrms_rhohv"
MRMS_PRECIPTYP_DIR = BASE_DIR / "mrms_precipflag"
THREDDS_RTMA_DIR = BASE_DIR / "rtma"
NOAA_RAP_DIR = BASE_DIR / "rap"
TEMP_DIR = BASE_DIR / "temp"

# NEW LATEST FILES FUNCTION
def latest_files(dir, n):
    """
    Return the n most recent files in a directory as a list (oldest to newest), excluding .idx files
    Inputs:
    - dir: Directory
    - n: Number of files
    Outputs:
    - List of files (oldest to newest) in the directory
    """
    if not dir.exists():
        print(f"WARNING: {dir} doesn't exist!")
        return
    files = sorted(
        [f for f in dir.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime
    )
    if len(files) < n:
        raise RuntimeError(f"Not enough files in {dir}")
    return [str(f) for f in files[-n:]]

def clean_idx_files(folders):
    """
    Remove IDX files in a specified list of folders.
    Inputs:
    - folders: list of folders you want to remove IDX files from
    """
    for folder in folders:
        if folder.exists():
            idx_files = list(folder.rglob("*.idx"))
            if len(idx_files) == 0:
                print(f"No IDX files in folder: {folder}")
                return
            else:
                for f in idx_files:
                    try:
                        f.unlink()
                        print(f"Deleted IDX file: {f}")
                    except Exception as e:
                        print(f"Failed to delete IDX file {f}: {e}")
        else:
            print(f"Folder not found: {folder}")

def wipe_temp():
    for f in TEMP_DIR.glob("*"):
        try:
            f.unlink()
            print(f"Deleted temporary file: {f.name}")
        except Exception as e:
            print(f"Could not delete temporary file {f.name}: {e}")

# ---------- CLEANUP ----------
def clean_old_files(directory: Path, max_age_minutes=20):
    now = datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    for f in directory.glob("*"):
        if f.is_file() and f.stat().st_mtime < cutoff:
            try:
                f.unlink()
                print(f"Deleted old file: {f.name}")
            except Exception as e:
                print(f"Could not delete {f.name}: {e}")