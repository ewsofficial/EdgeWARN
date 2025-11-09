from pathlib import Path
import platform
from datetime import datetime
from util.io import IOManager

io_manager = IOManager("[Util]")

# ---------- PATH CONFIG ----------
BASE_DIR = Path("C:/EdgeWARN_input") if platform.system() == "Windows" else Path("EdgeWARN_input")
MRMS_RALA_DIR = BASE_DIR / "RALA"
MRMS_NLDN_DIR = BASE_DIR / "NLDN"
MRMS_ECHOTOP18_DIR = BASE_DIR / "EchoTop18"
MRMS_ECHOTOP30_DIR = BASE_DIR / "EchoTop30"
MRMS_QPE_DIR = BASE_DIR / "QPE_01H"
MRMS_PRECIPRATE_DIR = BASE_DIR / "PrecipRate"
MRMS_PROBSEVERE_DIR = BASE_DIR / "ProbSevere"
MRMS_FLASH_DIR = BASE_DIR / "FLASH"
MRMS_VIL_DIR = BASE_DIR / "VILDensity"
MRMS_VII_DIR = BASE_DIR / "VII"
MRMS_ROTATIONT_DIR = BASE_DIR / "RotationTrack30min"
MRMS_COMPOSITE_DIR = BASE_DIR / "CompRefQC"
MRMS_RHOHV_DIR = BASE_DIR / "RhoHV"
MRMS_PRECIPTYP_DIR = BASE_DIR / "PrecipFlag"
STORMCELL_JSON = Path("stormcell_test.json")
GUI_MANIFEST_JSON = Path("overlay_manifest.json")
TEMP_DIR = BASE_DIR / "tmp"

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
        io_manager.write_warning(f"{dir} doesn't exist!")
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
                io_manager.write_debug(f"No IDX files in folder: {folder}")
                return
            else:
                deleted_files = 0
                for f in idx_files:
                    try:
                        f.unlink()
                        deleted_files += 1
                    except Exception as e:
                        io_manager.write_error(f"Failed to delete IDX file {f}: {e}")
                io_manager.write_debug(f"Deleted {deleted_files} files in {folder}")
        else:
            io_manager.write_error(f"Folder not found: {folder}")

def wipe_temp():
    for f in TEMP_DIR.glob("*"):
        try:
            f.unlink()
            io_manager.write_debug(f"Deleted temporary file: {f.name}")
        except Exception as e:
            io_manager.write_error(f"Could not delete temporary file {f.name}: {e}")

# ---------- CLEANUP ----------
def clean_old_files(directory: Path, max_age_minutes=60):
    now = datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    for f in directory.glob("*"):
        if f.is_file() and f.stat().st_mtime < cutoff:
            try:
                f.unlink()
                io_manager.write_debug(f"Deleted old file: {f.name}")
            except Exception as e:
                io_manager.write_error(f"Could not delete {f.name}: {e}")