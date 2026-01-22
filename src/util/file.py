from pathlib import Path
import sys
import os
import platform
from datetime import datetime

# Ensure the repository `src` directory is on sys.path so top-level imports
# like `from util.io import IOManager` work when files are executed directly.
try:
    _SRC_DIR = Path(__file__).resolve().parents[1]
    _src_str = str(_SRC_DIR)
    if _src_str not in sys.path:
        sys.path.insert(0, _src_str)
except Exception:
    # Best-effort only; fall back to normal import errors if this fails
    pass

from util.io import IOManager

io_manager = IOManager("[Util]")

def _define_paths(base_path):
    global BASE_DIR, DATA_DIR, MRMS_RALA_DIR, MRMS_CGFLASH_DIR, MRMS_NLDN_DIR, \
           MRMS_ECHOTOP18_DIR, MRMS_ECHOTOP30_DIR, MRMS_QPE_DIR, MRMS_RAIN_DIR, \
           MRMS_PRECIPRATE_DIR, MRMS_PROBSEVERE_DIR, MRMS_FLASH_DIR, MRMS_VIL_DIR, \
           MRMS_VII_DIR, MRMS_AZSHEARLOW_DIR, MRMS_AZSHEARMID_DIR, MRMS_COMPOSITE_DIR, \
           MRMS_RHOHV_DIR, MRMS_PRECIPTYP_DIR, MRMS_MESH_DIR, GOES_GLM_DIR, \
           ABI_CLOUDPRES_DIR, RAP_DIR, MRMS_NWS_RAW_DIR, MRMS_NWS_DIR, STORMCELL_DIR, METAR_DIR, CELL_DIR, GUI_DIR, SURFACE_DIR

    BASE_DIR = base_path
    
    # ---------- PATH CONFIG ----------
    DATA_DIR = BASE_DIR / "data"
    MRMS_NWS_RAW_DIR = DATA_DIR / "NWS_Raw"
    MRMS_NWS_DIR = DATA_DIR / "NWS"
    MRMS_RALA_DIR = DATA_DIR / "RALA"
    MRMS_CGFLASH_DIR = DATA_DIR / "NLDN"
    MRMS_NLDN_DIR = DATA_DIR / "NLDN_Density"
    MRMS_ECHOTOP18_DIR = DATA_DIR / "EchoTop18"
    MRMS_ECHOTOP30_DIR = DATA_DIR / "EchoTop30"
    MRMS_QPE_DIR = DATA_DIR / "QPE_01H"
    MRMS_RAIN_DIR = DATA_DIR / "WarmRainProbability"
    MRMS_PRECIPRATE_DIR = DATA_DIR / "PrecipRate"
    MRMS_PROBSEVERE_DIR = DATA_DIR / "ProbSevere"
    MRMS_FLASH_DIR = DATA_DIR / "FLASH"
    MRMS_VIL_DIR = DATA_DIR / "VILDensity"
    MRMS_VII_DIR = DATA_DIR / "VII"
    MRMS_AZSHEARLOW_DIR = DATA_DIR / "AzShearLow"
    MRMS_AZSHEARMID_DIR = DATA_DIR / "AzShearMid"
    MRMS_COMPOSITE_DIR = DATA_DIR / "CompRefQC"
    MRMS_RHOHV_DIR = DATA_DIR / "RhoHV"
    MRMS_PRECIPTYP_DIR = DATA_DIR / "PrecipFlag"
    MRMS_MESH_DIR = DATA_DIR / "MESH"
    GOES_GLM_DIR = DATA_DIR / "GLM"
    ABI_CLOUDPRES_DIR = DATA_DIR / "ABI-CloudPressure"
    RAP_DIR = DATA_DIR / "RAP"
    STORMCELL_DIR = DATA_DIR / "stormcells"
    CELL_DIR = DATA_DIR / "cells"
    METAR_DIR = DATA_DIR / "METAR"
    SURFACE_DIR = DATA_DIR / "surface_features"
    
    # ---------- GUI PATH CONFIG ----------
    GUI_DIR = BASE_DIR / "gui"

def initialize_filesystem(base_dir=None):
    """
    Initialize the filesystem paths. 
    If base_dir is provided, it uses that as the root.
    Otherwise, it defaults to the standard location.
    """
    if base_dir:
        _define_paths(Path(base_dir))
    else:
        # Re-run default detection logic if explicit reset is needed, 
        # basically doing what the module-level code below does.
        pass

# Default initialization
if platform.system() == "Windows":
    _define_paths(Path(r"C:\EdgeWARN_input"))
else:
    try:
        _define_paths(Path.home() / "EdgeWARN_input")
    except Exception as e:
        _define_paths(Path(r"/workspaces/EdgeWARN_input"))

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
                
                if deleted_files > 0:
                    io_manager.write_debug(f"Deleted {deleted_files} files in {folder}")
        else:
            io_manager.write_error(f"Folder not found: {folder}")

# ---------- CLEANUP ----------
def clean_old_files(directory: Path, max_age_minutes=60):
    # Safety Check: Ensure directory is within BASE_DIR
    try:
        # resolve() handles symlinks and . and .. components
        # is_relative_to (Python 3.9+) checks if BASE_DIR is a parent of directory
        if not directory.resolve().is_relative_to(BASE_DIR.resolve()):
             io_manager.write_error(f"SAFETY ERROR: Attempting to clean {directory} which is not inside {BASE_DIR}")
             return
    except Exception as e:
        # Fallback/Safety catch
        io_manager.write_error(f"Safety check failed for path {directory}: {e}")
        return

    now = datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    files_deleted = 0
    kept_files = []

    for f in directory.glob("*"):
        if f.is_file():
            try:
                mtime = f.stat().st_mtime
                if mtime < cutoff:
                    f.unlink()
                    files_deleted += 1
                else:
                    kept_files.append((f, mtime))
            except Exception as e:
                io_manager.write_error(f"Could not process/delete {f.name}: {e}")

    # If more than 10 files remain, delete the oldest ones until only 10 are left
    if len(kept_files) > 10:
        kept_files.sort(key=lambda x: x[1]) # Sort by mtime (oldest first)
        files_to_remove = len(kept_files) - 10
        for i in range(files_to_remove):
            f, _ = kept_files[i]
            try:
                f.unlink()
                files_deleted += 1
            except Exception as e:
                io_manager.write_error(f"Could not delete {f.name}: {e}")

    if files_deleted > 0:
        io_manager.write_debug(f"Deleted {files_deleted} files in {directory}")

def clean_files_by_age(directory: Path, max_age_minutes=60):
    """
    Delete files in a directory older than max_age_minutes.
    Does NOT enforce a maximum file count limit (unlike clean_old_files).
    """
    # Safety Check: Ensure directory is within BASE_DIR
    try:
        if not directory.resolve().is_relative_to(BASE_DIR.resolve()):
             io_manager.write_error(f"SAFETY ERROR: Attempting to clean {directory} which is not inside {BASE_DIR}")
             return
    except Exception as e:
        io_manager.write_error(f"Safety check failed for path {directory}: {e}")
        return

    now = datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    files_deleted = 0

    for f in directory.glob("*"):
        if f.is_file():
            try:
                mtime = f.stat().st_mtime
                if mtime < cutoff:
                    f.unlink()
                    files_deleted += 1
            except Exception as e:
                io_manager.write_error(f"Could not process/delete {f.name}: {e}")

    if files_deleted > 0:
        io_manager.write_debug(f"Deleted {files_deleted} old files in {directory}")
