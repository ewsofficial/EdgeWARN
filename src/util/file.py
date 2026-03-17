from datetime import datetime
from functools import partial
from pathlib import Path
import asyncio
import platform
import sys

from util.io import IOManager

io_manager = IOManager("[Util]")
BASE_DIR: Path = Path(".")


def _log(method, message):
    if hasattr(io_manager, method):
        getattr(io_manager, method)(message)


def _find_existing_path(candidates, missing_message=None):
    for candidate in candidates:
        candidate_path = Path(candidate)
        if candidate_path.exists():
            io_manager.write_debug(f"Using path: {candidate_path}")
            return candidate_path
    if missing_message:
        io_manager.write_warning(missing_message)
    return Path(candidates[0]) if candidates else Path(".")


def _define_paths(base_path):
    global BASE_DIR
    global DATA_DIR, ALERTS_DIR
    global EDGEWARN_ALERTS_DIR, EDGEWARN_ALERTS_IDS_DIR, EDGEWARN_ALERTS_TS_DIR
    global MRMS_NWS_RAW_DIR, MRMS_NWS_DIR, MRMS_NWS_IDS_DIR, MRMS_NWS_TS_DIR, NWS_REGISTRY_PATH
    global MRMS_RALA_DIR, MRMS_CGFLASH_DIR, MRMS_NLDN_DIR, MRMS_ECHOTOP18_DIR, MRMS_ECHOTOP30_DIR
    global MRMS_QPE_DIR, MRMS_RAIN_DIR, MRMS_PRECIPRATE_DIR, MRMS_PROBSEVERE_DIR
    global MRMS_FLASH_CREST_MAXUNIT_DIR, MRMS_FLASH_ARIMAX_DIR, MRMS_FLASH_ARI30M_DIR, MRMS_FLASH_ARI01H_DIR
    global MRMS_FLASH_HP_MAXUNIT_DIR, MRMS_FLASH_SAC_MAXSOIL_DIR, MRMS_FLASH_FFGMAX_DIR
    global MRMS_RQI_DIR, MRMS_DVIL_DIR, MRMS_VIL_DIR, MRMS_VII_DIR
    global MRMS_AZSHEARLOW_DIR, MRMS_AZSHEARMID_DIR, MRMS_COMPOSITE_DIR
    global MRMS_REF_0C_DIR, MRMS_REFM5C_DIR, MRMS_REFM15C_DIR
    global MRMS_RHOHV_DIR, MRMS_PRECIPTYP_DIR, MRMS_MESH_DIR
    global GOES_GLM_DIR, RAP_DIR, STORMCELL_DIR, CELL_DIR, METAR_DIR, SURFACE_DIR, FLASH_FLOOD_DIR
    global GUI_DIR, GUI_RALA_DIR, GUI_NLDN_DIR, GUI_ECHOTOP18_DIR, GUI_ECHOTOP30_DIR, GUI_QPE_DIR
    global GUI_AZSHEARLOW_DIR, GUI_AZSHEARMID_DIR, GUI_PRECIPRATE_DIR, GUI_PROBSEVERE_DIR, GUI_FLASH_DIR
    global GUI_VIL_DIR, GUI_VII_DIR, GUI_ROTATIONT_DIR, GUI_COMPOSITE_DIR, GUI_RHOHV_DIR, GUI_PRECIPTYP_DIR
    global GUI_MAP_DIR, GUI_MANIFEST_JSON, GUI_COLORMAP_JSON
    global WPC_DIR, WPC_SFC_DIR, STORMCELL_JSON

    base_path = Path(base_path)
    data_dir = base_path / "data"
    gui_dir = base_path / "gui"
    alerts_dir = data_dir / "Alerts"
    edgewarn_alerts_dir = alerts_dir / "EdgeWARN"
    official_alerts_dir = alerts_dir / "official"

    BASE_DIR = base_path
    DATA_DIR = data_dir
    ALERTS_DIR = alerts_dir
    EDGEWARN_ALERTS_DIR = edgewarn_alerts_dir
    EDGEWARN_ALERTS_IDS_DIR = edgewarn_alerts_dir / "ids"
    EDGEWARN_ALERTS_TS_DIR = edgewarn_alerts_dir / "timestamps"
    MRMS_NWS_RAW_DIR = data_dir / "NWS_Raw"
    MRMS_NWS_DIR = official_alerts_dir
    MRMS_NWS_IDS_DIR = official_alerts_dir / "ids"
    MRMS_NWS_TS_DIR = official_alerts_dir / "timestamps"
    NWS_REGISTRY_PATH = official_alerts_dir / "alerts_registry.json"
    MRMS_RALA_DIR = data_dir / "RALA"
    MRMS_CGFLASH_DIR = data_dir / "NLDN"
    MRMS_NLDN_DIR = data_dir / "NLDN_Density"
    MRMS_ECHOTOP18_DIR = data_dir / "EchoTop18"
    MRMS_ECHOTOP30_DIR = data_dir / "EchoTop30"
    MRMS_QPE_DIR = data_dir / "QPE_01H"
    MRMS_RAIN_DIR = data_dir / "WarmRainProbability"
    MRMS_PRECIPRATE_DIR = data_dir / "PrecipRate"
    MRMS_PROBSEVERE_DIR = data_dir / "ProbSevere"
    MRMS_FLASH_CREST_MAXUNIT_DIR = data_dir / "FLASH_CREST_MAXUNIT"
    MRMS_FLASH_ARIMAX_DIR = data_dir / "FLASH_ARIMAX"
    MRMS_FLASH_ARI30M_DIR = data_dir / "FLASH_ARI30M"
    MRMS_FLASH_ARI01H_DIR = data_dir / "FLASH_ARI01H"
    MRMS_FLASH_HP_MAXUNIT_DIR = data_dir / "FLASH_HP_MAXUNIT"
    MRMS_FLASH_SAC_MAXSOIL_DIR = data_dir / "FLASH_SAC_MAXSOIL"
    MRMS_FLASH_FFGMAX_DIR = data_dir / "FLASH_FFGMAX"
    MRMS_RQI_DIR = data_dir / "RadarQualityIndex"
    MRMS_DVIL_DIR = data_dir / "VILDensity"
    MRMS_VIL_DIR = data_dir / "VIL"
    MRMS_VII_DIR = data_dir / "VII"
    MRMS_AZSHEARLOW_DIR = data_dir / "AzShearLow"
    MRMS_AZSHEARMID_DIR = data_dir / "AzShearMid"
    MRMS_COMPOSITE_DIR = data_dir / "CompRefQC"
    MRMS_REF_0C_DIR = data_dir / "Ref0C"
    MRMS_REFM5C_DIR = data_dir / "RefM5C"
    MRMS_REFM15C_DIR = data_dir / "RefM15C"
    MRMS_RHOHV_DIR = data_dir / "RhoHV"
    MRMS_PRECIPTYP_DIR = data_dir / "PrecipFlag"
    MRMS_MESH_DIR = data_dir / "MESH"
    GOES_GLM_DIR = data_dir / "GLM"
    RAP_DIR = data_dir / "RAP"
    STORMCELL_DIR = data_dir / "stormcells"
    CELL_DIR = data_dir / "cells"
    METAR_DIR = data_dir / "METAR"
    SURFACE_DIR = data_dir / "surface_features"
    FLASH_FLOOD_DIR = data_dir / "FlashFlood"
    GUI_DIR = gui_dir
    GUI_RALA_DIR = gui_dir / "RALA"
    GUI_NLDN_DIR = gui_dir / "NLDN"
    GUI_ECHOTOP18_DIR = gui_dir / "EchoTop18"
    GUI_ECHOTOP30_DIR = gui_dir / "EchoTop30"
    GUI_QPE_DIR = gui_dir / "QPE_01H"
    GUI_AZSHEARLOW_DIR = gui_dir / "AzShearLow"
    GUI_AZSHEARMID_DIR = gui_dir / "AzShearMid"
    GUI_PRECIPRATE_DIR = gui_dir / "PrecipRate"
    GUI_PROBSEVERE_DIR = gui_dir / "ProbSevere"
    GUI_FLASH_DIR = gui_dir / "FLASH"
    GUI_VIL_DIR = gui_dir / "VILDensity"
    GUI_VII_DIR = gui_dir / "VII"
    GUI_ROTATIONT_DIR = gui_dir / "RotationTrack30min"
    GUI_COMPOSITE_DIR = gui_dir / "CompRefQC"
    GUI_RHOHV_DIR = gui_dir / "RhoHV"
    GUI_PRECIPTYP_DIR = gui_dir / "PrecipFlag"
    GUI_MAP_DIR = gui_dir / "maps"
    GUI_MANIFEST_JSON = gui_dir / "overlay_manifest.json"
    WPC_DIR = base_path / "wpc"
    WPC_SFC_DIR = WPC_DIR / "surface_analysis"
    STORMCELL_JSON = data_dir / "stormcells.json"
    GUI_COLORMAP_JSON = _find_existing_path([
        Path.cwd() / "colormaps.json",
        Path(__file__).resolve().parents[1] / "EWMRS" / "colormaps.json",
        gui_dir / "colormaps.json",
    ], missing_message="colormaps.json not found in common locations; using relative path 'colormaps.json'")


def initialize_filesystem(base_dir=None):
    if base_dir:
        _define_paths(Path(base_dir))


if platform.system() == "Windows":
    _define_paths(Path(r"C:\EdgeWARN_input"))
else:
    try:
        _define_paths(Path.home() / "EdgeWARN_input")
    except Exception:
        _define_paths(Path(r"/workspaces/EdgeWARN_input"))


def latest_files(directory, count):
    directory = Path(directory)
    if not directory.exists():
        _log("write_warning", f"{directory} doesn't exist!")
        return None

    files = []
    for file_path in directory.glob("*"):
        if not file_path.is_file() or file_path.suffix.lower() == ".idx":
            continue

        if file_path.suffix.lower() == ".gz" and file_path.with_suffix("").exists():
            continue

        files.append(file_path)

    files.sort(key=lambda f: (f.stat().st_mtime, f.suffix.lower() == ".gz"))
    return [str(f) for f in files[-count:]]


def clean_idx_files(folders):
    for folder in folders:
        folder = Path(folder)
        if not folder.exists():
            _log("write_error", f"Folder not found: {folder}")
            continue
        idx_files = list(folder.rglob("*.idx"))
        if not idx_files:
            _log("write_debug", f"No IDX files in folder: {folder}")
            continue
        deleted = 0
        for file_path in idx_files:
            try:
                file_path.unlink()
                deleted += 1
            except Exception as e:
                _log("write_error", f"Failed to delete IDX file {file_path}: {e}")
        if deleted > 0:
            _log("write_debug", f"Deleted {deleted} files in {folder}")


def _is_safe_directory(directory: Path, allow_logical_inside=False):
    base_dir = globals().get("BASE_DIR", Path("."))
    try:
        if allow_logical_inside:
            return directory.absolute().is_relative_to(base_dir.absolute()) or directory.resolve().is_relative_to(base_dir.resolve())
        return directory.resolve().is_relative_to(base_dir.resolve())
    except Exception:
        return False


def clean_old_files(directory: Path, max_age_minutes=60, max_files=10):
    directory = Path(directory)
    base_dir = globals().get("BASE_DIR", Path("."))
    if not _is_safe_directory(directory, allow_logical_inside=True):
        _log("write_error", f"SAFETY ERROR: Attempting to clean {directory} which is not inside {base_dir}")
        return

    now = datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    files_deleted = 0
    kept_files = []

    for file_path in directory.glob("*"):
        if not file_path.is_file() or file_path.suffix.lower() == ".idx":
            continue
        try:
            mtime = file_path.stat().st_mtime
            if mtime < cutoff:
                file_path.unlink()
                files_deleted += 1
            else:
                kept_files.append((file_path, mtime))
        except Exception as e:
            _log("write_error", f"Could not process/delete {file_path.name}: {e}")

    if max_files is not None and len(kept_files) > max_files:
        kept_files.sort(key=lambda item: item[1])
        for file_path, _ in kept_files[:len(kept_files) - max_files]:
            try:
                file_path.unlink()
                files_deleted += 1
            except Exception as e:
                _log("write_error", f"Could not delete {file_path.name}: {e}")

    if files_deleted > 0:
        _log("write_debug", f"Deleted {files_deleted} files in {directory}")


def clean_files_by_age(directory: Path, max_age_minutes=60):
    directory = Path(directory)
    base_dir = globals().get("BASE_DIR", Path("."))
    if not _is_safe_directory(directory):
        _log("write_error", f"SAFETY ERROR: Attempting to clean {directory} which is not inside {base_dir}")
        return

    now = datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    files_deleted = 0
    for file_path in directory.glob("*"):
        if not file_path.is_file():
            continue
        try:
            if file_path.stat().st_mtime < cutoff:
                file_path.unlink()
                files_deleted += 1
        except Exception as e:
            _log("write_error", f"Could not process/delete {file_path.name}: {e}")
    if files_deleted > 0:
        _log("write_debug", f"Deleted {files_deleted} files in {directory}")


async def async_clean_old_files(directory: Path, max_age_minutes=60, max_files=10):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, partial(clean_old_files, directory, max_age_minutes=max_age_minutes, max_files=max_files))


async def async_clean_files_by_age(directory: Path, max_age_minutes=60):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, partial(clean_files_by_age, directory, max_age_minutes=max_age_minutes))
