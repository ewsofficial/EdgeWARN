from pathlib import Path
import platform

from util.io import IOManager

io_manager = IOManager("[Util]")
GUI_DIR = Path(".")

_DEFAULT_BASE_DIR = Path(r"C:\EWMRS") if platform.system() == "Windows" else Path.home() / "EWMRS"
_arg_base_dir = IOManager.get_base_dir_arg()
BASE_DIR = Path(_arg_base_dir) if _arg_base_dir else _DEFAULT_BASE_DIR
if _arg_base_dir:
    io_manager.write_info(f"Using custom base directory: {BASE_DIR}")


def _find_existing_path(candidates, missing_message=None):
    for candidate in candidates:
        candidate_path = Path(candidate)
        if candidate_path.exists():
            io_manager.write_debug(f"Using path: {candidate_path}")
            return candidate_path
    if missing_message:
        io_manager.write_warning(missing_message)
    return Path(candidates[0]) if candidates else Path(".")


def _refresh_colormap_path():
    global GUI_COLORMAP_JSON
    GUI_COLORMAP_JSON = _find_existing_path([
        Path.cwd() / "colormaps.json",
        Path(__file__).resolve().parents[1] / "EWMRS" / "colormaps.json",
        Path(__file__).resolve().parents[2] / "src" / "EWMRS" / "colormaps.json",
        Path(__file__).resolve().parents[2] / "EWMRS" / "colormaps.json",
        GUI_DIR / "colormaps.json",
    ], missing_message="colormaps.json not found in common locations; using relative path 'colormaps.json'")


def _init_paths():
    _init_paths_base(Path(BASE_DIR))
    _refresh_colormap_path()


def _init_paths_base(base_path: Path):
    global BASE_DIR
    BASE_DIR = Path(base_path)
    data_dir = BASE_DIR / "data"
    gui_dir = BASE_DIR / "gui"

    global DATA_DIR, GUI_DIR
    global MRMS_RALA_DIR, MRMS_CGFLASH_DIR, MRMS_NLDN_DIR, MRMS_ECHOTOP18_DIR, MRMS_ECHOTOP30_DIR
    global MRMS_QPE_DIR, MRMS_RAIN_DIR, MRMS_PRECIPRATE_DIR, MRMS_PROBSEVERE_DIR, MRMS_FLASH_DIR
    global MRMS_VIL_DIR, MRMS_VII_DIR, MRMS_ROTATIONT_DIR, MRMS_COMPOSITE_DIR, MRMS_RHOHV_DIR
    global MRMS_PRECIPTYP_DIR, MRMS_MESH_DIR, MRMS_AZSHEARLOW_DIR, MRMS_AZSHEARMID_DIR
    global GOES_GLM_DIR, STORMCELL_JSON, WPC_DIR, WPC_SFC_DIR
    global GUI_RALA_DIR, GUI_NLDN_DIR, GUI_ECHOTOP18_DIR, GUI_ECHOTOP30_DIR, GUI_QPE_DIR
    global GUI_AZSHEARLOW_DIR, GUI_AZSHEARMID_DIR, GUI_PRECIPRATE_DIR, GUI_PROBSEVERE_DIR, GUI_FLASH_DIR
    global GUI_VIL_DIR, GUI_VII_DIR, GUI_ROTATIONT_DIR, GUI_COMPOSITE_DIR, GUI_RHOHV_DIR, GUI_PRECIPTYP_DIR
    global GUI_MAP_DIR, GUI_MANIFEST_JSON

    DATA_DIR = data_dir
    GUI_DIR = gui_dir
    MRMS_RALA_DIR = data_dir / "RALA"
    MRMS_CGFLASH_DIR = data_dir / "NLDN"
    MRMS_NLDN_DIR = data_dir / "NLDN_Density"
    MRMS_ECHOTOP18_DIR = data_dir / "EchoTop18"
    MRMS_ECHOTOP30_DIR = data_dir / "EchoTop30"
    MRMS_QPE_DIR = data_dir / "QPE_01H"
    MRMS_RAIN_DIR = data_dir / "WarmRainProbability"
    MRMS_PRECIPRATE_DIR = data_dir / "PrecipRate"
    MRMS_PROBSEVERE_DIR = data_dir / "ProbSevere"
    MRMS_FLASH_DIR = data_dir / "FLASH"
    MRMS_VIL_DIR = data_dir / "VILDensity"
    MRMS_VII_DIR = data_dir / "VII"
    MRMS_ROTATIONT_DIR = data_dir / "RotationTrack30min"
    MRMS_COMPOSITE_DIR = data_dir / "CompRefQC"
    MRMS_RHOHV_DIR = data_dir / "RhoHV"
    MRMS_PRECIPTYP_DIR = data_dir / "PrecipFlag"
    MRMS_MESH_DIR = data_dir / "MESH"
    MRMS_AZSHEARLOW_DIR = data_dir / "AzShearLow"
    MRMS_AZSHEARMID_DIR = data_dir / "AzShearMid"
    GOES_GLM_DIR = data_dir / "GLM"
    STORMCELL_JSON = data_dir / "stormcells.json"
    WPC_DIR = BASE_DIR / "wpc"
    WPC_SFC_DIR = WPC_DIR / "surface_analysis"
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


def set_base_dir(path):
    global BASE_DIR
    BASE_DIR = Path(path)
    io_manager.write_info(f"Base directory updated to: {BASE_DIR}")
    _init_paths()


_init_paths()


def latest_files(directory, count):
    directory = Path(directory)
    if not directory.exists():
        io_manager.write_warning(f"{directory} doesn't exist!")
        return None
    files = sorted([f for f in directory.glob("*") if f.is_file() and f.suffix.lower() != ".idx"], key=lambda f: f.stat().st_mtime)
    if len(files) < count:
        raise RuntimeError(f"Not enough files in {directory}")
    return [str(f) for f in files[-count:]]


def clean_idx_files(folders):
    from util.file import clean_idx_files as _clean_idx_files
    return _clean_idx_files(folders)


def clean_old_files(directory: Path, max_age_minutes=60):
    directory = Path(directory)
    try:
        if not directory.resolve().is_relative_to(BASE_DIR.resolve()):
            io_manager.write_error(f"SAFETY ERROR: Attempting to clean {directory} which is not inside {BASE_DIR}")
            return
    except Exception as e:
        io_manager.write_error(f"Safety check failed for path {directory}: {e}")
        return

    from datetime import datetime

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
            io_manager.write_error(f"Could not process/delete {file_path.name}: {e}")
    if files_deleted > 0:
        io_manager.write_debug(f"Deleted {files_deleted} files in {directory}")
