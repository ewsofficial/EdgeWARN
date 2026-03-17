from pathlib import Path
import platform

from common.file import (
    build_ewmrs_paths,
    clean_idx_files as _clean_idx_files,
    clean_old_files as _clean_old_files,
    find_existing_path,
    latest_files as _latest_files,
)
from common.io import IOManager

io_manager = IOManager("[Util]")

_DEFAULT_BASE_DIR = Path(r"C:\EWMRS") if platform.system() == "Windows" else Path.home() / "EWMRS"
_arg_base_dir = IOManager.get_base_dir_arg()
BASE_DIR = Path(_arg_base_dir) if _arg_base_dir else _DEFAULT_BASE_DIR
if _arg_base_dir:
    io_manager.write_info(f"Using custom base directory: {BASE_DIR}")


def _refresh_colormap_path():
    global GUI_COLORMAP_JSON
    GUI_COLORMAP_JSON = find_existing_path(
        [
            Path.cwd() / "colormaps.json",
            Path(__file__).resolve().parents[1] / "colormaps.json",
            Path(__file__).resolve().parents[2] / "colormaps.json",
            GUI_DIR / "colormaps.json",
        ],
        io_manager=io_manager,
        missing_message="colormaps.json not found in common locations; using relative path 'colormaps.json'",
    )


def _init_paths():
    globals().update(build_ewmrs_paths(BASE_DIR))
    _refresh_colormap_path()


def set_base_dir(path):
    global BASE_DIR
    BASE_DIR = Path(path)
    io_manager.write_info(f"Base directory updated to: {BASE_DIR}")
    _init_paths()


_init_paths()


def latest_files(directory, count):
    return _latest_files(directory, count, io_manager=io_manager, strict=True)


def clean_idx_files(folders):
    return _clean_idx_files(folders, io_manager=io_manager)


def clean_old_files(directory: Path, max_age_minutes=60):
    return _clean_old_files(
        directory,
        base_dir=BASE_DIR,
        io_manager=io_manager,
        max_age_minutes=max_age_minutes,
        max_files=None,
        exclude_idx=False,
        allow_logical_inside=False,
    )
