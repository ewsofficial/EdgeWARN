from pathlib import Path
import platform
import sys

try:
    _SRC_DIR = Path(__file__).resolve().parents[1]
    _src_str = str(_SRC_DIR)
    if _src_str not in sys.path:
        sys.path.insert(0, _src_str)
except Exception:
    pass

from common.file import (
    async_clean_files_by_age as _async_clean_files_by_age,
    async_clean_old_files as _async_clean_old_files,
    build_edgewarn_paths,
    clean_files_by_age as _clean_files_by_age,
    clean_idx_files as _clean_idx_files,
    clean_old_files as _clean_old_files,
    latest_files as _latest_files,
)
from common.io import IOManager

io_manager = IOManager("[Util]")


def _define_paths(base_path):
    globals().update(build_edgewarn_paths(Path(base_path)))


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
    return _latest_files(directory, count, io_manager=io_manager, strict=False)


def clean_idx_files(folders):
    return _clean_idx_files(folders, io_manager=io_manager)


def clean_old_files(directory: Path, max_age_minutes=60, max_files=10):
    return _clean_old_files(
        directory,
        base_dir=BASE_DIR,
        io_manager=io_manager,
        max_age_minutes=max_age_minutes,
        max_files=max_files,
        exclude_idx=True,
        allow_logical_inside=True,
    )


def clean_files_by_age(directory: Path, max_age_minutes=60):
    return _clean_files_by_age(
        directory,
        base_dir=BASE_DIR,
        io_manager=io_manager,
        max_age_minutes=max_age_minutes,
    )


async def async_clean_old_files(directory: Path, max_age_minutes=60, max_files=10):
    return await _async_clean_old_files(
        directory,
        base_dir=BASE_DIR,
        io_manager=io_manager,
        max_age_minutes=max_age_minutes,
        max_files=max_files,
        exclude_idx=True,
        allow_logical_inside=True,
    )


async def async_clean_files_by_age(directory: Path, max_age_minutes=60):
    return await _async_clean_files_by_age(
        directory,
        base_dir=BASE_DIR,
        io_manager=io_manager,
        max_age_minutes=max_age_minutes,
    )
