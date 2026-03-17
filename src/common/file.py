from datetime import datetime
from pathlib import Path
import asyncio
from functools import partial


def _log(io_manager, method, message):
    if io_manager is not None and hasattr(io_manager, method):
        getattr(io_manager, method)(message)


def build_edgewarn_paths(base_path: Path) -> dict:
    data_dir = base_path / "data"
    alerts_dir = data_dir / "Alerts"
    edgewarn_alerts_dir = alerts_dir / "EdgeWARN"
    edgewarn_alerts_ids_dir = edgewarn_alerts_dir / "ids"
    edgewarn_alerts_ts_dir = edgewarn_alerts_dir / "timestamps"
    official_alerts_dir = alerts_dir / "official"
    return {
        "BASE_DIR": base_path,
        "DATA_DIR": data_dir,
        "ALERTS_DIR": alerts_dir,
        "EDGEWARN_ALERTS_DIR": edgewarn_alerts_dir,
        "EDGEWARN_ALERTS_IDS_DIR": edgewarn_alerts_ids_dir,
        "EDGEWARN_ALERTS_TS_DIR": edgewarn_alerts_ts_dir,
        "MRMS_NWS_RAW_DIR": data_dir / "NWS_Raw",
        "MRMS_NWS_DIR": official_alerts_dir,
        "MRMS_NWS_IDS_DIR": official_alerts_dir / "ids",
        "MRMS_NWS_TS_DIR": official_alerts_dir / "timestamps",
        "NWS_REGISTRY_PATH": official_alerts_dir / "alerts_registry.json",
        "MRMS_RALA_DIR": data_dir / "RALA",
        "MRMS_CGFLASH_DIR": data_dir / "NLDN",
        "MRMS_NLDN_DIR": data_dir / "NLDN_Density",
        "MRMS_ECHOTOP18_DIR": data_dir / "EchoTop18",
        "MRMS_ECHOTOP30_DIR": data_dir / "EchoTop30",
        "MRMS_QPE_DIR": data_dir / "QPE_01H",
        "MRMS_RAIN_DIR": data_dir / "WarmRainProbability",
        "MRMS_PRECIPRATE_DIR": data_dir / "PrecipRate",
        "MRMS_PROBSEVERE_DIR": data_dir / "ProbSevere",
        "MRMS_FLASH_CREST_MAXUNIT_DIR": data_dir / "FLASH_CREST_MAXUNIT",
        "MRMS_FLASH_ARIMAX_DIR": data_dir / "FLASH_ARIMAX",
        "MRMS_FLASH_ARI30M_DIR": data_dir / "FLASH_ARI30M",
        "MRMS_FLASH_ARI01H_DIR": data_dir / "FLASH_ARI01H",
        "MRMS_FLASH_HP_MAXUNIT_DIR": data_dir / "FLASH_HP_MAXUNIT",
        "MRMS_FLASH_SAC_MAXSOIL_DIR": data_dir / "FLASH_SAC_MAXSOIL",
        "MRMS_FLASH_FFGMAX_DIR": data_dir / "FLASH_FFGMAX",
        "MRMS_RQI_DIR": data_dir / "RadarQualityIndex",
        "MRMS_DVIL_DIR": data_dir / "VILDensity",
        "MRMS_VIL_DIR": data_dir / "VIL",
        "MRMS_VII_DIR": data_dir / "VII",
        "MRMS_AZSHEARLOW_DIR": data_dir / "AzShearLow",
        "MRMS_AZSHEARMID_DIR": data_dir / "AzShearMid",
        "MRMS_COMPOSITE_DIR": data_dir / "CompRefQC",
        "MRMS_REF_0C_DIR": data_dir / "Ref0C",
        "MRMS_REFM5C_DIR": data_dir / "RefM5C",
        "MRMS_REFM15C_DIR": data_dir / "RefM15C",
        "MRMS_RHOHV_DIR": data_dir / "RhoHV",
        "MRMS_PRECIPTYP_DIR": data_dir / "PrecipFlag",
        "MRMS_MESH_DIR": data_dir / "MESH",
        "GOES_GLM_DIR": data_dir / "GLM",
        "RAP_DIR": data_dir / "RAP",
        "STORMCELL_DIR": data_dir / "stormcells",
        "CELL_DIR": data_dir / "cells",
        "METAR_DIR": data_dir / "METAR",
        "SURFACE_DIR": data_dir / "surface_features",
        "FLASH_FLOOD_DIR": data_dir / "FlashFlood",
    }


def build_ewmrs_paths(base_path: Path) -> dict:
    data_dir = base_path / "data"
    gui_dir = base_path / "gui"
    return {
        "BASE_DIR": base_path,
        "DATA_DIR": data_dir,
        "GUI_DIR": gui_dir,
        "MRMS_RALA_DIR": data_dir / "RALA",
        "MRMS_CGFLASH_DIR": data_dir / "NLDN",
        "MRMS_NLDN_DIR": data_dir / "NLDN_Density",
        "MRMS_ECHOTOP18_DIR": data_dir / "EchoTop18",
        "MRMS_ECHOTOP30_DIR": data_dir / "EchoTop30",
        "MRMS_QPE_DIR": data_dir / "QPE_01H",
        "MRMS_RAIN_DIR": data_dir / "WarmRainProbability",
        "MRMS_PRECIPRATE_DIR": data_dir / "PrecipRate",
        "MRMS_PROBSEVERE_DIR": data_dir / "ProbSevere",
        "MRMS_FLASH_DIR": data_dir / "FLASH",
        "MRMS_VIL_DIR": data_dir / "VILDensity",
        "MRMS_VII_DIR": data_dir / "VII",
        "MRMS_ROTATIONT_DIR": data_dir / "RotationTrack30min",
        "MRMS_COMPOSITE_DIR": data_dir / "CompRefQC",
        "MRMS_RHOHV_DIR": data_dir / "RhoHV",
        "MRMS_PRECIPTYP_DIR": data_dir / "PrecipFlag",
        "MRMS_MESH_DIR": data_dir / "MESH",
        "MRMS_AZSHEARLOW_DIR": data_dir / "AzShearLow",
        "MRMS_AZSHEARMID_DIR": data_dir / "AzShearMid",
        "GOES_GLM_DIR": data_dir / "GLM",
        "STORMCELL_JSON": data_dir / "stormcells.json",
        "WPC_DIR": base_path / "wpc",
        "WPC_SFC_DIR": base_path / "wpc" / "surface_analysis",
        "GUI_RALA_DIR": gui_dir / "RALA",
        "GUI_NLDN_DIR": gui_dir / "NLDN",
        "GUI_ECHOTOP18_DIR": gui_dir / "EchoTop18",
        "GUI_ECHOTOP30_DIR": gui_dir / "EchoTop30",
        "GUI_QPE_DIR": gui_dir / "QPE_01H",
        "GUI_AZSHEARLOW_DIR": gui_dir / "AzShearLow",
        "GUI_AZSHEARMID_DIR": gui_dir / "AzShearMid",
        "GUI_PRECIPRATE_DIR": gui_dir / "PrecipRate",
        "GUI_PROBSEVERE_DIR": gui_dir / "ProbSevere",
        "GUI_FLASH_DIR": gui_dir / "FLASH",
        "GUI_VIL_DIR": gui_dir / "VILDensity",
        "GUI_VII_DIR": gui_dir / "VII",
        "GUI_ROTATIONT_DIR": gui_dir / "RotationTrack30min",
        "GUI_COMPOSITE_DIR": gui_dir / "CompRefQC",
        "GUI_RHOHV_DIR": gui_dir / "RhoHV",
        "GUI_PRECIPTYP_DIR": gui_dir / "PrecipFlag",
        "GUI_MAP_DIR": gui_dir / "maps",
        "GUI_MANIFEST_JSON": gui_dir / "overlay_manifest.json",
    }


def find_existing_path(candidates, io_manager=None, missing_message=None):
    for candidate in candidates:
        candidate_path = Path(candidate)
        if candidate_path.exists():
            _log(io_manager, "write_debug", f"Using path: {candidate_path}")
            return candidate_path

    if missing_message:
        _log(io_manager, "write_warning", missing_message)
    return Path(candidates[0]) if candidates else Path('.')


def latest_files(directory, count, io_manager=None, strict=False):
    directory = Path(directory)
    if not directory.exists():
        _log(io_manager, "write_warning", f"{directory} doesn't exist!")
        return None

    files = sorted(
        [f for f in directory.glob("*") if f.is_file() and f.suffix.lower() != ".idx"],
        key=lambda f: f.stat().st_mtime,
    )
    if len(files) < count and strict:
        raise RuntimeError(f"Not enough files in {directory}")
    return [str(f) for f in files[-count:]]


def clean_idx_files(folders, io_manager=None):
    for folder in folders:
        folder = Path(folder)
        if not folder.exists():
            _log(io_manager, "write_error", f"Folder not found: {folder}")
            continue

        idx_files = list(folder.rglob("*.idx"))
        if not idx_files:
            _log(io_manager, "write_debug", f"No IDX files in folder: {folder}")
            continue

        deleted_files = 0
        for file_path in idx_files:
            try:
                file_path.unlink()
                deleted_files += 1
            except Exception as e:
                _log(io_manager, "write_error", f"Failed to delete IDX file {file_path}: {e}")

        if deleted_files > 0:
            _log(io_manager, "write_debug", f"Deleted {deleted_files} files in {folder}")


def _is_safe_directory(directory: Path, base_dir: Path, allow_logical_inside=False):
    try:
        if allow_logical_inside:
            logical_inside = directory.absolute().is_relative_to(base_dir.absolute())
            physical_inside = directory.resolve().is_relative_to(base_dir.resolve())
            return logical_inside or physical_inside
        return directory.resolve().is_relative_to(base_dir.resolve())
    except Exception:
        return False


def clean_old_files(
    directory: Path,
    *,
    base_dir: Path,
    io_manager=None,
    max_age_minutes=60,
    max_files=None,
    exclude_idx=True,
    allow_logical_inside=False,
):
    directory = Path(directory)
    base_dir = Path(base_dir)

    if not _is_safe_directory(directory, base_dir, allow_logical_inside=allow_logical_inside):
        _log(io_manager, "write_error", f"SAFETY ERROR: Attempting to clean {directory} which is not inside {base_dir}")
        return

    now = datetime.now().timestamp()
    cutoff = now - (max_age_minutes * 60)
    files_deleted = 0
    kept_files = []

    for file_path in directory.glob("*"):
        if not file_path.is_file():
            continue
        if exclude_idx and file_path.suffix.lower() == ".idx":
            continue

        try:
            mtime = file_path.stat().st_mtime
            if mtime < cutoff:
                file_path.unlink()
                files_deleted += 1
            else:
                kept_files.append((file_path, mtime))
        except Exception as e:
            _log(io_manager, "write_error", f"Could not process/delete {file_path.name}: {e}")

    if max_files is not None and len(kept_files) > max_files:
        kept_files.sort(key=lambda item: item[1])
        for file_path, _ in kept_files[:len(kept_files) - max_files]:
            try:
                file_path.unlink()
                files_deleted += 1
            except Exception as e:
                _log(io_manager, "write_error", f"Could not delete {file_path.name}: {e}")

    if files_deleted > 0:
        _log(io_manager, "write_debug", f"Deleted {files_deleted} files in {directory}")


def clean_files_by_age(directory: Path, *, base_dir: Path, io_manager=None, max_age_minutes=60):
    clean_old_files(
        directory,
        base_dir=base_dir,
        io_manager=io_manager,
        max_age_minutes=max_age_minutes,
        max_files=None,
        exclude_idx=False,
        allow_logical_inside=False,
    )


async def async_clean_old_files(directory: Path, *, base_dir: Path, io_manager=None, max_age_minutes=60, max_files=None, exclude_idx=True, allow_logical_inside=False):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        partial(
            clean_old_files,
            directory,
            base_dir=base_dir,
            io_manager=io_manager,
            max_age_minutes=max_age_minutes,
            max_files=max_files,
            exclude_idx=exclude_idx,
            allow_logical_inside=allow_logical_inside,
        ),
    )


async def async_clean_files_by_age(directory: Path, *, base_dir: Path, io_manager=None, max_age_minutes=60):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        partial(
            clean_files_by_age,
            directory,
            base_dir=base_dir,
            io_manager=io_manager,
            max_age_minutes=max_age_minutes,
        ),
    )
