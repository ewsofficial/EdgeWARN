from common.ingest.mrms.config import get_mrms_modifiers, get_goes_modifiers, bucket
from common.ingest.mrms.downloader import (
    download_all_files_async_internal,
    download_all_files_sync_fallback, download_files_sync_fallback,
    download_all_goes_files,
    download_all_goes_files_async
)
from common.ingest.mrms.pipeline import (
    get_output_dirs,
    run_ingestion_pipeline,
    run_with_async_fallback,
)
from common.config.loader import load_config
from util.io import IOManager
import util.file as fs
import asyncio

io_manager = IOManager("[Ingest]")


def get_detection_modifiers():
    return list(load_config("mrms_goes")["mrms"]["membership_lists"]["detection"])


def get_integration_modifiers():
    detection_mods = set(get_detection_modifiers())
    return [mod for _, mod, _ in get_mrms_modifiers() if mod not in detection_mods]


def get_ewmrs_modifiers():
    from EWMRS.render.config import get_file_list

    render_dirs = {item["filepath"] for item in get_file_list()}
    return [
        modifier
        for _, modifier, outdir in get_mrms_modifiers()
        if outdir in render_dirs
    ]


def get_ewmrs_support_modifiers():
    detection_mods = set(get_detection_modifiers())
    return [mod for mod in get_ewmrs_modifiers() if mod not in detection_mods]

async def download_all_files_async(dt, max_entries=10, remove_old_files=True):
    mrms_modifiers = get_mrms_modifiers()
    goes_modifiers = get_goes_modifiers()
    cleanup_dirs = get_output_dirs(mrms_modifiers, goes_modifiers=goes_modifiers)

    results = await run_ingestion_pipeline(
        io_manager=io_manager,
        async_downloads=[
            download_all_files_async_internal(dt, max_entries),
            download_all_goes_files_async(dt, max_entries),
        ],
        cleanup_dirs=cleanup_dirs if remove_old_files else (),
        cleanup_async=fs.async_clean_old_files,
        cleanup_kwargs={"max_age_minutes": 60},
    )
    return results[0]

async def download_detection_files_async(dt, max_entries=10, remove_old_files=True):
    """Downloads only files strictly required for detection phase."""
    mrms_modifiers = get_mrms_modifiers()
    detection_mods = get_detection_modifiers()
    cleanup_dirs = get_output_dirs(
        mrms_modifiers,
        include_modifiers=detection_mods,
        include_goes=False,
    )

    results = await run_ingestion_pipeline(
        io_manager=io_manager,
        async_downloads=[
            download_all_files_async_internal(dt, max_entries, target_modifiers=detection_mods),
        ],
        cleanup_dirs=cleanup_dirs if remove_old_files else (),
        cleanup_async=fs.async_clean_old_files,
        cleanup_kwargs={"max_age_minutes": 60},
        # Detection readiness is defined by its three downloads; background
        # housekeeping must not delay the EdgeWARN worker barrier.
        wait_for_cleanup=False,
    )
    return results[0]

async def download_integration_files_async(dt, max_entries=10, remove_old_files=True):
    """Downloads MRMS integration products, excluding detection products."""
    mrms_modifiers = get_mrms_modifiers()
    detection_mods = get_detection_modifiers()
    integration_mods = get_integration_modifiers()
    cleanup_dirs = get_output_dirs(
        mrms_modifiers,
        exclude_modifiers=detection_mods,
        include_goes=False,
    )

    results = await run_ingestion_pipeline(
        io_manager=io_manager,
        async_downloads=[
            download_all_files_async_internal(dt, max_entries, target_modifiers=integration_mods),
        ],
        cleanup_dirs=cleanup_dirs if remove_old_files else (),
        cleanup_async=fs.async_clean_old_files,
        cleanup_kwargs={"max_age_minutes": 60},
    )
    return results[0]


async def download_ewmrs_files_async(dt, max_entries=10, remove_old_files=True):
    """Downloads the MRMS products required by the EWMRS render pipeline."""
    mrms_modifiers = get_mrms_modifiers()
    render_mods = get_ewmrs_support_modifiers()
    cleanup_dirs = get_output_dirs(
        mrms_modifiers,
        include_modifiers=render_mods,
        include_goes=False,
    )

    await run_ingestion_pipeline(
        io_manager=io_manager,
        async_downloads=[
            download_all_files_async_internal(dt, max_entries, target_modifiers=render_mods),
        ],
        cleanup_dirs=cleanup_dirs if remove_old_files else (),
        cleanup_async=fs.async_clean_old_files,
        cleanup_kwargs={"max_age_minutes": 60},
    )

def download_all_files(dt, max_entries=10, remove_old_files=True):
    run_with_async_fallback(
        io_manager=io_manager,
        async_runner=lambda: download_all_files_async(dt, max_entries, remove_old_files),
        sync_fallback=lambda: (
            download_all_files_sync_fallback(dt, max_entries),
            download_all_goes_files(dt, max_entries),
        ),
    )


def download_detection_files(dt, max_entries=10):
    """Sync fallback scoped to the detection MRMS products."""
    return download_files_sync_fallback(dt, max_entries, get_detection_modifiers())


def download_integration_files(dt, max_entries=10):
    """Sync fallback scoped to the integration MRMS products."""
    return download_files_sync_fallback(dt, max_entries, get_integration_modifiers())
