import asyncio
import sys
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import xarray as xr

import EdgeWARN.process.detect.main as detect
import EdgeWARN.process.integrate.main as integration
import util.file as fs
import common.ingest.mrms.main as ingest_main
from EdgeWARN.api_integration.index_manager import APIIndexManager
from common.ingest.mrms.downloader import (
    download_all_goes_files,
    download_all_goes_files_async,
)
from common.ingest.synoptic.main import download_rap, download_rap_async
from util.io import IOManager, QueueWriter
from util.performance import tracker as perf_tracker

# Suppress cfgrib/xarray compatibility warnings.
xr.set_options(use_new_combine_kwarg_defaults=True)

# Some users report issues with DNS resolution with aiodns.
sys.modules.pop("aiodns", None)


def initialize_runtime(base_dir=None, io_manager=None, initialize_indexes=True):
    runtime_io = io_manager or IOManager("[Pipeline]")

    if base_dir:
        fs.initialize_filesystem(base_dir)

    if not initialize_indexes:
        return

    try:
        index_manager = APIIndexManager(runtime_io)
        index_manager.initialize_indexes()
    except Exception as exc:
        runtime_io.write_error(f"Failed to initialize API indexes: {exc}")


def parse_utc_time(time_str):
    dt = datetime.fromisoformat(time_str)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=timezone.utc)


def _write_profile_summary(log):
    lines = ["", "=" * 50, f"{'Component':<35} | {'Time (s)':<10}", "-" * 50]
    for name, duration in perf_tracker.get_timings().items():
        lines.append(f"{name:<35} | {duration:.4f}")
    lines.append("=" * 50)
    log("\n".join(lines))


def _prepare_realtime_detection_inputs(log):
    try:
        filepath_old, filepath_new = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 2)
        ps_old, ps_new = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)
        pt_old, pt_new = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 2)
        return filepath_old, filepath_new, ps_old, ps_new, pt_old, pt_new
    except (RuntimeError, ValueError):
        comp_files = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)
        ps_files = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)
        pt_files = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 1)

        log("INFO: Not enough files for tracking, falling back to single-frame mode")
        return (
            comp_files[-1] if comp_files else None,
            None,
            ps_files[-1] if ps_files else None,
            None,
            pt_files[-1] if pt_files else None,
            None,
        )


def _find_historical_file(directory, target_dt, io_manager):
    directory_path = Path(directory)
    if not directory_path.exists():
        io_manager.write_debug(f"Dir not found: {directory}")
        return None

    primary_pattern = f"*{target_dt.strftime('%Y%m%d-%H%M')}*"
    files = sorted(directory_path.glob(primary_pattern))

    if not files:
        fallback_pattern = f"*{target_dt.strftime('%Y%m%d_%H%M')}*"
        files = sorted(directory_path.glob(fallback_pattern))
    else:
        fallback_pattern = None

    if not files:
        io_manager.write_debug(
            f"No match for {primary_pattern}"
            f"{f' or {fallback_pattern}' if fallback_pattern else ''} in {directory}"
        )
        return None

    io_manager.write_debug(f"Found {files[-1]}")
    return str(files[-1])


def realtime_pipeline(log_queue, dt, lat_limits, lon_limits, profile=False):
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(message):
        log_queue.put(message)

    perf_tracker.reset()
    perf_tracker.start("Total Pipeline")

    async def run_pipeline_async():
        log(f"INFO: Starting Async Data Ingestion for timestamp {dt}")

        async def safe_ingest(task_name, async_func, sync_fallback, *args):
            try:
                await async_func(*args)
                log(f"INFO: Async {task_name} ingestion successful")
                return True
            except Exception as exc:
                log(f"WARN: Async {task_name} ingestion failed: {exc}. Falling back to sync.")
                try:
                    sync_fallback(*args)
                    log(f"INFO: Sync fallback for {task_name} successful")
                    return True
                except Exception as fallback_exc:
                    log(
                        "ERROR: Both async and sync ingestion failed for "
                        f"{task_name}: {fallback_exc}"
                    )
                    return False

        perf_tracker.start("Ingestion - Detection Files")
        detection_task = asyncio.create_task(
            safe_ingest(
                "MRMS Detection",
                ingest_main.download_detection_files_async,
                ingest_main.download_all_files,
                dt,
            )
        )

        integration_tasks = [
            asyncio.create_task(
                safe_ingest(
                    "MRMS Integration",
                    ingest_main.download_integration_files_async,
                    ingest_main.download_all_files,
                    dt,
                )
            ),
            asyncio.create_task(
                safe_ingest("GOES", download_all_goes_files_async, download_all_goes_files, dt, 10, 3)
            ),
            asyncio.create_task(safe_ingest("RAP", download_rap_async, download_rap, dt)),
        ]

        await detection_task
        perf_tracker.stop("Ingestion - Detection Files")

        log("INFO: Starting Storm Cell Detection")
        perf_tracker.start("Detection")

        def run_detect_sync():
            try:
                detection_inputs = _prepare_realtime_detection_inputs(log)
            except Exception as exc:
                log(f"ERROR: Failed to prepare detection inputs: {exc}")
                return None

            if detection_inputs is None:
                return None

            generated_file, _ = detect.main(
                *detection_inputs,
                lat_limits,
                lon_limits,
                Path("stormcell_test.json"),
            )
            return generated_file

        generated_file = await asyncio.to_thread(run_detect_sync)
        perf_tracker.stop("Detection")

        if not generated_file:
            log("ERROR: Detection failed to generate a file, skipping integration.")
            return

        perf_tracker.start("Ingestion - Integration Files (Wait)")
        await asyncio.gather(*integration_tasks, return_exceptions=True)
        perf_tracker.stop("Ingestion - Integration Files (Wait)")

        perf_tracker.start("Integration")
        await asyncio.to_thread(integration.main, generated_file)
        perf_tracker.stop("Integration")

    try:
        asyncio.run(run_pipeline_async())
        perf_tracker.stop("Total Pipeline")
        log("Pipeline completed successfully")

        if profile:
            _write_profile_summary(log)
    except Exception as exc:
        log(f"Error in pipeline: {exc}")
        log(traceback.format_exc())


def historical_pipeline(
    dt,
    lat_limits,
    lon_limits,
    json_output,
    profile=False,
    cached_objs=(None, None, None),
    io_manager=None,
):
    pipeline_io = io_manager or IOManager("[HistoricalProcess]")

    try:
        perf_tracker.reset()
        perf_tracker.start("Total Pipeline")

        pipeline_io.write_info(f"Starting Data Ingestion for timestamp {dt}")
        perf_tracker.start("Ingestion")
        ingest_main.download_all_files(dt, remove_old_files=False)
        download_rap(dt)
        perf_tracker.stop("Ingestion")

        pipeline_io.write_info("Starting Storm Cell Detection")

        radar_new = _find_historical_file(fs.MRMS_COMPOSITE_DIR, dt, pipeline_io)
        ps_new = _find_historical_file(fs.MRMS_PROBSEVERE_DIR, dt, pipeline_io)
        pt_new = _find_historical_file(fs.MRMS_PRECIPTYP_DIR, dt, pipeline_io)

        dt_old = dt - timedelta(minutes=2)
        radar_old = _find_historical_file(fs.MRMS_COMPOSITE_DIR, dt_old, pipeline_io)
        ps_old = _find_historical_file(fs.MRMS_PROBSEVERE_DIR, dt_old, pipeline_io)
        pt_old = _find_historical_file(fs.MRMS_PRECIPTYP_DIR, dt_old, pipeline_io)

        perf_tracker.start("Detection")
        rad_old_obj, ps_old_obj, pt_old_obj = cached_objs
        generated_file, new_objs = detect.main(
            radar_old,
            radar_new,
            ps_old,
            ps_new,
            pt_old,
            pt_new,
            lat_limits,
            lon_limits,
            json_output,
            radar_old_obj=rad_old_obj,
            ps_old_obj=ps_old_obj,
            pt_old_obj=pt_old_obj,
        )
        perf_tracker.stop("Detection")

        if generated_file:
            pipeline_io.write_info("Starting Integration")
            perf_tracker.start("Integration")
            integration.main(generated_file, remove_old_cells=False)
            perf_tracker.stop("Integration")
        else:
            pipeline_io.write_warning("Detection failed or produced no output, skipping integration")

        perf_tracker.stop("Total Pipeline")
        pipeline_io.write_info("Pipeline completed successfully")

        if profile:
            perf_tracker.print_summary()

        return generated_file, new_objs
    except Exception as exc:
        pipeline_io.write_error(f"Pipeline failed: {exc}")
        traceback.print_exc()
        return None, (None, None, None)
