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
from common.pipeline.coordinator import run_tandem_ingest_cycle
from EdgeWARN.api_integration.index_manager import APIIndexManager
from common.ingest.synoptic.main import download_rap
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


def run_edgewarn_detection_phase(log, lat_limits, lon_limits, output_path=Path("stormcell_test.json")):
    """Run only the realtime detection phase using already-ingested local files."""
    try:
        detection_inputs = _prepare_realtime_detection_inputs(log)
    except Exception as exc:
        log(f"ERROR: Failed to prepare detection inputs: {exc}")
        return None

    if detection_inputs is None:
        log("ERROR: Detection inputs are unavailable")
        return None

    generated_file, _ = detect.main(
        *detection_inputs,
        lat_limits,
        lon_limits,
        output_path,
    )
    return generated_file


def run_edgewarn_integration_phase(log, generated_file, remove_old_cells=True):
    """Run only the integration phase from an existing detection artifact."""
    if not generated_file:
        log("WARN: No detection artifact was produced; skipping integration")
        return False

    integration.main(generated_file, remove_old_cells=remove_old_cells)
    return True


def edgewarn_tandem_worker(
    log_queue,
    shared_state,
    detection_ready_event,
    integration_ready_event,
    dt,
    lat_limits,
    lon_limits,
    profile=False,
):
    """Process target for staged EdgeWARN execution within the tandem runner."""
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(message):
        log_queue.put(message)

    perf_tracker.reset()
    perf_tracker.start("Total Pipeline")

    try:
        log(f"INFO: EdgeWARN worker waiting for detection inputs for {dt}")
        detection_ready_event.wait()

        if not shared_state.get("detection_inputs_ready", False):
            log("ERROR: Detection inputs were not staged successfully; skipping EdgeWARN pipeline")
            return

        perf_tracker.start("Detection")
        generated_file = run_edgewarn_detection_phase(log, lat_limits, lon_limits)
        perf_tracker.stop("Detection")
        shared_state["edgewarn_generated_file"] = str(generated_file) if generated_file else ""

        log("INFO: EdgeWARN detection phase complete; waiting for integration inputs")
        integration_ready_event.wait()

        if not shared_state.get("edgewarn_integration_inputs_ready", False):
            log("ERROR: EdgeWARN integration inputs were not staged successfully; skipping integration")
            return

        perf_tracker.start("Integration")
        run_edgewarn_integration_phase(log, shared_state.get("edgewarn_generated_file") or None)
        perf_tracker.stop("Integration")
        log("INFO: EdgeWARN worker completed successfully")
    except Exception as exc:
        log(f"ERROR: EdgeWARN tandem worker failed: {exc}")
        log(traceback.format_exc())
    finally:
        try:
            perf_tracker.stop("Total Pipeline")
        except Exception:
            pass

        if profile:
            _write_profile_summary(log)


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
        detection_ready = asyncio.Event()
        integration_ready = asyncio.Event()
        cycle_state_holder = {}

        def on_detection_ready(state):
            cycle_state_holder["state"] = state
            detection_ready.set()

        def on_integration_ready(state):
            cycle_state_holder["state"] = state
            integration_ready.set()

        perf_tracker.start("Ingestion - Shared Cycle")
        cycle_task = asyncio.create_task(
            run_tandem_ingest_cycle(
                dt,
                log,
                on_detection_ready=on_detection_ready,
                on_edgewarn_integration_ready=on_integration_ready,
            )
        )

        await detection_ready.wait()
        cycle_state = cycle_state_holder.get("state")

        if not cycle_state.detection_inputs_ready:
            log("ERROR: Detection ingest did not complete successfully, skipping EdgeWARN pipeline")
            await cycle_task
            perf_tracker.stop("Ingestion - Shared Cycle")
            return

        log("INFO: Starting Storm Cell Detection")
        perf_tracker.start("Detection")
        generated_file = await asyncio.to_thread(
            run_edgewarn_detection_phase,
            log,
            lat_limits,
            lon_limits,
        )
        perf_tracker.stop("Detection")

        if not generated_file:
            log("ERROR: Detection failed to generate a file, skipping integration.")
            await cycle_task
            perf_tracker.stop("Ingestion - Shared Cycle")
            return

        await integration_ready.wait()
        cycle_state = cycle_state_holder.get("state", cycle_state)
        await cycle_task
        perf_tracker.stop("Ingestion - Shared Cycle")

        if not cycle_state.edgewarn_integration_inputs_ready:
            log("ERROR: Integration ingest did not complete successfully, skipping integration.")
            return

        perf_tracker.start("Integration")
        await asyncio.to_thread(run_edgewarn_integration_phase, log, generated_file)
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
