import asyncio
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import xarray as xr

import EdgeWARN.process.detect.main as detect
import EdgeWARN.process.integrate.main as integration
import util.file as fs
from common.ingest.mrms.config import get_goes_modifiers, get_mrms_modifiers
from common.ingest.mrms.pipeline import get_output_dirs
from common.pipeline.coordinator import run_tandem_ingest_cycle
from EdgeWARN.api_integration.index_manager import APIIndexManager
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

    runtime_io.write_info(
        f"Runtime filesystem initialized: base_dir={fs.BASE_DIR} rap_dir={fs.RAP_DIR}"
    )

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


def _cleanup_historical_data_dirs(pipeline_io):
    protected_dirs = {fs.CELL_DIR.resolve(), fs.STORMCELL_DIR.resolve()}
    cleanup_dirs = get_output_dirs(
        get_mrms_modifiers(),
        goes_modifiers=get_goes_modifiers(),
    )
    cleanup_dirs.append(fs.RAP_DIR)

    seen_dirs = set()
    for directory in cleanup_dirs:
        resolved_dir = directory.resolve()
        if resolved_dir in seen_dirs or resolved_dir in protected_dirs:
            continue

        seen_dirs.add(resolved_dir)
        fs.clean_old_files(directory, max_files=5)

    pipeline_io.write_debug(
        "Historical cleanup applied to ingest data directories only; "
        "cell and stormcell folders were excluded."
    )


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


def run_edgewarn_detection_phase(
    log,
    lat_limits,
    lon_limits,
    output_path=Path("stormcell_test.json"),
    disable_tracking=False,
    disable_polygon_expansion=False,
    refl_threshold=37.5,
    min_seed_percentage=0.001,
    drop_offset=10.0,
):
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
        disable_tracking=disable_tracking,
        disable_polygon_expansion=disable_polygon_expansion,
        refl_threshold=refl_threshold,
        min_seed_percentage=min_seed_percentage,
        drop_offset=drop_offset,
    )
    return generated_file


def run_edgewarn_integration_phase(
    log,
    generated_file,
    remove_old_cells=True,
    disable_ctam=False,
    mrms_core_only=False,
):
    """Run only the integration phase from an existing detection artifact."""
    if not generated_file:
        log("WARN: No detection artifact was produced; skipping integration")
        return False

    integration.main(
        generated_file,
        remove_old_cells=remove_old_cells,
        disable_ctam=disable_ctam,
        mrms_core_only=mrms_core_only,
    )
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
    disable_ctam=False,
    disable_tracking=False,
    disable_polygon_expansion=False,
    refl_threshold=37.5,
    min_seed_percentage=0.001,
    drop_offset=10.0,
    mrms_core_only=False,
):
    """Process target for staged EdgeWARN execution within the tandem runner."""
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)

    def log(message):
        log_queue.put(message)

    if profile:
        perf_tracker.set_enabled(True)
    perf_tracker.reset()
    perf_tracker.start("Total Pipeline")

    try:
        log(f"INFO: EdgeWARN worker waiting for detection inputs for {dt}")
        detection_ready_event.wait()

        if not shared_state.get("detection_inputs_ready", False):
            log("ERROR: Detection inputs were not staged successfully; skipping EdgeWARN pipeline")
            return

        perf_tracker.start("Detection")
        generated_file = run_edgewarn_detection_phase(
            log,
            lat_limits,
            lon_limits,
            disable_tracking=disable_tracking,
            disable_polygon_expansion=disable_polygon_expansion,
            refl_threshold=refl_threshold,
            min_seed_percentage=min_seed_percentage,
            drop_offset=drop_offset,
        )
        perf_tracker.stop("Detection")
        shared_state["edgewarn_generated_file"] = str(generated_file) if generated_file else ""

        log("INFO: EdgeWARN detection phase complete; waiting for integration inputs")
        integration_ready_event.wait()

        if not shared_state.get("edgewarn_integration_inputs_ready", False):
            log("ERROR: EdgeWARN integration inputs were not staged successfully; skipping integration")
            return

        perf_tracker.start("Integration")
        run_edgewarn_integration_phase(
            log,
            shared_state.get("edgewarn_generated_file") or None,
            disable_ctam=disable_ctam,
            mrms_core_only=mrms_core_only,
        )
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


def historical_pipeline(
    dt,
    lat_limits,
    lon_limits,
    json_output,
    profile=False,
    cached_objs=(None, None, None),
    io_manager=None,
    disable_ctam=False,
    disable_tracking=False,
    disable_polygon_expansion=False,
    refl_threshold=37.5,
    min_seed_percentage=0.001,
    drop_offset=10.0,
):
    pipeline_io = io_manager or IOManager("[HistoricalProcess]")

    try:
        if profile:
            perf_tracker.set_enabled(True)
        perf_tracker.reset()
        perf_tracker.start("Total Pipeline")

        pipeline_io.write_info(f"Starting staged ingest for timestamp {dt}")
        perf_tracker.start("Ingestion")
        _cleanup_historical_data_dirs(pipeline_io)
        cycle_state = asyncio.run(
            run_tandem_ingest_cycle(
                dt,
                lambda message: pipeline_io.write_info(message),
                include_goes=False,
                include_ewmrs=False,
            )
        )
        perf_tracker.stop("Ingestion")

        if not cycle_state.detection_inputs_ready:
            pipeline_io.write_warning("Detection inputs were not staged successfully; skipping historical pipeline")
            perf_tracker.stop("Total Pipeline")
            return None, (None, None, None)

        if "mrms_integration_ingest" in cycle_state.errors or "rap_ingest" in cycle_state.errors:
            pipeline_io.write_warning(
                "Historical integration inputs are incomplete after staged ingest; "
                "detection will run but integration may be skipped"
            )

        pipeline_io.write_info("Starting Storm Cell Detection")

        perf_tracker.start("Detection")
        generated_file = run_edgewarn_detection_phase(
            pipeline_io.write_info,
            lat_limits,
            lon_limits,
            json_output,
            disable_tracking=disable_tracking,
            disable_polygon_expansion=disable_polygon_expansion,
            refl_threshold=refl_threshold,
            min_seed_percentage=min_seed_percentage,
            drop_offset=drop_offset,
        )
        perf_tracker.stop("Detection")

        can_integrate = (
            generated_file
            and "mrms_integration_ingest" not in cycle_state.errors
            and "rap_ingest" not in cycle_state.errors
        )

        if can_integrate:
            pipeline_io.write_info("Starting Integration")
            perf_tracker.start("Integration")
            run_edgewarn_integration_phase(
                pipeline_io.write_info,
                generated_file,
                remove_old_cells=False,
                disable_ctam=disable_ctam,
            )
            perf_tracker.stop("Integration")
        else:
            pipeline_io.write_warning("Detection failed or staged integration inputs were unavailable; skipping integration")

        perf_tracker.stop("Total Pipeline")
        pipeline_io.write_info("Pipeline completed successfully")

        if profile:
            perf_tracker.print_summary()

        return generated_file, (None, None, None)
    except Exception as exc:
        pipeline_io.write_error(f"Pipeline failed: {exc}")
        traceback.print_exc()
        return None, (None, None, None)
