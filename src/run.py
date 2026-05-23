import sys
import re
import queue
import os
from datetime import datetime, timezone, timedelta
import time
import multiprocessing
import asyncio

import util.file as fs
import common.ingest.nws.main as nws_ingest
import common.ingest.metar as metar_ingest
from common.ingest.nexrad.pipeline import run_realtime_ingestion_pipeline
from common.ingest.mrms.config import get_abi_radc_channel_specs, get_check_modifiers, get_goes_modifiers
from common.ingest.mrms.downloader import (
    download_goes_product,
    download_goes_specs,
    download_goes_specs_async,
)
from common.ingest.wpc.main import run_wpc_ingest
from common.pipeline.goes_readiness import (
    check_local_glm_ready as _check_local_glm_ready_impl,
    check_local_goes_ready as _check_local_goes_ready_impl,
    get_ewmrs_goes_render_specs as _get_ewmrs_goes_render_specs_impl,
)
from common.pipeline.coordinator import run_tandem_ingest_cycle
from EdgeWARN import initialize_runtime
from EdgeWARN.pipeline import edgewarn_tandem_worker
from EWMRS.pipeline import ewmrs_goes_worker, ewmrs_tandem_worker, run_nexrad_render_loop
from EdgeWARN.schedule.scheduler import MRMSUpdateChecker
from util.io import TimestampedOutput, IOManager
from util.release import get_release_version

sys.stdout = TimestampedOutput(sys.stdout)
sys.stderr = TimestampedOutput(sys.stderr)

io_manager = IOManager("[Main]")
args = io_manager.get_args()

lat_limits = tuple(args.lat_limits)
lon_limits = tuple(args.lon_limits)

initialize_runtime(base_dir=args.base_dir, io_manager=io_manager)

GOES_POLL_SECONDS = 60
GOES_RENDER_WAIT_SECONDS = 30
GOES_RENDER_WAIT_INTERVAL_SECONDS = 1.0
GOES_CYCLE_ACTIVE = multiprocessing.Event()
GOES_RENDER_ACTIVE = multiprocessing.Event()
GOES_PAUSE_INGEST_DURING_RENDER = os.environ.get("EDGEWARN_PAUSE_GOES_INGEST_DURING_RENDER", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
EWMRS_ENABLED = not args.disable_ewmrs
NWS_ENABLED = not args.disable_nws
METAR_ENABLED = not args.disable_metar
GOES_ENABLED = not args.disable_goes


def _get_ewmrs_goes_render_specs():
    return _get_ewmrs_goes_render_specs_impl()


def goes_loop(activity_event, render_active_event):
    try:
        abi_specs = get_abi_radc_channel_specs()
        while True:
            while GOES_PAUSE_INGEST_DURING_RENDER and render_active_event.is_set():
                _sleep(1, interval=0.2)

            target_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
            try:
                activity_event.set()
                asyncio.run(download_goes_specs_async(abi_specs, target_dt))
            except Exception as exc:
                print(f"[GOES Loop] Async ingest failed ({target_dt}): {exc}. Falling back to sync.")
                try:
                    download_goes_specs(abi_specs, target_dt)
                except Exception as fallback_exc:
                    print(f"[GOES Loop] Sync fallback failed ({target_dt}): {fallback_exc}")
            finally:
                activity_event.clear()

            _sleep(GOES_POLL_SECONDS, interval=1.0)
    except KeyboardInterrupt:
        return


def _queue_log(log_queue, message):
    timestamp = datetime.now(timezone.utc).isoformat()
    log_queue.put(f"[{timestamp}] {message}")


def _drain_log_queue(log_queue):
    while not log_queue.empty():
        print(log_queue.get())


def goes_render_loop(task_queue, log_queue, render_active_event):
    try:
        while True:
            task = task_queue.get()
            if task is None:
                render_active_event.clear()
                return

            latest_task = task
            dropped_tasks = 0
            saw_shutdown = False
            while True:
                try:
                    queued_task = task_queue.get_nowait()
                except queue.Empty:
                    break

                if queued_task is None:
                    saw_shutdown = True
                    continue

                latest_task = queued_task
                dropped_tasks += 1

            if dropped_tasks > 0:
                _queue_log(log_queue, f"INFO: Dropped {dropped_tasks} stale queued GOES render task(s); latest-wins scheduling applied")

            if isinstance(latest_task, tuple) and len(latest_task) >= 2:
                dt, max_entries = latest_task[:2]
                queued_at_iso = latest_task[2] if len(latest_task) > 2 else None
            else:
                dt, max_entries = latest_task
                queued_at_iso = None

            if queued_at_iso:
                try:
                    queue_lag_s = (datetime.now(timezone.utc) - datetime.fromisoformat(str(queued_at_iso))).total_seconds()
                    _queue_log(log_queue, f"INFO: Starting freshest queued GOES render for {dt.isoformat()} after {queue_lag_s:.1f}s queue lag")
                except Exception:
                    pass

            render_active_event.set()
            ewmrs_goes_worker(log_queue, dt, max_entries=max_entries)

            render_active_event.clear()
            if saw_shutdown:
                return
    except KeyboardInterrupt:
        render_active_event.clear()
        return


def nexrad_ingest_loop():
    try:
        run_realtime_ingestion_pipeline(base_dir=args.base_dir)
    except KeyboardInterrupt:
        return


def nexrad_render_loop():
    try:
        run_nexrad_render_loop(base_dir=args.base_dir)
    except KeyboardInterrupt:
        return


def _sleep(total_seconds, interval=1.0):
    end_time = time.time() + total_seconds
    while time.time() < end_time:
        time.sleep(min(interval, max(0.0, end_time - time.time())))


def _sleep_until_boundary(minutes):
    now = datetime.now(timezone.utc)
    minutes_to_next = minutes - (now.minute % minutes)
    if minutes_to_next == 0 and now.second == 0 and now.microsecond == 0:
        minutes_to_next = minutes
    next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next)
    sleep_seconds = max(0.0, (next_run - now).total_seconds())
    if sleep_seconds > 0:
        _sleep(sleep_seconds, interval=1.0)


def _check_local_goes_ready(dt, *, specs=None):
    candidate_specs = _get_ewmrs_goes_render_specs() if specs is None else specs
    return _check_local_goes_ready_impl(dt, specs=candidate_specs)


def _wait_for_local_goes_ready(
    dt,
    *,
    specs=None,
    timeout_seconds=GOES_RENDER_WAIT_SECONDS,
    interval_seconds=GOES_RENDER_WAIT_INTERVAL_SECONDS,
):
    candidate_specs = _get_ewmrs_goes_render_specs() if specs is None else specs
    if not candidate_specs:
        return False, None

    timeout_seconds = max(0.0, float(timeout_seconds))
    interval_seconds = max(0.1, float(interval_seconds))
    deadline = time.time() + timeout_seconds

    while True:
        goes_ready, goes_path = _check_local_goes_ready(dt, specs=candidate_specs)
        if goes_ready and not GOES_CYCLE_ACTIVE.is_set():
            return True, goes_path

        if time.time() >= deadline:
            return False, None

        _sleep(min(interval_seconds, max(0.0, deadline - time.time())), interval=0.2)


def _check_local_glm_ready(dt):
    return _check_local_glm_ready_impl(dt, specs=get_goes_modifiers())


def _download_glm_for_scan(dt):
    glm_spec = next((spec for spec in get_goes_modifiers() if spec.is_glm), None)
    if glm_spec is None:
        return []

    return download_goes_product(glm_spec, dt)


def _stop_process(process, name, *, join_timeout=5):
    if process is None:
        return

    try:
        if process.is_alive():
            print(f"[Scheduler] Stopping {name} process...")
            process.terminate()

        process.join(timeout=join_timeout)

        if process.is_alive():
            print(f"[Scheduler] {name} did not stop in time; killing...")
            process.kill()
            process.join(timeout=1)
    except Exception as exc:
        print(f"[Scheduler] Failed to stop {name} process cleanly: {exc}")


def metar_loop():
    try:
        while True:
            _sleep_until_boundary(5)

            try:
                asyncio.run(metar_ingest.ingest_metars_async())
            except Exception as e:
                print(f"[METAR Loop] Error: {e}")
    except KeyboardInterrupt:
        return


def nws_loop():
    try:
        while True:
            try:
                asyncio.run(nws_ingest.download_alerts_async(datetime.now(timezone.utc)))
            except Exception as e:
                print(f"[NWS Loop] Error: {e}")

            _sleep(120, interval=1.0)
    except KeyboardInterrupt:
        return


def wpc_loop():
    try:
        while True:
            _sleep_until_boundary(15)

            try:
                run_wpc_ingest()
            except Exception as e:
                print(f"[WPC Loop] Error: {e}")
    except KeyboardInterrupt:
        return

def _run_tandem_cycle(dt, goes_render_task_queue, goes_render_log_queue):
    log_queue = multiprocessing.Queue()
    manager = multiprocessing.Manager()
    shared_state = manager.dict()

    detection_ready_event = multiprocessing.Event()
    ewmrs_mrms_ready_event = multiprocessing.Event()
    ewmrs_goes_ready_event = multiprocessing.Event()
    integration_ready_event = multiprocessing.Event()

    try:
        cycle_state = asyncio.run(
            run_tandem_ingest_cycle(
                dt,
                lambda msg: _queue_log(log_queue, msg),
                include_goes=False,
                include_ewmrs=EWMRS_ENABLED,
            )
        )
    except Exception as exc:
        _drain_log_queue(log_queue)
        manager.shutdown()
        print(f"[Scheduler] Tandem ingest cycle failed for {dt}: {exc}")
        return False

    glm_ready = False
    glm_path = None
    if GOES_ENABLED:
        try:
            glm_results = _download_glm_for_scan(dt)
            if glm_results:
                _queue_log(log_queue, f"INFO: Scan-time GLM ingest satisfied by {len(glm_results)} file(s)")
            else:
                _queue_log(log_queue, f"INFO: Scan-time GLM ingest found no files for {dt.isoformat()}")
        except Exception as exc:
            _queue_log(log_queue, f"WARN: Scan-time GLM ingest failed for {dt.isoformat()}: {exc}")

        glm_ready, glm_path = _check_local_glm_ready(dt)
    else:
        _queue_log(log_queue, "INFO: GOES/GLM components disabled; EdgeWARN integration will not wait for GLM inputs")

    goes_specs = _get_ewmrs_goes_render_specs() if EWMRS_ENABLED and GOES_ENABLED else []
    rap_ready = "rap_ingest" not in cycle_state.errors
    mrms_integration_ready = cycle_state.detection_inputs_ready and "mrms_integration_ingest" not in cycle_state.errors
    edgewarn_integration_ready = mrms_integration_ready and rap_ready and (glm_ready or not GOES_ENABLED)
    if GOES_ENABLED and not glm_ready:
        _queue_log(log_queue, f"INFO: No local GLM files staged at or after {dt.isoformat()}; EdgeWARN integration will wait for GOES")
    elif GOES_ENABLED:
        _queue_log(log_queue, f"INFO: Local GLM readiness satisfied by {glm_path}")

    shared_state["detection_inputs_ready"] = cycle_state.detection_inputs_ready
    shared_state["ewmrs_mrms_inputs_ready"] = cycle_state.ewmrs_mrms_inputs_ready
    shared_state["ewmrs_goes_inputs_ready"] = False
    shared_state["edgewarn_integration_inputs_ready"] = edgewarn_integration_ready
    shared_state["edgewarn_generated_file"] = ""
    errors = {
        key: value
        for key, value in dict(cycle_state.errors).items()
        if key not in {"goes_ingest", "ewmrs_goes_ingest", "edgewarn_integration_ingest"}
    }
    if GOES_ENABLED and not glm_ready:
        errors.setdefault("goes_ingest", "GOES inputs unavailable")
    if not edgewarn_integration_ready:
        errors.setdefault("edgewarn_integration_ingest", "EdgeWARN integration inputs unavailable")
    shared_state["errors"] = errors

    edgewarn_proc = multiprocessing.Process(
        target=edgewarn_tandem_worker,
        args=(
            log_queue,
            shared_state,
            detection_ready_event,
            integration_ready_event,
            dt,
            lat_limits,
            lon_limits,
            args.profile,
            args.disable_ctam,
            args.disable_tracking,
            args.refl_threshold,
            args.min_seed_percentage,
            args.drop_offset,
        ),
    )
    ewmrs_proc = None
    if EWMRS_ENABLED:
        ewmrs_proc = multiprocessing.Process(
            target=ewmrs_tandem_worker,
            args=(log_queue, shared_state, ewmrs_mrms_ready_event, ewmrs_goes_ready_event, dt),
        )

    edgewarn_proc.start()
    if ewmrs_proc is not None:
        ewmrs_proc.start()

    detection_ready_event.set()
    if EWMRS_ENABLED:
        ewmrs_mrms_ready_event.set()
    integration_ready_event.set()

    goes_ready = False
    goes_path = None
    try:
        if EWMRS_ENABLED and GOES_ENABLED:
            goes_ready, goes_path = _check_local_goes_ready(dt, specs=goes_specs)
            if goes_ready and GOES_CYCLE_ACTIVE.is_set():
                goes_ready = False
                goes_path = None

            if not goes_ready:
                _queue_log(
                    log_queue,
                    f"INFO: Waiting for background GOES ABI ingest cycle to fully stage render inputs for {dt.isoformat()}",
                )
                goes_ready, goes_path = _wait_for_local_goes_ready(
                    dt,
                    specs=goes_specs,
                    timeout_seconds=GOES_RENDER_WAIT_SECONDS,
                    interval_seconds=GOES_RENDER_WAIT_INTERVAL_SECONDS,
                )

            if not goes_ready:
                _queue_log(
                    log_queue,
                    f"INFO: Background GOES ABI ingest did not finish staging the full render input set for {dt.isoformat()}; GOES render phase will be skipped",
                )
            else:
                _queue_log(log_queue, f"INFO: Full GOES ABI render input set is staged; representative file {goes_path}")
                dropped_render_tasks = 0
                saw_shutdown = False
                while True:
                    try:
                        queued_task = goes_render_task_queue.get_nowait()
                    except queue.Empty:
                        break

                    if queued_task is None:
                        saw_shutdown = True
                        continue
                    dropped_render_tasks += 1

                goes_render_task_queue.put((dt, 10, datetime.now(timezone.utc).isoformat()))
                if saw_shutdown:
                    goes_render_task_queue.put(None)
                if dropped_render_tasks > 0:
                    _queue_log(
                        log_queue,
                        f"INFO: Replaced {dropped_render_tasks} stale queued GOES render task(s) with latest ready cycle {dt.isoformat()}",
                    )
                _queue_log(log_queue, f"INFO: Queued decoupled EWMRS GOES render for {dt.isoformat()}")
        else:
            goes_ready = False
    except Exception as exc:
        if EWMRS_ENABLED and GOES_ENABLED:
            _queue_log(log_queue, f"WARN: Local GOES readiness check failed for {dt.isoformat()}: {exc}")
    finally:
        shared_state["ewmrs_goes_inputs_ready"] = EWMRS_ENABLED and goes_ready
        errors = dict(shared_state.get("errors", {}))
        if not EWMRS_ENABLED:
            errors.pop("ewmrs_ingest", None)
            errors.pop("ewmrs_goes_ingest", None)
            errors.pop("ewmrs_rap_uint16", None)
        elif goes_ready:
            errors.pop("ewmrs_goes_ingest", None)
        elif GOES_ENABLED:
            errors.setdefault("ewmrs_goes_ingest", "EWMRS GOES inputs unavailable")
        shared_state["errors"] = errors
        if EWMRS_ENABLED:
            ewmrs_goes_ready_event.set()

    while edgewarn_proc.is_alive() or (ewmrs_proc is not None and ewmrs_proc.is_alive()) or not log_queue.empty():
        _drain_log_queue(log_queue)
        _drain_log_queue(goes_render_log_queue)
        time.sleep(1)

    edgewarn_proc.join()
    ewmrs_proc_exitcode = 0
    if ewmrs_proc is not None:
        ewmrs_proc.join()
        ewmrs_proc_exitcode = ewmrs_proc.exitcode
    _drain_log_queue(log_queue)
    _drain_log_queue(goes_render_log_queue)
    manager.shutdown()
    return edgewarn_proc.exitcode == 0 and ewmrs_proc_exitcode == 0



def main():
    """Scheduler: run a shared ingest cycle and launch EdgeWARN/EWMRS in tandem."""
    print("Scheduler started. Press CTRL+C to exit.")
    if args.disable_ctam:
        print("[Scheduler] CTAM execution disabled via --disable-ctam")
    if args.disable_tracking:
        print("[Scheduler] Tracking disabled via --disable-tracking")
    if args.disable_ewmrs:
        print("[Scheduler] EWMRS pipeline disabled via --disable-ewmrs")
    if args.disable_nws:
        print("[Scheduler] NWS background ingest disabled via --disable-nws")
    if args.disable_metar:
        print("[Scheduler] METAR background ingest disabled via --disable-metar")
    if args.disable_goes:
        print("[Scheduler] GOES/GLM ingest and GOES rendering disabled via --disable-goes")
    print(
        "[Scheduler] Detection thresholds: "
        f"refl_threshold={args.refl_threshold}, "
        f"min_seed_percentage={args.min_seed_percentage}, "
        f"drop_offset={args.drop_offset}"
    )
    if GOES_ENABLED:
        print("[Scheduler] GOES ingest decoupled: running as independent background process")
    checker = MRMSUpdateChecker(verbose=True)
    last_processed = None  # Track last processed timestamp

    # Check for existing JSON output to initialize last_processed
    try:
        if fs.STORMCELL_DIR.exists():
            files = sorted(fs.STORMCELL_DIR.glob("stormcells_*.json"))
            if files:
                latest_file = files[-1]
                match = re.search(r"stormcells_(\d{8}-\d{6})\.json", latest_file.name)
                if match:
                    ts_str = match.group(1) 
                    dt_exact = datetime.strptime(ts_str, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
                    last_processed = dt_exact.replace(second=0, microsecond=0)
                    print(f"[Scheduler] Initialized last_processed from {latest_file}: {last_processed}")
                else:
                    print(f"[Scheduler] Could not parse timestamp from {latest_file}")
            else:
                 print(f"[Scheduler] No previous stormcell data found in {fs.STORMCELL_DIR}. Starting fresh.")
        else:
             print(f"[Scheduler] {fs.STORMCELL_DIR} does not exist. Starting fresh.")

    except Exception as e:
        print(f"[Scheduler] Failed to initialize last_processed: {e}")

    print("[Scheduler] Starting background accessory ingests...")
    metar_proc = multiprocessing.Process(target=metar_loop, daemon=True) if METAR_ENABLED else None
    nws_proc = multiprocessing.Process(target=nws_loop, daemon=True) if NWS_ENABLED else None
    wpc_proc = multiprocessing.Process(target=wpc_loop, daemon=True)
    goes_render_task_queue = multiprocessing.Queue()
    goes_render_log_queue = multiprocessing.Queue()
    goes_render_proc = multiprocessing.Process(
        target=goes_render_loop,
        args=(goes_render_task_queue, goes_render_log_queue, GOES_RENDER_ACTIVE),
        daemon=True,
    ) if EWMRS_ENABLED and GOES_ENABLED else None
    nexrad_render_proc = multiprocessing.Process(target=nexrad_render_loop, daemon=True) if EWMRS_ENABLED else None
    # NEXRAD ingest uses a ProcessPoolExecutor for parser workers, so this
    # process must not be daemonic or child worker creation will fail.
    nexrad_ingest_proc = multiprocessing.Process(target=nexrad_ingest_loop, daemon=False) if EWMRS_ENABLED else None
    if metar_proc is not None:
        metar_proc.start()
    if nws_proc is not None:
        nws_proc.start()
    wpc_proc.start()
    goes_proc = multiprocessing.Process(target=goes_loop, args=(GOES_CYCLE_ACTIVE, GOES_RENDER_ACTIVE), daemon=True) if GOES_ENABLED else None
    if goes_proc is not None:
        goes_proc.start()
    if goes_render_proc is not None:
        goes_render_proc.start()
    if nexrad_render_proc is not None:
        nexrad_render_proc.start()
    if nexrad_ingest_proc is not None:
        nexrad_ingest_proc.start()

    background_processes = [
        (metar_proc, "METAR"),
        (nws_proc, "NWS"),
        (wpc_proc, "WPC"),
        (goes_proc, "GOES"),
        (goes_render_proc, "GOES Render"),
        (nexrad_render_proc, "NEXRAD Render"),
        (nexrad_ingest_proc, "NEXRAD Ingest"),
    ]

    try:
        while True:
            _drain_log_queue(goes_render_log_queue)
            now = datetime.now(timezone.utc)
            check_modifiers = get_check_modifiers()
            # Pass last_processed to allow StartAfter optimization
            latest_common = checker.latest_common_minute_1h(check_modifiers, last_processed=last_processed)

            # Strict check: Only accept if new AND strictly newer than last_processed
            is_new_s3 = False
            if latest_common:
                if last_processed is None:
                    is_new_s3 = True
                elif latest_common > last_processed:
                    is_new_s3 = True
            
            # If S3 didn't give us a NEW timestamp, try HTTPS immediately
            if not is_new_s3:
                # print(f"[Scheduler] S3 yielded no new data (Latest: {latest_common}, Last: {last_processed}). Checking HTTPS...")
                latest_https = checker.check_https_fallback(check_modifiers, now)
                
                # Check if HTTPS result is better
                if latest_https:
                    is_new_https = False
                    if last_processed is None:
                         is_new_https = True
                    elif latest_https > last_processed:
                         is_new_https = True
                    
                    if is_new_https:
                        print(f"[Scheduler] HTTPS Fallback found NEWER timestamp: {latest_https}")
                        latest_common = latest_https # Take the HTTPS one
                    else:
                        pass # HTTPS found data but it's also old
                else:
                    pass # HTTPS failed or no files

            # Now proceed with latest_common if it is effectively new
            # Re-evaluate newness (in case we switched to HTTPS one)
            should_run_pipeline = False
            if latest_common:
                 if last_processed is None:
                     should_run_pipeline = True
                 elif latest_common > last_processed:
                     should_run_pipeline = True

            if should_run_pipeline:
                print(f"[Scheduler] DEBUG: New latest common timestamp: {latest_common}")
                dt = latest_common
                last_processed = latest_common

                cycle_ok = _run_tandem_cycle(dt, goes_render_task_queue, goes_render_log_queue)
                if cycle_ok:
                    print(f"Tandem cycle for {dt} finished")
                else:
                    print(f"Tandem cycle for {dt} did not complete successfully")

            else:
                if not latest_common:
                     print("[Scheduler] No new data found (S3 or HTTPS). Waiting...")
                else:
                     print(f"[Scheduler] Timestamp {latest_common} already processed. Waiting...")

            # Wait/Check loop (15 seconds)
            for _ in range(30):
                time.sleep(0.5)

    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
    finally:
        try:
            goes_render_task_queue.put(None)
        except Exception:
            pass
        for process, name in background_processes:
            _stop_process(process, name)

if __name__ == "__main__":
    try:
        print(f"Running EdgeWARN v{get_release_version()}")
        print(f"Latitude limits: {lat_limits}, Longitude limits: {lon_limits}")
        main()
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
