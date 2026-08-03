import asyncio
import ctypes
from datetime import datetime, timezone
import json
import os
import queue
import signal
import sys
import traceback

import common.ingest.metar as metar_ingest
import common.ingest.nws.main as nws_ingest
from common.ingest.mrms.config import get_abi_radc_channel_specs
from common.ingest.mrms.downloader import download_goes_specs, download_goes_specs_async
from common.ingest.nexrad.pipeline import run_realtime_ingestion_pipeline
from common.ingest.wpc.main import run_wpc_ingest
from EWMRS.pipeline import ewmrs_goes_worker, run_nexrad_render_loop as _run_nexrad_render_loop
from util.io import QueueWriter

from .logging import queue_log
from .timing import sleep_for, sleep_until_boundary


_SHUTDOWN_REQUESTED = False


def _set_process_name(name: str) -> None:
    try:
        import multiprocessing

        multiprocessing.current_process().name = name
    except Exception:
        pass

    try:
        libc = ctypes.CDLL(None)
        pr_set_name = 15
        encoded = name.encode("utf-8")[:15]
        libc.prctl(pr_set_name, ctypes.c_char_p(encoded), 0, 0, 0)
    except Exception:
        pass


def _set_parent_death_signal(sig: int = signal.SIGTERM) -> None:
    try:
        libc = ctypes.CDLL(None)
        pr_set_pdeathsig = 1
        libc.prctl(pr_set_pdeathsig, sig, 0, 0, 0)
    except Exception:
        pass


def _install_exit_signal_handlers() -> None:
    def _raise_system_exit(signum, _frame):
        global _SHUTDOWN_REQUESTED
        _SHUTDOWN_REQUESTED = True
        raise SystemExit(signum)

    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(signum, _raise_system_exit)
        except Exception:
            pass


def _configure_process_runtime(name: str) -> None:
    global _SHUTDOWN_REQUESTED
    _SHUTDOWN_REQUESTED = False
    _set_process_name(name)
    _set_parent_death_signal()
    _install_exit_signal_handlers()


def goes_loop(activity_event, render_active_event, pause_during_render=False, poll_seconds=60):
    try:
        abi_specs = get_abi_radc_channel_specs()
        while True:
            while pause_during_render and render_active_event.is_set():
                sleep_for(1, interval=0.2)

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

            sleep_for(poll_seconds, interval=1.0)
    except KeyboardInterrupt:
        return


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
                queue_log(log_queue, f"INFO: Dropped {dropped_tasks} stale queued GOES render task(s); latest-wins scheduling applied")

            if isinstance(latest_task, tuple) and len(latest_task) >= 2:
                dt, max_entries = latest_task[:2]
                queued_at_iso = latest_task[2] if len(latest_task) > 2 else None
                input_manifest = latest_task[3] if len(latest_task) > 3 else None
            else:
                dt, max_entries = latest_task
                queued_at_iso = None
                input_manifest = None

            if queued_at_iso:
                try:
                    queue_lag_s = (datetime.now(timezone.utc) - datetime.fromisoformat(str(queued_at_iso))).total_seconds()
                    queue_log(log_queue, f"INFO: Starting freshest queued GOES render for {dt.isoformat()} after {queue_lag_s:.1f}s queue lag")
                except Exception:
                    pass

            render_active_event.set()
            ewmrs_goes_worker(
                log_queue,
                dt,
                max_entries=max_entries,
                input_manifest=input_manifest,
            )

            render_active_event.clear()
            if saw_shutdown:
                return
    except KeyboardInterrupt:
        render_active_event.clear()
        return


def _write_nexrad_heartbeat(heartbeat_path, payload, *, latest_output):
    """Atomically publish completed NEXRAD-cycle progress for the supervisor."""
    if heartbeat_path is None:
        return latest_output
    records = payload.get("completed_records", [])
    if records:
        record = records[-1]
        latest_output = {
            "site": record.site,
            "volume_id": record.volume_id,
            "scan_timestamp": record.scan_timestamp,
        }
    heartbeat = {
        "pid": os.getpid(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "cycle": payload["cycle"],
        "output_count": payload["output_count"],
        "timed_out": payload["timed_out"],
        "latest_output": latest_output,
    }
    os.makedirs(os.path.dirname(heartbeat_path), exist_ok=True)
    temporary = f"{heartbeat_path}.tmp.{os.getpid()}"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump(heartbeat, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, heartbeat_path)
    return latest_output


def nexrad_ingest_loop(log_queue, base_dir, heartbeat_path=None):
    from common.ingest.nexrad.worker_pool import shutdown_nexrad_pool

    _configure_process_runtime("NEXRAD-Ingest")
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)
    latest_output = None

    def _heartbeat(payload):
        nonlocal latest_output
        latest_output = _write_nexrad_heartbeat(
            heartbeat_path,
            payload,
            latest_output=latest_output,
        )

    try:
        while not _SHUTDOWN_REQUESTED:
            try:
                if _SHUTDOWN_REQUESTED:
                    return
                queue_log(log_queue, "INFO: Starting NEXRAD ingest pipeline")
                run_realtime_ingestion_pipeline(base_dir=base_dir, heartbeat_callback=_heartbeat)
                if _SHUTDOWN_REQUESTED:
                    return
                queue_log(log_queue, "WARNING: NEXRAD ingest pipeline exited; restarting in 5s")
            except (KeyboardInterrupt, SystemExit):
                return
            except Exception as exc:
                queue_log(log_queue, f"ERROR: NEXRAD ingest pipeline crashed: {exc}")
                for line in traceback.format_exc().splitlines():
                    queue_log(log_queue, f"ERROR: {line}")

            if _SHUTDOWN_REQUESTED:
                return
            sleep_for(5, interval=0.2)
    finally:
        shutdown_nexrad_pool(wait=False)


def nexrad_render_loop(base_dir):
    _configure_process_runtime("NEXRAD-Render")
    try:
        _run_nexrad_render_loop(base_dir=base_dir)
    except (KeyboardInterrupt, SystemExit):
        return


def metar_loop():
    try:
        while True:
            sleep_until_boundary(5)

            try:
                asyncio.run(metar_ingest.ingest_metars_async())
            except Exception as exc:
                print(f"[METAR Loop] Error: {exc}")
    except KeyboardInterrupt:
        return


def nws_loop():
    try:
        while True:
            try:
                asyncio.run(nws_ingest.download_alerts_async(datetime.now(timezone.utc)))
            except Exception as exc:
                print(f"[NWS Loop] Error: {exc}")

            sleep_for(120, interval=1.0)
    except KeyboardInterrupt:
        return


def wpc_loop():
    try:
        while True:
            sleep_until_boundary(15)

            try:
                run_wpc_ingest()
            except Exception as exc:
                print(f"[WPC Loop] Error: {exc}")
    except KeyboardInterrupt:
        return
