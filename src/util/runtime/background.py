import asyncio
from datetime import datetime, timezone
import multiprocessing
import os
import queue
import sys
import time
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
            else:
                dt, max_entries = latest_task
                queued_at_iso = None

            if queued_at_iso:
                try:
                    queue_lag_s = (datetime.now(timezone.utc) - datetime.fromisoformat(str(queued_at_iso))).total_seconds()
                    queue_log(log_queue, f"INFO: Starting freshest queued GOES render for {dt.isoformat()} after {queue_lag_s:.1f}s queue lag")
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


def nexrad_ingest_loop(log_queue, base_dir):
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)
    stall_timeout_seconds = _nexrad_pipeline_stall_timeout_seconds()
    restart_delay_seconds = _NEXRAD_PIPELINE_RESTART_DELAY_SECONDS
    heartbeat_poll_seconds = _NEXRAD_PIPELINE_HEARTBEAT_POLL_SECONDS

    while True:
        heartbeat_queue = multiprocessing.Queue()
        process = multiprocessing.Process(
            target=_nexrad_ingest_pipeline_entry,
            args=(log_queue, heartbeat_queue, base_dir),
            daemon=False,
        )
        process.start()

        try:
            queue_log(log_queue, f"INFO: Starting NEXRAD ingest pipeline supervisor (pid={process.pid})")
            stalled = _supervise_nexrad_pipeline_process(
                log_queue,
                process,
                heartbeat_queue,
                stall_timeout_seconds=stall_timeout_seconds,
                heartbeat_poll_seconds=heartbeat_poll_seconds,
            )
            if not stalled and process.exitcode == 0:
                queue_log(log_queue, "WARNING: NEXRAD ingest pipeline exited cleanly; restarting")
            elif not stalled and process.exitcode is not None:
                queue_log(log_queue, f"ERROR: NEXRAD ingest pipeline exited with code {process.exitcode}; restarting")
        except KeyboardInterrupt:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            return
        finally:
            try:
                heartbeat_queue.close()
            except Exception:
                pass

        sleep_for(restart_delay_seconds, interval=0.2)


_NEXRAD_PIPELINE_STALL_TIMEOUT_SECONDS = 60.0
_NEXRAD_PIPELINE_RESTART_DELAY_SECONDS = 5.0
_NEXRAD_PIPELINE_HEARTBEAT_POLL_SECONDS = 1.0


def _nexrad_pipeline_stall_timeout_seconds() -> float:
    return max(30.0, float(os.environ.get("NEXRAD_PIPELINE_STALL_TIMEOUT_SECONDS", _NEXRAD_PIPELINE_STALL_TIMEOUT_SECONDS)))


def _supervise_nexrad_pipeline_process(
    log_queue,
    process,
    heartbeat_queue,
    *,
    stall_timeout_seconds: float,
    heartbeat_poll_seconds: float,
) -> bool:
    last_heartbeat_monotonic = time.monotonic()
    while process.is_alive():
        try:
            heartbeat_queue.get(timeout=heartbeat_poll_seconds)
            last_heartbeat_monotonic = time.monotonic()
        except queue.Empty:
            if (time.monotonic() - last_heartbeat_monotonic) >= stall_timeout_seconds:
                queue_log(
                    log_queue,
                    "ERROR: NEXRAD ingest pipeline stalled; terminating and restarting child process",
                )
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    process.kill()
                    process.join(timeout=1)
                return True

    return False


def _nexrad_heartbeat_writer(heartbeat_queue):
    heartbeat_queue.put_nowait(time.monotonic())


def _nexrad_ingest_pipeline_entry(log_queue, heartbeat_queue, base_dir):
    sys.stdout = QueueWriter(log_queue)
    sys.stderr = QueueWriter(log_queue)
    try:
        queue_log(log_queue, "INFO: Starting NEXRAD ingest pipeline")
        run_realtime_ingestion_pipeline(
            base_dir=base_dir,
            heartbeat_callback=lambda: _nexrad_heartbeat_writer(heartbeat_queue),
        )
    except KeyboardInterrupt:
        return
    except Exception as exc:
        queue_log(log_queue, f"ERROR: NEXRAD ingest pipeline crashed: {exc}")
        for line in traceback.format_exc().splitlines():
            queue_log(log_queue, f"ERROR: {line}")
        raise


def nexrad_render_loop(base_dir):
    try:
        _run_nexrad_render_loop(base_dir=base_dir)
    except KeyboardInterrupt:
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
