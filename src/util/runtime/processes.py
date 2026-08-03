from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import os
import threading
import time


def stop_process(process, name, *, join_timeout=5):
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


@dataclass
class StartedProcessRegistry:
    processes: list[tuple[object, str]] = field(default_factory=list)

    def start(self, process, name):
        if process is None:
            return None

        process.start()
        self.processes.append((process, name))
        return process

    def shutdown(self, *, queue_sentinels=(), manager=None):
        for queue_obj, sentinel in queue_sentinels:
            try:
                queue_obj.put(sentinel)
            except Exception:
                pass

        while self.processes:
            process, name = self.processes.pop()
            stop_process(process, name)

        if manager is not None:
            try:
                manager.shutdown()
            except Exception:
                pass


@dataclass
class AccessorySupervisor:
    """Monitor and restart accessory child processes with bounded backoff.

    Each registered process is checked periodically.  Dead processes are
    restarted with exponential backoff.  Crash-loop detection disables
    restarts after *max_restarts* deaths within *restart_window_seconds*.
    """

    _process_info: list = field(default_factory=list)
    _restart_times: dict = field(default_factory=lambda: defaultdict(list))
    _stop_event: threading.Event = field(default_factory=threading.Event)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    max_restarts: int = 5
    restart_window_seconds: float = 60.0
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 30.0
    health_path: str | None = None

    def add(self, name, target, *, enabled=True, args=None, kwargs=None, daemon=True, cleanup_event=None):
        entry = {
            "name": name,
            "target": target,
            "args": args or (),
            "kwargs": kwargs or {},
            "daemon": daemon,
            "enabled": enabled,
            "process": None,
            "cleanup_event": cleanup_event,
        }
        self._process_info.append(entry)
        return entry

    def start_all(self):
        import multiprocessing
        for info in self._process_info:
            if not info["enabled"]:
                continue
            proc = multiprocessing.Process(
                target=info["target"],
                args=info["args"],
                kwargs=info["kwargs"],
                name=info["name"],
            )
            proc.daemon = info["daemon"]
            proc.start()
            info["process"] = proc
            self._record_health(info["name"], "running")

    def request_stop(self):
        self._stop_event.set()

    def check(self):
        import multiprocessing
        now = time.monotonic()
        for info in self._process_info:
            if not info["enabled"] or self._stop_event.is_set():
                continue
            proc = info.get("process")
            if proc is None or not proc.is_alive():
                self._handle_death(info, now)

    def _handle_death(self, info, now):
        import multiprocessing
        name = info["name"]
        with self._lock:
            times = self._restart_times[name]
            times.append(now)
            times[:] = [t for t in times if now - t < self.restart_window_seconds]

            attempt = len(times)
            exit_code = info["process"].exitcode if info["process"] is not None else "N/A"
            print(f"[Supervisor] {name} process is dead (pid exited {exit_code}); restart attempt {attempt}")

            # Clear any shared flag the process may have left asserted before
            # deciding on a restart: blocking loops (e.g. GOES ingest pausing
            # while the renderer is active) must never stay stuck behind a
            # dead process, whether or not it is going to be restarted.
            cleanup_event = info.get("cleanup_event")
            if cleanup_event is not None:
                try:
                    cleanup_event.clear()
                except Exception:
                    pass

            if attempt > self.max_restarts:
                print(
                    f"[Supervisor] {name} has crashed {attempt} times "
                    f"in {self.restart_window_seconds}s; disabling restarts"
                )
                info["enabled"] = False
                self._record_health(name, "crashed", error=f"crashed {attempt} times")
                return

            delay = min(
                self.max_backoff_seconds,
                self.base_backoff_seconds * (2 ** (attempt - 1)),
            )
            self._record_health(name, "restarting", error="dead, restart pending", attempt=attempt)

        time.sleep(delay)

        if self._stop_event.is_set():
            return

        proc = multiprocessing.Process(
            target=info["target"],
            args=info["args"],
            kwargs=info["kwargs"],
            name=info["name"],
        )
        proc.daemon = info["daemon"]
        proc.start()
        info["process"] = proc
        self._record_health(name, "running")

    def _record_health(self, name, status, *, error=None, attempt=None):
        if self.health_path is None:
            return
        try:
            os.makedirs(os.path.dirname(self.health_path), exist_ok=True)
            current = {}
            if os.path.isfile(self.health_path):
                try:
                    with open(self.health_path, "r") as f:
                        current = json.load(f)
                except Exception:
                    current = {}
            now_iso = datetime.now(timezone.utc).isoformat()
            entry = dict(current.get(name, {}))
            entry["status"] = status
            entry["last_updated"] = now_iso
            if error is not None:
                entry["last_error"] = error
            if attempt is not None:
                entry["restart_attempt"] = attempt
            current[name] = entry
            temporary = self.health_path + ".tmp"
            with open(temporary, "w") as f:
                json.dump(current, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temporary, self.health_path)
        except Exception:
            pass

    def shutdown(self):
        for info in self._process_info:
            proc = info.get("process")
            if proc is not None:
                stop_process(proc, info["name"])
