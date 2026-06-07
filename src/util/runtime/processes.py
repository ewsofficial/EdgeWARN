from dataclasses import dataclass, field


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
