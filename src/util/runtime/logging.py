from datetime import datetime, timezone


def queue_log(log_queue, message):
    timestamp = datetime.now(timezone.utc).isoformat()
    log_queue.put(f"[{timestamp}] {message}")


def drain_log_queue(log_queue):
    while not log_queue.empty():
        print(log_queue.get())
