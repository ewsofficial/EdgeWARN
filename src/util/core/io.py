from datetime import datetime, timezone

# ===== Wrap stdout/stderr to add timestamps to all prints =====
class TimestampedOutput:
    def __init__(self, stream):
        self.stream = stream

    def write(self, message):
        if message.strip():  # skip empty lines
            timestamp = datetime.now(timezone.utc).isoformat()
            self.stream.write(f"[{timestamp}] {message}")
        else:
            self.stream.write(message)

    def flush(self):
        self.stream.flush()