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

class IOManager:
    def __init__(self, header):
        self.header = header
    
    def write_debug(self, msg):
        print(f"{self.header} DEBUG: {msg}")
        return

    def write_warning(self, msg):
        print(f"{self.header} WARN: {msg}")
        return

    def write_error(self, msg):
        print(f"{self.header} ERROR: {msg}")
        return