"""Utility for tailing server log output inside the tkinter UI."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable, Any
import queue

class LogTailer:
    """Minimal tail-like reader for a log file."""

    def __init__(self, path: Path, max_lines: int = 500) -> None:
        self.path = path
        self.max_lines = max_lines
        self._position = 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def read_new_lines(self) -> Iterable[str]:
        """Read any new lines since the last call."""

        with self.path.open("r", encoding="utf-8", errors="ignore") as handle:
            handle.seek(self._position)
            lines = handle.readlines()
            self._position = handle.tell()
        return lines

    def seed(self, lines: Iterable[str]) -> None:
        """Write initial lines to the log file if it is empty."""

        if self.path.stat().st_size > 0:
            return
        content = "".join(lines)
        if not content:
            return
        self.path.write_text(content, encoding="utf-8")
        self._position = self.path.stat().st_size

class QueueLogAdapter:
    """Reader that drains a multiprocessing.Queue."""

    def __init__(self, log_queue: Any, max_lines: int = 500) -> None:
        self.queue = log_queue
        self.max_lines = max_lines

    def read_new_lines(self) -> Iterable[str]:
        """Read all available lines from the queue."""
        lines = []
        try:
            while True:
                # Non-blocking get
                line = self.queue.get_nowait()
                lines.append(line + "\n" if not line.endswith("\n") else line)
        except queue.Empty:
            pass
        return lines

    def seed(self, lines: Iterable[str]) -> None:
        """No-op for queue adapter, or could put into queue."""
        pass
