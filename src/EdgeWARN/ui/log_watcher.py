"""Utility for tailing server log output inside the tkinter UI."""
from __future__ import annotations

from pathlib import Path
from typing import Iterable


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
