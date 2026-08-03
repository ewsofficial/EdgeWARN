"""Small filesystem commit primitives for runtime artifacts.

Final artifact names are observable by independent API processes.  Writers
must therefore create and validate a sibling temporary file before atomically
replacing the visible name.
"""

from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def atomic_output_path(destination: str | Path, *, suffix: str = ".part") -> Iterator[Path]:
    """Yield a sibling temporary path and replace *destination* on success.

    The existing destination is deliberately left untouched if writing or
    validation fails.  The temporary path is removed on every failure path.
    """
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, raw_temp = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=suffix,
    )
    os.close(fd)
    temporary = Path(raw_temp)
    try:
        yield temporary
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def atomic_write_bytes(destination: str | Path, payload: bytes) -> Path:
    destination = Path(destination)
    with atomic_output_path(destination) as temporary:
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    return destination


def atomic_write_text(destination: str | Path, payload: str, *, encoding="utf-8") -> Path:
    return atomic_write_bytes(Path(destination), payload.encode(encoding))


def atomic_write_json(destination: str | Path, value, *, indent=None, default=None) -> Path:
    payload = json.dumps(value, indent=indent, default=default)
    return atomic_write_text(destination, payload)
