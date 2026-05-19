"""Stream boundary detection utilities for NEXRAD Level-II overlap-safe parsing.

Modeled after nexrad_overlap_scan_poc.py boundary semantics.
"""

from dataclasses import dataclass, field
from pathlib import Path


VOLUME_MAGICS = (b"AR2V", b"ARCHIVE2")
MAX_MAGIC_OVERLAP = max(len(magic) for magic in VOLUME_MAGICS) - 1


@dataclass
class VolumeState:
    index: int
    volume_id: str
    file_path: str
    bytes_written: int = 0
    finalized: bool = False


@dataclass
class BoundaryResult:
    found: bool
    boundary_offset: int = -1
    before: bytes = b""
    after: bytes = b""


def detect_next_volume_offset(
    previous_tail: bytes,
    payload: bytes,
    stream_has_started: bool,
) -> tuple[bool, int]:
    """Detect the next volume magic in the combined tail+payload stream.

    Returns (found, payload_offset) where payload_offset is the position
    within *payload* where the new volume magic begins, or -1 if not found.
    """
    search_space = previous_tail + payload
    best_offset = -1
    best_magic_len = 0

    for magic in VOLUME_MAGICS:
        offset = search_space.find(magic)
        if offset < 0:
            continue
        if not stream_has_started and offset == 0 and best_offset < 0:
            continue
        if offset < best_offset or best_offset < 0:
            best_offset = offset
            best_magic_len = len(magic)

    if best_offset < 0:
        return False, -1

    payload_offset = best_offset - len(previous_tail)
    return True, max(payload_offset, 0)


def split_at_boundary(
    payload: bytes,
    boundary_offset: int,
) -> tuple[bytes, bytes]:
    """Split payload into before/after at the detected boundary offset.

    before = payload[:boundary_offset]  (belongs to current scan)
    after  = payload[boundary_offset:]  (belongs to next scan)
    """
    before = payload[:boundary_offset]
    after = payload[boundary_offset:]
    return before, after


def create_scan_state(
    index: int,
    volume_id: str,
    file_path: str | Path,
) -> VolumeState:
    """Create a new scan state for a fresh volume."""
    return VolumeState(
        index=index,
        volume_id=str(volume_id),
        file_path=str(file_path),
    )


def finalize_scan_state(state: VolumeState) -> VolumeState:
    """Mark a scan state as finalized."""
    return VolumeState(
        index=state.index,
        volume_id=state.volume_id,
        file_path=state.file_path,
        bytes_written=state.bytes_written,
        finalized=True,
    )


def iter_transport_chunks(
    payloads: list[bytes],
    transport_size: int = 1024 * 1024,
) -> list[bytes]:
    """Repack raw payloads into transport-sized chunks.

    This flattens all payloads into a continuous byte stream and
    yields chunks of at most *transport_size* bytes each.
    """
    combined = b"".join(payloads)
    result = []
    offset = 0
    while offset < len(combined):
        chunk = combined[offset:offset + transport_size]
        result.append(chunk)
        offset += transport_size
    return result


def iter_volume_payloads(
    site: str,
    volume_id: str,
    *,
    chunk_fetcher,
    chunks,
) -> list[bytes]:
    """Fetch and return ordered chunk payloads for a volume.

    Returns a list of raw byte payloads in chunk-number order.
    """
    sorted_chunks = sorted(chunks, key=lambda c: (c.chunk_number, c.chunk_type))
    payloads = []
    for chunk in sorted_chunks:
        payload = chunk_fetcher(chunk)
        if payload:
            payloads.append(payload)
    return payloads
