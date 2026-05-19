"""NEXRAD Level-II parser helpers.

This module centralizes both:
- lightweight byte-level AR2V/message-31 parsing used for sweep discovery
- dataset opening/helpers used by downstream NetCDF and render writers

The byte-level parsing follows the same basic approach as
`scripts/demo_nexrad_chunk_elevations.py` so the worker can reason about
ordered chunk streams without depending on xradar for sweep boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
import bz2
import struct
import warnings

import numpy as np


RECORD_BYTES = 2432
VOLUME_HEADER_BYTES = 24
MSG_HEADER_LEN = 16
MSG_31_PREFIX_LEN = 28
VOLUME_MAGICS = (b"AR2V", b"ARCHIVE2")
SUPPORTED_METADATA_TYPES = {2, 3, 5, 13, 15, 18}
START_STATUSES = {0, 3, 5}
END_STATUSES = {2, 4}


def _decompress_chunked_record_stream(volume_bytes: bytes) -> bytes:
    """Decompress the live chunked compressed AR2V record stream."""
    if len(volume_bytes) <= 28:
        return b""

    record_parts: list[bytes] = []

    first = bz2.BZ2Decompressor()
    record_parts.append(first.decompress(volume_bytes[28:]))
    if first.unused_data:
        cursor = len(volume_bytes) - len(first.unused_data)
    else:
        cursor = len(volume_bytes)

    while cursor + 4 <= len(volume_bytes):
        block_size = struct.unpack(">I", volume_bytes[cursor : cursor + 4])[0]
        cursor += 4
        if block_size <= 0 or cursor + block_size > len(volume_bytes):
            break
        compressed_block = volume_bytes[cursor : cursor + block_size]
        cursor += block_size
        record_parts.append(bz2.decompress(compressed_block))

    result = b"".join(record_parts)
    del record_parts
    return result


@dataclass
class RawSweep:
    index: int
    group_name: str
    elevation_number: int
    fixed_angle: float
    first_timestamp: str | None
    last_timestamp: str | None
    radial_count: int = 0
    waveform: str | None = None
    records: list[bytes] = field(default_factory=list)
    complete: bool = False


@dataclass
class RawVolume:
    volume_header: bytes
    site: str
    metadata_records: list[bytes] = field(default_factory=list)
    sweeps: list[RawSweep] = field(default_factory=list)
    trailing_bytes: bytes = b""
    compression_record_count: int = 0


def normalize_chunk_payload(payload: bytes, *, first_chunk_of_volume: bool) -> bytes:
    """Normalize a live chunk payload into an uncompressed rewritable buffer.

    First chunk of a volume keeps the 24-byte AR2V header, followed by an
    uncompressed record stream. Subsequent chunks append only record bytes.
    """
    if not payload:
        return b""

    if first_chunk_of_volume:
        if len(payload) < VOLUME_HEADER_BYTES:
            return payload
        header = payload[:VOLUME_HEADER_BYTES]
        compression_record_count = struct.unpack(">I", payload[24:28])[0] if len(payload) >= 28 else 0
        if compression_record_count > 0:
            return header + bz2.decompress(payload[28:])
        return header + payload[VOLUME_HEADER_BYTES:]

    if len(payload) >= 8 and payload[4:8] == b"BZh9":
        block_size = struct.unpack(">I", payload[:4])[0]
        block = payload[4 : 4 + block_size]
        if block:
            return bz2.decompress(block)
    return payload


def _collect_timestamp(date_number: int, milliseconds: int) -> str | None:
    if date_number <= 0:
        return None
    base = datetime(1970, 1, 1, tzinfo=UTC)
    timestamp = base + timedelta(days=date_number - 1, milliseconds=milliseconds)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_msg_header(record: bytes) -> tuple[int, int]:
    if len(record) < 12 + MSG_HEADER_LEN:
        raise ValueError("Record is too short for a Level II message header")
    size_words, _channels, msg_type, _seq_id, _date, _ms, _segments, _seg_num = struct.unpack(
        ">HBBHHIHH", record[12 : 12 + MSG_HEADER_LEN]
    )
    return size_words, msg_type


def _record_size(record: bytes, msg_type: int, size_words: int) -> int:
    size = size_words * 2 + 12
    if msg_type != 31:
        return max(size, RECORD_BYTES)
    return size


def _read_msg31_metadata(record: bytes) -> tuple[int, int, int, float, str | None]:
    start = 12 + MSG_HEADER_LEN
    end = start + MSG_31_PREFIX_LEN
    if len(record) < end:
        raise ValueError("Message 31 record is truncated")
    (
        _identifier,
        collect_ms,
        collect_date,
        _azimuth_number,
        _azimuth_angle,
        _compress_flag,
        _spare_0,
        _radial_length,
        _azimuth_resolution,
        radial_status,
        elevation_number,
        _cut_sector,
        elevation_angle,
    ) = struct.unpack(">4sIHHfBBHBBBBf", record[start:end])
    timestamp = _collect_timestamp(collect_date, collect_ms)
    return radial_status, elevation_number, collect_ms, elevation_angle, timestamp


def split_stream_into_volumes(stream_bytes: bytes) -> list[bytes]:
    """Split an ordered chunk stream into inline AR2V/ARCHIVE2 volumes."""
    if not stream_bytes:
        return []

    starts = [0]
    cursor = 1
    while cursor < len(stream_bytes):
        next_offsets = []
        for magic in VOLUME_MAGICS:
            found = stream_bytes.find(magic, cursor)
            if found >= 0:
                next_offsets.append(found)
        if not next_offsets:
            break
        next_start = min(next_offsets)
        starts.append(next_start)
        cursor = next_start + 1

    volumes: list[bytes] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(stream_bytes)
        volumes.append(stream_bytes[start:end])
    return volumes


def parse_raw_volume_bytes(volume_bytes: bytes) -> RawVolume:
    """Parse raw AR2V bytes into ordered sweep metadata.

    This parser intentionally handles the modern message-31 path used by the
    current chunk-ingest flow. It preserves raw records and sweep boundaries so
    downstream code can reason about complete sweeps without asking xradar to do
    that work.
    """
    if len(volume_bytes) < VOLUME_HEADER_BYTES:
        raise ValueError("Volume is too short to contain an AR2V header")
    if not any(volume_bytes.startswith(magic) for magic in VOLUME_MAGICS):
        raise ValueError("Volume does not start with AR2V/ARCHIVE2 magic")

    volume_header = volume_bytes[:VOLUME_HEADER_BYTES]
    site = volume_header[20:24].decode("ascii", errors="ignore").strip() or "UNKNOWN"
    compression_record_count = struct.unpack(">I", volume_bytes[24:28])[0] if len(volume_bytes) >= 28 else 0

    if compression_record_count > 0:
        record_stream = _decompress_chunked_record_stream(volume_bytes)
        offset = 0
    else:
        record_stream = volume_bytes
        offset = VOLUME_HEADER_BYTES

    del volume_bytes

    metadata_records: list[bytes] = []
    sweeps: list[RawSweep] = []
    current: RawSweep | None = None
    sweep_index = 0

    while offset + 12 + MSG_HEADER_LEN <= len(record_stream):
        header_probe = record_stream[offset : min(offset + RECORD_BYTES, len(record_stream))]
        size_words, msg_type = _read_msg_header(header_probe)
        record_len = _record_size(header_probe, msg_type, size_words)
        if offset + record_len > len(record_stream):
            break

        record = record_stream[offset : offset + record_len]
        offset += record_len

        if msg_type in SUPPORTED_METADATA_TYPES:
            metadata_records.append(record)
            continue

        if msg_type != 31:
            continue

        radial_status, elevation_number, _collect_ms, elevation_angle, timestamp = _read_msg31_metadata(record)

        if radial_status in START_STATUSES:
            if current is not None and current.records:
                sweeps.append(current)
            current = RawSweep(
                index=sweep_index,
                group_name=f"/sweep_{sweep_index}",
                elevation_number=elevation_number,
                fixed_angle=float(elevation_angle),
                first_timestamp=timestamp,
                last_timestamp=timestamp,
            )
            sweep_index += 1
        elif current is None:
            current = RawSweep(
                index=sweep_index,
                group_name=f"/sweep_{sweep_index}",
                elevation_number=elevation_number,
                fixed_angle=float(elevation_angle),
                first_timestamp=timestamp,
                last_timestamp=timestamp,
            )
            sweep_index += 1

        current.records.append(record)
        current.radial_count += 1
        current.last_timestamp = timestamp or current.last_timestamp

        if radial_status in END_STATUSES:
            current.complete = True
            sweeps.append(current)
            current = None

    if current is not None and current.records:
        sweeps.append(current)

    trailing = record_stream[offset:]
    del record_stream

    return RawVolume(
        volume_header=volume_header,
        site=site.upper(),
        metadata_records=metadata_records,
        sweeps=sweeps,
        trailing_bytes=trailing,
        compression_record_count=compression_record_count,
    )


def parse_raw_volume_file(path: str | Path) -> RawVolume:
    return parse_raw_volume_bytes(Path(path).read_bytes())


def open_partial_volume(path: str | Path):
    """Open a partial NEXRAD Level-II file with xradar, dropping incomplete sweeps."""
    try:
        import xradar as xd
    except ImportError as exc:
        raise RuntimeError("xradar is required for NEXRAD dataset decoding") from exc

    opener = getattr(xd.io.backends.nexrad_level2, "open_nexradlevel2_datatree", None)
    if opener is None:
        raise RuntimeError("xradar nexrad Level-II DataTree opener is unavailable")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            return opener(str(path), loaddata=False, incomplete_sweep="drop")
        except TypeError:
            return opener(str(path))


def extract_sweep_timestamp(ds) -> str | None:
    """Extract sweep timestamp from xarray time coordinate."""
    try:
        values = np.asarray(ds["time"].values).reshape(-1)
        values = values[~np.isnat(values)]
        if len(values) == 0:
            return None
        return np.datetime_as_string(values.max(), unit="s", timezone="UTC")
    except Exception:
        return None


def extract_sweep_angle(ds) -> float | None:
    """Extract fixed angle from sweep dataset."""
    try:
        angle_var = ds.get("sweep_fixed_angle")
        if angle_var is None:
            return None
        return float(angle_var.values.item())
    except Exception:
        return None


def extract_waveform(node) -> str | None:
    """Extract waveform type from sweep node."""
    try:
        attrs = getattr(node, "attrs", {}) or {}
        dataset = node.ds if hasattr(node, "ds") else node.to_dataset()
        return (
            attrs.get("waveform_type")
            or dataset.attrs.get("waveform_type")
            or dataset.attrs.get("prt_mode")
            or dataset.attrs.get("sweep_mode")
        )
    except Exception:
        return None


def extract_azimuth_count(ds) -> int:
    """Extract azimuth count from sweep dataset."""
    try:
        return int(ds.sizes.get("azimuth", ds.sizes.get("time", 0)))
    except Exception:
        return 0
