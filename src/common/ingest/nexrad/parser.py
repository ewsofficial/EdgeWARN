"""NEXRAD Level-II parser helpers.

This module centralizes lightweight byte-level AR2V/message-31 parsing used for
sweep discovery and grouped AR2V inspection.

The byte-level parsing follows the same basic approach as
`scripts/demo_nexrad_chunk_elevations.py` so the worker can reason about
ordered chunk streams without dataset decoders for sweep boundaries.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import bz2
import mmap
import struct

from common.ingest.nexrad.models import RawSweepRange, RawVolumeBuffer


RECORD_BYTES = 2432
VOLUME_HEADER_BYTES = 24
MSG_HEADER_LEN = 16
MSG_31_PREFIX_LEN = 28
MSG_31_BLOCK_POINTERS = 10
VOLUME_MAGICS = (b"AR2V", b"ARCHIVE2")
SUPPORTED_METADATA_TYPES = {2, 3, 5, 13, 15, 18}
START_STATUSES = {0, 3, 5}
END_STATUSES = {2, 4}
DUALPOL_BLOCKS = {"DZDR", "DPHI", "DRHO", "DCFP"}
DOPPLER_BLOCKS = {"DVEL", "DSW "}
MIN_SWEEP_ANGLE_DEG = 0.4
DREF_BLOCK = frozenset({"DREF"})


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


def _is_chunked_bzip_volume(volume_bytes: bytes) -> bool:
    """Return True when bytes follow the chunked-compressed AR2V layout."""
    if len(volume_bytes) < 31:
        return False
    compression_record_count = struct.unpack(">I", volume_bytes[24:28])[0]
    return compression_record_count > 0 and volume_bytes[28:31] == b"BZh"


def _is_chunked_bzip_volume_buffer(buffer, length: int) -> bool:
    """Return True when a byte-like buffer follows the chunked BZip layout."""
    if length < 31:
        return False
    compression_record_count = struct.unpack(">I", buffer[24:28])[0]
    return compression_record_count > 0 and bytes(buffer[28:31]) == b"BZh"


def _decompress_and_parse_stream(
    data: bytes,
    start_offset: int,
    metadata_ranges: list[tuple[int, int]],
    sweeps: list[RawSweepRange],
) -> int:
    """Streaming BZ2 decompression: decompress one block, parse records, discard.

    Returns the final offset within the decompressed stream.
    """
    decompressor = bz2.BZ2Decompressor()
    decompressed = decompressor.decompress(data[start_offset:])
    if decompressor.unused_data:
        cursor = len(data) - len(decompressor.unused_data)
    else:
        cursor = len(data)

    offset = 0
    offset = _walk_records(decompressed, offset, metadata_ranges, sweeps)

    while cursor + 4 <= len(data):
        block_size = struct.unpack(">I", data[cursor : cursor + 4])[0]
        cursor += 4
        if block_size <= 0 or cursor + block_size > len(data):
            break
        compressed_block = data[cursor : cursor + block_size]
        cursor += block_size
        decompressed_block = bz2.decompress(compressed_block)
        offset = _walk_records(decompressed_block, offset, metadata_ranges, sweeps)
        del decompressed_block

    return offset


def _walk_records(
    record_stream,
    offset: int,
    metadata_ranges: list[tuple[int, int]],
    sweeps: list[RawSweepRange],
    sweep_index: int | None = None,
) -> tuple[int, int]:
    """Walk message-31 records from a byte-like stream starting at offset.

    Works with bytes, bytearray, mmap, or memoryview (zero-copy slices).
    Returns (final_offset, sweep_index).
    """
    if sweep_index is None:
        sweep_index = len(sweeps)

    current: RawSweepRange | None = None
    if sweeps and not sweeps[-1].complete:
        current = sweeps.pop()

    stream_len = len(record_stream)
    while offset + 12 + MSG_HEADER_LEN <= stream_len:
        header_probe = record_stream[offset : min(offset + RECORD_BYTES, stream_len)]
        size_words, msg_type = _read_msg_header(header_probe)
        record_len = _record_size(header_probe, msg_type, size_words)
        if offset + record_len > stream_len:
            break

        record = record_stream[offset : offset + record_len]
        offset += record_len

        if msg_type in SUPPORTED_METADATA_TYPES:
            metadata_ranges.append((offset - record_len, offset))
            continue

        if msg_type != 31:
            continue

        radial_status, elevation_number, _collect_ms, elevation_angle, timestamp = _read_msg31_metadata(record)
        sweep_angle = float(elevation_angle)
        if sweep_angle < MIN_SWEEP_ANGLE_DEG:
            if radial_status in START_STATUSES and current is not None and current.record_ranges:
                sweeps.append(current)
                sweep_index = current.index + 1
                current = None
            continue
        waveform = _classify_msg31_waveform(record, sweep_angle)

        if radial_status in START_STATUSES:
            if current is not None and current.record_ranges:
                sweeps.append(current)
                sweep_index = current.index + 1
            current = RawSweepRange(
                index=sweep_index,
                group_name=f"/sweep_{sweep_index}",
                elevation_number=elevation_number,
                fixed_angle=sweep_angle,
                waveform=waveform,
                first_timestamp=timestamp,
                last_timestamp=timestamp,
            )
            sweep_index += 1
        elif current is None:
            current = RawSweepRange(
                index=sweep_index,
                group_name=f"/sweep_{sweep_index}",
                elevation_number=elevation_number,
                fixed_angle=sweep_angle,
                waveform=waveform,
                first_timestamp=timestamp,
                last_timestamp=timestamp,
            )
            sweep_index += 1

        if current.waveform is None and waveform is not None:
            current.waveform = waveform

        current.record_ranges.append((offset - record_len, offset))
        current.radial_count += 1
        current.last_timestamp = timestamp or current.last_timestamp

        if radial_status in END_STATUSES:
            current.complete = True
            sweeps.append(current)
            current = None

    if current is not None and current.record_ranges:
        sweeps.append(current)

    return offset, sweep_index


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


def _message31_block_names(record: bytes) -> list[str]:
    msg31_start = 12 + MSG_HEADER_LEN
    pointer_start = msg31_start + MSG_31_PREFIX_LEN
    pointer_end = pointer_start + 4 + MSG_31_BLOCK_POINTERS * 4
    if len(record) < pointer_end:
        return []

    offsets = struct.unpack(
        ">" + "I" * MSG_31_BLOCK_POINTERS,
        record[pointer_start + 4 : pointer_end],
    )

    names: list[str] = []
    for offset in offsets:
        if offset <= 0:
            continue
        block_start = msg31_start + offset
        block_end = block_start + 4
        if block_end > len(record):
            continue
        name = record[block_start:block_end].decode("ascii", errors="ignore")
        if name:
            names.append(name)
    return names


def filter_msg31_blocks(record: bytes, drop_block_names: set[str] | frozenset[str]) -> bytes:
    """Return a message-31 record with selected data blocks removed.

    Non-message-31 records are returned unchanged.
    """
    size_words, msg_type = _read_msg_header(record)
    if msg_type != 31 or not drop_block_names:
        return record

    msg31_start = 12 + MSG_HEADER_LEN
    pointer_start = msg31_start + MSG_31_PREFIX_LEN
    pointer_end = pointer_start + 4 + MSG_31_BLOCK_POINTERS * 4
    if len(record) < pointer_end:
        return record

    prefix = record[msg31_start : msg31_start + MSG_31_PREFIX_LEN]
    word1, _block_count = struct.unpack(">HH", record[pointer_start : pointer_start + 4])
    offsets = list(struct.unpack(">" + "I" * MSG_31_BLOCK_POINTERS, record[pointer_start + 4 : pointer_end]))

    positive_offsets = [offset for offset in offsets if offset > 0]
    if not positive_offsets:
        return record

    record_end = min(_record_size(record, msg_type, size_words), len(record))
    body_end = record_end - msg31_start
    selected_blocks: list[bytes] = []

    boundaries = positive_offsets[1:] + [body_end]
    for offset, next_offset in zip(positive_offsets, boundaries):
        block_start = msg31_start + offset
        block_end = msg31_start + next_offset
        if block_end > record_end or block_start + 4 > block_end:
            continue
        name = record[block_start : block_start + 4].decode("ascii", errors="ignore")
        if name in drop_block_names:
            continue
        selected_blocks.append(record[block_start:block_end])

    if len(selected_blocks) == len(positive_offsets):
        return record

    next_offset = MSG_31_PREFIX_LEN + 4 + MSG_31_BLOCK_POINTERS * 4
    new_offsets: list[int] = []
    payload = bytearray()
    for block in selected_blocks:
        new_offsets.append(next_offset)
        payload.extend(block)
        next_offset += len(block)
    new_offsets.extend([0] * (MSG_31_BLOCK_POINTERS - len(new_offsets)))

    body = bytearray(prefix)
    body.extend(struct.pack(">HH", word1, len(selected_blocks)))
    body.extend(struct.pack(">" + "I" * MSG_31_BLOCK_POINTERS, *new_offsets))
    body.extend(payload)

    new_record = bytearray(record[:12])
    msg_header = list(struct.unpack(">HBBHHIHH", record[12 : 12 + MSG_HEADER_LEN]))
    total_after_prefix = MSG_HEADER_LEN + len(body)
    if total_after_prefix % 2 != 0:
        body.append(0)
        total_after_prefix += 1
    msg_header[0] = total_after_prefix // 2
    new_record.extend(struct.pack(">HBBHHIHH", *msg_header))
    new_record.extend(body)
    return bytes(new_record)


def _classify_msg31_waveform(record: bytes, elevation_angle: float) -> str | None:
    block_names = set(_message31_block_names(record))
    has_doppler = any(name in block_names for name in DOPPLER_BLOCKS)
    has_dualpol = any(name in block_names for name in DUALPOL_BLOCKS)
    has_reflectivity = "DREF" in block_names

    if has_doppler and has_dualpol:
        return "batch" if elevation_angle >= 4.0 else "staggered_pulse_pair"
    if has_doppler:
        return "contiguous_doppler"
    if has_dualpol or has_reflectivity:
        return "contiguous_surveillance"
    return None


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


def parse_raw_volume_bytes(volume_bytes: bytes) -> RawVolumeBuffer:
    """Parse raw AR2V bytes into ordered sweep metadata.

    This parser intentionally handles the modern message-31 path used by the
    current chunk-ingest flow. It preserves raw records and sweep boundaries so
    downstream code can reason about complete sweeps directly from the raw byte
    stream.
    """
    if len(volume_bytes) < VOLUME_HEADER_BYTES:
        raise ValueError("Volume is too short to contain an AR2V header")
    if not any(volume_bytes.startswith(magic) for magic in VOLUME_MAGICS):
        raise ValueError("Volume does not start with AR2V/ARCHIVE2 magic")

    volume_header = volume_bytes[:VOLUME_HEADER_BYTES]
    site = volume_header[20:24].decode("ascii", errors="ignore").strip() or "UNKNOWN"
    compressed_stream = _is_chunked_bzip_volume(volume_bytes)
    compression_record_count = struct.unpack(">I", volume_bytes[24:28])[0] if compressed_stream else 0

    if compressed_stream:
        record_stream = _decompress_chunked_record_stream(volume_bytes)
        offset = 0
    else:
        record_stream = volume_bytes
        offset = VOLUME_HEADER_BYTES

    del volume_bytes

    metadata_ranges: list[tuple[int, int]] = []
    sweeps: list[RawSweepRange] = []
    current: RawSweepRange | None = None
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
            metadata_ranges.append((offset - record_len, offset))
            continue

        if msg_type != 31:
            continue

        radial_status, elevation_number, _collect_ms, elevation_angle, timestamp = _read_msg31_metadata(record)
        sweep_angle = float(elevation_angle)
        if sweep_angle < MIN_SWEEP_ANGLE_DEG:
            if radial_status in START_STATUSES:
                if current is not None and current.record_ranges:
                    sweeps.append(current)
                current = None
            continue
        waveform = _classify_msg31_waveform(record, sweep_angle)

        if radial_status in START_STATUSES:
            if current is not None and current.record_ranges:
                sweeps.append(current)
            current = RawSweepRange(
                index=sweep_index,
                group_name=f"/sweep_{sweep_index}",
                elevation_number=elevation_number,
                fixed_angle=sweep_angle,
                waveform=waveform,
                first_timestamp=timestamp,
                last_timestamp=timestamp,
            )
            sweep_index += 1
        elif current is None:
            current = RawSweepRange(
                index=sweep_index,
                group_name=f"/sweep_{sweep_index}",
                elevation_number=elevation_number,
                fixed_angle=sweep_angle,
                waveform=waveform,
                first_timestamp=timestamp,
                last_timestamp=timestamp,
            )
            sweep_index += 1

        if current.waveform is None and waveform is not None:
            current.waveform = waveform

        current.record_ranges.append((offset - record_len, offset))
        current.radial_count += 1
        current.last_timestamp = timestamp or current.last_timestamp

        if radial_status in END_STATUSES:
            current.complete = True
            sweeps.append(current)
            current = None

    if current is not None and current.record_ranges:
        sweeps.append(current)

    buffer_bytes = record_stream
    trailing = record_stream[offset:]

    return RawVolumeBuffer(
        volume_header=volume_header,
        site=site.upper(),
        record_buffer=buffer_bytes,
        metadata_ranges=metadata_ranges,
        sweeps=sweeps,
        trailing_bytes=trailing,
        compression_record_count=compression_record_count,
    )


def parse_raw_volume_file(path: str | Path) -> RawVolumeBuffer:
    return parse_raw_volume_bytes(Path(path).read_bytes())


def parse_raw_volume_file_mmap(
    path: str | Path,
) -> RawVolumeBuffer:
    """Parse AR2V file using mmap for zero-copy, on-demand paging.

    Returns a RawVolume with parsed sweep metadata.
    """
    path = Path(path)
    file_size = path.stat().st_size
    if file_size < VOLUME_HEADER_BYTES:
        raise ValueError("Volume is too short to contain an AR2V header")

    with open(path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            volume_header = bytes(mm[:VOLUME_HEADER_BYTES])
            site = volume_header[20:24].decode("ascii", errors="ignore").strip() or "UNKNOWN"
            compressed_stream = _is_chunked_bzip_volume_buffer(mm, file_size)
            compression_record_count = struct.unpack(">I", mm[24:28])[0] if compressed_stream else 0

            metadata_ranges: list[tuple[int, int]] = []
            sweeps: list[RawSweepRange] = []
            final_offset: int = 0

            if compressed_stream:
                record_buffer = _decompress_chunked_record_stream(mm)
                final_offset, _ = _walk_records(record_buffer, 0, metadata_ranges, sweeps)
                trailing = record_buffer[final_offset:]
            else:
                final_offset, _ = _walk_records(mm, VOLUME_HEADER_BYTES, metadata_ranges, sweeps)
                record_buffer = bytes(mm)
                trailing = record_buffer[final_offset:]

            return RawVolumeBuffer(
                volume_header=volume_header,
                site=site.upper(),
                record_buffer=record_buffer,
                metadata_ranges=metadata_ranges,
                sweeps=sweeps,
                trailing_bytes=trailing,
                compression_record_count=compression_record_count,
            )


def parse_grouped_ar2v_file_mmap(path: str | Path) -> RawVolumeBuffer:
    """Parse an uncompressed grouped-elevation AR2V file written by this repo.

    Grouped elevation artifacts currently persist the 24-byte volume header
    followed directly by metadata and message-31 records, without the standard
    4-byte compression-record count used by full runtime volumes.
    """
    path = Path(path)
    file_size = path.stat().st_size
    if file_size < VOLUME_HEADER_BYTES:
        raise ValueError("Volume is too short to contain an AR2V header")

    with open(path, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            volume_header = bytes(mm[:VOLUME_HEADER_BYTES])
            if not any(volume_header.startswith(magic) for magic in VOLUME_MAGICS):
                raise ValueError("Volume does not start with AR2V/ARCHIVE2 magic")

            site = volume_header[20:24].decode("ascii", errors="ignore").strip() or "UNKNOWN"
            record_buffer = bytes(mm[:])
            metadata_ranges: list[tuple[int, int]] = []
            sweeps: list[RawSweepRange] = []
            final_offset, _ = _walk_records(record_buffer, VOLUME_HEADER_BYTES, metadata_ranges, sweeps)
            trailing = record_buffer[final_offset:]

            return RawVolumeBuffer(
                volume_header=volume_header,
                site=site.upper(),
                record_buffer=record_buffer,
                metadata_ranges=metadata_ranges,
                sweeps=sweeps,
                trailing_bytes=trailing,
                compression_record_count=0,
            )


def materialize_record_range(raw_volume: RawVolumeBuffer, record_range: tuple[int, int]) -> bytes:
    start, end = record_range
    return raw_volume.record_buffer[start:end]


def iter_metadata_records(raw_volume: RawVolumeBuffer):
    for record_range in raw_volume.metadata_ranges:
        yield materialize_record_range(raw_volume, record_range)


def iter_sweep_records(raw_volume: RawVolumeBuffer, sweep: RawSweepRange):
    for record_range in sweep.record_ranges:
        yield materialize_record_range(raw_volume, record_range)
def extract_azimuth_count(ds) -> int:
    """Extract azimuth count from sweep dataset."""
    try:
        return int(ds.sizes.get("azimuth", ds.sizes.get("time", 0)))
    except Exception:
        return 0
