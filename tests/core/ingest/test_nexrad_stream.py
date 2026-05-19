import pytest

from common.ingest.nexrad.stream import (
    MAX_MAGIC_OVERLAP,
    VOLUME_MAGICS,
    VolumeState,
    create_scan_state,
    detect_next_volume_offset,
    finalize_scan_state,
    iter_transport_chunks,
    split_at_boundary,
)


def test_volume_magics_and_overlap():
    assert b"AR2V" in VOLUME_MAGICS
    assert b"ARCHIVE2" in VOLUME_MAGICS
    assert MAX_MAGIC_OVERLAP == len(b"ARCHIVE2") - 1


def test_detect_next_volume_offset_finds_ar2v():
    payload = b"some_data_AR2V_more_data"
    found, offset = detect_next_volume_offset(b"", payload, stream_has_started=True)
    assert found is True
    assert offset == payload.index(b"AR2V")


def test_detect_next_volume_offset_finds_archive2():
    payload = b"some_data_ARCHIVE2_more_data"
    found, offset = detect_next_volume_offset(b"", payload, stream_has_started=True)
    assert found is True
    assert offset == payload.index(b"ARCHIVE2")


def test_detect_next_volume_offset_no_magic():
    payload = b"some_data_without_magic"
    found, offset = detect_next_volume_offset(b"", payload, stream_has_started=True)
    assert found is False
    assert offset == -1


def test_detect_next_volume_offset_ignores_first_magic_at_stream_start():
    payload = b"AR2V_data"
    found, offset = detect_next_volume_offset(b"", payload, stream_has_started=False)
    assert found is False
    assert offset == -1


def test_detect_next_volume_offset_boundary_straddles_tail_and_payload():
    previous_tail = b"some_data_AR"
    payload = b"2V_more_data"
    found, offset = detect_next_volume_offset(previous_tail, payload, stream_has_started=True)
    assert found is True
    assert offset == 0


def test_detect_next_volume_offset_archive2_straddles_boundary():
    previous_tail = b"data_ARCH"
    payload = b"IVE2_rest"
    found, offset = detect_next_volume_offset(previous_tail, payload, stream_has_started=True)
    assert found is True
    assert offset == 0


def test_detect_next_volume_offset_prefers_earlier_magic():
    payload = b"xxxAR2VyyyARCHIVE2zzz"
    found, offset = detect_next_volume_offset(b"", payload, stream_has_started=True)
    assert found is True
    assert offset == payload.index(b"AR2V")


def test_split_at_boundary():
    payload = b"before_boundaryafter"
    boundary_offset = 15
    before, after = split_at_boundary(payload, boundary_offset)
    assert before == b"before_boundary"
    assert after == b"after"


def test_split_at_boundary_zero_offset():
    payload = b"full_after"
    before, after = split_at_boundary(payload, 0)
    assert before == b""
    assert after == b"full_after"


def test_split_at_boundary_end_offset():
    payload = b"full_before"
    before, after = split_at_boundary(payload, len(payload))
    assert before == b"full_before"
    assert after == b""


def test_create_scan_state():
    state = create_scan_state(0, "VOL123", "/path/to/file.ar2v")
    assert state.index == 0
    assert state.volume_id == "VOL123"
    assert state.file_path == "/path/to/file.ar2v"
    assert state.bytes_written == 0
    assert state.finalized is False


def test_finalize_scan_state():
    state = create_scan_state(0, "VOL123", "/path/to/file.ar2v")
    state.bytes_written = 1000
    finalized = finalize_scan_state(state)
    assert finalized.finalized is True
    assert finalized.bytes_written == 1000


def test_iter_transport_chunks_splits_large_payload():
    payloads = [b"a" * 600, b"b" * 600]
    chunks = iter_transport_chunks(payloads, transport_size=500)
    assert len(chunks) == 3
    assert len(chunks[0]) == 500
    assert len(chunks[1]) == 500
    assert len(chunks[2]) == 200


def test_iter_transport_chunks_single_small_payload():
    payloads = [b"small"]
    chunks = iter_transport_chunks(payloads, transport_size=1000)
    assert len(chunks) == 1
    assert chunks[0] == b"small"
