from common.ingest.nexrad.s3_chunks import parse_chunk_key


def test_parse_chunk_key_supports_timestamped_chunk_format():
    parsed = parse_chunk_key("KDDC/97/20260505-025330-061-I")
    assert parsed is not None
    assert parsed.site == "KDDC"
    assert parsed.volume_id == "97"
    assert parsed.chunk_number == 61
    assert parsed.chunk_type == "I"


def test_parse_chunk_key_rejects_non_chunk_keys():
    assert parse_chunk_key("KDDC/97/not-a-chunk") is None
