import pytest

from common.ingest.nexrad.s3_async import AsyncNexradChunkStore, async_get_chunk_bytes
from common.ingest.nexrad.s3_chunks import order_recent_volume_ids, parse_chunk_key


def test_parse_chunk_key_supports_timestamped_chunk_format():
    parsed = parse_chunk_key("KDDC/97/20260505-025330-061-I")
    assert parsed is not None
    assert parsed.site == "KDDC"
    assert parsed.volume_id == "97"
    assert parsed.chunk_number == 61
    assert parsed.chunk_type == "I"


def test_parse_chunk_key_rejects_non_chunk_keys():
    assert parse_chunk_key("KDDC/97/not-a-chunk") is None


class _FakeAsyncPaginator:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    async def paginate(self, **_kwargs):
        self.calls.append(_kwargs)
        for page in self.pages:
            yield page


class _FakeAsyncBody:
    def __init__(self, payload):
        self.payload = payload

    async def read(self):
        return self.payload


class _FakeAsyncS3Client:
    def __init__(self, *, pages=None, objects=None):
        self.pages = pages or []
        self.objects = objects or {}
        self.paginator = _FakeAsyncPaginator(self.pages)

    def get_paginator(self, _name):
        return self.paginator

    async def get_object(self, *, Bucket, Key):
        return {"Body": _FakeAsyncBody(self.objects[(Bucket, Key)])}


@pytest.mark.asyncio
async def test_async_list_recent_volume_ids_collects_unique_sorted_volume_ids():
    client = _FakeAsyncS3Client(
        pages=[
            {"CommonPrefixes": [{"Prefix": "KTLH/002/"}, {"Prefix": "KTLH/003/"}]},
            {"CommonPrefixes": [{"Prefix": "KTLH/001/"}, {"Prefix": "KTLH/003/"}]},
        ]
    )

    store = AsyncNexradChunkStore(s3_client=client, bucket="bucket")

    assert await store.async_list_recent_volume_ids("ktlh", limit=2) == ["003", "002"]
    assert client.paginator.calls == [{"Bucket": "bucket", "Prefix": "KTLH/", "Delimiter": "/"}]


def test_order_recent_volume_ids_prefers_wrapped_ids_before_limiting():
    assert order_recent_volume_ids(["999", "998", "2", "1"])[:3] == ["2", "1", "999"]


@pytest.mark.asyncio
async def test_async_list_volume_chunks_filters_invalid_keys_and_sorts_chunks():
    client = _FakeAsyncS3Client(
        pages=[
            {
                "Contents": [
                    {"Key": "KTLH/999/not-a-chunk"},
                    {"Key": "KTLH/999/20260507-150000-002-I"},
                    {"Key": "KTLH/999/20260507-150000-001-S"},
                    {"Key": "KTLH/999/20260507-150000-001-I"},
                ]
            }
        ]
    )

    store = AsyncNexradChunkStore(s3_client=client, bucket="bucket")
    chunks = await store.async_list_volume_chunks("KTLH", "999")

    assert [chunk.key for chunk in chunks] == [
        "KTLH/999/20260507-150000-001-I",
        "KTLH/999/20260507-150000-001-S",
        "KTLH/999/20260507-150000-002-I",
    ]


@pytest.mark.asyncio
async def test_async_get_chunk_bytes_reads_body_content():
    chunk = parse_chunk_key("KTLH/999/20260507-150000-001-I")
    client = _FakeAsyncS3Client(objects={("bucket", chunk.key): b"payload"})

    assert await async_get_chunk_bytes(chunk, s3_client=client, bucket="bucket") == b"payload"
