from datetime import datetime, timezone
import re
from functools import lru_cache

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from common.ingest.nexrad.config import CHUNKS_BUCKET, MIN_VOLUME_FILE_CHUNKS
from common.ingest.nexrad.models import ChunkKey
from util.handler import extract_timestamp

_CHUNK_KEY_RE = re.compile(
    r"^(?P<site>[A-Z0-9]+)/(?P<volume_id>[^/]+)/"
    r"(?:(?P<stamp>[0-9]{8}-[0-9]{6})-)?"
    r"(?P<chunk>[0-9]{3})-(?P<chunk_type>[A-Z])$"
)
_TIMESTAMP_RE = re.compile(r"(?P<stamp>[0-9]{8}-[0-9]{6})")
_VOLUME_ID_TS_RE = re.compile(r"(?P<date>[0-9]{8})[_-](?P<time>[0-9]{6})")
_TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"


@lru_cache(maxsize=1)
def get_unsigned_s3_client():
    return boto3.client("s3", config=Config(signature_version=UNSIGNED))


def parse_chunk_key(key: str) -> ChunkKey | None:
    match = _CHUNK_KEY_RE.match(key)
    if not match:
        return None
    return ChunkKey(
        site=match.group("site"),
        volume_id=match.group("volume_id"),
        chunk_number=int(match.group("chunk")),
        chunk_type=match.group("chunk_type") or "I",
        key=key,
    )


def order_recent_volume_ids(volume_ids):
    ordered = sorted({str(volume_id) for volume_id in volume_ids}, reverse=True)
    numeric_pairs = []
    for volume_id in ordered:
        if not volume_id.isdigit():
            return ordered
        numeric_pairs.append((volume_id, int(volume_id)))

    numeric_values = [value for _text, value in numeric_pairs]
    if 1 not in numeric_values or len(numeric_pairs) < 2:
        return ordered

    ascending = sorted(numeric_pairs, key=lambda item: item[1])
    largest_gap = 0
    gap_index = -1
    for index in range(len(ascending) - 1):
        gap = ascending[index + 1][1] - ascending[index][1]
        if gap > largest_gap:
            largest_gap = gap
            gap_index = index

    if largest_gap <= 1 or gap_index < 0:
        return ordered

    wrapped_segment = ascending[: gap_index + 1]
    prior_segment = ascending[gap_index + 1 :]
    if not wrapped_segment or wrapped_segment[0][1] != 1:
        return ordered

    wrapped_ordered = sorted(wrapped_segment, key=lambda item: item[1], reverse=True)
    wrapped_ordered.extend(sorted(prior_segment, key=lambda item: item[1], reverse=True))
    return [text for text, _value in wrapped_ordered]


def parse_nexrad_timestamp(value) -> datetime | None:
    return extract_timestamp(value, use_timezone_utc=True)


def format_nexrad_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime(_TIMESTAMP_FORMAT)


def extract_volume_timestamp(volume_id: str, chunks) -> str:
    if chunks:
        first = chunks[0]
        filename = first.key.rsplit("/", 1)[-1]
        match = _TIMESTAMP_RE.search(filename)
        if match:
            return match.group("stamp")

    match = _VOLUME_ID_TS_RE.search(volume_id)
    if match:
        return f"{match.group('date')}-{match.group('time')}"
    return volume_id


def required_volume_chunks(chunks):
    needed = [chunk for chunk in chunks if chunk.chunk_number <= MIN_VOLUME_FILE_CHUNKS]
    if len(needed) < MIN_VOLUME_FILE_CHUNKS:
        return []
    return needed


class NexradChunkStore:
    def __init__(self, *, s3_client=None, bucket=CHUNKS_BUCKET):
        self.s3_client = s3_client
        self.bucket = bucket

    def _client(self, override=None):
        return override or self.s3_client or get_unsigned_s3_client()

    def list_recent_volume_ids(self, site: str, limit=1, *, s3_client=None):
        client = self._client(override=s3_client)
        prefix = f"{site.upper()}/"
        paginator = client.get_paginator("list_objects_v2")
        volume_ids = set()
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
            for common_prefix in page.get("CommonPrefixes", []):
                child_prefix = common_prefix.get("Prefix", "")
                parts = child_prefix.rstrip("/").split("/", 1)
                if len(parts) == 2 and parts[1]:
                    volume_ids.add(parts[1])
        return order_recent_volume_ids(volume_ids)[:limit]

    def list_volume_chunks(self, site: str, volume_id: str, *, s3_client=None):
        client = self._client(override=s3_client)
        prefix = f"{site.upper()}/{volume_id}/"
        paginator = client.get_paginator("list_objects_v2")
        chunks = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                parsed = parse_chunk_key(obj["Key"])
                if parsed is not None:
                    chunks.append(parsed)
        chunks.sort(key=lambda item: (item.chunk_number, item.chunk_type))
        return chunks

    def get_chunk_bytes(self, chunk_key: ChunkKey, *, s3_client=None):
        client = self._client(override=s3_client)
        response = client.get_object(Bucket=self.bucket, Key=chunk_key.key)
        return response["Body"].read()


def list_recent_volume_ids(site: str, limit=1, *, s3_client=None, bucket=CHUNKS_BUCKET):
    store = NexradChunkStore(s3_client=s3_client, bucket=bucket)
    return store.list_recent_volume_ids(site, limit=limit)


def list_volume_chunks(site: str, volume_id: str, *, s3_client=None, bucket=CHUNKS_BUCKET):
    store = NexradChunkStore(s3_client=s3_client, bucket=bucket)
    return store.list_volume_chunks(site, volume_id)


def get_chunk_bytes(chunk_key: ChunkKey, *, s3_client=None, bucket=CHUNKS_BUCKET):
    store = NexradChunkStore(s3_client=s3_client, bucket=bucket)
    return store.get_chunk_bytes(chunk_key)
