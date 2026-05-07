import re
from functools import lru_cache

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from common.ingest.nexrad.config import CHUNKS_BUCKET
from common.ingest.nexrad.models import ChunkKey

_CHUNK_KEY_RE = re.compile(
    r"^(?P<site>[A-Z0-9]+)/(?P<volume_id>[^/]+)/"
    r"(?:(?P<stamp>[0-9]{8}-[0-9]{6})-)?"
    r"(?P<chunk>[0-9]{3})-(?P<chunk_type>[A-Z])$"
)


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


def list_recent_volume_ids(site: str, limit=1, *, s3_client=None, bucket=CHUNKS_BUCKET):
    client = s3_client or get_unsigned_s3_client()
    prefix = f"{site.upper()}/"
    paginator = client.get_paginator("list_objects_v2")
    volume_ids = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/", 2)
            if len(parts) >= 2 and parts[1]:
                volume_ids.add(parts[1])
    return sorted(volume_ids, reverse=True)[:limit]


def list_volume_chunks(site: str, volume_id: str, *, s3_client=None, bucket=CHUNKS_BUCKET):
    client = s3_client or get_unsigned_s3_client()
    prefix = f"{site.upper()}/{volume_id}/"
    paginator = client.get_paginator("list_objects_v2")
    chunks = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            parsed = parse_chunk_key(obj["Key"])
            if parsed is not None:
                chunks.append(parsed)
    chunks.sort(key=lambda item: (item.chunk_number, item.chunk_type))
    return chunks


def get_chunk_bytes(chunk_key: ChunkKey, *, s3_client=None, bucket=CHUNKS_BUCKET):
    client = s3_client or get_unsigned_s3_client()
    response = client.get_object(Bucket=bucket, Key=chunk_key.key)
    return response["Body"].read()


def iter_chunks_until(site: str, volume_id: str, stop_condition, *, s3_client=None, bucket=CHUNKS_BUCKET):
    chunks = list_volume_chunks(site, volume_id, s3_client=s3_client, bucket=bucket)
    for chunk in chunks:
        yield chunk
        if stop_condition(chunk):
            break
