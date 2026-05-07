from __future__ import annotations

from contextlib import asynccontextmanager

import aioboto3
from botocore import UNSIGNED
from botocore.client import Config

from common.ingest.aws_async_compat import ensure_aiobotocore_endpoint_compat
from common.ingest.nexrad.config import CHUNKS_BUCKET
from common.ingest.nexrad.s3_chunks import parse_chunk_key


@asynccontextmanager
async def get_unsigned_s3_client_async():
    ensure_aiobotocore_endpoint_compat()
    async with aioboto3.Session().client("s3", config=Config(signature_version=UNSIGNED)) as client:
        yield client


class AsyncNexradChunkStore:
    def __init__(self, *, s3_client=None, bucket=CHUNKS_BUCKET):
        self.s3_client = s3_client
        self.bucket = bucket

    def _client(self, override=None):
        client = override or self.s3_client
        if client is None:
            raise ValueError("An async S3 client is required for async NEXRAD chunk operations")
        return client

    async def async_list_recent_volume_ids(self, site: str, limit=1, *, s3_client=None):
        client = self._client(override=s3_client)
        prefix = f"{site.upper()}/"
        paginator = client.get_paginator("list_objects_v2")
        volume_ids = set()
        async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
            for common_prefix in page.get("CommonPrefixes", []):
                child_prefix = common_prefix.get("Prefix", "")
                parts = child_prefix.rstrip("/").split("/", 1)
                if len(parts) == 2 and parts[1]:
                    volume_ids.add(parts[1])
        return sorted(volume_ids, reverse=True)[:limit]

    async def async_list_volume_chunks(self, site: str, volume_id: str, *, s3_client=None):
        client = self._client(override=s3_client)
        prefix = f"{site.upper()}/{volume_id}/"
        paginator = client.get_paginator("list_objects_v2")
        chunks = []
        async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                parsed = parse_chunk_key(obj["Key"])
                if parsed is not None:
                    chunks.append(parsed)
        chunks.sort(key=lambda item: (item.chunk_number, item.chunk_type))
        return chunks

    async def async_get_chunk_bytes(self, chunk_key, *, s3_client=None):
        client = self._client(override=s3_client)
        response = await client.get_object(Bucket=self.bucket, Key=chunk_key.key)
        return await response["Body"].read()


async def async_list_recent_volume_ids(site: str, limit=1, *, s3_client=None, bucket=CHUNKS_BUCKET):
    store = AsyncNexradChunkStore(s3_client=s3_client, bucket=bucket)
    return await store.async_list_recent_volume_ids(site, limit=limit, s3_client=s3_client)


async def async_list_volume_chunks(site: str, volume_id: str, *, s3_client=None, bucket=CHUNKS_BUCKET):
    store = AsyncNexradChunkStore(s3_client=s3_client, bucket=bucket)
    return await store.async_list_volume_chunks(site, volume_id, s3_client=s3_client)


async def async_get_chunk_bytes(chunk_key, *, s3_client=None, bucket=CHUNKS_BUCKET):
    store = AsyncNexradChunkStore(s3_client=s3_client, bucket=bucket)
    return await store.async_get_chunk_bytes(chunk_key, s3_client=s3_client)
