import re
from pathlib import Path
import os
import gzip
import shutil
import asyncio
import aiofiles
import aiofiles.os
from EdgeWARN.core.ingest.utils import extract_timestamp


class AsyncFileFinder:
    """Async version of FileFinder using aioboto3 for non-blocking S3 operations"""
    
    def __init__(self, dt, bucket, max_entries, io_manager, s3_client=None):
        self.dt = dt
        self.bucket = bucket
        self.max_entries = max_entries
        self.io_manager = io_manager
        self.s3 = s3_client  # Shared S3 client is injected for performance

    async def async_lookup_files(self, prefix):
        """Async version of file lookup with non-blocking S3 operations"""
        try:
            # Normalize prefix to list
            prefixes = [prefix] if isinstance(prefix, str) else prefix

            paginator = self.s3.get_paginator("list_objects_v2")

            files = []

            for search_prefix in prefixes:
                # Handle None/empty prefix
                p = search_prefix if search_prefix else ""
                
                async for page in paginator.paginate(Bucket=self.bucket, Prefix=p):
                    if "Contents" not in page:
                        continue
                    for obj in page["Contents"]:
                        s3_path = obj["Key"]
                        ts = extract_timestamp(s3_path)
                        files.append((s3_path, ts))
                
                # Optimization: Stop if we have enough files
                if len(files) >= self.max_entries:
                    break

            files.sort(key=lambda x: x[1], reverse=True)
            return files[:self.max_entries]

        except Exception as e:
            self.io_manager.write_error(f"Error in async lookup: {e}")
            return []


class AsyncFileDownloader:
    """Async version of FileDownloader using aioboto3 and aiofiles for non-blocking operations"""
    
    def __init__(self, dt, bucket, io_manager, s3_client=None):
        self.dt = dt
        self.bucket = bucket
        self.io_manager = io_manager
        self.s3 = s3_client

    async def async_download_latest(self, file_list, outdir: Path):
        """Download the latest file asynchronously"""
        if not file_list:
            self.io_manager.write_warning("No files to download")
            return None

        try:
            # Get the latest file (first item in sorted list)
            latest_file_path, ts = file_list[0]

            outdir.mkdir(parents=True, exist_ok=True)
            filename = os.path.basename(latest_file_path)
            local_path = outdir / filename

            # Check if file already exists
            if local_path.exists():
                self.io_manager.write_debug(f"File already exists, skipping: {filename}")
                return local_path

            self.io_manager.write_debug(f"Downloading: {latest_file_path}")

            # Download using async S3 client
            resp = await self.s3.get_object(Bucket=self.bucket, Key=latest_file_path)
            body = resp["Body"]

            async with aiofiles.open(local_path, "wb") as f:
                async for chunk in body.iter_chunks():
                    await f.write(chunk)

            self.io_manager.write_debug(f"Successfully downloaded: {filename}")
            return local_path

        except Exception as e:
            self.io_manager.write_error(f"Async download error: {e}")
            return None

    async def async_decompress_file(self, gz_path: Path):
        """Async decompression using thread pool for CPU-bound gzip operation"""
        if not gz_path.exists():
            return None

        if gz_path.suffix != ".gz":
            return gz_path

        output_path = gz_path.with_suffix("")

        try:
            # Offload synchronous gzip to a worker thread (fast, avoids blocking event loop)
            loop = asyncio.get_running_loop()

            def _sync_decompress():
                with gzip.open(gz_path, "rb") as f_in, open(output_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            await loop.run_in_executor(None, _sync_decompress)

            await aiofiles.os.remove(gz_path)
            self.io_manager.write_debug(f"Decompressed to: {output_path}")
            return output_path

        except Exception as e:
            self.io_manager.write_error(f"Gzip decompress failed: {e}")
            return None
