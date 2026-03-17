import heapq
import re
from pathlib import Path
import os
import gzip
import shutil
import asyncio
import aiofiles
import aiofiles.os
from datetime import timedelta
from EWMRS.util.handler import extract_timestamp
from EWMRS.ingest.mrms.timestamp_utils import round_to_nearest_even_minute


_DECOMPRESS_CHUNK_SIZE = 1024 * 1024  # 1MB chunks to reduce syscall overhead during gzip copy

class AsyncFileFinder:
    """Async version of FileFinder using aioboto3 for non-blocking S3 operations"""

    __slots__ = ("dt", "bucket", "max_entries", "io_manager", "s3", "paginator")

    def __init__(self, dt, bucket, max_entries, io_manager, s3_client=None):
        self.dt = dt
        self.bucket = bucket
        self.max_entries = max_entries
        self.io_manager = io_manager
        self.s3 = s3_client  # Shared S3 client is injected for performance
        self.paginator = self.s3.get_paginator("list_objects_v2")

    async def async_lookup_files(self, prefix):
        """Async version of file lookup with non-blocking S3 operations"""
        try:
            # Normalize prefix to list
            prefixes = [prefix] if isinstance(prefix, str) else prefix

            top_files = []

            push = heapq.heappush
            replace = heapq.heapreplace
            max_entries = self.max_entries

            for search_prefix in prefixes:
                # Handle None/empty prefix
                p = search_prefix if search_prefix else ""

                async for page in self.paginator.paginate(Bucket=self.bucket, Prefix=p):
                    if "Contents" not in page:
                        continue
                    for obj in page["Contents"]:
                        s3_path = obj["Key"]
                        try:
                            ts = extract_timestamp(s3_path, use_timezone_utc=True, round_to_minute=True, isoformat=False)
                            if ts > self.dt:
                                continue
                        except Exception:
                            continue

                        entry = (ts, s3_path)

                        if len(top_files) < max_entries:
                            push(top_files, entry)
                        elif entry[0] > top_files[0][0]:
                            replace(top_files, entry)

                # Optimization: Stop if we have enough files
                if len(top_files) >= self.max_entries:
                    break

            top_files.sort(key=lambda x: x[0], reverse=True)
            return [(path, ts) for ts, path in top_files[:self.max_entries]]

        except Exception as e:
            self.io_manager.write_error(f"Error in async lookup: {e}")
            return []


class AsyncFileDownloader:
    """Async version of FileDownloader using aioboto3 and aiofiles for non-blocking operations"""

    __slots__ = (
        "dt",
        "bucket",
        "io_manager",
        "s3",
        "target_minute",
        "target_key",
    )

    def __init__(self, dt, bucket, io_manager, s3_client=None):
        self.dt = dt
        self.bucket = bucket
        self.io_manager = io_manager
        self.s3 = s3_client
        self.target_minute = dt.replace(second=0, microsecond=0)
        self.target_key = (dt.year, dt.month, dt.day, dt.hour, dt.minute)

    def _select_target_file(self, file_list, context: str):
        """
        Select the file that best matches the target datetime.
        
        Uses round_to_nearest_even_minute for matching, with debug logging
        when a non-exact match is selected.
        """
        target_rounded = round_to_nearest_even_minute(self.dt)
        target_key = (target_rounded.year, target_rounded.month, target_rounded.day, 
                      target_rounded.hour, target_rounded.minute)
        
        for s3_path, ts in file_list:
            ts_rounded = round_to_nearest_even_minute(ts)
            ts_key = (ts_rounded.year, ts_rounded.month, ts_rounded.day,
                      ts_rounded.hour, ts_rounded.minute)
            
            if ts_key == target_key:
                # Log if not an exact match (rounding was applied)
                if ts.minute != target_rounded.minute or ts.hour != target_rounded.hour:
                    self.io_manager.write_debug(
                        f"Rounded match: {ts.strftime('%H:%M:%S')} → {target_rounded.strftime('%H:%M')} for {context}"
                    )
                return s3_path

        self.io_manager.write_warning(
            f"No file found matching timestamp {target_rounded} for {context}. Falling back to latest available."
        )
        # Fallback to the latest file (first in the list)
        return file_list[0][0]

    async def async_download_matching(self, file_list, outdir: Path):
        """
        Download the file that matches the target datetime.
        
        Args:
            file_list: List of (s3_path, timestamp) tuples
            outdir: Output directory
            
        Returns:
            Path to downloaded file or None if no match found
        """
        if not file_list:
            self.io_manager.write_warning("No files to download")
            return None

        try:
            # Find file with matching timestamp
            target_file_path = self._select_target_file(file_list, outdir)

            outdir.mkdir(parents=True, exist_ok=True)
            filename = os.path.basename(target_file_path)
            local_path = outdir / filename

            # Check if file already exists (both zipped and unzipped versions)
            zipped_path = local_path
            unzipped_path = local_path.with_suffix("") if local_path.suffix == ".gz" else local_path
            if zipped_path.exists() or unzipped_path.exists():
                existing_file = zipped_path if zipped_path.exists() else unzipped_path
                self.io_manager.write_debug(f"File already exists, skipping: {existing_file}")
                return existing_file

            self.io_manager.write_info(f"Downloading matching file: {target_file_path}")

            # Download using async S3 client
            resp = await self.s3.get_object(Bucket=self.bucket, Key=target_file_path)
            body = resp["Body"]

            async with aiofiles.open(local_path, "wb") as f:
                async for chunk in body.iter_chunks():
                    await f.write(chunk)

            self.io_manager.write_info(f"Successfully downloaded: {filename}")
            return local_path

        except Exception as e:
            self.io_manager.write_error(f"Async download error: {e}")
            return None

    async def async_download_all_matching(self, file_list, outdir: Path):
        """
        Async version: Download all files that match the target datetime minute (sliding window).
        
        Args:
            file_list: List of (s3_path, timestamp) tuples
            outdir: Output directory
            
        Returns:
            list[Path]: List of paths to downloaded files
        """
        if not file_list:
            self.io_manager.write_warning("No files to download")
            return []

        downloaded_files = []

        try:
            # Sliding window logic:
            # Target window is (dt - 1 minute, dt]
            window_end = self.dt
            window_start = window_end - timedelta(minutes=1)
            
            matching_files = [
                s3_path for s3_path, ts in file_list 
                if window_start < ts <= window_end
            ]
            
            if not matching_files:
                self.io_manager.write_warning(f"No files found matching window {window_start} to {window_end}.")
                return []

            outdir.mkdir(parents=True, exist_ok=True)
            
            for target_file_path in matching_files:
                filename = os.path.basename(target_file_path)
                local_path = outdir / filename

                # Check if file already exists (both zipped and unzipped versions)
                zipped_path = local_path
                unzipped_path = local_path.with_suffix("") if local_path.suffix == ".gz" else local_path
                if zipped_path.exists() or unzipped_path.exists():
                    existing_file = zipped_path if zipped_path.exists() else unzipped_path
                    self.io_manager.write_debug(f"File already exists, skipping: {existing_file}")
                    downloaded_files.append(existing_file)
                    continue

                self.io_manager.write_info(f"Downloading matching file: {target_file_path}")

                # Download using async S3 client
                resp = await self.s3.get_object(Bucket=self.bucket, Key=target_file_path)
                body = resp["Body"]

                async with aiofiles.open(local_path, "wb") as f:
                    async for chunk in body.iter_chunks():
                        await f.write(chunk)

                self.io_manager.write_info(f"Successfully downloaded: {filename}")
                downloaded_files.append(local_path)
            
            return downloaded_files

        except Exception as e:
            self.io_manager.write_error(f"Async download error: {e}")
            return downloaded_files

    async def async_decompress_file(self, gz_path: Path):
        """Async decompression using thread pool for CPU-bound gzip operation"""
        if not gz_path.exists():
            return None

        if gz_path.suffix != ".gz":
            return gz_path

        output_path = gz_path.with_suffix("")

        if output_path.exists():
            self.io_manager.write_debug(f"Decompressed target already exists, skipping: {output_path}")
            return output_path

        try:
            # Offload synchronous gzip to a worker thread (fast, avoids blocking event loop)
            def _sync_decompress():
                with gzip.open(gz_path, "rb") as f_in, open(output_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out, length=_DECOMPRESS_CHUNK_SIZE)

            await asyncio.to_thread(_sync_decompress)

            await aiofiles.os.remove(gz_path)
            self.io_manager.write_info(f"Decompressed to: {output_path}")
            return output_path

        except Exception as e:
            self.io_manager.write_error(f"Gzip decompress failed: {e}")
            return None
