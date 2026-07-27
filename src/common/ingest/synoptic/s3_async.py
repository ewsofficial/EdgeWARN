from pathlib import Path
import os

import aiofiles

from util.io import IOManager


class AsyncSynopticFileDownloader:
    """Asynchronous S3 downloader for Synoptic files (e.g., RAP) using aioboto3."""
    
    def __init__(self, bucket, io_manager: IOManager = None, s3_client=None):
        self.bucket = bucket
        self.io_manager = io_manager if io_manager else IOManager("[DataIngestion]")
        self.s3 = s3_client # Shared S3 client is injected for performance

    async def async_download_file(self, s3_key, local_path: Path):
        """
        Download a file from S3 asynchronously if it doesn't already exist.
        
        Args:
            s3_key (str): The S3 key/path to download.
            local_path (Path): The local path where the file should be saved.
            
        Returns:
            Path: The path to the downloaded file.

        Raises:
            FileNotFoundError: The S3 object does not exist.
            Exception: The underlying S3 or local I/O operation failed.
        """
        try:
            # Check if file already exists
            if local_path.exists():
                self.io_manager.write_debug(f"File already exists on disk, skipping: {local_path.name}")
                return local_path

            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Download using async S3 client
            resp = await self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            body = resp["Body"]

            part_path = local_path.with_name(f".{local_path.name}.part")
            written = 0
            try:
                async with aiofiles.open(part_path, "wb") as f:
                    async for chunk in body.iter_chunks():
                        if chunk:
                            written += len(chunk)
                            await f.write(chunk)
                    await f.flush()
                expected = resp.get("ContentLength")
                if expected is not None and written != expected:
                    raise IOError(f"incomplete S3 download: expected {expected} bytes, got {written}")
                if written == 0:
                    raise IOError("empty S3 download")
                os.replace(part_path, local_path)
            except BaseException:
                part_path.unlink(missing_ok=True)
                raise

            self.io_manager.write_info(f"Successfully downloaded: {local_path.name}")
            return local_path
            
        except Exception as e:
            err_msg = str(e)
            if "404" in err_msg or "NoSuchKey" in err_msg:
                raise FileNotFoundError(f"s3://{self.bucket}/{s3_key}") from e
            self.io_manager.write_error(f"Async error downloading synoptic file from {self.bucket}: {e}")
            raise
