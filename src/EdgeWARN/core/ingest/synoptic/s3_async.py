import os
import asyncio
from pathlib import Path
import aiofiles
import aioboto3
from botocore import UNSIGNED
from botocore.client import Config
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
            Path: The path to the downloaded file, or None if failed.
        """
        try:
            # Check if file already exists
            if local_path.exists():
                self.io_manager.write_debug(f"File already exists on disk, skipping: {local_path}")
                return local_path

            # Ensure parent directory exists
            local_path.parent.mkdir(parents=True, exist_ok=True)

            self.io_manager.write_info(f"Downloading synoptic file (Async): s3://{self.bucket}/{s3_key}")
            
            # Download using async S3 client
            resp = await self.s3.get_object(Bucket=self.bucket, Key=s3_key)
            body = resp["Body"]

            async with aiofiles.open(local_path, "wb") as f:
                async for chunk in body.iter_chunks():
                    await f.write(chunk)

            self.io_manager.write_info(f"Successfully downloaded: {local_path.name}")
            return local_path
            
        except Exception as e:
            self.io_manager.write_error(f"Async error downloading synoptic file from {self.bucket}: {e}")
            return None
