from pathlib import Path
import os

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from util.io import IOManager


class SynopticFileDownloader:
    """Synchronous S3 downloader for Synoptic files (e.g., RAP)."""
    
    def __init__(self, bucket, io_manager: IOManager = None):
        self.bucket = bucket
        self.io_manager = io_manager if io_manager else IOManager("[DataIngestion]")
        self.client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    def download_file(self, s3_key, local_path: Path):
        """
        Download a file from S3 if it doesn't already exist.
        
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

            part_path = local_path.with_name(f".{local_path.name}.part")
            try:
                self.client.download_file(self.bucket, s3_key, str(part_path))
                if not part_path.is_file() or part_path.stat().st_size == 0:
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
            self.io_manager.write_error(f"Error downloading synoptic file from {self.bucket}: {e}")
            raise
