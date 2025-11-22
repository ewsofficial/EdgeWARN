import boto3
from botocore import UNSIGNED
from botocore.client import Config
from pathlib import Path
import os
import gzip
import shutil
from EdgeWARN.core.ingest.utils import extract_timestamp

class FileFinder:
    def __init__(self, dt, bucket, max_entries, io_manager):
        self.dt = dt
        self.bucket = bucket
        self.max_entries = max_entries  # Maximum number of entries to return
        self.io_manager = io_manager # Use the IOManager class in util.io
        self.client = boto3.client(
            's3',
            config=Config(signature_version=UNSIGNED)
        )
    
    def lookup_files(self, modifier, verbose=False):
        """
        Look up latest S3 files and return as list of (path, datetime_obj) tuples.
        
        Args:
            modifier (str | list[str]): Specify which part(s) of the bucket to search (e.g., folder prefix).
                                      Can be a single string or a list of strings to search sequentially.
            verbose (bool): Whether to print debug information
        
        Uses S3 client and instance variables to find and filter files.
        Returns files sorted by timestamp in descending order (latest first).
        
        Returns:
            list: List of tuples (s3_path, datetime_obj) sorted by latest timestamp first
        """
        try:
            # Normalize modifier to list
            modifiers = [modifier] if isinstance(modifier, str) else modifier
            
            files_data = []
            
            for prefix in modifiers:
                # List objects in S3 bucket with pagination
                paginator = self.client.get_paginator('list_objects_v2')
                
                # Set up prefix filter for bucket search
                search_prefix = prefix if prefix else ""
                
                # Iterate through all pages of results
                for page in paginator.paginate(Bucket=self.bucket, Prefix=search_prefix):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            s3_path = obj['Key']
                            
                            try:
                                # Extract timestamp from S3 path
                                timestamp = extract_timestamp(s3_path)
                                
                                files_data.append((s3_path, timestamp))
                            except Exception:
                                # Skip files that don't have valid timestamps
                                continue
                
                # Optimization: If we have enough files, stop searching subsequent modifiers
                # We only check this after finishing a prefix to ensure we get all files from that prefix
                # (e.g. all files from the current hour) before deciding if we need more.
                if len(files_data) >= self.max_entries:
                    break
            
            # Sort by timestamp (latest first)
            files_data.sort(key=lambda x: x[1], reverse=True)
            
            # Limit to max_entries
            return files_data[:self.max_entries]
            
        except Exception as e:
            # Log error and return empty list
            self.io_manager.write_error(f"Error looking up files: {e}")
            return []

class FileDownloader:
    def __init__(self, dt, bucket, io_manager):
        self.dt = dt
        self.bucket = bucket
        self.io_manager = io_manager # IOManager class from util.io
        self.client = boto3.client(
            's3',
            config=Config(signature_version=UNSIGNED)
        )
    
    def download_latest(self, file_list, outdir: Path):
        """
        Download the latest file from the file list to the specified output directory.
        
        Args:
            file_list (list): List of tuples (s3_path, datetime_obj) from FileFinder.lookup_files()
            outdir (Path): Output directory path where the file will be downloaded
            
        Returns:
            Path: Path to the downloaded file, or None if download failed
        """
        if not file_list:
            self.io_manager.write_warning("No files to download from empty file_list")
            return None
        
        try:
            # Get the latest file (first item in sorted list)
            latest_file_path, latest_timestamp = file_list[0]
            
            # Create output directory if it doesn't exist
            outdir = Path(outdir)
            outdir.mkdir(parents=True, exist_ok=True)
            
            # Extract filename from S3 path
            filename = os.path.basename(latest_file_path)
            local_path = outdir / filename
            
            # Check if file already exists (both zipped and unzipped versions)
            zipped_path = local_path
            unzipped_path = local_path.with_suffix("") if local_path.suffix == ".gz" else local_path
            
            if zipped_path.exists() or unzipped_path.exists():
                existing_file = str(zipped_path) if zipped_path.exists() else str(unzipped_path)
                self.io_manager.write_debug(f"File already exists, skipping download: {existing_file}")
                return zipped_path if zipped_path.exists() else unzipped_path

            # Log the download attempt
            self.io_manager.write_debug(f"Downloading latest file: {latest_file_path}")
            
            # Use the bucket from constructor and the file path as S3 key
            s3_key = latest_file_path
            
            # Download the file from S3
            self.client.download_file(self.bucket, s3_key, str(local_path))
            
            self.io_manager.write_debug(f"Successfully downloaded: {filename}")
            return Path(str(local_path))
            
        except Exception as e:
            self.io_manager.write_error(f"Error downloading latest file from {self.bucket}: {e}")
            return None

    def download_matching(self, file_list, outdir: Path):
        """
        Download the file that matches the target datetime.
        
        Args:
            file_list (list): List of tuples (s3_path, datetime_obj) from FileFinder.lookup_files()
            outdir (Path): Output directory path where the file will be downloaded
            
        Returns:
            Path: Path to the downloaded file, or None if download failed
        """
        if not file_list:
            self.io_manager.write_warning("No files to download from empty file_list")
            return None
        
        try:
            # Find file with matching timestamp
            target_file_path = None
            
            # Target minute (ignore seconds/microseconds for matching)
            target_minute = self.dt.replace(second=0, microsecond=0)
            
            for s3_path, ts in file_list:
                # Compare down to the minute
                file_minute = ts.replace(second=0, microsecond=0)
                
                if file_minute == target_minute:
                    target_file_path = s3_path
                    break
            
            if not target_file_path:
                self.io_manager.write_warning(f"No file found matching timestamp {target_minute}. Falling back to latest available.")
                # Fallback to the latest file (first in the list)
                target_file_path, _ = file_list[0]
            
            # Create output directory if it doesn't exist
            outdir = Path(outdir)
            outdir.mkdir(parents=True, exist_ok=True)
            
            # Extract filename from S3 path
            filename = os.path.basename(target_file_path)
            local_path = outdir / filename
            
            # Check if file already exists (both zipped and unzipped versions)
            zipped_path = local_path
            unzipped_path = local_path.with_suffix("") if local_path.suffix == ".gz" else local_path
            
            if zipped_path.exists() or unzipped_path.exists():
                existing_file = str(zipped_path) if zipped_path.exists() else str(unzipped_path)
                self.io_manager.write_debug(f"File already exists, skipping download: {existing_file}")
                return zipped_path if zipped_path.exists() else unzipped_path

            # Log the download attempt
            self.io_manager.write_debug(f"Downloading matching file: {target_file_path}")
            
            # Use the bucket from constructor and the file path as S3 key
            s3_key = target_file_path
            
            # Download the file from S3
            self.client.download_file(self.bucket, s3_key, str(local_path))
            
            self.io_manager.write_debug(f"Successfully downloaded: {filename}")
            return Path(str(local_path))
            
        except Exception as e:
            self.io_manager.write_error(f"Error downloading matching file from {self.bucket}: {e}")
            return None

    def decompress_file(self, gz_path: Path) -> Path | None:
        """
        Decompress a .gz file into its parent directory and delete the original .gz.
        """
        if not gz_path.exists():
            self.io_manager.write_error(f"File does not exist: {gz_path}")
            return None

        if gz_path.suffix != ".gz":
            self.io_manager.write_warning(f"Not a .gz file: {gz_path}")
            return None

        try:
            # Decompressed file path (remove .gz)
            output_path = gz_path.with_suffix("")

            # Decompress into the same parent directory
            with gzip.open(gz_path, "rb") as f_in, open(output_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            self.io_manager.write_debug(f"Decompressed to: {output_path}")

            # Remove original gz file
            gz_path.unlink(missing_ok=True)

            return output_path
        
        except Exception as e:
            self.io_manager.write_error(f"Unable to decompress {gz_path}: {e}")
            return None
