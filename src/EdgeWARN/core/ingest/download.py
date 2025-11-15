import boto3
import re
from datetime import datetime, timedelta, timezone

class FileFinder:
    def __init__(self, dt, bucket, max_time, max_entries, io_manager):
        self.dt = dt
        self.bucket = bucket
        self.max_time = max_time  # Time delta (seconds) to go back from current time
        self.max_entries = max_entries  # Maximum number of entries to return
        self.io_manager = io_manager # Use the IOManager class in util.io
        self.client = boto3.client('s3')
    
    @staticmethod
    def extract_timestamp(filepath):
        """
        Extract timestamp from filepath and return timezone-aware datetime object.
        
        Searches for YYYYMMDD-HHMMSS or YYYYMMDD_HHMMSS patterns in the input filepath.
        Returns a default timestamp if no pattern is found.
        
        Args:
            filepath (str): The filename/filepath string to search for timestamp
            
        Returns:
            datetime: A timezone-aware datetime object (UTC)
        """
        # Pattern for YYYYMMDD-HHMMSS or YYYYMMDD_HHMMSS
        pattern = r'(\d{4})(\d{2})(\d{2})[-_](\d{2})(\d{2})(\d{2})'
        
        match = re.search(pattern, filepath)
        if not match:
            # Return current time in UTC as default
            return datetime.now(timezone.utc).replace(second=0, microsecond=0)
        
        year, month, day, hour, minute, second = map(int, match.groups())
        
        # Create timezone-aware datetime object directly (UTC)
        dt_aware = datetime(year, month, day, hour, minute, 0, 0, tzinfo=timezone.utc)
        
        return dt_aware
    
    def lookup_files(self, modifier):
        """
        Look up latest S3 files and return as list of (path, datetime_obj) tuples.
        
        Args:
            modifier (str): Specify which part of the bucket to search (e.g., folder prefix)
        
        Uses S3 client and instance variables to find and filter files.
        Returns files sorted by timestamp in descending order (latest first).
        
        Returns:
            list: List of tuples (s3_path, datetime_obj) sorted by latest timestamp first
        """
        try:
            # Calculate time cutoff (max_time seconds ago from current time)
            current_time = datetime.now(timezone.utc)
            if self.max_time is not None:
                time_cutoff = current_time - timedelta(seconds=self.max_time)
            else:
                time_cutoff = None
            
            # List objects in S3 bucket with pagination
            paginator = self.client.get_paginator('list_objects_v2')
            
            # Set up prefix filter for bucket search
            if modifier:
                prefix = modifier
            else:
                prefix = ""
            
            files_data = []
            
            # Iterate through all pages of results
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        s3_path = obj['Key']
                        
                        try:
                            # Extract timestamp from S3 path
                            timestamp = self.extract_timestamp(s3_path)
                            
                            # Apply time filter if max_time is set
                            if time_cutoff is not None and timestamp < time_cutoff:
                                continue
                            
                            files_data.append((s3_path, timestamp))
                        except Exception:
                            # Skip files that don't have valid timestamps
                            continue
            
            # Sort by timestamp (latest first)
            files_data.sort(key=lambda x: x[1], reverse=True)
            
            # Limit to max_entries
            return files_data[:self.max_entries]
            
        except Exception as e:
            # Log error and return empty list
            self.io_manager.write_error(f"Error looking up files: {e}")
            return []

class FileDownloader:
    def __init__(self, dt, io_manager):
        self.dt = dt
        self.io_manager = io_manager # IOManager class from util.io
        self.client = boto3.client('s3')
    
    def download_latest(self, file_list):
        pass
        
if __name__ == "__main__":
    # Test staging area (Currently for timestamp verification)
    filename = rf"C:\EdgeWARN_input\data\CompRefQC\MRMS_MergedReflectivityQC_00.50_20251112-135644.grib2.gz"
    ts = FileFinder.extract_timestamp(filename)
