import requests
import re
import datetime
from urllib.parse import urljoin
from util.io import IOManager
from pathlib import Path
import gzip
import shutil
import os

class FileFinder:
    def __init__(self, dt, base_url, max_time, max_entries, io_manager):
        self.dt = dt
        self.base_url = base_url.rstrip('/') + '/'  # Store as string
        self.max_time = max_time
        self.max_entries = max_entries
        self.io_manager = io_manager

    @staticmethod
    def extract_timestamp_from_filename(filename):
        """
        Extract timestamp from MRMS filename with multiple pattern support.
        Returns timezone-aware datetime object rounded DOWN to minute precision.
        """
        patterns = [
            r'MRMS_MergedReflectivityQC_3D_(\d{8})-(\d{6})',
            r'(\d{8})-(\d{6})_renamed',
            r'(\d{8})-(\d{6})',
            r'(\d{8})_(\d{6})',
            r'.*(\d{8})-(\d{6}).*',
            r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)"
        ]
        
        for pattern_idx, pattern in enumerate(patterns):
            match = re.search(pattern, filename)
            if match:
                groups = match.groups()
                
                if len(groups) == 2:
                    date_str, time_str = groups
                elif len(groups) == 6:
                    year, doy, hour, minute, second, _ = groups
                    date_obj = datetime.datetime(int(year), 1, 1) + datetime.timedelta(days=int(doy) - 1)
                    date_str = date_obj.strftime('%Y%m%d')
                    time_str = hour + minute + second
                else:
                    combined = groups[0]
                    if '-' in combined and len(combined) == 15:
                        date_str, time_str = combined[:8], combined[9:]
                    else:
                        continue
                
                try:
                    # Create timezone-aware datetime object with full precision
                    dt_obj = datetime.datetime(
                        year=int(date_str[:4]),
                        month=int(date_str[4:6]),
                        day=int(date_str[6:8]),
                        hour=int(time_str[:2]),
                        minute=int(time_str[2:4]),
                        second=int(time_str[4:6]),
                        tzinfo=datetime.timezone.utc
                    )
                    # ROUND DOWN TO MINUTE PRECISION (truncate seconds/microseconds)
                    dt_obj = dt_obj.replace(second=0, microsecond=0)
                    return dt_obj
                except (IndexError, ValueError) as e:
                    continue
        
        # Return timezone-aware fallback rounded down to minute
        fallback = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)
        return fallback

    def list_http_directory(self, url, verbose=True):
        """List files in an HTTP directory by parsing HTML response."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            files = []
            for line in response.text.split('\n'):
                if 'href="' in line:
                    match = re.search(r'href="([^"]+)"', line)
                    if match:
                        filename = match.group(1)
                        if (filename.endswith('/') or 
                            '?' in filename or 
                            '=' in filename or 
                            'latest' in filename.lower() or
                            filename in ['../', 'Parent Directory/'] or
                            filename.startswith('?')):
                            continue
                        if (filename.endswith('.gz') or 
                            filename.endswith('.grib2') or 
                            filename.endswith('.nc') or
                            filename.endswith('.json') or
                            re.search(r'\d{8}-\d{6}', filename)):
                            files.append(filename)
            if verbose:
                self.io_manager.write_debug(f"Found {len(files)} potential files to process in {url}")
            return files
            
        except requests.RequestException as e:
            print(f"[DataIngestion] ERROR: Could not access {url}: {e}")
            return []

    def lookup_files(self, modifier, verbose=False):
        """
        Attempts file lookup for files matching the modifier pattern in HTTP directory.
        Returns list of (file_url, timestamp) tuples.
        """
        matching_files = []
        
        if self.dt.tzinfo is None:
            self.dt = self.dt.replace(tzinfo=datetime.timezone.utc)
        
        if isinstance(self.max_time, datetime.timedelta):
            max_time_cutoff = self.dt - self.max_time
            if max_time_cutoff.tzinfo is None:
                max_time_cutoff = max_time_cutoff.replace(tzinfo=datetime.timezone.utc)
        else:
            max_time_cutoff = self.max_time
            if max_time_cutoff.tzinfo is None:
                max_time_cutoff = max_time_cutoff.replace(tzinfo=datetime.timezone.utc)
        
        full_url = urljoin(self.base_url, modifier)
        if not full_url.endswith('/'):
            full_url += '/'
        
        if verbose:
            self.io_manager.write_debug(f"Searching URL: {full_url}")
        
        files = self.list_http_directory(full_url, verbose=False)
        
        for filename in files:
            if 'latest' in filename.lower():
                continue
            timestamp = self.extract_timestamp_from_filename(filename)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
            if timestamp >= max_time_cutoff and timestamp <= self.dt:
                file_url = urljoin(full_url, filename)
                matching_files.append((file_url, timestamp))
        
        matching_files.sort(key=lambda x: x[1], reverse=True)
        
        if self.max_entries:
            return matching_files[:self.max_entries]
        return matching_files
    
class FileDownloader:
    def __init__(self, dt, io_manager):
        self.dt = dt
        self.io_manager = io_manager

    def download_latest(self, files, outdir: Path):
        """
        Download the dataset matching the exact timestamp (self.dt),
        or fallback to the most recent available file if an exact match isn't found.

        Args:
            files (list[tuple[str, datetime]]): List of (url, timestamp) pairs.
            outdir (Path): Directory to save the downloaded file.
        """
        if not files:
            raise ValueError("ERROR: No files provided")

        # Find exact timestamp match
        matched = [(url, ts) for url, ts in files if ts == self.dt]

        # If no exact match, fallback to the most recent available timestamp
        if not matched:
            latest_file = max(files, key=lambda x: x[1], default=None)
            if not latest_file:
                self.io_manager.write_error(f"No files available for fallback")
                return None
            self.io_manager.write_warning(f"No exact match for {self.dt:%Y-%m-%d %H:%M:%S %Z}. "
                f"Using latest available dataset instead ({latest_file[1]:%Y-%m-%d %H:%M:%S %Z})")
            matched = [latest_file]

        # Take first match
        latest, ts = matched[0]

        # Ensure output directory exists
        outdir.mkdir(parents=True, exist_ok=True)

        # Extract just the filename
        filename = Path(latest).name
        outfile = outdir / filename

        # Skip if already downloaded
        if outfile.exists():
            self.io_manager.write_debug(f"{outfile} already exists locally")
            return outfile

        # Download file
        try:
            self.io_manager.write_debug(f"Downloading file: {filename}")
            response = requests.get(latest, stream=True, timeout=30)
            response.raise_for_status()
            with open(outfile, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            self.io_manager.write_debug(f"Downloaded file successfully -> {outfile}")
            return outfile
        except Exception as e:
            self.io_manager.write_error(f"Failed to download {filename}: {e}")
            return None
    
    def download_specific(self, files, n: int, outdir: Path):
        """
        Download the nth file from the files list.
        
        Args:
            files: List of (file_url, timestamp) tuples
            n: Index of the file to download (0-based)
            outdir: Output directory path
            
        Returns:
            Path to the downloaded file
            
        Raises:
            ValueError: If no files provided or invalid index
        """
        if not files:
            raise ValueError("[DataIngestion] ERROR: No files provided")
        
        if n < 0 or n >= len(files):
            raise ValueError(f"[DataIngestion] ERROR: Invalid index {n}. Must be between 0 and {len(files) - 1}")
        
        # Get the nth file
        file_url, timestamp = files[n]
        
        # Ensure output directory exists
        outdir.mkdir(parents=True, exist_ok=True)

        # Extract just the filename from the URL
        filename = Path(file_url).name
        outfile = outdir / filename

        # Download the file
        response = requests.get(file_url, stream=True)
        response.raise_for_status()
        with open(outfile, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        return outfile
    
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