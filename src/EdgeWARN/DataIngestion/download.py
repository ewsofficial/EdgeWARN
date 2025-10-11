import requests
import re
import datetime
from urllib.parse import urljoin
from pathlib import Path
import gzip
import shutil
import os


class FileFinder:
    def __init__(self, dt, base_url, max_time, max_entries):
        self.dt = dt
        self.base_url = base_url.rstrip('/') + '/'  # Store as string
        self.max_time = max_time
        self.max_entries = max_entries

    @staticmethod
    def extract_timestamp_from_filename(filename):
        """
        Extract timestamp from MRMS filename with multiple pattern support.
        Returns timezone-aware datetime object.
        """
        # DEBUG: print(f"DEBUG: Extracting timestamp from filename: {filename}")
        
        patterns = [
            r'MRMS_MergedReflectivityQC_3D_(\d{8})-(\d{6})',
            r'(\d{8})-(\d{6})_renamed',
            r'(\d{8})-(\d{6})',  # Pattern for timestamps like 20250915-230042
            r'(\d{8})_(\d{6})',
            r'.*(\d{8})-(\d{6}).*',
            r"s(\d{4})(\d{3})(\d{2})(\d{2})(\d{2})(\d)"
        ]
        
        for pattern_idx, pattern in enumerate(patterns):
            match = re.search(pattern, filename)
            if match:
                groups = match.groups()
                # DEBUG: print(f"DEBUG: Pattern {pattern_idx+1} matched: {groups}")
                
                if len(groups) == 2:
                    date_str, time_str = groups
                elif len(groups) == 6:
                    # Handle the 6-group pattern: sYYYYDDDHHMMSS
                    year, doy, hour, minute, second, _ = groups
                    # Convert DOY to month and day (simplified)
                    date_obj = datetime.datetime(int(year), 1, 1) + datetime.timedelta(days=int(doy) - 1)
                    date_str = date_obj.strftime('%Y%m%d')
                    time_str = hour + minute + second
                else:
                    # Handle single group pattern like ('20250915-230042',)
                    combined = groups[0]
                    if '-' in combined and len(combined) == 15:
                        date_str, time_str = combined[:8], combined[9:]
                    else:
                        continue
                
                try:
                    # Create timezone-aware datetime object
                    dt_obj = datetime.datetime(
                        year=int(date_str[:4]),
                        month=int(date_str[4:6]),
                        day=int(date_str[6:8]),
                        hour=int(time_str[:2]),
                        minute=int(time_str[2:4]),
                        second=int(time_str[4:6]),
                        tzinfo=datetime.timezone.utc
                    )
                    # DEBUG: print(f"DEBUG: Extracted timestamp: {dt_obj}")
                    return dt_obj
                except (IndexError, ValueError) as e:
                    # DEBUG: print(f"DEBUG: Error formatting timestamp: {e}")
                    continue
        
        # Return timezone-aware fallback
        fallback = datetime.datetime.now(datetime.timezone.utc)
        # DEBUG: print(f"DEBUG: Using fallback timestamp: {fallback}")
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
                print(f"[DataIngestion] DEBUG: Found {len(files)} potential files to process in {url}")
            return files
            
        except requests.RequestException as e:
            print(f"[DataIngestion] ERROR: Could not access {url}: {e}")
            return []

    def lookup_files(self, modifier, verbose=True):
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
            print(f"[DataIngestion] DEBUG: Searching URL: {full_url}")
        
        files = self.list_http_directory(full_url, verbose=verbose)
        
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
    def __init__(self, dt):
        self.dt = dt

    def download_latest(self, files, outdir: Path):
        if not files:
            raise ValueError("ERROR: No files provided")

        # Pick the file closest to self.dt
        latest, ts = min(files, key=lambda x: abs(x[1] - self.dt))

        print(f"[DataIngestion] DEBUG: Latest file: {latest}")

        # Ensure output directory exists
        outdir.mkdir(parents=True, exist_ok=True)

        # Extract just the filename from the URL
        filename = Path(latest).name
        outfile = outdir / filename

        # Skip download if it already exists
        if outfile.exists():
            print(f"[DataIngestion] DEBUG: {outfile} already exists locally")
            return outfile

        # Download the file
        try:
            print(f"[DataIngestion] DEBUG: Downloading file: {filename}")
            response = requests.get(latest, stream=True)
            response.raise_for_status()
            with open(outfile, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"[DataIngestion] DEBUG: Downloaded file: {filename}")
            return outfile
        except Exception as e:
            print(f"[DataIngestion] ERROR: Failed to download {filename}: {e}")
            return
    
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
    
    @staticmethod
    def decompress_file(gz_path: Path) -> Path | None:
        """
        Decompress a .grib2.gz file.
        
        - If the file is inside a timestamp folder, move the .grib2 to the dataset dir 
        and delete the timestamp folder.
        - If the file is directly inside the dataset dir, just decompress in place.
        """
        if not gz_path.exists():
            print(f"[DataIngestion] DEBUG: File does not exist: {gz_path}")
            return None

        if gz_path.suffix != ".gz":
            print(f"[DataIngestion] DEBUG: Not a .gz file: {gz_path}")
            return None

        try:
            # Decompress
            grib_path = gz_path.with_suffix("")  # remove .gz
            with gzip.open(gz_path, "rb") as f_in, open(grib_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            print(f"[DataIngestion] DEBUG: Decompressed: {grib_path}")

            # Determine paths
            parent_dir = gz_path.parent           # where .gz lived
            dataset_dir = parent_dir.parent       # one level up
            
            # Check if parent_dir is a timestamp directory (contains numbers and dashes)
            # If it's a named directory like "mrms_preciprate", don't move the file
            is_timestamp_dir = any(char.isdigit() for char in parent_dir.name) and '-' in parent_dir.name
            
            if is_timestamp_dir:
                # Move decompressed file into dataset dir
                target_path = dataset_dir / grib_path.name
                shutil.move(str(grib_path), target_path)
                print(f"[DataIngestion] DEBUG: Moved file to: {target_path}")

                # Delete timestamp folder if empty
                try:
                    os.rmdir(parent_dir)
                    print(f"[DataIngestion] DEBUG: Deleted folder: {parent_dir}")
                except OSError:
                    print(f"[DataIngestion] ERROR: Could not delete {parent_dir} (not empty)")
            else:
                # File is already in dataset directory, keep it there
                target_path = grib_path
                print(f"[DataIngestion] DEBUG: File is already in dataset dir: {target_path}")

            # Delete original .gz
            gz_path.unlink(missing_ok=True)

            return target_path

        except Exception as e:
            print(f"[DataIngestion] ERROR: Unable to decompress {gz_path}: {e}")
            return None
    