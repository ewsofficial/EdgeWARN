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
    def _extract_timestamp_from_filename(filename):
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

    def list_http_directory(self, url):
        """List files in an HTTP directory by parsing HTML response."""
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            # Simple HTML parsing to extract links with better filtering
            files = []
            for line in response.text.split('\n'):
                if 'href="' in line:
                    match = re.search(r'href="([^"]+)"', line)
                    if match:
                        filename = match.group(1)
                        # Skip directories, query parameters, navigation links, and "latest" files
                        if (filename.endswith('/') or 
                            '?' in filename or 
                            '=' in filename or 
                            'latest' in filename.lower() or  # Exclude files with "latest"
                            filename in ['../', 'Parent Directory/'] or
                            filename.startswith('?')):
                            continue
                        # Only include files that look like actual data files
                        if (filename.endswith('.gz') or 
                            filename.endswith('.grib2') or 
                            filename.endswith('.nc') or
                            filename.endswith('.json') or
                            re.search(r'\d{8}-\d{6}', filename)):
                            files.append(filename)
            return files
            
        except requests.RequestException as e:
            print(f"Error accessing {url}: {e}")
            return []

    def lookup_files(self, modifier):
        """
        Attempts file lookup for files matching the modifier pattern in HTTP directory.
        Returns list of (file_url, timestamp) tuples.
        """
        matching_files = []
        
        # Ensure self.dt is timezone-aware
        if self.dt.tzinfo is None:
            self.dt = self.dt.replace(tzinfo=datetime.timezone.utc)
        
        # Convert max_time to datetime if it's a timedelta
        if isinstance(self.max_time, datetime.timedelta):
            max_time_cutoff = self.dt - self.max_time
            # Ensure max_time_cutoff is timezone-aware
            if max_time_cutoff.tzinfo is None:
                max_time_cutoff = max_time_cutoff.replace(tzinfo=datetime.timezone.utc)
        else:
            # Ensure max_time is also timezone-aware if it's a datetime
            max_time_cutoff = self.max_time
            if max_time_cutoff.tzinfo is None:
                max_time_cutoff = max_time_cutoff.replace(tzinfo=datetime.timezone.utc)
        
        # Build the full URL
        full_url = urljoin(self.base_url, modifier)
        if not full_url.endswith('/'):
            full_url += '/'
        
        print(f"Searching URL: {full_url}")
        
        # List files in the HTTP directory
        files = self.list_http_directory(full_url)
        print(f"Found {len(files)} potential files to process")
        
        for filename in files:
            # Skip any files with "latest" (double safety check)
            if 'latest' in filename.lower():
                # DEBUG: print(f"Skipping file with 'latest': {filename}")
                continue
                
            # DEBUG: print(f"Processing file: {filename}")
            # Process each file
            timestamp = self._extract_timestamp_from_filename(filename)
            
            # Ensure timestamp is timezone-aware for comparison
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
            
            # DEBUG: print(f"File timestamp: {timestamp}")
            # DEBUG: print(f"Time range: {max_time_cutoff} to {self.dt}")
            
            if timestamp >= max_time_cutoff and timestamp <= self.dt:
                file_url = urljoin(full_url, filename)
                matching_files.append((file_url, timestamp))
                # DEBUG: print(f"Added file: {filename} with timestamp: {timestamp}")
        
        # Sort by timestamp (newest first) and apply max_entries limit
        matching_files.sort(key=lambda x: x[1], reverse=True)
        return matching_files[:self.max_entries]
    
class FileDownloader:
    def __init__(self, dt):
        self.dt = dt

    def download_latest(self, files, outdir: Path):
        if not files:
            raise ValueError("No files provided")

        # Pick the file closest to self.dt
        latest, ts = min(files, key=lambda x: abs(x[1] - self.dt))

        print(f"Latest file: {latest}")

        # Ensure output directory exists
        outdir.mkdir(parents=True, exist_ok=True)

        # Extract just the filename from the URL
        filename = Path(latest).name
        outfile = outdir / filename

        # Skip download if it already exists
        if outfile.exists():
            print(f"{outfile} already exists locally")
            return outfile

        # Download the file
        try:
            print(f"Downloading file: {filename}")
            response = requests.get(latest, stream=True)
            response.raise_for_status()
            with open(outfile, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded file: {filename}")
            return outfile
        except Exception as e:
            print(f"Failed to download {filename}: {e}")
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
            raise ValueError("No files provided")
        
        if n < 0 or n >= len(files):
            raise ValueError(f"Invalid index {n}. Must be between 0 and {len(files) - 1}")
        
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
            print(f"❌ File does not exist: {gz_path}")
            return None

        if gz_path.suffix != ".gz":
            print(f"⚠️ Not a .gz file: {gz_path}")
            return None

        try:
            # Decompress
            grib_path = gz_path.with_suffix("")  # remove .gz
            with gzip.open(gz_path, "rb") as f_in, open(grib_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

            print(f"✅ Decompressed: {grib_path}")

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
                print(f"📂 Moved file to: {target_path}")

                # Delete timestamp folder if empty
                try:
                    os.rmdir(parent_dir)
                    print(f"🗑️ Deleted folder: {parent_dir}")
                except OSError:
                    print(f"⚠️ Could not delete {parent_dir} (not empty)")
            else:
                # File is already in dataset directory, keep it there
                target_path = grib_path
                print(f"📂 File is already in dataset dir: {target_path}")

            # Delete original .gz
            gz_path.unlink(missing_ok=True)

            return target_path

        except Exception as e:
            print(f"❌ Error decompressing {gz_path}: {e}")
            return None
        
import util.core.file as fs

if __name__ == "__main__":
    # Set up the parameters with timezone-aware datetime
    base_url = "https://mrms.ncep.noaa.gov/"
    modifier = "2D/NLDN_CG_001min_AvgDensity/"
    dt = datetime.datetime.now(datetime.timezone.utc)  # Current UTC time (timezone-aware)
    max_time = datetime.timedelta(hours=6)  # Look back 6 hours
    max_entries = 7 # Return top 10 most recent files

    # Create FileFinder instance
    file_finder = FileFinder(dt, base_url, max_time, max_entries)
    downloader = FileDownloader(dt)

    # Search for files
    try:
        files_with_timestamps = file_finder.lookup_files(modifier)
        
        print(f"Found {len(files_with_timestamps)} files:")
        print("-" * 80)

        print(files_with_timestamps)
            
    except Exception as e:
        print(f"Error searching directory: {e}")
