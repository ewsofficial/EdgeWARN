from pathlib import Path
import xarray as xr
import shutil
import gzip
import requests
import os
import re
from bs4 import BeautifulSoup
import datetime
from util.core import file as fs

# NEEDS OPTIMIZATION WORK

class MRMSDownloader:
    @staticmethod
    def download_mrms_composite_reflectivity(outdir: Path, tempdir: Path,
                                         sweep_heights=None,
                                         base_dir_url=None) -> dict | None:
        """
        Downloads the latest MRMS Merged Reflectivity QC 3D data, decompresses.
        Falls back to the most recent timestamp where ALL sweeps are available.
        Returns dictionary of {height: file_path} for downloaded files.
        """
        if sweep_heights is None:
            sweep_heights = [
                "00.50", "00.75", "01.00", "01.25", "01.50", "02.00",
                "02.50", "03.00", "03.50", "04.00", "04.50", "05.00", "05.50",
                "06.00", "06.50", "07.00", "07.50", "08.00", "08.50", "09.00", "10.00",
                "11.00", "12.00", "13.00", "14.00", "15.00"
            ]

        if base_dir_url is None:
            base_dir_url = "https://mrms.ncep.noaa.gov/3DRefl/MergedReflectivityQC_00.50/"

        # Clean tempdir
        if tempdir.exists():
            shutil.rmtree(tempdir)
        tempdir.mkdir(parents=True, exist_ok=True)
        outdir.mkdir(parents=True, exist_ok=True)

        print("🔍 Fetching available timestamps...")
        try:
            r = requests.get(base_dir_url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"❌ Failed to get directory listing: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")
        hrefs = [a.get("href") for a in soup.find_all("a", href=True)]
        pattern = re.compile(r"MRMS_MergedReflectivityQC_00\.50_(\d{8}-\d{6})\.grib2\.gz")
        timestamps = sorted(
            {m.group(1) for href in hrefs if (m := pattern.match(href))},
            reverse=True
        )

        if not timestamps:
            print("❌ No timestamped GRIB2 files found.")
            return None

        def build_url(height: str, timestamp: str) -> str:
            return (
                f"https://mrms.ncep.noaa.gov/3DRefl/MergedReflectivityQC_{height}/"
                f"MRMS_MergedReflectivityQC_{height}_{timestamp}.grib2.gz"
            )

        # Find latest timestamp where all sweeps exist (with better timeout)
        valid_timestamp = None
        for ts in timestamps[:5]:  # Only check most recent 5 timestamps
            print(f"Checking timestamp {ts}...")
            all_exist = True
            
            for height in sweep_heights:
                url = build_url(height, ts)
                try:
                    # Use GET instead of HEAD with longer timeout
                    head_r = requests.head(url, timeout=5)
                    if head_r.status_code != 200:
                        print(f"❌ Missing {height} for {ts}")
                        all_exist = False
                        break
                except Exception as e:
                    print(f"❌ Error checking {height}: {e}")
                    all_exist = False
                    break
            
            if all_exist:
                valid_timestamp = ts
                break

        if not valid_timestamp:
            print("❌ No timestamp found with all sweep levels.")
            return None

        print(f"🕒 Using timestamp {valid_timestamp}")

        downloaded = {}
        for height in sweep_heights:
            url = build_url(height, valid_timestamp)
            gz_path = tempdir / f"MRMS_MergedReflectivityQC_{height}_{valid_timestamp}.grib2.gz"

            # Skip if already exists
            if gz_path.exists():
                print(f"⚠️ File already exists: {gz_path.name}")
                downloaded[height] = gz_path
                continue
                
            try:
                print(f"⬇️ Downloading {height}...")
                # Use streaming download for large files
                r = requests.get(url, stream=True, timeout=60)
                if r.status_code != 200:
                    print(f"⚠️ Failed to download {height}: HTTP {r.status_code}")
                    continue
                    
                with open(gz_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                downloaded[height] = gz_path
                print(f"✅ Downloaded {height}")
                
            except Exception as e:
                print(f"❌ Download error {height}: {e}")
                continue

        if len(downloaded) < len(sweep_heights) * 0.8:  # Allow 20% missing files
            print("❌ Too many missing files even for validated timestamp.")
            return None

        # Decompress .gz files
        grib_paths = {}
        for height, gz_path in downloaded.items():
            grib_path = tempdir / gz_path.name.replace(".gz", "")
            try:
                with gzip.open(gz_path, "rb") as f_in, open(grib_path, "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
                os.remove(gz_path)
                grib_paths[height] = grib_path
                print(f"✅ Decompressed {height}")
            except Exception as e:
                print(f"❌ Error decompressing {height}: {e}")

        if not grib_paths:
            print("❌ No GRIB files decompressed.")
            return None
            
        return grib_paths  # RETURN THE PATHS!
    
    @staticmethod
    def find_all_refl_files():
        """
        Find all files in fs.TEMP_DIR that contain any of the MRMS sweep elevations
        in their filename. Ignores .idx files. Returns a list of POSIX-style strings.
        """
        sweep_heights = [
            "00.50", "00.75", "01.00", "01.25", "01.50", "02.00",
            "02.50", "03.00", "03.50", "04.00", "04.50", "05.00", "05.50",
            "06.00", "06.50", "07.00", "07.50", "08.00", "08.50", "09.00", "10.00",
            "11.00", "12.00", "13.00", "14.00", "15.00"
        ]

        matching_files = []

        for sweep in sweep_heights:
            for f in fs.TEMP_DIR.glob(f"*_{sweep}_*"):
                if f.is_file() and not f.name.endswith(".idx"):
                    # Convert to POSIX string
                    matching_files.append(f.as_posix())

        if not matching_files:
            print("❌ No sweep files found.")
            return None

        # Optional: sort alphabetically
        matching_files = sorted(matching_files)

        print(f"Found {len(matching_files)} sweep files:")
        for f in matching_files:
            print(f)

        return matching_files

    @staticmethod
    def concat_refl(files):
        """
        Merge multiple NetCDF sweep files into a single NetCDF file containing
        the maximum reflectivity at each grid point across all sweeps.
        Uses lazy evaluation with dask to handle large datasets.
        """

        if not files:
            raise ValueError("No files provided.")

        # Extract timestamp from the first file
        first_file = os.path.basename(files[0])
        match = re.search(r"\d{8}-\d{6}", first_file)
        if not match:
            raise ValueError(f"Could not find YYYYMMDD-HHMMSS in filename: {first_file}")
        timestamp = match.group(0)
        output_path = rf"C:\input_data\nexrad_merged\MRMS_MergedReflectivityQC_max_{timestamp}.nc"

        # Open all files lazily with xarray
        datasets = [xr.open_dataset(f, chunks={'x': 500, 'y': 500}, decode_timedelta=True) for f in files]

        # Select the reflectivity variable from each
        refl_vars = [ds['unknown'] for ds in datasets]

        # Compute the element-wise maximum across all sweeps lazily
        max_reflectivity = xr.concat(refl_vars, dim='sweep').max(dim='sweep')

        # Copy coordinate variables and attributes from the first file
        max_reflectivity.attrs.update(datasets[0]['unknown'].attrs)

        # Save to NetCDF (computed lazily in chunks)
        max_reflectivity.to_netcdf(output_path, format='NETCDF4')

        # Close all datasets
        for ds in datasets:
            ds.close()

        print(f"Maximum reflectivity (lazy) file saved to {output_path}")
    
    def find_and_concat_refl():
        files = MRMSDownloader.find_all_refl_files()
        if not files:
            print("No files found!")
            return
        MRMSDownloader.concat_refl(files)

class SynopticDownloader:
    @staticmethod
    def download_latest_rtma(dt, outdir: Path):
        outdir.mkdir(parents=True, exist_ok=True)

        base_url = "https://thredds.ucar.edu/thredds/fileServer/grib/NCEP/RTMA/CONUS_2p5km"
        filename_template = "RTMA_CONUS_2p5km_{date}_{hour}00.grib2"

        for hour_offset in range(2):  # current hour, fallback 1 hour earlier
            attempt_dt = dt - datetime.timedelta(hours=hour_offset)
            date_str = attempt_dt.strftime("%Y%m%d")
            hour_str = attempt_dt.strftime("%H")
            filename = filename_template.format(date=date_str, hour=hour_str)
            outpath = outdir / filename

            if outpath.exists():
                print(f"[RTMA] Already downloaded: {filename}")
                LATEST_RTMA_FILE = str(outpath)
                return outpath

            print(f"[RTMA] Attempting download: {filename}")
            try:
                r = requests.get(f"{base_url}/{filename}", stream=True, timeout=30)
                r.raise_for_status()
                with open(outpath, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"[RTMA] Downloaded: {filename}")
                LATEST_RTMA_FILE = str(outpath)
                return outpath
            except Exception as e:
                print(f"[RTMA] Failed to download {filename}: {e}")

        print("[RTMA] Could not find any valid file within fallback window.")
        return None
    
    @staticmethod
    def download_rap_awp(dt, outdir: Path):
        """
        Download RAP AWP product files (00hr forecast only)
        
        Args:
            dt: datetime object for the run time
            outdir: output directory path
        """
        outdir.mkdir(parents=True, exist_ok=True)
        
        # Construct filename and URL directly
        date_str = dt.strftime("%Y%m%d")
        hour_str = dt.strftime("%H")
        filename = f"rap.t{hour_str}z.awp130pgrbf00.grib2"
        outpath = outdir / filename
        
        # Direct URL format
        url = f"https://nomads.ncep.noaa.gov/cgi-bin/filter_rap.pl?dir=/rap/{date_str}&file={filename}"
        
        # Check if file already exists
        if outpath.exists():
            print(f"[RAP] Already downloaded: {filename}")
            return outpath
        
        print(f"[RAP] Attempting download: {filename}")
        try:
            r = requests.get(url, stream=True, timeout=30)
            r.raise_for_status()
            
            with open(outpath, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"[RAP] Downloaded: {filename}")
            return outpath
            
        except Exception as e:
            print(f"[RAP] Failed to download {filename}: {e}")
            # Clean up partial download if it exists
            if outpath.exists():
                outpath.unlink()
            return None
