from pathlib import Path
import xarray as xr
import shutil
import gzip
import requests
import os
import re
from bs4 import BeautifulSoup
from util import file as fs

# ---------- MRMS MERGED REFLECTIVITY QC ----------
def download_mrms_composite_reflectivity(outdir: Path, tempdir: Path,
                                          sweep_heights=None,
                                          base_dir_url=None) -> Path | None:
    """
    Downloads the latest MRMS Merged Reflectivity QC 3D data, decompresses.
    Falls back to the most recent timestamp where ALL sweeps are available.
    """
    if sweep_heights is None:
        sweep_heights = [
            "00.50", "00.75", "01.00", "01.25", "01.50", "1.75", "02.00", "2.25",
            "02.50", "2.75", "03.00", "03.50", "04.00", "04.50", "05.00", "05.50",
            "06.00", "06.50", "07.00", "07.50", "08.00", "08.50", "09.00", "10.00",
            "11.00", "12.00", "13.00", "14.00", "15.00", "16.00", "17.00", "18.00"
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

    # Find latest timestamp where all sweeps exist
    valid_timestamp = None
    for ts in timestamps:
        all_exist = True
        for height in sweep_heights:
            url = build_url(height, ts)
            try:
                head_r = requests.head(url, timeout=10)
                if head_r.status_code != 200:
                    all_exist = False
                    break
            except Exception:
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

        if gz_path.exists():
            print("[MRMS Refl] Terminating download. Reason: File already exists")
            return None
        try:
            print(f"⬇️  Downloading {url}")
            r = requests.get(url, timeout=30)
            if r.status_code != 200:
                print(f"⚠️  Failed to download {height}: HTTP {r.status_code}")
                continue
            with open(gz_path, "wb") as f:
                f.write(r.content)
            downloaded[height] = gz_path
        except Exception as e:
            print(f"❌ Download error {height}: {e}")

    if len(downloaded) != len(sweep_heights):
        print("❌ Missing files even for validated timestamp.")
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
        except Exception as e:
            print(f"❌ Error decompressing {height}: {e}")

    if not grib_paths:
        print("❌ No GRIB files decompressed.")
        return None
    
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
    files = find_all_refl_files()
    if not files:
        print("No files found!")
        return
    concat_refl(files)