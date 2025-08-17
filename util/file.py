from pathlib import Path
import platform

# ---------- PATH CONFIG ----------
BASE_DIR = Path("C:/input_data") if platform.system() == "Windows" else Path("data_ingest")
NEXRAD_L2_DIR = BASE_DIR / "nexrad_l2"
MRMS_3D_DIR = BASE_DIR / "nexrad_merged"
GOES_GLM_DIR = BASE_DIR / "goes_glm"
MRMS_LTNG_DIR = BASE_DIR / "mrms_lightning"
MRMS_NLDN_DIR = BASE_DIR / "mrms_nldn"
MRMS_ECHOTOP18_DIR = BASE_DIR / "mrms_echotop18"
MRMS_QPE15_DIR = BASE_DIR / "mrms_qpe15"
MRMS_PRECIPRATE_DIR = BASE_DIR / "mrms_preciprate"
MRMS_MESH_DIR = BASE_DIR / "mrms_mesh"
MRMS_PROBSEVERE_DIR = BASE_DIR / "mrms_probsevere"
MRMS_COMBINED_DIR = BASE_DIR / "mrms_combined_strikes"
MRMS_RADAR_DIR = BASE_DIR / "nexrad_merged"
THREDDS_RTMA_DIR = BASE_DIR / "rtma"
TEMP_DIR = BASE_DIR / "temp"
MRMS_COMBINED_DIR.mkdir(parents=True, exist_ok=True)

def latest_nexrad(n):
    """Return the n most recent NEXRAD L2 files as a list (oldest to newest)."""
    if not NEXRAD_L2_DIR.exists():
        print("WARNING: NEXRAD directory doesn't exist!")
        return
    files = sorted([f for f in NEXRAD_L2_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"Not enough NEXRAD L2 files in {NEXRAD_L2_DIR}")
    return [str(f) for f in files[-n:]]

def latest_mosaic(n):
    """Return the n most recent MRMS Radar Mosaic files as a list (oldest to newest)."""
    if not MRMS_RADAR_DIR.exists():
        print("WARNING: MRMS mosaic directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_RADAR_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"Not enough MRMS Mosaic files in {MRMS_RADAR_DIR}")
    return [str(f) for f in files[-n:]]

def latest_glm(n):
    """Return the n most recent GOES GLM files as a list (oldest to newest)."""
    if not GOES_GLM_DIR.exists():
        print("WARNING: GOES GLM directory doesn't exist!")
        return
    files = sorted([f for f in GOES_GLM_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No GOES GLM files in {GOES_GLM_DIR}")
    return [str(f) for f in files[-n:]]

def latest_ltng(n): #For Debug Purposes; use get_latest_mrms_lightning_combined_files for operational use
    """Return the n most recent MRMS Lightning files as a list (oldest to newest)."""
    if not MRMS_LTNG_DIR.exists():
        print("WARNING: MRMS Lightning directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_LTNG_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS Lightning files in {MRMS_LTNG_DIR}")
    return [str(f) for f in files[-n:]]

def latest_nldn(n): #For Debug Purposes; use get_latest_mrms_lightning_combined_files for operational use
    """Return the n most recent MRMS NLDN files as a list (oldest to newest)."""
    if not MRMS_NLDN_DIR.exists():
        print("WARNING: MRMS NLDN directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_NLDN_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS NLDN files in {MRMS_NLDN_DIR}")
    return [str(f) for f in files[-n:]]

def latest_echotop18(n):
    """Return the n most recent MRMS EchoTop18 files as a list (oldest to newest)."""
    if not MRMS_ECHOTOP18_DIR.exists():
        print("WARNING: MRMS EchoTop18 directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_ECHOTOP18_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS EchoTop18 files in {MRMS_ECHOTOP18_DIR}")
    return [str(f) for f in files[-n:]]

def latest_qpe15(n):
    """Return the n most recent MRMS QPE15 files as a list (oldest to newest)."""
    if not MRMS_QPE15_DIR.exists():
        print("WARNING: MRMS QPE15 directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_QPE15_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS QPE15 files in {MRMS_QPE15_DIR}")
    return [str(f) for f in files[-n:]]

def latest_preciprate(n):
    """Return the n most recent MRMS PrecipRate files as a list (oldest to newest)."""
    if not MRMS_PRECIPRATE_DIR.exists():
        print("WARNING: MRMS PrecipRate directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_PRECIPRATE_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS PrecipRate files in {MRMS_PRECIPRATE_DIR}")
    return [str(f) for f in files[-n:]]

"""
def latest_mesh(n=2):
    ""Return the n most recent MRMS MESH files as a list (oldest to newest).""
    files = sorted([f for f in MRMS_MESH_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS MESH files in {MRMS_MESH_DIR}")
    return [str(f) for f in files[-n:]]
"""

def latest_probsevere(n=2):
    """Return the n most recent MRMS ProbSevere files as a list (oldest to newest)."""
    if not MRMS_PROBSEVERE_DIR.exists():
        print("WARNING: MRMS ProbSevere directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_PROBSEVERE_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No MRMS ProbSevere files in {MRMS_PROBSEVERE_DIR}")
    return [str(f) for f in files[-n:]]

def latest_rtma(n=1):
    """Return the n most recent RTMA files as a list (oldest to newest)."""
    if not THREDDS_RTMA_DIR.exists():
        print("WARNING: THREDDS RTMA directory doesn't exist!")
        return
    files = sorted([f for f in THREDDS_RTMA_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No RTMA files in {THREDDS_RTMA_DIR}")
    return [str(f) for f in files[-n:]]

def latest_ltng_combined(n=2):
    """Return the n most recent combined MRMS Lightning/NLDN files as a list (oldest to newest)."""
    if not MRMS_LTNG_DIR.exists():
        print("WARNING: MRMS Lightning directory doesn't exist!")
        return
    if not MRMS_NLDN_DIR.exists():
        print("WARNING: MRMS NLDN directory doesn't exist!")
        return
    if not MRMS_COMBINED_DIR.exists():
        print("WARNING: MRMS Combined Strikes directory doesn't exist!")
        return
    files = sorted([f for f in MRMS_COMBINED_DIR.glob("*") if f.is_file()], key=lambda f: f.stat().st_mtime)
    if len(files) < n:
        raise RuntimeError(f"No combined MRMS Lightning/NLDN files in {MRMS_COMBINED_DIR}")
    return [str(f) for f in files[-n:]]

def clean_idx_files(folders):
    """
    Remove IDX files in a specified list of folders.
    Inputs:
    - folders: list of folders you want to remove IDX files from
    """
    for folder in folders:
        if folder.exists():
            idx_files = list(folder.rglob("*.idx"))
            if len(idx_files) == 0:
                print(f"No IDX files in folder: {folder}")
                return
            else:
                for f in idx_files:
                    try:
                        f.unlink()
                        print(f"Deleted IDX file: {f}")
                    except Exception as e:
                        print(f"Failed to delete IDX file {f}: {e}")
        else:
            print(f"Folder not found: {folder}")

