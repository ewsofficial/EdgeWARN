import eccodes
import sys
from pathlib import Path

def get_wind_levels(filepath):
    """
    Scans a GRIB2 file and prints unique pressure levels for U/V wind components.
    """
    filepath = Path(filepath)
    if not filepath.exists():
        print(f"Error: File {filepath} not found.")
        return

    print(f"Scanning {filepath} for wind data...")
    
    unique_levels = set()
    
    with open(filepath, 'rb') as f:
        while True:
            gid = eccodes.codes_grib_new_from_file(f)
            if gid is None:
                break

            try:
                shortName = eccodes.codes_get_string(gid, "shortName")
                typeOfLevel = eccodes.codes_get_string(gid, "typeOfLevel")
                
                # Check for U or V component of wind
                if shortName in ['u', 'v'] and typeOfLevel == 'isobaricInhPa':
                    level = eccodes.codes_get_long(gid, "level")
                    unique_levels.add(level)
            
            except Exception as e:
                # Some messages might not have these keys
                pass
            finally:
                eccodes.codes_release(gid)
    
    if unique_levels:
        sorted_levels = sorted(list(unique_levels), reverse=True)
        print("\nFound Wind Pressure Levels (hPa):")
        print("-------------------------------")
        for level in sorted_levels:
            print(f"{level} hPa")
        print("-------------------------------")
        print(f"Total levels: {len(sorted_levels)}")
        
        # Also print as a list for easy copying
        print(f"\nPython list format:\n{sorted_levels}")
    else:
        print("No wind data on isobaric levels found.")

if __name__ == "__main__":
    # Default to the snapshot file if no argument provided
    default_file = "snapshot_20260204-0614/data/RAP/RAP.20260204-06z.awp130pgrbf00.grib2"
    
    if len(sys.argv) > 1:
        target_file = sys.argv[1]
    else:
        target_file = default_file
        print(f"No file provided, using default: {target_file}")
    
    get_wind_levels(target_file)
