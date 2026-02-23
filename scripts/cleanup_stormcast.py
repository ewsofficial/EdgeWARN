import os
import sys
from pathlib import Path
import shutil

def cleanup_directory(base_dir):
    base_path = Path(base_dir).expanduser()
    
    if not base_path.exists():
        print(f"Directory {base_path} does not exist.")
        return

    print(f"Cleaning up {base_path}...")
    
    # Walk through all directories
    for root, dirs, files in os.walk(base_path):
        root_path = Path(root)
        
        # Check if current directory is 'cells' or 'stormcells' - if so, skip it entirely
        if root_path.name in ['cells', 'stormcells']:
            print(f"Skipping exempt directory: {root_path}")
            # Also clear dirs to prevent modifying subdirectories if any (though unlikely for these)
            dirs[:] = [] 
            continue
            
        # Filter out 'cells' and 'stormcells' from traversal so we don't even enter them
        dirs[:] = [d for d in dirs if d not in ['cells', 'stormcells']]
        
        # Now process files in the current folder (e.g. CompRefQC, RAP, etc.)
        # We only want to clean 'data' subfolders, usually. 
        # But user said "all folders in ~/StormCast_inputs"
        
        if not files:
            continue
            
        file_paths = []
        for f in files:
            fp = root_path / f
            file_paths.append(fp)
            
        # Sort by mtime (oldest first)
        file_paths.sort(key=lambda f: f.stat().st_mtime)
        
        # Keep last 4
        if len(file_paths) > 5:
            files_to_delete = file_paths[:-5]
            print(f"Cleaning {root_path}: Deleting {len(files_to_delete)} of {len(file_paths)} files")
            
            for fp in files_to_delete:
                try:
                    fp.unlink()
                except Exception as e:
                    print(f"Error deleting {fp}: {e}")

if __name__ == "__main__":
    import time
    for i in range(50):
        cleanup_directory("~/StormCast_inputs")
        time.sleep(480)
