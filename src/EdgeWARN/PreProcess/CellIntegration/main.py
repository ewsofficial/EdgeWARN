import util.file as fs
from EdgeWARN.PreProcess.CellIntegration.integrate import StormCellIntegrator
from EdgeWARN.PreProcess.CellIntegration.utils import StatFileHandler
import copy
import gc
import json
import numpy as np
import os
import shutil

# ------------------------------
# MRMS dataset list
# ------------------------------
datasets = [
    ("NLDN", fs.MRMS_NLDN_DIR, "CGFlashRate"),
    ("EchoTop18", fs.MRMS_ECHOTOP18_DIR, "EchoTop18"),
    ("EchoTop30", fs.MRMS_ECHOTOP30_DIR, "EchoTop30"),
    ("PrecipRate", fs.MRMS_PRECIPRATE_DIR, "PrecipRate"),
    ("VIL Density", fs.MRMS_VIL_DIR, "VILDensity"),
    ("Reflectivity at Lowest Altitude", fs.MRMS_RALA_DIR, "RALA"),
    ("VII", fs.MRMS_VII_DIR, "VII")
]

def main(lat_limits, lon_limits):
    fs.clean_idx_files([d[1] for d in datasets]) # Clean IDX files!
    
    handler = StatFileHandler()
    integrator = StormCellIntegrator()
    json_path = "stormcell_test.json"
    cells = handler.load_json(json_path)

    result_cells = cells

    # Integrate datasets
    for name, outdir, key in datasets:
        try:
            print(f"[CellIntegration] DEBUG: Integrating {name} data for {len(cells)} cells")
            latest_file = fs.latest_files(outdir, 1)[-1]
            print(f"[CellIntegration] DEBUG: Using latest {name} file: {latest_file}")

            result_cells = integrator.integrate_ds(latest_file, result_cells, key, lat_limits=lat_limits, lon_limits=lon_limits)
            print(f"[CellIntegration] DEBUG: {name} integration completed successfully!")
        
        except Exception as e:
            print(f"[CellIntegration] ERROR: Failed to integrate {name} data: {e}")

    # Integrate ProbSevere
    try:
        print(f"[CellIntegration] DEBUG: Integrating ProbSevere data for {len(cells)} cells")
        latest_file = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1]
        probsevere_data = handler.load_json(latest_file)
        print(f"[CellIntegration] DEBUG: Using latest ProbSevere file: {latest_file}")

        result_cells = integrator.integrate_probsevere(probsevere_data, result_cells)
        print(f"[CellIntegration] DEBUG: Successfully integrated ProbSevere data")
    
    except Exception as e:
        print(f"[CellIntegration] ERROR: Failed to integrate ProbSevere data: {e}")
    
    # Save data
    print(f"[CellIntegration] DEBUG: Saving final data to {json_path}")
    handler.write_json(result_cells, json_path)

if __name__ == "__main__":
    main(lat_limits=(32, 35), lon_limits=(278, 281))
