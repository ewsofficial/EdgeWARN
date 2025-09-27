import util.core.file as fs
from EdgeWARN.PreProcess.CellIntegration.integrate import StormCellIntegrator
from EdgeWARN.PreProcess.CellIntegration.utils import StatFileHandler


datasets = [
    ("NLDN", fs.MRMS_NLDN_DIR, "max_flash_rate"), # Format: (Debug Name, Data Folder, Key)
    ("EchoTop18", fs.MRMS_ECHOTOP18_DIR, "echotop18_km"),
    ("EchoTop30", fs.MRMS_ECHOTOP30_DIR, "echotop30_km"),
    ("PrecipRate", fs.MRMS_PRECIPRATE_DIR, "preciprate"),
    ("VIL Density", fs.MRMS_VIL_DIR, "vil_density"),
    ("RotationTrack", fs.MRMS_ROTATIONT_DIR, "rotationtrack"),
    ("RhoHV", fs.MRMS_RHOHV_DIR, "rhohv"),
    ("Reflectivity at Lowest Altitude", fs.MRMS_LOWREFL_DIR, "rala"),
    ("VII", fs.MRMS_VII_DIR, "vii")
]

def main():
    handler = StatFileHandler()
    json_path = "stormcell_test.json"
    cells = handler.load_json(json_path)
    if cells is None:
        print("No storm cells loaded; aborting.")
        return
    
    integrator = StormCellIntegrator()
    result_cells = cells

    # Integrate Data
    for dataset in datasets:
        print(f"DEBUG: Integrating {dataset[0]} with {len(cells)} storm cells")
        if dataset[1].exists():
            try:
                latest_file = fs.latest_files(dataset[1], 1)[-1]
                if latest_file:
                    print(f"DEBUG: Using {dataset[0]} file {latest_file}")
                    result_cells = integrator.integrate_ds(latest_file, result_cells, dataset[2])
                    print(f"DEBUG: Successfully integrated {dataset[0]} data for {len(result_cells)} storm cells")

                else:
                    print(f"ERROR: Could not find {dataset[0]} files")

            except Exception as e:
                print(f"ERROR: Failed to integrate {dataset[0]} data: {e}")
        
        else:
            print(f"ERROR: {dataset[1]} does not exist!")
    
    # Integrate ProbSevere Data
    print(f"DEBUG: Integrating ProbSevere data with {len(cells)} storm cells")
    try:
        latest_file = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1]
        if latest_file:
            print(f"DEBUG: Using ProbSevere file {latest_file}")
            probsevere_data = handler.load_json(latest_file)
            result_cells = integrator.integrate_probsevere(probsevere_data, result_cells)
            print(f"DEBUG: Successfully integrated ProbSevere data for {len(result_cells)} storm cells")
        
        else:
            print(f"ERROR: Could not find ProbSevere files")
    
    except Exception as e:
        print(f"ERROR: Failed to integrate ProbSevere data: {e}")

    print(f"Saving integrated cells to {json_path}")
    handler.write_json(result_cells, json_path)

    # Garbage collection
    del result_cells, cells

if __name__ == "__main__":
    main()