from concurrent.futures import ProcessPoolExecutor, as_completed
import util.file as fs
from EdgeWARN.PreProcess.CellIntegration.integrate import StormCellIntegrator
from EdgeWARN.PreProcess.CellIntegration.utils import StatFileHandler
import copy
import gc

datasets = [
    ("NLDN", fs.MRMS_NLDN_DIR, "CGFlashRate"),
    ("EchoTop18", fs.MRMS_ECHOTOP18_DIR, "EchoTop18"),
    ("EchoTop30", fs.MRMS_ECHOTOP30_DIR, "EchoTop30"),
    ("PrecipRate", fs.MRMS_PRECIPRATE_DIR, "PrecipRate"),
    ("VIL Density", fs.MRMS_VIL_DIR, "VILDensity"),
    ("RotationTrack", fs.MRMS_ROTATIONT_DIR, "RotationTrack"),
    ("Reflectivity at Lowest Altitude", fs.MRMS_RALA_DIR, "RALA"),
    ("VII", fs.MRMS_VII_DIR, "VII"),
]


def integrate_dataset(dataset, cells):
    """Run in a separate process to isolate eccodes context."""
    import os                              # ✅ Fix: add this
    from EdgeWARN.PreProcess.CellIntegration.integrate import StormCellIntegrator
    from util import file as fs
    import copy

    name, folder, key = dataset
    result = None
    integrator = StormCellIntegrator()

    try:
        print(f"[CellIntegration] DEBUG: (PID={os.getpid()}) Integrating {name}")
        if folder.exists():
            latest_file = fs.latest_files(folder, 1)[-1]
            if latest_file:
                result = integrator.integrate_ds(latest_file, copy.deepcopy(cells), key)
                print(f"[CellIntegration] DEBUG: {name} integration done.")
            else:
                print(f"[CellIntegration] ERROR: No files for {name}.")
        else:
            print(f"[CellIntegration] ERROR: Folder {folder} not found.")
    except Exception as e:
        print(f"[CellIntegration] ERROR: {name} integration failed: {e}")

    return name, result

def main(max_processes=4):
    fs.clean_idx_files([d[1] for d in datasets])
    handler = StatFileHandler()

    json_path = "stormcell_test.json"
    cells = handler.load_json(json_path)
    if not cells:
        print("[CellIntegration] DEBUG: No storm cells loaded; aborting.")
        return

    result_cells = copy.deepcopy(cells)

    # Run up to 4 datasets at once
    with ProcessPoolExecutor(max_workers=max_processes) as executor:
        futures = {executor.submit(integrate_dataset, ds, cells): ds for ds in datasets}

        for future in as_completed(futures):
            dataset = futures[future]
            name, ds_result = future.result()
            if ds_result:
                print(f"[CellIntegration] DEBUG: Merging {name} results...")
                for i, cell in enumerate(result_cells):
                    for key, val in ds_result[i].items():
                        if key not in ["id", "centroid", "bbox", "storm_history"]:
                            cell[key] = val
                print(f"[CellIntegration] DEBUG: {name} merged successfully.")

            gc.collect()

    # Integrate ProbSevere sequentially
    try:
        latest_file = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1]
        if latest_file:
            print(f"[CellIntegration] DEBUG: Integrating ProbSevere from {latest_file}")
            probsevere_data = handler.load_json(latest_file)
            integrator = StormCellIntegrator()
            result_cells = integrator.integrate_probsevere(probsevere_data, result_cells)
        else:
            print(f"[CellIntegration] ERROR: Could not find ProbSevere files")
    except Exception as e:
        print(f"[CellIntegration] ERROR: ProbSevere integration failed: {e}")

    print(f"Saving integrated cells to {json_path}")
    handler.write_json(result_cells, json_path)

    del result_cells, cells
    gc.collect()


if __name__ == "__main__":
    import os
    main()
