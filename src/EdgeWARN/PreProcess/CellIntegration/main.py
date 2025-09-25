import util.core.file as fs
from EdgeWARN.PreProcess.CellIntegration.integrate import StormCellIntegrator
from EdgeWARN.PreProcess.CellIntegration.utils import StatFileHandler

debug_shi = -3
def main():
    handler = StatFileHandler()
    json_path = "stormcell_test.json"
    cells = handler.load_json(json_path)
    if cells is None:
        print("No storm cells loaded; aborting.")
        return
    
    integrator = StormCellIntegrator()
    result_cells = cells

    # Integrate NLDN Data
    print(f"DEBUG: Integrating NLDN Data with {len(cells)} storm cells")
    try:
        nldn_file = fs.latest_files(fs.MRMS_NLDN_DIR, 5)[debug_shi]
        if nldn_file:
            print(f"DEBUG: Using NLDN file {nldn_file}")
            result_cells = integrator.integrate_ds(nldn_file, result_cells, "max_flash_density")
            print(f"DEBUG: Successfully integrated NLDN data for {len(result_cells)} storm cells")
        
        else:
            print("ERROR: Could not find NLDN files")
    
    except Exception as e:
        print(f"ERROR: Failed to integrate NLDN data: {e}")

    print(f"Saving integrated cells to {json_path}")
    handler.write_json(result_cells, json_path)

if __name__ == "__main__":
    main()