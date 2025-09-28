from EdgeWARN.PreProcess.CellDetection.tools.utils import DetectionDataHandler
from EdgeWARN.PreProcess.CellDetection.tools.gatemapper import GateMapper
from EdgeWARN.PreProcess.CellDetection.tools.save import CellDataSaver
from EdgeWARN.PreProcess.CellDetection.tools.vecmath import StormVectorCalculator
from EdgeWARN.PreProcess.CellDetection.track import StormCellTracker
from EdgeWARN.PreProcess.CellDetection.detect import detect_cells
import util.core.file as fs
import json as js

def main(radar_old, radar_new, ps_old, ps_new, lat_bounds: tuple, lon_bounds: tuple, json_output):
    lat_min, lat_max = lat_bounds[0], lat_bounds[1]
    lon_min, lon_max = lon_bounds[0], lon_bounds[1]

    if json_output.exists() and json_output.stat().st_size > 0:
        try:
            # Load existing data
            with open(json_output, 'r') as f:
                entries_old = js.load(f)
                print(f"DEBUG: Loaded {len(entries_old)} cells from {json_output}")
        
        except (js.JSONDecodeError, KeyError, IndexError) as e:
            print(f"DEBUG: Failed to load existing storm cell data: {e}. Creating new data from old scan ...")
            entries_old = detect_cells(radar_old, ps_old, lat_min, lat_max, lon_min, lon_max)
            print(f"DEBUG: Detected {len(entries_old)} cells in old scan.")
    
    else:
        print(f"DEBUG: JSON output doesn't exist, detecting from old scan ...")
        entries_old = detect_cells(radar_old, ps_old, lat_min, lat_max, lon_min, lon_max)
        print(f"Detected {len(entries_old)} cells in old scan.")

    print("DEBUG: Detecting cells in new scan ...")
    entries_new = detect_cells(radar_new, ps_new, lat_min, lat_max, lon_min, lon_max)
    print(f"DEBUG: Detected {len(entries_new)} cells in new scan")

    print("DEBUG: Matching and updating cell data")
    ps_old = DetectionDataHandler(radar_old, ps_old, lat_min, lat_max, lon_min, lon_max).load_probsevere()
    ps_new = DetectionDataHandler(radar_new, ps_new, lat_min, lat_max, lon_min, lon_max).load_probsevere()
    
    tracker = StormCellTracker(ps_old, ps_new)
    saver = CellDataSaver(None, radar_new, None, None, ps_new, None)
    entries = tracker.update_cells(entries_old, entries_new)
    entries = saver.append_storm_history(entries, radar_new)
    entries = StormVectorCalculator.calculate_vectors(entries)

    import json

    with open(json_output, 'w') as f:
        json.dump(entries, f, indent=2, default=str)


if __name__ == "__main__":
    from pathlib import Path
    radar_files = fs.latest_files(fs.MRMS_3D_DIR, 6)
    radar_old, radar_new = radar_files[-2], radar_files[-1]
    ps_files = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 6)
    ps_old, ps_new = ps_files[-2], ps_files[-1]
    lat_bounds = (35.0, 38.0)
    lon_bounds = (283.0, 285.0)
    main(radar_old, radar_new, ps_old, ps_new, lat_bounds, lon_bounds, Path("stormcell_test_ps.json"))
         

    
    

