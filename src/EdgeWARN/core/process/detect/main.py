from EdgeWARN.core.process.detect.tools.utils import DetectionDataHandler
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from EdgeWARN.core.process.detect.tools.vecmath import StormVectorCalculator
from EdgeWARN.core.process.detect.track import StormCellTracker
from EdgeWARN.core.process.detect.detect import detect_cells
import util.file as fs
import json as js

def main(radar_old, radar_new, ps_old, ps_new, pt_old, pt_new, lat_bounds: tuple, lon_bounds: tuple, json_output):
    lat_min, lat_max = lat_bounds
    lon_min, lon_max = lon_bounds

    # === Single-frame fallback ===
    single_frame = radar_new is None or ps_new is None
    if single_frame:
        print("[CellDetection] DEBUG: No new scan specified — running single-frame detection mode")

    # === Load or create previous entries ===
    if json_output.exists() and json_output.stat().st_size > 0:
        try:
            with open(json_output, 'r') as f:
                entries_old = js.load(f)
            print(f"[CellDetection] DEBUG: Loaded {len(entries_old)} cells from {json_output}")
        except (js.JSONDecodeError, KeyError, IndexError) as e:
            print(f"[CellDetection] ERROR: Failed to load existing data: {e}. Redetecting from old scan ...")
            entries_old = detect_cells(radar_old, ps_old, pt_old, lat_min, lat_max, lon_min, lon_max)
            print(f"[CellDetection] DEBUG: Detected {len(entries_old)} cells in old scan.")
    else:
        print("[CellDetection] DEBUG: JSON output doesn't exist, detecting from old scan ...")
        entries_old = detect_cells(radar_old, ps_old, pt_new, lat_min, lat_max, lon_min, lon_max)
        print(f"[CellDetection] DEBUG: Detected {len(entries_old)} cells in old scan.")

    # === If single-frame mode, just update/save ===
    if single_frame:
        print("[CellDetection] DEBUG: Saving single-frame detection results (no tracking possible).")
        saver = CellDataSaver(None, radar_old, None, None, ps_old, None)
        entries = saver.append_storm_history(entries_old, radar_old)
        entries = StormVectorCalculator.calculate_vectors(entries)
        with open(json_output, 'w') as f:
            js.dump(entries, f, indent=2, default=str)
        return

    # === Dual-frame mode ===
    print("[CellDetection] DEBUG: Detecting cells in new scan ...")
    entries_new = detect_cells(radar_new, ps_new, pt_new, lat_min, lat_max, lon_min, lon_max)
    print(f"[CellDetection] DEBUG: Detected {len(entries_new)} cells in new scan")

    print("[CellDetection] DEBUG: Matching and updating cell data")
    ps_old = DetectionDataHandler(radar_old, ps_old, pt_old, lat_min, lat_max, lon_min, lon_max).load_probsevere()
    ps_new = DetectionDataHandler(radar_new, ps_new, pt_new, lat_min, lat_max, lon_min, lon_max).load_probsevere()
    
    tracker = StormCellTracker(ps_old, ps_new)
    saver = CellDataSaver(None, radar_new, None, None, ps_new, None)
    entries = tracker.update_cells(entries_old, entries_new)
    entries = saver.append_storm_history(entries, radar_new)
    entries = StormVectorCalculator.calculate_vectors(entries)

    with open(json_output, 'w') as f:
        js.dump(entries, f, indent=2, default=str)

if __name__ == "__main__":
    from pathlib import Path
    fs.clean_idx_files([fs.MRMS_COMPOSITE_DIR])
    radar_files = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 2)
    radar_old, radar_new = radar_files[-2], radar_files[-1]
    ps_files = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 2)
    ps_old, ps_new = ps_files[-2], ps_files[-1]
    pt_files = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 2)
    pt_old, pt_new = pt_files[-2], pt_files[-1]
    lat_bounds = (42, 46)
    lon_bounds = (287, 293)
    main(radar_old, radar_new, ps_old, ps_new, pt_old, pt_new, lat_bounds, lon_bounds, Path("stormcell_test.json"))
