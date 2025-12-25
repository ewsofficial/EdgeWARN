from EdgeWARN.core.process.detect.tools.utils import DetectionDataHandler
from pathlib import Path
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from EdgeWARN.core.process.detect.tools.vecmath import StormVectorCalculator
from EdgeWARN.core.process.detect.track import StormCellTracker
from EdgeWARN.core.process.detect.detect import detect_cells
from util.io import IOManager
import util.file as fs
import json as js
from datetime import datetime

io_manager = IOManager("[CellDetection]")


def _detect_with_optional_probsevere(
    radar_path,
    ps_path,
    pt_path,
    lat_min,
    lat_max,
    lon_min,
    lon_max,
    need_probsevere,
):
    if need_probsevere:
        return detect_cells(
            radar_path,
            ps_path,
            pt_path,
            io_manager,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            return_probsevere=True,
        )

    return detect_cells(
        radar_path,
        ps_path,
        pt_path,
        io_manager,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
    ), None


def main(radar_old, radar_new, ps_old, ps_new, pt_old, pt_new, lat_bounds: tuple, lon_bounds: tuple, json_output):
    lat_min, lat_max = lat_bounds
    lon_min, lon_max = lon_bounds

    # === Single-frame fallback ===
    single_frame = radar_new is None or ps_new is None or pt_new is None
    if single_frame:
        io_manager.write_debug("No new scan specified — running single-frame detection mode")

    # === Calculate Timestamp Early ===
    current_radar = radar_old if single_frame else radar_new
    ts_str = DetectionDataHandler.find_timestamp(current_radar)
    try:
        dt = datetime.fromisoformat(ts_str)
        # dt = dt.replace(second=0, microsecond=0) # Removed: Keep seconds
        final_ts = dt.strftime("%Y%m%d-%H%M%S") # Format for filename with seconds
        json_ts = dt.isoformat() # Format for JSON content
    except ValueError:
        final_ts = ts_str
        json_ts = ts_str

    ps_old_data = None

    # === Load or create previous entries ===
    data_old = None
    try:
        # Find the most recent stormcells_*.json file in the stormcell directory
        stormcell_dir = fs.STORMCELL_DIR
        if stormcell_dir.exists():
            json_files = sorted(stormcell_dir.glob("stormcells_*.json"))
            if json_files:
                latest_json = json_files[-1]
                io_manager.write_debug(f"Loading previous cells from {latest_json}")
                with open(latest_json, 'r') as f:
                    data_old = js.load(f)
    except Exception as e:
        io_manager.write_error(f"Error loading previous data: {e}")

    entries_old = None
    if data_old:
        try:
            if isinstance(data_old, dict) and "features" in data_old:
                entries_old = data_old["features"]
            elif isinstance(data_old, list):
                entries_old = data_old
            
            if entries_old:
                io_manager.write_debug(f"Loaded {len(entries_old)} cells from previous file")
            else:
                io_manager.write_warning("Loaded data contained no features")
        except Exception as e:
            io_manager.write_error(f"Error parsing loaded data: {e}")
            entries_old = None

    if not entries_old:
        io_manager.write_info("No valid previous storm cell data found, detecting from old scan ...")
        entries_old, ps_old_data = _detect_with_optional_probsevere(
            radar_old,
            ps_old,
            pt_old,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            need_probsevere=not single_frame,
        )


    # === If single-frame mode, just update/save ===
    if single_frame:
        io_manager.write_info("Saving single-frame detection results (no tracking possible).")
        saver = CellDataSaver(None, radar_old, None, None, ps_old, None)
        # entries = saver.append_storm_history(entries_old, radar_old) # Removed
        entries = entries_old
        entries = StormVectorCalculator.calculate_vectors(entries)

        # Apply timestamp (all cells are "current" in single frame)
        for cell in entries:
            cell["timestamp"] = json_ts

        output_data = saver.create_json_structure(json_ts, entries)
        
        # Save to stormcell directory
        stormcell_dir = fs.STORMCELL_DIR
        stormcell_dir.mkdir(exist_ok=True)
        output_file = stormcell_dir / f"stormcells_{final_ts}.json"
        
        with open(output_file, 'w') as f:
            js.dump(output_data, f, indent=2, default=str)
        
        io_manager.write_info(f"Saved single-frame results to {output_file}")
        return output_file

    # === Dual-frame mode ===
    io_manager.write_debug("Detecting cells in new scan ...")
    entries_new, ps_new_data = _detect_with_optional_probsevere(
        radar_new,
        ps_new,
        pt_new,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        need_probsevere=True,
    )
    io_manager.write_debug(f"Detected {len(entries_new)} cells in new scan")

    io_manager.write_info("Matching and updating cell data")
    if ps_old_data is None:
        ps_old_data = DetectionDataHandler(
            radar_old,
            ps_old,
            pt_old,
            io_manager,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
        ).load_probsevere()

    tracker = StormCellTracker(ps_old_data, ps_new_data, io_manager)
    saver = CellDataSaver(None, radar_new, None, None, ps_new_data, None)
    # Pass timestamp to tracker
    entries = tracker.update_cells(entries_old, entries_new, timestamp=json_ts)
    entries = StormVectorCalculator.calculate_vectors(entries)

    output_data = saver.create_json_structure(json_ts, entries)
    
    stormcell_dir = fs.STORMCELL_DIR
    stormcell_dir.mkdir(exist_ok=True)
    output_file = stormcell_dir / f"stormcells_{final_ts}.json"

    with open(output_file, 'w') as f:
        js.dump(output_data, f, indent=2, default=str)
        
    io_manager.write_info(f"Saved detection results to {output_file}")
    
    # Update API stormcell index
    try:
        from EdgeWARN.core.api_integration.index_manager import APIIndexManager
        api_index = APIIndexManager(io_manager)
        api_index.update_stormcell_index(final_ts)
    except Exception as e:
        io_manager.write_error(f"Failed to update API index: {e}")
    
    return output_file