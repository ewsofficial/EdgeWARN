from EdgeWARN.process.detect.tools.utils import DetectionDataHandler
from pathlib import Path
from EdgeWARN.process.detect.tools.save import CellDataSaver
from EdgeWARN.process.detect.tools.vecmath import StormVectorCalculator
from EdgeWARN.process.detect.tools.alert_matcher import match_alerts_to_cells
from EdgeWARN.process.detect.track import StormCellTracker
from EdgeWARN.process.detect.kalman.config import TrackingConfig, AssignmentConfig
from EdgeWARN.process.detect.detect import detect_cells
from util.io import IOManager
import util.file as fs
import json as js
from datetime import datetime
from util.performance import tracker as perf_tracker

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
    refl_threshold=37.5,
    min_seed_percentage=0.001,
    drop_offset=10.0,
    radar_obj=None,
    ps_obj=None,
    pt_obj=None,
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
            refl_threshold=refl_threshold,
            min_seed_percentage=min_seed_percentage,
            drop_offset=drop_offset,
            radar_obj=radar_obj,
            ps_obj=ps_obj,
            preciptype_obj=pt_obj,
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
        refl_threshold=refl_threshold,
        min_seed_percentage=min_seed_percentage,
        drop_offset=drop_offset,
        radar_obj=radar_obj,
        ps_obj=ps_obj,
        preciptype_obj=pt_obj,
    ), None


def main(
    radar_old,
    radar_new,
    ps_old,
    ps_new,
    pt_old,
    pt_new,
    lat_bounds: tuple,
    lon_bounds: tuple,
    json_output,
    radar_old_obj=None,
    ps_old_obj=None,
    pt_old_obj=None,
    refl_threshold=37.5,
    min_seed_percentage=0.001,
    drop_offset=10.0,
):
    fs.clean_files_by_age(fs.STORMCELL_DIR, max_age_minutes=120)
    lat_min, lat_max = lat_bounds
    lon_min, lon_max = lon_bounds

    # === Input Data Safeguards ===
    # Check "New" data existence
    if radar_new and not Path(radar_new).exists():
        io_manager.write_warning(f"New radar file not found: {radar_new}")
        radar_new = None
    if ps_new and not Path(ps_new).exists():
        io_manager.write_warning(f"New ProbSevere file not found: {ps_new}")
        ps_new = None
    if pt_new and not Path(pt_new).exists():
        io_manager.write_warning(f"New PrecipType file not found: {pt_new}")
        pt_new = None

    # Check "Old" data existence
    if radar_old and not Path(radar_old).exists():
        io_manager.write_warning(f"Old radar file not found: {radar_old}")
        radar_old = None
    if ps_old and not Path(ps_old).exists():
        io_manager.write_warning(f"Old ProbSevere file not found: {ps_old}")
        ps_old = None
    if pt_old and not Path(pt_old).exists():
        io_manager.write_warning(f"Old PrecipType file not found: {pt_old}")
        pt_old = None

    # === Single-frame fallback ===
    single_frame = radar_new is None or ps_new is None or pt_new is None
    if single_frame:
        io_manager.write_debug("No new scan specified — running single-frame detection mode")

    # === Calculate Timestamp Early ===
    current_radar = radar_old if single_frame else radar_new
    if current_radar is None:
        io_manager.write_warning("No valid radar input data found. Aborting detection.")
        return None, None # Modified return signature
        
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
                # Filter out files that are from the future relative to this scan
                valid_files = [f for f in json_files if f.stem < f"stormcells_{final_ts}"]
                if valid_files:
                    latest_json = valid_files[-1]
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

    # Get old timestamp for dt calculation
    old_ts_str = None
    if data_old and isinstance(data_old, dict):
        old_ts_str = data_old.get("latest_timestamp")

    if not entries_old:
        io_manager.write_info("No valid previous storm cell data found, detecting from old scan ...")
        perf_tracker.start("Detection - Old Scan Fallback")
        # Use cached objects if available for OLD scan
        entries_old, ps_old_data = _detect_with_optional_probsevere(
            radar_old,
            ps_old,
            pt_old,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            need_probsevere=not single_frame,
            refl_threshold=refl_threshold,
            min_seed_percentage=min_seed_percentage,
            drop_offset=drop_offset,
            radar_obj=radar_old_obj, # Pass cached object
            ps_obj=ps_old_obj,
            pt_obj=pt_old_obj,
        )
        perf_tracker.stop("Detection - Old Scan Fallback")


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

        # Match convective/flood alerts to cells
        entries = match_alerts_to_cells(entries, fs.MRMS_NWS_DIR, target_timestamp=json_ts)

        output_data = saver.create_json_structure(json_ts, entries)
        
        # Save to stormcell directory
        stormcell_dir = fs.STORMCELL_DIR
        stormcell_dir.mkdir(exist_ok=True)
        output_file = stormcell_dir / f"stormcells_{final_ts}.json"
        
        with open(output_file, 'w') as f:
            js.dump(output_data, f, indent=2, default=str)
        
        io_manager.write_info(f"Saved single-frame results to {output_file}")
        
        # In single frame mode (fallback), we might want to return the loaded dataset too?
        # But usually this is the startup or error case.
        return output_file, None

    # === Dual-frame mode ===
    io_manager.write_debug("Detecting cells in new scan ...")
    perf_tracker.start("Detection - New Scan")
    
    # We need to capture the loaded datasets from this call to return them
    # detect_cells returns entries, ps_ds.
    # It does NOT return the radar_ds explicitly unless we change it.
    # However, we can instantiate a handler here to get them, OR change detect_cells to return more.
    # Changing detect_cells return signature impacts _detect_with_optional_probsevere.
    
    # Alternative: Instantiate Handler here manually for valid caching?
    # Or just rely on the file system cache in IO?
    # Our simple persistent strategy is to return the OBJECT.
    
    # Let's modify logic: We want to get the loaded radar_ds NEW so we can return it.
    # But `detect_cells` creates local variables.
    
    # Hack: We can just use the fact that if we pass `radar_obj` later, it works.
    # But we need to EXTRACT `radar_obj` from this run.
    
    # Let's create a temporary handler just to load the NEW data, so we have the object reference.
    # This might seem redundant but `DetectionDataHandler` is thin.
    # Actually, `detect_cells` does: handler = ...; radar_ds = handler.load_subset();
    
    # If we want to return `radar_ds`, we should change `detect_cells` to return it?
    # That changes signature widely.
    
    # Strategy: Just Load it here using handler, then pass it to detect_cells.
    
    new_data_handler = DetectionDataHandler(
        radar_new, ps_new, pt_new, io_manager, 
        lat_min, lat_max, lon_min, lon_max
    )
    
    perf_tracker.start("Detection - New Scan - Preload")
    radar_new_obj = new_data_handler.load_subset()
    # We can also preload PS and PT if we want full persistence
    ps_new_obj = new_data_handler.load_probsevere()
    pt_new_obj = new_data_handler.load_preciptype()
    perf_tracker.stop("Detection - New Scan - Preload")
    
    entries_new, ps_new_data = _detect_with_optional_probsevere(
        radar_new,
        ps_new,
        pt_new,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        need_probsevere=True,
        refl_threshold=refl_threshold,
        min_seed_percentage=min_seed_percentage,
        drop_offset=drop_offset,
        radar_obj=radar_new_obj, # Pass preloaded
        ps_obj=ps_new_obj,
        pt_obj=pt_new_obj 
    )
    
    perf_tracker.stop("Detection - New Scan")
    io_manager.write_debug(f"Detected {len(entries_new)} cells in new scan")

    io_manager.write_info("Matching and updating cell data")
    if ps_old_data is None:
        perf_tracker.start("Detection - Load ProbSevere Old")
        ps_old_data = DetectionDataHandler(
            radar_old,
            ps_old,
            pt_old,
            io_manager,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            radar_obj=radar_old_obj, # Use cached
            ps_obj=ps_old_obj,
            preciptype_obj=pt_old_obj
        ).load_probsevere()
        perf_tracker.stop("Detection - Load ProbSevere Old")

    perf_tracker.start("Detection - Tracking")
    
    # Load Kalman configurations
    tracking_config = TrackingConfig.from_yaml()
    assignment_config = AssignmentConfig.from_yaml()
    
    tracker = StormCellTracker(
        ps_old_data, 
        ps_new_data, 
        io_manager,
        tracking_config=tracking_config,
        assignment_config=assignment_config
    )
    saver = CellDataSaver(None, radar_new, None, None, ps_new_data, None)
    
    # Lineage detection (merge/split events)
    stormcell_dir = fs.STORMCELL_DIR
    stormcell_dir.mkdir(exist_ok=True)
    lineage = tracker.detect_lineage_events(entries_old, entries_new, stormcell_dir)
    
    # Calculate dt for Kalman filter
    dt_seconds = 120.0 # Default
    if old_ts_str:
        try:
            old_dt = datetime.fromisoformat(old_ts_str)
            current_dt = datetime.fromisoformat(json_ts)
            dt_seconds = (current_dt - old_dt).total_seconds()
            if dt_seconds <= 0:
                io_manager.write_warning(f"Calculated dt={dt_seconds}s is non-positive. Defaulting to 120s.")
                dt_seconds = 120.0
        except Exception as e:
            io_manager.write_warning(f"Failed to calculate dt from timestamps: {e}. Defaulting to 120s.")
            
    # Pass timestamp, dt, and lineage to tracker
    entries = tracker.update_cells(
        entries_old, 
        entries_new, 
        timestamp=json_ts, 
        dt_seconds=dt_seconds,
        lineage=lineage
    )
    
    # Save lineage buffer state for next scan
    tracker.save_lineage_buffer(stormcell_dir)
    
    perf_tracker.stop("Detection - Tracking")
    
    perf_tracker.start("Detection - Vector Calc")
    entries = StormVectorCalculator.calculate_vectors(entries)
    perf_tracker.stop("Detection - Vector Calc")

    # Match convective/flood alerts to cells
    perf_tracker.start("Detection - Alert Matching")
    entries = match_alerts_to_cells(entries, fs.MRMS_NWS_DIR, target_timestamp=json_ts)
    perf_tracker.stop("Detection - Alert Matching")

    perf_tracker.start("Detection - Save")
    output_data = saver.create_json_structure(json_ts, entries)
    
    output_file = stormcell_dir / f"stormcells_{final_ts}.json"

    with open(output_file, 'w') as f:
        js.dump(output_data, f, indent=2, default=str)
    perf_tracker.stop("Detection - Save")
        
    io_manager.write_info(f"Saved detection results to {output_file}")
    
    # Update API stormcell index
    try:
        from EdgeWARN.api_integration.index_manager import APIIndexManager
        api_index = APIIndexManager(io_manager)
        api_index.update_stormcell_index(final_ts)
    except Exception as e:
        io_manager.write_error(f"Failed to update API index: {e}")
    
    # Return output file AND the new dataset objects for next iteration
    return output_file, (radar_new_obj, ps_new_obj, pt_new_obj)
