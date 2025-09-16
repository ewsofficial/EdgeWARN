from pathlib import Path
from util.PreProcess.cellmask import CellProcessor, CellMatcher
from .tracker import RadarHandler, CellDetector, CellTracker, StormCellDataManager
from util.PreProcess.save import write_vectors
import json

print(f"EdgeWARN Storm Detection and Tracking Algorithm\nCurrent Build: 0.1.0a\nContributors: Yuchen Wei")

def deduplicate_storm_data(storm_data):
    """Remove duplicate cells by ID, keeping the one with most history."""
    unique_cells = {}
    for cell in storm_data:
        cell_id = cell['id']
        if cell_id not in unique_cells:
            unique_cells[cell_id] = cell
        else:
            # Keep the cell with more history entries
            existing = unique_cells[cell_id]
            if len(cell.get('storm_history', [])) > len(existing.get('storm_history', [])):
                unique_cells[cell_id] = cell
    return list(unique_cells.values())

def main(filepath_old, filepath_new, storm_json, lat_limits, lon_limits):
    print("=== DEBUG: Starting tracking process ===")

    radar = RadarHandler(lat_limits, lon_limits)
    refl, lat_crop, lon_crop = radar.load_reflectivity(filepath_new)

    detector = CellDetector(lat_limits, lon_limits)
    storm_manager = StormCellDataManager(storm_json)

    # Check if storm JSON exists and has data
    if storm_json.exists() and storm_json.stat().st_size > 0:
        try:
            # Load existing storm data
            storm_data = storm_manager.load()
            print(f"DEBUG: Loaded {len(storm_data)} existing cells from {storm_json}")
            
            # Deduplicate the storm data
            storm_data = deduplicate_storm_data(storm_data)
            print(f"DEBUG: After deduplication: {len(storm_data)} unique cells: {[cell['id'] for cell in storm_data]}")
            
            # Extract the latest positions from existing storm cells for matching
            cells_old = []
            for cell in storm_data:
                if cell.get('storm_history'):
                    # Get the latest history entry (most recent)
                    latest_entry = cell['storm_history'][-1]
                    # Create a cell dict with the latest data for matching
                    old_cell = {
                        'id': cell['id'],
                        'num_gates': latest_entry.get('num_gates', 0),
                        'centroid': latest_entry.get('centroid', [0, 0]),
                        'max_reflectivity_dbz': latest_entry.get('max_reflectivity_dbz', 0),
                        'bbox': cell.get('bbox', {}),
                        'alpha_shape': cell.get('alpha_shape', []),
                        'area_km2': cell.get('area_km2', 0)
                    }
                    cells_old.append(old_cell)
            
            print(f"DEBUG: Using {len(cells_old)} existing cells with latest positions for matching")
            if cells_old:
                print(f"DEBUG: Latest timestamps from existing cells: {[cell['storm_history'][-1]['timestamp'] for cell in storm_data if cell.get('storm_history')]}")
        
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"DEBUG: Error loading existing storm data: {e}. Creating new data.")
            storm_data = []
            cells_old, _ = detector.detect(filepath_old)
    else:
        # No existing storm data - detect from old scan and set up initial data
        print("DEBUG: No existing storm data found. Detecting cells from old scan...")
        cells_old, _ = detector.detect(filepath_old)

        if len(cells_old) == 0:
            print("DEBUG: No Cells Detected in scan, aborting ...")
        storm_data = cells_old.copy()  # Start with the old cells as initial data
        print(f"DEBUG: Created new storm data with {len(cells_old)} cells from old scan")

    print("DEBUG: Detecting cells from new scan...")
    cells_new, _ = detector.detect(filepath_new)
    print(f"DEBUG: Found {len(cells_new)} cells in new scan: {[cell['id'] for cell in cells_new]}")
    if cells_new:
        print(f"DEBUG: New scan timestamp: {cells_new[0]['storm_history'][0]['timestamp']}")
    elif len(cells_new) == 0:
        print("DEBUG: No Cells Detected in scan, aborting ...")
        return

    # Add area to cells before matching
    CellProcessor.add_area_to_cells(cells_old)
    CellProcessor.add_area_to_cells(cells_new)

    existing_cells = {cell['id']: cell for cell in storm_data}
    print(f"DEBUG: Existing cells index: {list(existing_cells.keys())}")

    print("DEBUG: Matching cells between scans...")
    matches = CellMatcher.match_cells(cells_old, cells_new)
    print(f"DEBUG: Found {len(matches)} matches: {matches}")

    tracker = CellTracker(storm_data, existing_cells)
    tracker.process_matches(cells_old, cells_new, matches)
    unmatched_count = tracker.add_unmatched_new(cells_new, matches)

    # Save the updated storm data
    storm_manager.storm_data = storm_data
    storm_manager.save()

    print("DEBUG: Final storm data structure:")
    for cell in storm_data:
        print(f"  Cell ID {cell['id']}: {len(cell['storm_history'])} history entries")
        for hist_idx, hist_entry in enumerate(cell['storm_history']):
            print(f"    History {hist_idx + 1}: {hist_entry['timestamp']} - {hist_entry['num_gates']} gates")

    print(f"=== DEBUG: Completed tracking process ===")
    print(f"Updated {storm_json} with {len(matches)} matched pairs and {unmatched_count} new cells.")
    print(f"Total cells in database: {len(storm_data)}")

    write_vectors()
    radar.plot(refl, lat_crop, lon_crop, cells_old, cells_new, matches)

if __name__ == "__main__":
    filepath_old = Path(r"C:\Users\weiyu\Downloads\THREAT_TEST\nexrad_merged\MRMS_MergedReflectivityQC_max_20250913-004041.nc")
    filepath_new = Path(r"C:\Users\weiyu\Downloads\THREAT_TEST\nexrad_merged\MRMS_MergedReflectivityQC_max_20250913-004442.nc")
    storm_json = Path("stormcell_test.json")
    lat_limits = (45.3, 47.3)
    lon_limits = (256.6, 260.2)

    main(filepath_old, filepath_new, storm_json, lat_limits, lon_limits)