"""
CTAM Pipeline Entry Point

Provides a single entry point for running all registered CTAM modules
on storm cell data.

Supports both:
- Cell-based modules: Operate on storm cells and modify them in-place
- Grid-based modules: Operate on raster data (GRIB/NetCDF) and produce GeoJSON
"""

import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from .registry import CellModuleRegistry, GridModuleRegistry, ModuleRegistry
from .engine import initialize_modules
from EdgeWARN.alerts import AlertManager
from .util.history_cache import CellHistoryCache

# Import modules to trigger auto-registration
from . import modules  # noqa: F401

# File system utilities
import util.file as fs


def run_ctam(cells: List[Dict[str, Any]], timestamp: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Run all registered CTAM modules on the provided storm cells.
    
    First runs cell-based modules, then runs grid-based modules.
    Grid results are attached to the first cell for output.
    
    This is the single entry point for CTAM processing in the pipeline.
    Modules are automatically discovered from the registry.
    
    Args:
        cells: List of storm cell dictionaries, each with 'properties' key.
        timestamp: Optional scan timestamp (e.g. YYYYMMDD-HHMMSS) to save as an alert snapshot.
        
    Returns:
        The same list of cells with 'modules' populated by each registered module.
    """
    start_time = time.time()
    
    # Clean up expired alerts before running modules
    # This prevents expired alerts from piling up on disk
    try:
        AlertManager.cleanup_expired()
    except Exception as e:
        print(f"[CTAM] Failed to clean up expired alerts: {e}")
    
    # GeoMapper is now integrated into NWS Ingest, so we don't need to run it here.
    # Data in fs.MRMS_NWS_DIR is already processed with polygons.

    print("[CTAM] Starting CTAM pipeline...")
    
    # Get registered modules
    cell_modules = CellModuleRegistry.get_all()
    grid_modules = GridModuleRegistry.get_all()
    
    if not cell_modules and not grid_modules:
        print("[CTAM] No modules registered. Skipping processing.")
        return cells
    
    print(f"[CTAM] Cell modules: {list(cell_modules.keys())}")
    print(f"[CTAM] Grid modules: {list(grid_modules.keys())}")
    print(f"[CTAM] Processing {len(cells)} storm cell(s)...")
    
    # Step 1: Run cell-based modules
    
    # Pre-initialize history cache
    hist_cache = CellHistoryCache()
    active_cell_ids = [c["id"] for c in cells if "id" in c]
    hist_cache.preload_active(active_cells=active_cell_ids)
    
    cell_success_count = 0
    cell_error_count = 0
    
    for cell_idx, cell in enumerate(cells):
        # Initialize modules dict
        module_names = list(cell_modules.keys())
        initialize_modules(cell, module_names)
        pending_cell_alerts = []
        
        # Run each cell-based module
        for module in cell_modules.values():
            try:
                module_start = time.time()
                module.run(cell, environment=None, history_cache=hist_cache)
                module_elapsed = time.time() - module_start
                cell_success_count += 1
            except Exception as e:
                # Store error in modules dict
                cell["modules"][module.name] = {
                    "status": "error",
                    "error": str(e)
                }
                print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: Module '{module.name}' FAILED: {e}")
                cell_error_count += 1

            # Collect and publish alerts from cell module
            try:
                module_alerts = module.alerts(cell)
                if module_alerts:
                    pending_cell_alerts.extend(module_alerts)
            except Exception as e:
                print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: Alerts from '{module.name}' FAILED: {e}")

        if pending_cell_alerts:
            AlertManager.publish_many(pending_cell_alerts)
    
    # Step 2: Run grid-based modules
    grid_results = {}
    grid_success_count = 0
    grid_error_count = 0
    grid_alert_count = 0
    
    for module in grid_modules.values():
        try:
            module_start = time.time()
            result = module.run()
            module_elapsed = time.time() - module_start
            grid_results[module.name] = result
            grid_success_count += 1
            print(f"[CTAM]   Grid module '{module.name}' completed in {module_elapsed:.3f}s")
            
            # Generate alerts from grid results
            if result.get("features") and result["features"].get("features"):
                features = result["features"]["features"]
                alerts = module.alerts(features)
                if alerts:
                    grid_alert_count += AlertManager.publish_many(alerts)
                    
        except Exception as e:
            grid_results[module.name] = {"status": "error", "error": str(e)}
            print(f"[CTAM]   Grid module '{module.name}' FAILED: {e}")
            grid_error_count += 1
    
    # Attach grid results to first cell (or create placeholder)
    if cells:
        cells[0]["modules"]["_grid_outputs"] = grid_results
    else:
        # No cells, but grid modules ran - store results separately
        cells = [{"modules": {"_grid_outputs": grid_results}}]
    
    total_elapsed = time.time() - start_time
    print(f"[CTAM] Pipeline complete: {cell_success_count} cell success, {cell_error_count} cell error(s), {grid_success_count} grid success, {grid_error_count} grid error(s), {grid_alert_count} grid alert(s) in {total_elapsed:.3f}s")
    
    # Generate timestamp snapshot of active alerts if provided
    if timestamp:
        try:
            AlertManager.create_snapshot(timestamp)
        except Exception as e:
            print(f"[CTAM] Failed to create alert snapshot for {timestamp}: {e}")

    return cells

