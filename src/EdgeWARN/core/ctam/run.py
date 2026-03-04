"""
CTAM Pipeline Entry Point

Provides a single entry point for running all registered CTAM modules
on storm cell data.
"""

import time
from pathlib import Path
from typing import List, Dict, Any, Optional
from .registry import ModuleRegistry
from .engine import initialize_modules
from EdgeWARN.core.alerts import AlertManager

# Import modules to trigger auto-registration
from . import modules  # noqa: F401

# File system utilities
import util.file as fs


def run_ctam(cells: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Run all registered CTAM modules on the provided storm cells.
    
    This is the single entry point for CTAM processing in the pipeline.
    Modules are automatically discovered from the registry.
    
    Args:
        cells: List of storm cell dictionaries, each with 'properties' key.
        
    Returns:
        The same list of cells with 'modules' populated by each registered module.
    """
    start_time = time.time()
    
    # GeoMapper is now integrated into NWS Ingest, so we don't need to run it here.
    # Data in fs.MRMS_NWS_DIR is already processed with polygons.

    print("[CTAM] Starting CTAM pipeline...")
    
    registered_modules = ModuleRegistry.get_all()
    
    if not registered_modules:
        print("[CTAM] No modules registered. Skipping processing.")
        return cells
    
    module_names = list(registered_modules.keys())
    print(f"[CTAM] Discovered {len(registered_modules)} registered module(s): {module_names}")
    print(f"[CTAM] Processing {len(cells)} storm cell(s)...")
    
    success_count = 0
    error_count = 0
    alert_count = 0
    
    for cell_idx, cell in enumerate(cells):
        # Initialize modules dict
        initialize_modules(cell, module_names)
        
        # Run each module
        for module in registered_modules.values():
            try:
                module_start = time.time()
                module.run(cell)
                module_elapsed = time.time() - module_start
                success_count += 1
            except Exception as e:
                # Store error in modules dict
                cell["modules"][module.name] = {
                    "status": "error",
                    "error": str(e)
                }
                print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: Module '{module.name}' FAILED: {e}")
                error_count += 1

            # Collect and publish alerts from module
            try:
                module_alerts = module.alerts(cell)
                if module_alerts:
                    alert_count += AlertManager.publish_many(module_alerts)
            except Exception as e:
                print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: Alerts from '{module.name}' FAILED: {e}")
    
    total_elapsed = time.time() - start_time
    print(f"[CTAM] Pipeline complete: {success_count} success, {error_count} error(s), {alert_count} alert(s) in {total_elapsed:.3f}s")
    
    return cells


