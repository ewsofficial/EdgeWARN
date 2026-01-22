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

# Import modules to trigger auto-registration
from . import modules  # noqa: F401
from .modules.GeoMapper import process_file as geomapper_process_file

# File system utilities
import util.file as fs


def run_geomapper() -> Optional[Path]:
    """
    Process the latest NWS_RAW file using GeoMapper.
    
    Finds the most recent file in NWS_RAW_DIR, processes it with GeoMapper
    (geocode-to-polygon mapping, junk key removal), and saves to NWS_DIR.
    
    Returns:
        Path to the processed output file, or None if no input file found.
    """
    start_time = time.time()
    print("[CTAM/GeoMapper] Starting GeoMapper processing...")
    
    # Ensure directories exist
    if not fs.MRMS_NWS_RAW_DIR.exists():
        print(f"[CTAM/GeoMapper] NWS_RAW_DIR does not exist: {fs.MRMS_NWS_RAW_DIR}")
        return None
    
    fs.MRMS_NWS_DIR.mkdir(parents=True, exist_ok=True)
    
    # Find latest raw file
    raw_files = sorted(
        [f for f in fs.MRMS_NWS_RAW_DIR.glob("*.json") if f.is_file()],
        key=lambda f: f.stat().st_mtime,
        reverse=True
    )
    
    if not raw_files:
        print("[CTAM/GeoMapper] No raw NWS files found.")
        return None
    
    input_path = raw_files[0]
    
    # Generate output filename (same name as input)
    output_path = fs.MRMS_NWS_DIR / input_path.name
    
    print(f"[CTAM/GeoMapper] Processing: {input_path.name}")
    
    try:
        count = geomapper_process_file(input_path, output_path)
        elapsed = time.time() - start_time
        print(f"[CTAM/GeoMapper] Processed {count} warnings in {elapsed:.3f}s")
        print(f"[CTAM/GeoMapper] Output: {output_path}")
        return output_path
    except Exception as e:
        print(f"[CTAM/GeoMapper] ERROR: {e}")
        return None


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
    
    # Run GeoMapper first to prepare NWS data
    try:
        run_geomapper()
    except Exception as e:
        print(f"[CTAM] WARN: GeoMapper execution failed: {e}")

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
    
    for cell_idx, cell in enumerate(cells):
        # Initialize modules dict
        initialize_modules(cell, module_names)
        
        # Run each module
        for module in registered_modules.values():
            try:
                module_start = time.time()
                module.run(cell)
                module_elapsed = time.time() - module_start
                print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: Module '{module.name}' completed in {module_elapsed:.3f}s")
                success_count += 1
            except Exception as e:
                # Store error in modules dict
                cell["modules"][module.name] = {
                    "status": "error",
                    "error": str(e)
                }
                print(f"[CTAM]   Cell {cell_idx + 1}/{len(cells)}: Module '{module.name}' FAILED: {e}")
                error_count += 1
    
    total_elapsed = time.time() - start_time
    print(f"[CTAM] Pipeline complete: {success_count} success, {error_count} error(s) in {total_elapsed:.3f}s")
    
    return cells

