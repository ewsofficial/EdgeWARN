from typing import List, Dict, Any, Union
from .interface import AnalysisModule

def initialize_modules(entry: Dict[str, Any], module_names: List[str]) -> None:
    """
    Ensure a 'modules' key exists in a storm entry and
    pre-create empty namespaces for all modules.
    
    Args:
        entry: A single storm cell data dictionary (snapshot or history entry).
        module_names: List of module names to initialize.
    """
    entry.setdefault("modules", {})
    if not isinstance(entry["modules"], dict):
        entry["modules"] = {}
        
    for name in module_names:
        entry["modules"].setdefault(name, {})

def run_modules(
    data: Union[Dict[str, Any], List[Dict[str, Any]]], 
    modules: Dict[str, AnalysisModule], 
    module_names: List[str]
) -> None:
    """
    Apply all enabled modules to storm data.

    Parameters:
    - data: dict containing either:
        - 'features' key: snapshot format (GeoJSON-like)
        - list of entries: cell history format
    - modules: dict mapping module_name -> AnalysisModule instance
    - module_names: list of enabled module names to run (in order)
    """
    # Determine if we are working with a snapshot (features list) or history (list of entries)
    entries = []
    if isinstance(data, dict) and "features" in data and isinstance(data["features"], list):
        entries = data["features"]
    elif isinstance(data, list):
        entries = data
    else:
        # Fallback: if data is a single dict but not "features", treat as one entry?
        # Or maybe it's invalid. For now, assume if it's not the above, it might be a single entry context
        # but the spec says "snapshot features" or "cell history".
        # Let's stick to the spec.
        return

    for entry in entries:
        initialize_modules(entry, module_names)
        for name in module_names:
            if name in modules:
                # We assume modules might depend on previous ones, so we run in order
                # but independent modules is the goal.
                try:
                    modules[name].run(entry)
                except Exception as e:
                    # Log error in the module output to avoid crashing the pipeline
                    entry["modules"][name]["error"] = str(e)
