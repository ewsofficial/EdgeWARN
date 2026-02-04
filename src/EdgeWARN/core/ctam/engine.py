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
    module_names: List[str],
    **kwargs
) -> None:
    """
    Apply all enabled modules to storm data.

    Parameters:
    - data: dict containing 'features' (snapshot) or list of entries (history)
    - modules: dict mapping module_name -> AnalysisModule instance
    - module_names: list of enabled module names to run (in order)
    - **kwargs: additional arguments passed to module.run() (e.g. environment)
    """
    # Determine if we are working with a snapshot (features list) or history (list of entries)
    entries = []
    if isinstance(data, dict) and "features" in data and isinstance(data["features"], list):
        entries = data["features"]
    elif isinstance(data, list):
        entries = data
    else:
        return

    for entry in entries:
        initialize_modules(entry, module_names)
        for name in module_names:
            if name in modules:
                try:
                    modules[name].run(entry, **kwargs)
                except Exception as e:
                    entry["modules"][name]["error"] = str(e)
