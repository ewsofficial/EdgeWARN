"""
CTAM Modules Package

Automatically registers all available analysis modules on import.
"""

from ..registry import ModuleRegistry
from .Footprint import FootprintModule
from .StormCast import StormCastModule
from .CellAlert import CellAlertModule
from .MorphoWind import MorphoWindModule

# Register all modules in execution order
# 1. Footprint (ensures base polygons are ready)
ModuleRegistry.register(FootprintModule())

# 2. Main Analysis Modules
ModuleRegistry.register(StormCastModule())
ModuleRegistry.register(MorphoWindModule())

# 3. Post-Analysis Modules
# CellAlert depends on StormCast results
ModuleRegistry.register(CellAlertModule())

# GeoMapper is a file processor, not a per-entry module, so we don't register it
# It's accessed directly via: from EdgeWARN.core.ingest.nws.geomapper import process_warning

__all__ = [
    "StormCastModule",
    "MorphoWindModule",
]
