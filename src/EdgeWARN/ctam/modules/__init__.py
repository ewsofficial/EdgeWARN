"""
CTAM Modules Package

Automatically registers all available analysis modules on import.
"""

from ..registry import ModuleRegistry, GridModuleRegistry
from .StormCast import StormCastModule
from .MorphoWind import MorphoWindModule
from .FLOHAR import FLOHARModule

# Register all modules in execution order
# 1. Base Tracking Features (e.g. History if needed)

# 2. Main Analysis Modules (Cell-based)
ModuleRegistry.register(StormCastModule())
ModuleRegistry.register(MorphoWindModule())

# 3. Grid-based Modules
GridModuleRegistry.register(FLOHARModule())

# 4. Post-Analysis Modules

# GeoMapper is a file processor, not a per-entry module, so we don't register it
# It's accessed directly via: from EdgeWARN.ingest.nws.geomapper import process_warning

__all__ = [
    "StormCastModule",
    "MorphoWindModule",
    "FLOHARModule",
]
