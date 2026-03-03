"""
CTAM Modules Package

Automatically registers all available analysis modules on import.
"""

from ..registry import ModuleRegistry
from .StormCast import StormCastModule
from .MorphoWind import MorphoWindModule

# Register all modules in execution order
# 1. Base Tracking Features (e.g. History if needed)

# 2. Main Analysis Modules
ModuleRegistry.register(StormCastModule())
ModuleRegistry.register(MorphoWindModule())

# 3. Post-Analysis Modules

# GeoMapper is a file processor, not a per-entry module, so we don't register it
# It's accessed directly via: from EdgeWARN.core.ingest.nws.geomapper import process_warning

__all__ = [
    "StormCastModule",
    "MorphoWindModule",
]
