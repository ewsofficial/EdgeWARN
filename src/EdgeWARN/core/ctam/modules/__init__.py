"""
CTAM Modules Package

Automatically registers all available analysis modules on import.
"""

from ..registry import ModuleRegistry
from .StormCast import StormCastModule
from .GeoMapper import process_file as geo_mapper_process_file

# Register all modules
ModuleRegistry.register(StormCastModule())

# GeoMapper is a file processor, not a per-entry module, so we don't register it
# It's accessed directly via: from EdgeWARN.core.ctam.modules.GeoMapper import process_file

__all__ = [
    "StormCastModule",
    "geo_mapper_process_file",
]
