"""
CTAM Module Registry

Provides a central registry for analysis modules, allowing automatic
discovery and registration without modifying the main execution script.

Two registry classes are provided:
- CellModuleRegistry: For cell-based CTAM modules (operate on storm cells)
- GridModuleRegistry: For grid-based CTAM modules (operate on raster data)
"""

from typing import Dict, Type, List
from .interface import AnalysisModule, GridAnalysisModule


class CellModuleRegistry:
    """
    Registry for cell-based CTAM modules.
    
    These modules operate on storm cell data and modify cells in-place.
    Modules register themselves on import, and the registry provides
    access to all registered modules for the execution engine.
    """
    _modules: Dict[str, AnalysisModule] = {}
    
    @classmethod
    def register(cls, module: AnalysisModule) -> None:
        """
        Register an analysis module.
        
        Args:
            module: An instance of AnalysisModule to register.
        """
        cls._modules[module.name] = module
    
    @classmethod
    def get(cls, name: str) -> AnalysisModule:
        """
        Get a specific module by name.
        
        Args:
            name: The module name.
            
        Returns:
            The registered AnalysisModule instance.
            
        Raises:
            KeyError: If no module with that name is registered.
        """
        if name not in cls._modules:
            raise KeyError(f"No module registered with name: {name}")
        return cls._modules[name]
    
    @classmethod
    def get_all(cls) -> Dict[str, AnalysisModule]:
        """
        Get all registered modules.
        
        Returns:
            Dict mapping module names to module instances.
        """
        return cls._modules.copy()
    
    @classmethod
    def list_names(cls) -> List[str]:
        """
        List all registered module names.
        
        Returns:
            List of registered module names.
        """
        return list(cls._modules.keys())
    
    @classmethod
    def clear(cls) -> None:
        """Clear all registered modules (mainly for testing)."""
        cls._modules.clear()


class GridModuleRegistry:
    """
    Registry for grid-based CTAM modules.
    
    These modules operate on raw data files (GRIB/NetCDF) and produce
    GeoJSON features rather than per-cell results.
    """
    _modules: Dict[str, GridAnalysisModule] = {}
    
    @classmethod
    def register(cls, module: GridAnalysisModule) -> None:
        """
        Register a grid-based analysis module.
        
        Args:
            module: An instance of GridAnalysisModule to register.
        """
        cls._modules[module.name] = module
    
    @classmethod
    def get(cls, name: str) -> GridAnalysisModule:
        """
        Get a specific module by name.
        
        Args:
            name: The module name.
            
        Returns:
            The registered GridAnalysisModule instance.
            
        Raises:
            KeyError: If no module with that name is registered.
        """
        if name not in cls._modules:
            raise KeyError(f"No grid module registered with name: {name}")
        return cls._modules[name]
    
    @classmethod
    def get_all(cls) -> Dict[str, GridAnalysisModule]:
        """
        Get all registered grid modules.
        
        Returns:
            Dict mapping module names to module instances.
        """
        return cls._modules.copy()
    
    @classmethod
    def list_names(cls) -> List[str]:
        """
        List all registered grid module names.
        
        Returns:
            List of registered module names.
        """
        return list(cls._modules.keys())
    
    @classmethod
    def clear(cls) -> None:
        """Clear all registered modules (mainly for testing)."""
        cls._modules.clear()


# Backward compatibility alias
ModuleRegistry = CellModuleRegistry
