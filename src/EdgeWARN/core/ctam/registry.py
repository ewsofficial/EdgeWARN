"""
CTAM Module Registry

Provides a central registry for analysis modules, allowing automatic
discovery and registration without modifying the main execution script.
"""

from typing import Dict, Type, List
from .interface import AnalysisModule


class ModuleRegistry:
    """
    Central registry for CTAM analysis modules.
    
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
