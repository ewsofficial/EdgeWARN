"""
Tests for CTAM module registry
"""

import importlib

import pytest
from EdgeWARN.ctam.interface import AnalysisModule
import EdgeWARN.ctam.modules as ctam_modules
from EdgeWARN.ctam.registry import GridModuleRegistry, ModuleRegistry


class MockModule(AnalysisModule):
    """Mock module for testing"""
    
    def __init__(self, name):
        self._name = name
    
    @property
    def name(self):
        return self._name
    
    def run(self, storm_entry, environment=None):
        storm_entry.setdefault('modules', {})
        storm_entry['modules'][self._name] = {'test': True}


class TestModuleRegistry:
    """Tests for ModuleRegistry class"""

    def test_register_module(self):
        """Test registering a module"""
        ModuleRegistry.clear()
        
        module = MockModule("test_module")
        ModuleRegistry.register(module)
        
        assert "test_module" in ModuleRegistry._modules
        assert ModuleRegistry._modules["test_module"] == module

    def test_register_multiple_modules(self):
        """Test registering multiple modules"""
        ModuleRegistry.clear()
        
        mod1 = MockModule("module1")
        mod2 = MockModule("module2")
        mod3 = MockModule("module3")
        
        ModuleRegistry.register(mod1)
        ModuleRegistry.register(mod2)
        ModuleRegistry.register(mod3)
        
        assert len(ModuleRegistry._modules) == 3
        assert "module1" in ModuleRegistry._modules
        assert "module2" in ModuleRegistry._modules
        assert "module3" in ModuleRegistry._modules

    def test_register_overwrites_existing(self):
        """Test that registering same name overwrites"""
        ModuleRegistry.clear()
        
        mod1 = MockModule("test_module")
        mod2 = MockModule("test_module")
        
        ModuleRegistry.register(mod1)
        ModuleRegistry.register(mod2)
        
        # Should have overwritten
        assert ModuleRegistry._modules["test_module"] == mod2

    def test_get_module(self):
        """Test getting a module by name"""
        ModuleRegistry.clear()
        
        module = MockModule("test_module")
        ModuleRegistry.register(module)
        
        retrieved = ModuleRegistry.get("test_module")
        
        assert retrieved == module

    def test_get_nonexistent_module(self):
        """Test getting a module that doesn't exist"""
        ModuleRegistry.clear()
        
        with pytest.raises(KeyError, match="nonexistent"):
            ModuleRegistry.get("nonexistent")

    def test_get_all_modules(self):
        """Test getting all registered modules"""
        ModuleRegistry.clear()
        
        mod1 = MockModule("module1")
        mod2 = MockModule("module2")
        
        ModuleRegistry.register(mod1)
        ModuleRegistry.register(mod2)
        
        all_modules = ModuleRegistry.get_all()
        
        assert len(all_modules) == 2
        assert "module1" in all_modules
        assert "module2" in all_modules
        assert all_modules["module1"] == mod1
        assert all_modules["module2"] == mod2

    def test_get_all_returns_copy(self):
        """Test that get_all returns a copy, not reference"""
        ModuleRegistry.clear()
        
        module = MockModule("test_module")
        ModuleRegistry.register(module)
        
        all1 = ModuleRegistry.get_all()
        all2 = ModuleRegistry.get_all()
        
        # Should be different objects
        assert all1 is not all2

    def test_list_names(self):
        """Test listing module names"""
        ModuleRegistry.clear()
        
        mod1 = MockModule("module1")
        mod2 = MockModule("module2")
        mod3 = MockModule("module3")
        
        ModuleRegistry.register(mod1)
        ModuleRegistry.register(mod2)
        ModuleRegistry.register(mod3)
        
        names = ModuleRegistry.list_names()
        
        assert len(names) == 3
        assert "module1" in names
        assert "module2" in names
        assert "module3" in names

    def test_list_names_empty(self):
        """Test listing names when no modules registered"""
        ModuleRegistry.clear()
        
        names = ModuleRegistry.list_names()
        
        assert names == []

    def test_clear(self):
        """Test clearing all modules"""
        ModuleRegistry.clear()
        
        mod1 = MockModule("module1")
        mod2 = MockModule("module2")
        
        ModuleRegistry.register(mod1)
        ModuleRegistry.register(mod2)
        
        assert len(ModuleRegistry._modules) == 2
        
        ModuleRegistry.clear()
        
        assert len(ModuleRegistry._modules) == 0

    def test_clear_and_reregister(self):
        """Test that modules can be registered after clearing"""
        ModuleRegistry.clear()
        
        mod1 = MockModule("module1")
        ModuleRegistry.register(mod1)
        
        ModuleRegistry.clear()
        assert len(ModuleRegistry._modules) == 0
        
        mod2 = MockModule("module2")
        ModuleRegistry.register(mod2)
        
        assert len(ModuleRegistry._modules) == 1
        assert "module2" in ModuleRegistry._modules

    def test_default_module_registration_excludes_mesocyclone(self):
        """Test that the active CTAM registry omits Mesocyclone for now."""
        ModuleRegistry.clear()
        GridModuleRegistry.clear()

        importlib.reload(ctam_modules)

        cell_module_names = ModuleRegistry.list_names()
        grid_module_names = GridModuleRegistry.list_names()

        assert "StormCast" in cell_module_names
        assert "MorphoWind" in cell_module_names
        assert "Mesocyclone" not in cell_module_names
        assert "FLOHAR" in grid_module_names
