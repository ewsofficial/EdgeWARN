"""
Integration tests for CTAM analysis pipeline
"""

import pytest
from unittest.mock import MagicMock, patch
from EdgeWARN.ctam import engine
from EdgeWARN.ctam.interface import AnalysisModule
from EdgeWARN.ctam.modules.StormCast.core.types import StormState, EnvironmentProfile


class MockAnalysisModule(AnalysisModule):
    """Mock module for testing"""
    
    def __init__(self, name):
        self._name = name
    
    @property
    def name(self):
        return self._name
    
    def run(self, storm_entry, environment=None):
        storm_entry.setdefault('modules', {})
        storm_entry['modules'][self._name] = {'test': True, 'environment': environment}


class TestCTAMPipeline:
    """Tests for CTAM analysis pipeline"""

    @pytest.fixture
    def mock_io(self):
        """Create a mock IOManager"""
        return MagicMock()

    @pytest.fixture
    def sample_storm_cells(self):
        """Create sample storm cells for testing"""
        return [
            {
                "id": 101,
                "centroid": [35.0, -97.0],
                "bbox": [[34.9, -97.1], [34.9, -96.9], [35.1, -96.9], [35.1, -97.1]],
                "num_gates": 50,
                "max_refl": 55.0,
                "timestamp": "2023-10-15T14:30:00",
                "properties": {}
            },
            {
                "id": 102,
                "centroid": [36.0, -96.0],
                "bbox": [[35.9, -96.1], [35.9, -95.9], [36.1, -95.9], [36.1, -96.1]],
                "num_gates": 30,
                "max_refl": 45.0,
                "timestamp": "2023-10-15T14:30:00",
                "properties": {}
            }
        ]

    @pytest.fixture
    def sample_environment_profile(self):
        """Create sample environment profile"""
        return EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )

    def test_initialize_modules_on_snapshot(self, sample_storm_cells):
        """Test that modules are initialized on snapshot data"""
        modules = {
            "module1": MockAnalysisModule("module1"),
            "module2": MockAnalysisModule("module2")
        }
        
        snapshot_data = {"features": sample_storm_cells}
        
        engine.initialize_modules(snapshot_data["features"][0], ["module1", "module2"])
        engine.initialize_modules(snapshot_data["features"][1], ["module1", "module2"])
        
        # Verify modules were initialized
        for cell in snapshot_data["features"]:
            assert "modules" in cell
            assert "module1" in cell["modules"]
            assert "module2" in cell["modules"]

    def test_initialize_modules_on_history(self, sample_storm_cells):
        """Test that modules are initialized on history data"""
        modules = {
            "module1": MockAnalysisModule("module1"),
            "module2": MockAnalysisModule("module2")
        }
        
        history_data = sample_storm_cells
        
        for cell in history_data:
            engine.initialize_modules(cell, ["module1", "module2"])
        
        # Verify modules were initialized
        for cell in history_data:
            assert "modules" in cell
            assert "module1" in cell["modules"]
            assert "module2" in cell["modules"]

    def test_run_modules_on_snapshot(self, sample_storm_cells, sample_environment_profile):
        """Test running modules on snapshot"""
        modules = {
            "module1": MockAnalysisModule("module1"),
            "module2": MockAnalysisModule("module2")
        }
        
        snapshot_data = {"features": sample_storm_cells}
        
        engine.run_modules(snapshot_data, modules, list(modules.keys()), environment=sample_environment_profile)
        
        # Verify modules were run
        for cell in snapshot_data["features"]:
            assert cell["modules"]["module1"]["test"] is True
            assert cell["modules"]["module2"]["test"] is True

    def test_run_modules_on_history(self, sample_storm_cells, sample_environment_profile):
        """Test running modules on history"""
        modules = {
            "module1": MockAnalysisModule("module1"),
            "module2": MockAnalysisModule("module2")
        }
        
        history_data = sample_storm_cells
        
        engine.run_modules(history_data, modules, list(modules.keys()), environment=sample_environment_profile)
        
        # Verify modules were run
        for cell in history_data:
            assert cell["modules"]["module1"]["test"] is True
            assert cell["modules"]["module2"]["test"] is True

    def test_run_modules_with_environment(self, sample_storm_cells, sample_environment_profile):
        """Test that environment is passed to modules"""
        modules = {
            "module1": MockAnalysisModule("module1"),
            "module2": MockAnalysisModule("module2")
        }
        
        snapshot_data = {"features": sample_storm_cells}
        env = sample_environment_profile
        
        engine.run_modules(snapshot_data, modules, list(modules.keys()), environment=env)
        
        # Verify environment was passed
        for cell in snapshot_data["features"]:
            assert cell["modules"]["module1"]["environment"] == env

    def test_run_modules_handles_errors(self, sample_storm_cells):
        """Test that module errors are caught and logged"""
        class ErrorModule(AnalysisModule):
            def __init__(self):
                self._name = "error_module"
            
            @property
            def name(self):
                return self._name
            
            def run(self, storm_entry, environment=None):
                raise Exception("Module error")
        
        modules = {
            "error_module": ErrorModule(),
            "module2": MockAnalysisModule("module2")
        }
        
        snapshot_data = {"features": sample_storm_cells}
        
        # Should not raise exception
        engine.run_modules(snapshot_data, modules, list(modules.keys()))
        
        # Verify error was logged to entry
        for cell in snapshot_data["features"]:
            assert "error" in cell["modules"]["error_module"]

    def test_run_modules_empty_data(self):
        """Test running modules on empty data"""
        modules = {
            "module1": MockAnalysisModule("module1"),
            "module2": MockAnalysisModule("module2")
        }
        
        # Empty data
        empty_data = []
        
        # Should not raise exception
        engine.run_modules(empty_data, modules, list(modules.keys()))
        
        # Should return without error
        assert empty_data == []

    def test_run_modules_invalid_format(self):
        """Test running modules on invalid data format"""
        modules = {
            "module1": MockAnalysisModule("module1"),
            "module2": MockAnalysisModule("module2")
        }
        
        # Invalid data (not dict with features or list)
        invalid_data = "invalid string"
        
        # Should not raise exception
        engine.run_modules(invalid_data, modules, list(modules.keys()))
        
        # Should return without modification
        assert invalid_data == "invalid string"

    def test_module_registry_integration(self, sample_storm_cells):
        """Test that modules can be registered and retrieved"""
        from EdgeWARN.ctam.registry import ModuleRegistry
        
        # Clear registry
        ModuleRegistry.clear()
        
        # Register modules
        mod1 = MockAnalysisModule("test_module")
        mod2 = MockAnalysisModule("test_module2")
        ModuleRegistry.register(mod1)
        ModuleRegistry.register(mod2)
        
        # Verify registration
        assert "test_module" in ModuleRegistry._modules
        
        # Retrieve modules
        retrieved1 = ModuleRegistry.get("test_module")
        retrieved2 = ModuleRegistry.get("test_module2")
        
        assert retrieved1 == mod1
        assert retrieved2 == mod2
        
        # Get all modules
        all_modules = ModuleRegistry.get_all()
        
        assert len(all_modules) == 2
        assert "test_module" in all_modules
        assert "test_module2" in all_modules
