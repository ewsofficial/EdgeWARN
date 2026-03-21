"""
Tests for CTAM interface module
"""

import pytest
from EdgeWARN.ctam.interface import AnalysisModule


class TestAnalysisModule:
    """Tests for AnalysisModule abstract class"""

    def test_cannot_instantiate_directly(self):
        """Test that AnalysisModule cannot be instantiated directly"""
        with pytest.raises(TypeError):
            AnalysisModule()

    def test_subclass_must_implement_name(self):
        """Test that subclasses must implement name property"""
        
        class InvalidModule(AnalysisModule):
            @property
            def name(self):
                raise NotImplementedError()
            
            def run(self, storm_entry, environment=None):
                pass
        
        # Should be able to instantiate
        module = InvalidModule()
        
        # But accessing name should raise NotImplementedError
        with pytest.raises(NotImplementedError):
            _ = module.name

    def test_subclass_must_implement_run(self):
        """Test that subclasses must implement run method"""
        
        class InvalidModule(AnalysisModule):
            @property
            def name(self):
                return "test"
            
            def run(self, storm_entry, environment=None):
                raise NotImplementedError()
        
        module = InvalidModule()
        
        # Calling run should raise NotImplementedError
        with pytest.raises(NotImplementedError):
            module.run({}, {})

    def test_valid_subclass(self):
        """Test that a valid subclass can be instantiated"""
        
        class ValidModule(AnalysisModule):
            @property
            def name(self):
                return "test_module"
            
            def run(self, storm_entry, environment=None):
                storm_entry.setdefault('modules', {})
                storm_entry['modules'][self.name] = {'test': True}
        
        # Should be able to instantiate
        module = ValidModule()
        
        # Should have correct name
        assert module.name == "test_module"
        
        # Should be able to run
        entry = {}
        module.run(entry)
        
        assert 'test_module' in entry['modules']
        assert entry['modules']['test_module'] == {'test': True}

    def test_run_with_environment(self):
        """Test that run method receives environment parameter"""
        
        class ValidModule(AnalysisModule):
            @property
            def name(self):
                return "test_module"
            
            def run(self, storm_entry, environment=None):
                storm_entry.setdefault('modules', {})
                storm_entry['modules'][self.name] = {'environment': environment}
        
        module = ValidModule()
        entry = {}
        env = {'test': 'data'}
        
        module.run(entry, environment=env)
        
        assert entry['modules']['test_module']['environment'] == env

    def test_run_without_environment(self):
        """Test that run works without environment parameter"""
        
        class ValidModule(AnalysisModule):
            @property
            def name(self):
                return "test_module"
            
            def run(self, storm_entry, environment=None):
                storm_entry.setdefault('modules', {})
                storm_entry['modules'][self.name] = {'environment': environment}
        
        module = ValidModule()
        entry = {}
        
        module.run(entry)
        
        assert entry['modules']['test_module']['environment'] is None
