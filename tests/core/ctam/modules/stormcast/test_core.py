"""
Tests for StormCast core engine
"""

import pytest
from EdgeWARN.core.ctam.modules.StormCast.core.core import StormCastEngine, ForecastResult
from EdgeWARN.core.ctam.modules.StormCast.core.types import (
    StormState,
    EnvironmentProfile
)
from EdgeWARN.core.ctam.modules.StormCast.core.config import DEFAULT_LEAD_TIMES


class TestStormCastEngine:
    """Tests for StormCastEngine class"""

    def test_initialization(self):
        """Test engine initialization"""
        engine = StormCastEngine()
        
        assert engine.reference_lat == 35.0
        assert engine.reference_lon == -97.0
        assert engine.motion_history == []
        assert engine.position_history == []
        assert engine.environment is None
        assert engine.last_update_time is None

    def test_initialization_custom_reference(self):
        """Test initialization with custom reference coordinates"""
        engine = StormCastEngine(reference_lat=40.0, reference_lon=-100.0)
        
        assert engine.reference_lat == 40.0
        assert engine.reference_lon == -100.0

    def test_set_environment(self):
        """Test setting environment profile"""
        engine = StormCastEngine()
        profile = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        
        engine.set_environment(profile)
        
        assert engine.environment == profile

    def test_add_observation(self):
        """Test adding radar observations"""
        engine = StormCastEngine()
        
        # Add first observation (dt=0 won't add to motion_history)
        engine.add_observation(
            x=1000.0, y=2000.0, dt_seconds=0.0,
            echo_top_30=10.0, echo_top_50=8.0
        )
        
        # Add second observation with dt > 0
        engine.add_observation(
            x=1060.0, y=2030.0, dt_seconds=60.0,
            echo_top_30=10.0, echo_top_50=8.0
        )
        
        assert len(engine.position_history) == 2
        assert engine.current_h_core == pytest.approx(9.0, abs=0.1)

    def test_add_multiple_observations(self):
        """Test adding multiple observations"""
        engine = StormCastEngine()
        
        # Add three observations
        engine.add_observation(1000.0, 2000.0, 0.0, 10.0, 8.0)  # First, dt=0
        engine.add_observation(1060.0, 2030.0, 60.0, 10.0, 8.0)  # Second
        engine.add_observation(1120.0, 2060.0, 60.0, 10.0, 8.0)  # Third
        
        assert len(engine.position_history) == 3

    def test_add_observation_with_timestamp(self):
        """Test adding observation with timestamp"""
        engine = StormCastEngine()
        from datetime import datetime, timezone
        
        ts = datetime(2023, 10, 15, 14, 30, tzinfo=timezone.utc)
        
        engine.add_observation(
            x=1000.0, y=2000.0, dt_seconds=0.0,
            timestamp=ts
        )
        
        assert engine.last_update_time == ts

    def test_generate_forecast(self):
        """Test forecast generation"""
        engine = StormCastEngine()
        
        # Add observations with significant movement to pass velocity thresholds
        # Velocity thresholds: MIN=2.0, MAX=50.0 m/s
        # With dt=60s, need displacement between 120m and 3000m
        engine.add_observation(0.0, 0.0, 0.0, 10.0, 8.0)
        engine.add_observation(200.0, 100.0, 60.0, 10.0, 8.0)  # velocity ~3.7 m/s
        engine.add_observation(400.0, 200.0, 60.0, 10.0, 8.0)  # velocity ~3.7 m/s
        
        # Set environment
        profile = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        engine.set_environment(profile)
        
        # Generate forecast
        result = engine.generate_forecast()
        
        assert isinstance(result, ForecastResult)
        assert hasattr(result, 'u')
        assert hasattr(result, 'v')
        assert hasattr(result, 'forecast_cones')

    def test_generate_forecast_without_environment(self):
        """Test forecast generation without environment raises error"""
        engine = StormCastEngine()
        engine.add_observation(1000.0, 2000.0, 0.0, 10.0, 8.0)
        engine.add_observation(1060.0, 2030.0, 60.0, 10.0, 8.0)
        
        # Should raise error without environment
        with pytest.raises(ValueError, match="Environment profile not set"):
            engine.generate_forecast()

    def test_generate_forecast_insufficient_history(self):
        """Test forecast generation with insufficient history raises error"""
        engine = StormCastEngine()
        
        profile = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        engine.set_environment(profile)
        
        # Only one observation, not enough for motion history
        engine.add_observation(1000.0, 2000.0, 0.0, 10.0, 8.0)
        
        # Should raise error with insufficient history
        with pytest.raises(ValueError, match="Insufficient motion history"):
            engine.generate_forecast()
