"""
Tests for StormCast core engine
"""

import pytest
from EdgeWARN.core.ctam.modules.StormCast.core.core import StormCastEngine
from EdgeWARN.core.ctam.modules.StormCast.core.types import (
    StormState,
    EnvironmentProfile,
    ForecastResult
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
            u850=10.0, v850=5.0,
            u700=15.0, v700=8.0,
            u500=20.0, v500=10.0,
            u250=25.0, v250=12.0
        )
        
        engine.set_environment(profile)
        
        assert engine.environment == profile

    def test_add_observation(self):
        """Test adding radar observations"""
        engine = StormCastEngine()
        
        # Add first observation
        engine.add_observation(
            x=1000.0, y=2000.0, dt_seconds=60.0,
            echo_top_30=10.0, echo_top_50=8.0
        )
        
        assert len(engine.motion_history) == 1
        assert len(engine.position_history) == 1
        assert engine.current_h_core == pytest.approx(9.0, abs=0.1)

    def test_add_multiple_observations(self):
        """Test adding multiple observations"""
        engine = StormCastEngine()
        
        # Add three observations
        engine.add_observation(1000.0, 2000.0, 60.0, 10.0, 8.0)
        engine.add_observation(1060.0, 2030.0, 60.0, 10.0, 8.0)
        engine.add_observation(1120.0, 2060.0, 60.0, 10.0, 8.0)
        
        assert len(engine.motion_history) == 3
        assert len(engine.position_history) == 3

    def test_add_observation_with_timestamp(self):
        """Test adding observation with timestamp"""
        engine = StormCastEngine()
        from datetime import datetime, timezone
        
        ts = datetime(2023, 10, 15, 14, 30, tzinfo=timezone.utc)
        
        engine.add_observation(
            x=1000.0, y=2000.0, dt_seconds=60.0,
            timestamp=ts
        )
        
        assert engine.last_update_time == ts

    def test_generate_forecast(self):
        """Test forecast generation"""
        engine = StormCastEngine()
        
        # Add observation
        engine.add_observation(1000.0, 2000.0, 60.0, 10.0, 8.0)
        
        # Set environment
        profile = EnvironmentProfile(
            u850=10.0, v850=5.0,
            u700=15.0, v700=8.0,
            u500=20.0, v500=10.0,
            u250=25.0, v250=12.0
        )
        engine.set_environment(profile)
        
        # Generate forecast
        result = engine.generate_forecast()
        
        assert isinstance(result, ForecastResult)
        assert hasattr(result, 'u')
        assert hasattr(result, 'v')
        assert hasattr(result, 'forecast_cones')
        assert len(result.forecast_cones) == len(DEFAULT_LEAD_TIMES)

    def test_generate_forecast_without_environment(self):
        """Test forecast generation without environment"""
        engine = StormCastEngine()
        engine.add_observation(1000.0, 2000.0, 60.0, 10.0, 8.0)
        
        # Should still work (uses observed motion only)
        result = engine.generate_forecast()
        
        assert isinstance(result, ForecastResult)

    def test_get_current_state(self):
        """Test getting current storm state"""
        engine = StormCastEngine()
        engine.add_observation(1000.0, 2000.0, 60.0, 10.0, 8.0)
        
        state = engine.get_current_state()
        
        assert isinstance(state, StormState)
        assert state.x == 1000.0
        assert state.y == 2000.0

    def test_get_current_velocity(self):
        """Test getting current velocity"""
        engine = StormCastEngine()
        engine.add_observation(1000.0, 2000.0, 60.0, 10.0, 8.0)
        
        u, v = engine.get_current_velocity()
        
        # Velocity should be approximately (60, 30) m/s
        assert u == pytest.approx(60.0, abs=1.0)
        assert v == pytest.approx(30.0, abs=1.0)

    def test_get_speed(self):
        """Test getting current speed"""
        engine = StormCastEngine()
        engine.add_observation(1000.0, 2000.0, 60.0, 10.0, 8.0)
        
        speed = engine.get_speed()
        
        # Speed = sqrt(60^2 + 30^2) ≈ 67.08 m/s
        assert speed == pytest.approx(67.08, abs=0.01)

    def test_get_speed_zero_velocity(self):
        """Test speed with zero velocity"""
        engine = StormCastEngine()
        engine.add_observation(1000.0, 2000.0, 60.0, 0.0, 0.0)
        
        speed = engine.get_speed()
        
        assert speed == 0.0

    def test_reset(self):
        """Test resetting engine state"""
        engine = StormCastEngine()
        engine.add_observation(1000.0, 2000.0, 60.0, 10.0, 8.0)
        
        # Reset
        engine.reset()
        
        # History should be cleared
        assert len(engine.motion_history) == 0
        assert len(engine.position_history) == 0
        assert engine.last_update_time is None
