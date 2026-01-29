"""
Tests for StormCast forecast module
"""

import pytest
from EdgeWARN.core.ctam.modules.StormCast.core.forecast import (
    forecast_position,
    generate_forecast_track,
    forecast_with_uncertainty,
    forecast_motion_cone
)
from EdgeWARN.core.ctam.modules.StormCast.core.types import StormState, ForecastPoint


class TestForecastPosition:
    """Tests for forecast_position function"""

    def test_linear_advection(self):
        """Test basic linear advection"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        dt = 60.0  # 1 minute
        
        x_forecast, y_forecast = forecast_position(state, dt)
        
        # x = 0 + 10*60 = 600
        # y = 0 + 5*60 = 300
        assert x_forecast == pytest.approx(600.0, abs=0.01)
        assert y_forecast == pytest.approx(300.0, abs=0.01)

    def test_zero_velocity(self):
        """Test forecast with zero velocity"""
        state = StormState(x=1000, y=2000, u=0, v=0)
        dt = 60.0
        
        x_forecast, y_forecast = forecast_position(state, dt)
        
        # Position should not change
        assert x_forecast == 1000
        assert y_forecast == 2000

    def test_negative_velocity(self):
        """Test forecast with negative velocity (moving west/south)"""
        state = StormState(x=1000, y=2000, u=-10.0, v=-5.0)
        dt = 60.0
        
        x_forecast, y_forecast = forecast_position(state, dt)
        
        # x = 1000 + (-10)*60 = 400
        # y = 2000 + (-5)*60 = 1700
        assert x_forecast == pytest.approx(400.0, abs=0.01)
        assert y_forecast == pytest.approx(1700.0, abs=0.01)

    def test_large_dt(self):
        """Test forecast with large time delta"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        dt = 3600.0  # 1 hour
        
        x_forecast, y_forecast = forecast_position(state, dt)
        
        # x = 0 + 10*3600 = 36000
        # y = 0 + 5*3600 = 18000
        assert x_forecast == pytest.approx(36000.0, abs=0.01)
        assert y_forecast == pytest.approx(18000.0, abs=0.01)


class TestGenerateForecastTrack:
    """Tests for generate_forecast_track function"""

    def test_default_lead_times(self):
        """Test with default lead times"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        
        track = generate_forecast_track(state)
        
        # Default lead times: [900, 1800, 2700, 3600] seconds
        assert len(track) == 4
        
        # Check first point (15 minutes)
        assert track[0].lead_time == 900
        assert track[0].x == pytest.approx(9000.0, abs=0.01)
        assert track[0].y == pytest.approx(3000.0, abs=0.01)
        
        # Check last point (60 minutes)
        assert track[3].lead_time == 3600
        assert track[3].x == pytest.approx(36000.0, abs=0.01)
        assert track[3].y == pytest.approx(18000.0, abs=0.01)

    def test_custom_lead_times(self):
        """Test with custom lead times"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        custom_times = [300, 600, 900]  # 5, 10, 15 minutes
        
        track = generate_forecast_track(state, lead_times=custom_times)
        
        assert len(track) == 3
        assert track[0].lead_time == 300
        assert track[1].lead_time == 600
        assert track[2].lead_time == 900

    def test_empty_lead_times(self):
        """Test with empty lead times"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        
        track = generate_forecast_track(state, lead_times=[])
        
        assert len(track) == 0

    def test_forecast_point_structure(self):
        """Test that forecast points have correct structure"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        
        track = generate_forecast_track(state)
        
        for point in track:
            assert hasattr(point, 'x')
            assert hasattr(point, 'y')
            assert hasattr(point, 'lead_time')


class TestForecastWithUncertainty:
    """Tests for forecast_with_uncertainty function"""

    def test_with_uncertainty_bounds(self):
        """Test forecast with uncertainty bounds"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        lead_times = [900, 1800]
        uncertainty = 1000.0  # 1 km uncertainty
        
        track = forecast_with_uncertainty(state, lead_times, uncertainty)
        
        assert len(track) == 2
        
        # Check that uncertainty bounds are present
        for point in track:
            assert hasattr(point, 'x')
            assert hasattr(point, 'y')
            assert hasattr(point, 'x_min')
            assert hasattr(point, 'x_max')
            assert hasattr(point, 'y_min')
            assert hasattr(point, 'y_max')

    def test_uncertainty_bounds(self):
        """Test that uncertainty bounds are correct"""
        state = StormState(x=1000, y=2000, u=10.0, v=5.0)
        lead_times = [900]
        uncertainty = 500.0
        
        track = forecast_with_uncertainty(state, lead_times, uncertainty)
        
        point = track[0]
        
        # x = 1000 + 10*900 = 10000
        # Bounds should be 10000 +/- 500
        assert point.x == pytest.approx(10000.0, abs=0.01)
        assert point.x_min == pytest.approx(9500.0, abs=0.01)
        assert point.x_max == pytest.approx(10500.0, abs=0.01)
        
        # y = 2000 + 5*900 = 6500
        # Bounds should be 6500 +/- 500
        assert point.y == pytest.approx(6500.0, abs=0.01)
        assert point.y_min == pytest.approx(6000.0, abs=0.01)
        assert point.y_max == pytest.approx(7000.0, abs=0.01)


class TestForecastMotionCone:
    """Tests for forecast_motion_cone function"""

    def test_cone_structure(self):
        """Test that motion cone has correct structure"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        lead_times = [900, 1800, 2700, 3600]
        uncertainty = 1000.0
        
        cone = forecast_motion_cone(state, lead_times, uncertainty)
        
        # Cone should have left and right bounds
        assert hasattr(cone, 'left')
        assert hasattr(cone, 'right')
        assert len(cone.left) == len(lead_times)
        assert len(cone.right) == len(lead_times)

    def test_cone_bounds(self):
        """Test that cone bounds are correct"""
        state = StormState(x=0, y=0, u=10.0, v=5.0)
        lead_times = [900]
        uncertainty = 500.0
        
        cone = forecast_motion_cone(state, lead_times, uncertainty)
        
        # Left and right bounds should be different
        left_point = cone.left[0]
        right_point = cone.right[0]
        
        # Both should have x, y coordinates
        assert hasattr(left_point, 'x')
        assert hasattr(left_point, 'y')
        assert hasattr(right_point, 'x')
        assert hasattr(right_point, 'y')
        
        # Bounds should be symmetric around center
        center_x = 0 + 10*900  # 9000
        assert left_point.x < center_x
        assert right_point.x > center_x
