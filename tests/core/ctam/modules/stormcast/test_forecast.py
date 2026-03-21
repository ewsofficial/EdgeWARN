"""
Tests for StormCast forecast module
"""

import pytest
from EdgeWARN.ctam.modules.StormCast.core.forecast import (
    forecast_position,
    generate_forecast_track,
    forecast_with_uncertainty,
    forecast_motion_cone
)
from EdgeWARN.ctam.modules.StormCast.core.types import StormState, ForecastPoint


class TestForecastPosition:
    """Tests for forecast_position function"""

    def test_linear_advection(self):
        """Test basic linear advection"""
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
        dt = 60.0  # 1 minute
        
        x_forecast, y_forecast = forecast_position(state, dt)
        
        # x = 0 + 10*60 = 600
        # y = 0 + 5*60 = 300
        assert x_forecast == pytest.approx(600.0, abs=0.01)
        assert y_forecast == pytest.approx(300.0, abs=0.01)

    def test_zero_velocity(self):
        """Test forecast with zero velocity"""
        state = StormState(x=1000, y=2000, u=0, v=0, h_core=5.0)
        dt = 60.0
        
        x_forecast, y_forecast = forecast_position(state, dt)
        
        # Position should not change
        assert x_forecast == 1000
        assert y_forecast == 2000

    def test_negative_velocity(self):
        """Test forecast with negative velocity (moving west/south)"""
        state = StormState(x=1000, y=2000, u=-10.0, v=-5.0, h_core=5.0)
        dt = 60.0
        
        x_forecast, y_forecast = forecast_position(state, dt)
        
        # x = 1000 + (-10)*60 = 400
        # y = 2000 + (-5)*60 = 1700
        assert x_forecast == pytest.approx(400.0, abs=0.01)
        assert y_forecast == pytest.approx(1700.0, abs=0.01)

    def test_large_dt(self):
        """Test forecast with large time delta"""
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
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
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
        
        track = generate_forecast_track(state)
        
        # Default lead times: [900, 1800, 2700, 3600] seconds
        assert len(track) == 4
        
        # Check first point (15 minutes)
        assert track[0].lead_time == 900
        assert track[0].x == pytest.approx(9000.0, abs=0.01)
        assert track[0].y == pytest.approx(4500.0, abs=0.01)  # 5 * 900 = 4500
        
        # Check last point (60 minutes)
        assert track[3].lead_time == 3600
        assert track[3].x == pytest.approx(36000.0, abs=0.01)
        assert track[3].y == pytest.approx(18000.0, abs=0.01)

    def test_custom_lead_times(self):
        """Test with custom lead times"""
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
        custom_times = [300, 600, 900]  # 5, 10, 15 minutes
        
        track = generate_forecast_track(state, lead_times=custom_times)
        
        assert len(track) == 3
        assert track[0].lead_time == 300
        assert track[1].lead_time == 600
        assert track[2].lead_time == 900

    def test_empty_lead_times(self):
        """Test with empty lead times"""
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
        
        track = generate_forecast_track(state, lead_times=[])
        
        assert len(track) == 0

    def test_forecast_point_structure(self):
        """Test that forecast points have correct structure"""
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
        
        track = generate_forecast_track(state)
        
        for point in track:
            assert hasattr(point, 'x')
            assert hasattr(point, 'y')
            assert hasattr(point, 'lead_time')


class TestForecastWithUncertainty:
    """Tests for forecast_with_uncertainty function"""

    def test_with_uncertainty_bounds(self):
        """Test forecast with uncertainty bounds"""
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
        lead_times = [900, 1800]
        # uncertainty passed as tuple (sigma_x, sigma_y)
        initial_sigma = (1000.0, 1000.0)
        
        track = forecast_with_uncertainty(state, lead_times, initial_sigma_pos=initial_sigma)
        
        assert len(track) == 2
        
        # Check that uncertainty is present
        for point in track:
            assert hasattr(point, 'x')
            assert hasattr(point, 'y')
            assert hasattr(point, 'sigma_x')
            assert hasattr(point, 'sigma_y')
            assert point.sigma_x >= 1000.0
            assert point.sigma_y >= 1000.0

    def test_uncertainty_bounds(self):
        """Test that uncertainty bounds are correct"""
        state = StormState(x=1000, y=2000, u=10.0, v=5.0, h_core=5.0)
        lead_times = [900]
        initial_sigma = (500.0, 500.0)
        # sigma_vel will be computed if None, but we can pass 0 for simpler deterministic check
        sigma_vel = (0.0, 0.0)
        
        track = forecast_with_uncertainty(state, lead_times, initial_sigma_pos=initial_sigma, sigma_vel=sigma_vel)
        
        point = track[0]
        
        # x = 1000 + 10*900 = 10000
        # Position should be exact
        assert point.x == pytest.approx(10000.0, abs=0.01)
        assert point.y == pytest.approx(6500.0, abs=0.01)
        
        # Uncertainty should be propagated (with 0 velocity uncertainty, it stays same)
        assert point.sigma_x == pytest.approx(500.0, abs=0.01)
        assert point.sigma_y == pytest.approx(500.0, abs=0.01)


class TestForecastMotionCone:
    """Tests for forecast_motion_cone function"""

    def test_cone_structure(self):
        """Test that motion cone has correct structure"""
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
        lead_times = [900, 1800, 2700, 3600]
        
        cones = forecast_motion_cone(state, lead_times)
        
        assert len(cones) == len(lead_times)
        
        for cone in cones:
            assert 'center' in cone
            assert 'lead_time' in cone
            assert 'ellipse' in cone
            assert 'sigma_x' in cone
            assert 'sigma_y' in cone
            
            # center should be tuple
            assert isinstance(cone['center'], tuple)
            # ellipse should be list of points
            assert isinstance(cone['ellipse'], list)
            assert len(cone['ellipse']) > 0

    def test_cone_bounds(self):
        """Test that cone bounds are consistent"""
        state = StormState(x=0, y=0, u=10.0, v=5.0, h_core=5.0)
        lead_times = [900]
        
        cones = forecast_motion_cone(state, lead_times)
        cone = cones[0]
        
        ellipse = cone['ellipse']
        xs = [p[0] for p in ellipse]
        ys = [p[1] for p in ellipse]
        
        center_x = 0 + 10*900  # 9000
        
        # Ellipse should span around center
        assert min(xs) < center_x
        assert max(xs) > center_x
