"""
Tests for StormCast Kalman filter module
"""

import pytest
import numpy as np
from EdgeWARN.ctam.modules.StormCast.core.kalman import StormKalmanFilter
from EdgeWARN.ctam.modules.StormCast.core.config import KALMAN_PARAMS


class TestStormKalmanFilter:
    """Tests for StormKalmanFilter class"""

    def test_default_initialization(self):
        """Test initialization with default values"""
        kf = StormKalmanFilter()
        
        assert kf.state == [0.0, 0.0, 0.0, 0.0]
        assert kf.alpha == KALMAN_PARAMS.alpha

    def test_custom_initialization(self):
        """Test initialization with custom values"""
        initial_state = [1000.0, 2000.0, 10.0, 5.0]
        initial_covariance = np.eye(4) * 100
        
        kf = StormKalmanFilter(
            initial_state=initial_state,
            initial_covariance=initial_covariance,
            alpha=0.5
        )
        
        assert kf.state == initial_state
        assert kf.alpha == 0.5

    def test_predict_updates_state(self):
        """Test that predict updates the state"""
        kf = StormKalmanFilter(initial_state=[0, 0, 10, 5])
        
        # Predict 60 seconds ahead
        kf.predict(dt=60.0)
        
        # Position should change: x = x0 + u*dt, y = y0 + v*dt
        assert kf.state[0] == pytest.approx(600.0, abs=0.1)  # 0 + 10*60
        assert kf.state[1] == pytest.approx(300.0, abs=0.1)  # 0 + 5*60
        # Velocity should remain the same
        assert kf.state[2] == 10.0
        assert kf.state[3] == 5.0

    def test_predict_with_zero_velocity(self):
        """Test predict with zero velocity"""
        kf = StormKalmanFilter(initial_state=[100, 200, 0, 0])
        
        kf.predict(dt=60.0)
        
        # Position should not change
        assert kf.state[0] == 100.0
        assert kf.state[1] == 200.0

    def test_update_corrects_state(self):
        """Test that update corrects the state based on observation"""
        kf = StormKalmanFilter(initial_state=[0, 0, 10, 5])
        
        # Predict first
        kf.predict(dt=60.0)
        
        # Update with observation
        observation = [650, 350, 12, 6]  # Slightly different from prediction
        kf.update(observation)
        
        # State should be adjusted toward observation
        assert kf.state[0] > 600  # Should be between 600 and 650
        assert kf.state[0] < 650
        assert kf.state[2] > 10  # Velocity should increase toward 12
        assert kf.state[2] < 12

    def test_multiple_predict_update_cycles(self):
        """Test multiple predict-update cycles"""
        kf = StormKalmanFilter(initial_state=[0, 0, 10, 5])
        
        for i in range(5):
            kf.predict(dt=60.0)
            observation = [600 * (i + 1), 300 * (i + 1), 10, 5]
            kf.update(observation)
        
        # State should converge toward the observations
        assert kf.state[0] > 0
        assert kf.state[2] > 0

    def test_velocity_smoothing(self):
        """Test velocity smoothing with alpha parameter"""
        kf = StormKalmanFilter(initial_state=[0, 0, 10, 5], alpha=0.5)
        
        # First observation
        kf.predict(dt=60.0)
        kf.update([600, 300, 15, 8])
        
        vel_after_first = kf.state[2:].copy()
        
        # Second observation with different velocity
        kf.predict(dt=60.0)
        kf.update([1200, 600, 20, 10])
        
        vel_after_second = kf.state[2:]
        
        # With smoothing, velocity shouldn't jump immediately to new value
        assert vel_after_second[0] < 20
        assert vel_after_second[1] < 10

    def test_get_position(self):
        """Test position property"""
        kf = StormKalmanFilter(initial_state=[100, 200, 10, 5])
        
        pos = kf.position
        
        assert pos == (100, 200)

    def test_get_velocity(self):
        """Test velocity property"""
        kf = StormKalmanFilter(initial_state=[100, 200, 10, 5])
        
        vel = kf.velocity
        
        assert vel == (10, 5)

    def test_get_speed(self):
        """Test get_speed (calculated from velocity)"""
        kf = StormKalmanFilter(initial_state=[0, 0, 3, 4])  # 3-4-5 triangle
        
        u, v = kf.velocity
        speed = (u**2 + v**2) ** 0.5
        
        assert speed == pytest.approx(5.0, abs=0.01)

    def test_get_speed_zero(self):
        """Test get_speed with zero velocity"""
        kf = StormKalmanFilter(initial_state=[0, 0, 0, 0])
        
        u, v = kf.velocity
        speed = (u**2 + v**2) ** 0.5
        
        assert speed == 0.0
