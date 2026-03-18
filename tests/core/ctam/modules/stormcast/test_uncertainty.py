"""
Tests for StormCast uncertainty module
"""

import pytest
import math
from EdgeWARN.ctam.modules.StormCast.core.uncertainty import (
    compute_tracking_uncertainty,
    compute_velocity_covariance,
    propagate_position_uncertainty
)
from EdgeWARN.ctam.modules.StormCast.core.config import UNCERTAINTY_PARAMS


class TestComputeTrackingUncertainty:
    """Tests for compute_tracking_uncertainty function"""

    def test_uncertainty_decreases_with_samples(self):
        """Test that uncertainty decreases with more samples"""
        # Default params: sigma_min=2.0, sigma_range=4.0, alpha=0.5
        u1 = compute_tracking_uncertainty(n_samples=1)
        u2 = compute_tracking_uncertainty(n_samples=4)
        u3 = compute_tracking_uncertainty(n_samples=16)
        
        # More samples = lower uncertainty
        assert u1 > u2 > u3

    def test_uncertainty_floor(self):
        """Test that uncertainty has minimum floor"""
        u1 = compute_tracking_uncertainty(n_samples=100)
        
        # Should be at least sigma_min
        assert u1 >= UNCERTAINTY_PARAMS.sigma_min

    def test_uncertainty_with_jitter(self):
        """Test that jitter increases uncertainty"""
        u_no_motion = compute_tracking_uncertainty(n_samples=10, motion_jitter=0.0)
        u_with_motion = compute_tracking_uncertainty(n_samples=10, motion_jitter=5.0)
        
        # Jitter should increase uncertainty
        assert u_with_motion > u_no_motion

    def test_uncertainty_custom_params(self):
        """Test with custom parameters"""
        u = compute_tracking_uncertainty(
            n_samples=5,
            sigma_min=1.0,
            sigma_range=5.0,
            alpha=0.7
        )
        
        # u = 1.0 + 5.0 / (5^0.7) = 1.0 + 5.0 / 3.09 ≈ 2.62
        assert u == pytest.approx(2.62, abs=0.01)

    def test_uncertainty_zero_samples(self):
        """Test with zero samples (should use n=1)"""
        u = compute_tracking_uncertainty(n_samples=0)
        
        # Should use n=1 internally
        expected = UNCERTAINTY_PARAMS.sigma_min + UNCERTAINTY_PARAMS.sigma_range
        assert u == pytest.approx(expected, abs=0.01)


class TestComputeVelocityCovariance:
    """Tests for compute_velocity_covariance function"""

    def test_return_values(self):
        """Test that function returns tuple of sigmas"""
        sigmas = compute_velocity_covariance(sigma_obs=2.0, sigma_env=1.5, sigma_hist=0.0)
        
        # Should return (sigma_u, sigma_v, sigma_total)
        assert len(sigmas) == 3
        
        # Combined variance: 2.0^2 + 1.5^2 + 0^2 = 4 + 2.25 = 6.25
        # sigma = sqrt(6.25) = 2.5
        assert sigmas[0] == pytest.approx(2.5, abs=0.01)
        assert sigmas[1] == pytest.approx(2.5, abs=0.01)
        assert sigmas[2] == pytest.approx(2.5, abs=0.01)

    def test_isotropic(self):
        """Test that uncertainty is isotropic (sigma_u == sigma_v)"""
        sigmas = compute_velocity_covariance(sigma_obs=2.0)
        assert sigmas[0] == sigmas[1]

    def test_zero_sigma(self):
        """Test with zero parameters"""
        sigmas = compute_velocity_covariance(sigma_obs=0.0, sigma_env=0.0, sigma_hist=0.0)
        assert sigmas == (0.0, 0.0, 0.0)


class TestPropagatePositionUncertainty:
    """Tests for propagate_position_uncertainty function"""

    def test_uncertainty_grows_with_time(self):
        """Test that uncertainty grows with time"""
        sigma_pos = (500.0, 500.0)
        sigma_vel = (10.0, 10.0)
        
        # Propagate 60 seconds
        sigma_new = propagate_position_uncertainty(sigma_pos, sigma_vel, dt=60.0)
        
        # Uncertainty should increase
        # sqrt(500^2 + (10*60)^2) = sqrt(250000 + 360000) = sqrt(610000) ≈ 781
        assert sigma_new[0] > sigma_pos[0]
        assert sigma_new[1] > sigma_pos[1]
        assert sigma_new[0] == pytest.approx(781.02, abs=0.1)

    def test_uncertainty_zero_time(self):
        """Test with zero time delta"""
        sigma_pos = (500.0, 500.0)
        sigma_vel = (10.0, 10.0)
        
        sigma_new = propagate_position_uncertainty(sigma_pos, sigma_vel, dt=0.0)
        
        # Should be unchanged
        assert sigma_new == sigma_pos

    def test_uncertainty_large_time(self):
        """Test with large time delta"""
        sigma_pos = (500.0, 500.0)
        sigma_vel = (10.0, 10.0)
        
        sigma_new = propagate_position_uncertainty(sigma_pos, sigma_vel, dt=3600.0)
        
        # Uncertainty should grow significantly
        assert sigma_new[0] > sigma_pos[0] * 10
