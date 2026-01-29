"""
Tests for StormCast uncertainty module
"""

import pytest
import math
from EdgeWARN.core.ctam.modules.StormCast.core.uncertainty import (
    compute_tracking_uncertainty,
    compute_velocity_covariance,
    propagate_position_uncertainty
)
from EdgeWARN.core.ctam.modules.StormCast.core.config import UNCERTAINTY_PARAMS


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

    def test_covariance_matrix_shape(self):
        """Test that covariance matrix is 2x2"""
        cov = compute_velocity_covariance(u=10.0, v=5.0, sigma_u=2.0, sigma_v=1.5)
        
        assert cov.shape == (2, 2)

    def test_covariance_diagonal(self):
        """Test that diagonal elements are variances"""
        cov = compute_velocity_covariance(u=10.0, v=5.0, sigma_u=2.0, sigma_v=1.5)
        
        # Diagonal should be sigma^2
        assert cov[0, 0] == pytest.approx(4.0, abs=0.01)  # 2.0^2
        assert cov[1, 1] == pytest.approx(2.25, abs=0.01)  # 1.5^2

    def test_covariance_symmetric(self):
        """Test that covariance matrix is symmetric"""
        cov = compute_velocity_covariance(u=10.0, v=5.0, sigma_u=2.0, sigma_v=1.5)
        
        assert cov[0, 1] == pytest.approx(cov[1, 0], abs=0.01)

    def test_covariance_zero_sigma(self):
        """Test with zero sigma"""
        cov = compute_velocity_covariance(u=10.0, v=5.0, sigma_u=0.0, sigma_v=0.0)
        
        # Should be zero matrix
        assert cov[0, 0] == 0.0
        assert cov[1, 1] == 0.0
        assert cov[0, 1] == 0.0
        assert cov[1, 0] == 0.0


class TestPropagatePositionUncertainty:
    """Tests for propagate_position_uncertainty function"""

    def test_uncertainty_grows_with_time(self):
        """Test that uncertainty grows with time"""
        cov = [[4.0, 0.0], [0.0, 2.25]]  # From compute_velocity_covariance
        
        # Propagate 60 seconds
        cov_60 = propagate_position_uncertainty(cov, dt=60.0)
        
        # Uncertainty should increase
        assert cov_60[0, 0] >= cov[0, 0]
        assert cov_60[1, 1] >= cov[1, 1]

    def test_uncertainty_zero_time(self):
        """Test with zero time delta"""
        cov = [[4.0, 0.0], [0.0, 2.25]]
        
        cov_0 = propagate_position_uncertainty(cov, dt=0.0)
        
        # Should be unchanged
        assert cov_0[0, 0] == pytest.approx(cov[0, 0], abs=0.01)
        assert cov_0[1, 1] == pytest.approx(cov[1, 1], abs=0.01)

    def test_uncertainty_large_time(self):
        """Test with large time delta"""
        cov = [[4.0, 0.0], [0.0, 2.25]]
        
        # Propagate 3600 seconds (1 hour)
        cov_3600 = propagate_position_uncertainty(cov, dt=3600.0)
        
        # Uncertainty should grow significantly
        assert cov_3600[0, 0] > cov[0, 0] * 10
        assert cov_3600[1, 1] > cov[1, 1] * 10

    def test_uncertainty_preserves_symmetry(self):
        """Test that propagated covariance remains symmetric"""
        cov = [[4.0, 0.0], [0.0, 2.25]]
        
        cov_propagated = propagate_position_uncertainty(cov, dt=60.0)
        
        assert cov_propagated[0, 1] == pytest.approx(cov_propagated[1, 0], abs=0.01)
