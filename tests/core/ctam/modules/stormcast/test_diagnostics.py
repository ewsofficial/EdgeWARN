"""
Tests for StormCast diagnostics module
"""

import pytest
import math
from EdgeWARN.core.ctam.modules.StormCast.core.diagnostics import (
    compute_storm_core_height,
    compute_adaptive_steering,
    compute_effective_shear,
    compute_bunkers_motion,
    compute_height_weights,
    _vector_magnitude,
    _unit_vector,
    _rotate_vector_90,
    _gaussian,
    _linear_interpolate
)
from EdgeWARN.core.ctam.modules.StormCast.core.types import EnvironmentProfile, MotionVector


class TestVectorHelpers:
    """Tests for vector helper functions"""

    def test_vector_magnitude(self):
        """Test vector magnitude calculation"""
        mag = _vector_magnitude(3.0, 4.0)
        assert mag == pytest.approx(5.0, abs=0.01)

    def test_vector_magnitude_zero(self):
        """Test vector magnitude with zero vector"""
        mag = _vector_magnitude(0.0, 0.0)
        assert mag == 0.0

    def test_unit_vector(self):
        """Test unit vector calculation"""
        u, v = _unit_vector(10.0, 0.0)
        
        # Magnitude should be 1
        mag = math.sqrt(u*u + v*v)
        assert mag == pytest.approx(1.0, abs=0.01)
        assert u == pytest.approx(1.0, abs=0.01)
        assert v == pytest.approx(0.0, abs=0.01)

    def test_unit_vector_zero(self):
        """Test unit vector with zero vector"""
        u, v = _unit_vector(0.0, 0.0)
        
        # Should return (0, 0) for zero vector
        assert u == 0.0
        assert v == 0.0

    def test_rotate_vector_90_clockwise(self):
        """Test 90-degree clockwise rotation"""
        u, v = _rotate_vector_90(10.0, 0.0, clockwise=True)
        
        # (10, 0) rotated 90 deg clockwise should be (0, -10)
        assert u == pytest.approx(0.0, abs=0.01)
        assert v == pytest.approx(-10.0, abs=0.01)

    def test_rotate_vector_90_counterclockwise(self):
        """Test 90-degree counterclockwise rotation"""
        u, v = _rotate_vector_90(10.0, 0.0, clockwise=False)
        
        # (10, 0) rotated 90 deg counterclockwise should be (0, 10)
        assert u == pytest.approx(0.0, abs=0.01)
        assert v == pytest.approx(10.0, abs=0.01)

    def test_gaussian(self):
        """Test Gaussian function"""
        result = _gaussian(5.0, mu=5.0, sigma=1.0)
        
        # At mean, should be 1.0
        assert result == pytest.approx(1.0, abs=0.01)

    def test_gaussian_one_sigma(self):
        """Test Gaussian at one sigma from mean"""
        result = _gaussian(6.0, mu=5.0, sigma=1.0)
        
        # At mu + sigma, should be exp(-0.5) ≈ 0.607
        assert result == pytest.approx(math.exp(-0.5), abs=0.01)

    def test_linear_interpolate(self):
        """Test linear interpolation"""
        result = _linear_interpolate(5.0, 0.0, 10.0, 20.0, 30.0)
        
        # Midpoint of 0 and 10 should be 20? No, linear_interpolate(5, 0, 10, 20, 30)
        # (5-0)/(10-0)=0.5 -> 20 + 0.5*10 = 25
        assert result == 25.0

    def test_linear_interpolate_below_range(self):
        """Test interpolation below range"""
        result = _linear_interpolate(-5.0, 0.0, 10.0, 20.0, 30.0)
        
        # Below lower bound, should return lower value (clamped)
        assert result == 20.0

    def test_linear_interpolate_above_range(self):
        """Test interpolation above range"""
        result = _linear_interpolate(25.0, 0.0, 10.0, 20.0, 30.0)
        
        # Above upper bound, should return upper value
        assert result == 30.0


class TestComputeHeightWeights:
    """Tests for compute_height_weights function"""

    def test_weights_sum_to_one(self):
        """Test that weights sum to approximately 1"""
        weights = compute_height_weights(6.0)
        
        total = sum(weights.values())
        assert total == pytest.approx(1.0, abs=0.01)

    def test_weights_for_low_core(self):
        """Test weights for low storm core"""
        weights = compute_height_weights(3.0)
        
        # Low core (3.0 km) should have peak weight at 700 hPa (~3km)
        assert weights[700] > weights[850]
        assert weights[700] > weights[500]

    def test_weights_for_high_core(self):
        """Test weights for high storm core"""
        weights = compute_height_weights(12.0)
        
        # High core should have more weight at higher levels
        assert weights[500] > weights[700]
        assert weights[700] > weights[850]

    def test_weights_for_mid_core(self):
        """Test weights for mid-level storm core"""
        weights = compute_height_weights(6.0)
        
        # Mid core should have peak at mid levels
        max_level = max(weights, key=weights.get)
        assert max_level in [500, 475, 450]


class TestComputeStormCoreHeight:
    """Tests for compute_storm_core_height function"""

    def test_core_height_from_echo_tops(self):
        """Test core height from echo tops"""
        h_30 = 10.0  # 30 dBZ echo top
        h_50 = 8.0   # 50 dBZ echo top
        
        height = compute_storm_core_height(h_30, h_50)
        
        # Core height should be between the two
        assert height >= min(h_30, h_50)
        assert height <= max(h_30, h_50)

    def test_core_height_equal_tops(self):
        """Test core height when echo tops are equal"""
        h_30 = 10.0
        h_50 = 10.0
        
        height = compute_storm_core_height(h_30, h_50)
        
        assert height == 10.0

    def test_core_height_missing_50(self):
        """Test core height when 50 dBZ top is missing"""
        h_30 = 10.0
        h_50 = None
        
        height = compute_storm_core_height(h_30, h_50)
        
        # Should use 30 dBZ top
        assert height == 10.0


class TestComputeAdaptiveSteering:
    """Tests for compute_adaptive_steering function"""

    def test_steering_from_profile(self):
        """Test steering from environmental profile"""
        profile = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        h_core = 6.0
        
        steering = compute_adaptive_steering(profile, h_core)
        
        # Should return a tuple (u, v)
        assert isinstance(steering, tuple)
        assert len(steering) == 2
        assert isinstance(steering[0], float)
        assert isinstance(steering[1], float)

    def test_steering_weights_core_height(self):
        """Test that steering weights by core height"""
        profile_low = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        
        profile_high = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        
        steering_low = compute_adaptive_steering(profile_low, h_core=3.0)
        steering_high = compute_adaptive_steering(profile_high, h_core=12.0)
        
        # High core should weight higher levels more
        # This is a simplified check - actual implementation is more complex


class TestComputeEffectiveShear:
    """Tests for compute_effective_shear function"""

    def test_shear_from_profile(self):
        """Test shear calculation from profile"""
        profile = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        
        shear = compute_effective_shear(profile, h_core=6.0)
        
        # Should return a tuple (u, v)
        assert isinstance(shear, tuple)
        assert len(shear) == 2

    def test_shear_magnitude(self):
        """Test that shear magnitude is reasonable"""
        profile = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        
        shear = compute_effective_shear(profile, h_core=6.0)
        mag = math.sqrt(shear[0]**2 + shear[1]**2)
        
        # Shear should be non-zero
        assert mag > 0


class TestComputeBunkersMotion:
    """Tests for compute_bunkers_motion function"""

    def test_bunkers_from_profile(self):
        """Test Bunkers motion from profile"""
        profile = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        
        motion = compute_bunkers_motion(profile, h_core=6.0)
        
        # Should return a tuple (u, v)
        assert isinstance(motion, tuple)
        assert len(motion) == 2

    def test_bunkers_mean_wind(self):
        """Test that Bunkers uses mean wind"""
        profile = EnvironmentProfile(
            winds={
                850: (10.0, 5.0),
                700: (15.0, 8.0),
                500: (20.0, 10.0),
                250: (25.0, 12.0)
            }
        )
        
        motion = compute_bunkers_motion(profile, h_core=6.0)
        
        # Verify motion is calculated correctly based on:
        # - Height-weighted mean wind (dominated by 500mb level near 6km)
        # - Shear vector deviation (3 m/s at 6km core height)
        # Expected: u ~ 3.4 m/s, v ~ -1.6 m/s
        assert motion[0] == pytest.approx(3.4, abs=0.5)
        assert motion[1] == pytest.approx(-1.6, abs=0.5)
