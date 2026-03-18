"""
Tests for StormCast blending module
"""

import pytest
import math
from EdgeWARN.ctam.modules.StormCast.core.blending import (
    smooth_observed_motion,
    blend_motion,
    adjust_weights_for_maturity,
    _exponential_filter
)
from EdgeWARN.ctam.modules.StormCast.core.config import BlendingWeights


class TestExponentialFilter:
    """Tests for _exponential_filter function"""

    def test_single_value(self):
        """Test filtering with single value"""
        values = [(5.0, 10.0)]
        result = _exponential_filter(values, alpha=0.3)
        
        assert result == (5.0, 10.0)

    def test_multiple_values(self):
        """Test filtering with multiple values"""
        values = [(0.0, 0.0), (10.0, 10.0), (20.0, 20.0)]
        result = _exponential_filter(values, alpha=0.5)
        
        # With alpha=0.5:
        # Start: (0, 0)
        # After (10, 10): 0.5*10 + 0.5*0 = 5 for both
        # After (20, 20): 0.5*20 + 0.5*5 = 12.5 for both
        assert result[0] == pytest.approx(12.5, abs=0.01)
        assert result[1] == pytest.approx(12.5, abs=0.01)

    def test_empty_values_raises_error(self):
        """Test that empty values raises ValueError"""
        with pytest.raises(ValueError, match="empty"):
            _exponential_filter([])

    def test_different_alpha_values(self):
        """Test with different alpha values"""
        values = [(0.0, 0.0), (10.0, 10.0)]
        
        # High alpha (more weight to recent)
        result_high = _exponential_filter(values, alpha=0.8)
        
        # Low alpha (more weight to past)
        result_low = _exponential_filter(values, alpha=0.2)
        
        # High alpha should be closer to recent value
        assert result_high[0] > result_low[0]


class TestSmoothObservedMotion:
    """Tests for smooth_observed_motion function"""

    def test_exponential_smoothing(self):
        """Test exponential smoothing method"""
        history = [(0.0, 0.0), (10.0, 5.0), (20.0, 10.0)]
        result = smooth_observed_motion(history, method="exponential", alpha=0.5)
        
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_mean_smoothing(self):
        """Test mean smoothing method"""
        history = [(10.0, 5.0), (20.0, 10.0), (30.0, 15.0)]
        result = smooth_observed_motion(history, method="mean")
        
        # Mean of u: (10 + 20 + 30) / 3 = 20
        # Mean of v: (5 + 10 + 15) / 3 = 10
        assert result[0] == pytest.approx(20.0, abs=0.01)
        assert result[1] == pytest.approx(10.0, abs=0.01)

    def test_single_value_history(self):
        """Test with single value in history"""
        history = [(5.0, 10.0)]
        result = smooth_observed_motion(history)
        
        assert result == (5.0, 10.0)

    def test_empty_history_raises_error(self):
        """Test that empty history raises ValueError"""
        with pytest.raises(ValueError, match="empty"):
            smooth_observed_motion([])

    def test_invalid_method_raises_error(self):
        """Test that invalid method raises error"""
        history = [(10.0, 5.0), (20.0, 10.0)]
        
        with pytest.raises(ValueError):
            smooth_observed_motion(history, method="invalid")


class TestBlendMotion:
    """Tests for blend_motion function"""

    def test_blend_observed_and_environmental(self):
        """Test blending observed and environmental motion"""
        observed = (10.0, 5.0)
        mean_wind = (15.0, 8.0)
        bunkers = (12.0, 6.0)
        
        # Test primarily environmental vs observed
        weights = BlendingWeights(w_obs=0.6, w_mean=0.3, w_bunkers=0.1)
        
        result = blend_motion(observed, mean_wind, bunkers, weights)
        
        # Expected: 
        # u = 0.6*10 + 0.3*15 + 0.1*12 = 6 + 4.5 + 1.2 = 11.7
        # v = 0.6*5 + 0.3*8 + 0.1*6 = 3 + 2.4 + 0.6 = 6.0
        assert result[0] == pytest.approx(11.7, abs=0.01)
        assert result[1] == pytest.approx(6.0, abs=0.01)

    def test_equal_weights(self):
        """Test with equal weights"""
        observed = (10.0, 10.0)
        mean_wind = (20.0, 20.0)
        bunkers = (30.0, 30.0)
        
        weights = BlendingWeights(w_obs=0.333, w_mean=0.333, w_bunkers=0.334)
        
        result = blend_motion(observed, mean_wind, bunkers, weights)
        
        # 10/3 + 20/3 + 30/3 = 60/3 = 20
        assert result[0] == pytest.approx(20.0, abs=0.1)

    def test_all_observed(self):
        """Test with 100% observed weight"""
        observed = (10.0, 5.0)
        mean_wind = (100.0, 100.0)
        bunkers = (100.0, 100.0)
        
        weights = BlendingWeights(w_obs=1.0, w_mean=0.0, w_bunkers=0.0)
        
        result = blend_motion(observed, mean_wind, bunkers, weights)
        
        assert result == observed

    def test_all_environmental(self):
        """Test with 100% environmental (mean) weight"""
        observed = (10.0, 5.0)
        mean_wind = (100.0, 100.0)
        bunkers = (50.0, 50.0)
        
        weights = BlendingWeights(w_obs=0.0, w_mean=1.0, w_bunkers=0.0)
        
        result = blend_motion(observed, mean_wind, bunkers, weights)
        
        assert result == mean_wind


class TestAdjustWeightsForMaturity:
    """Tests for adjust_weights_for_maturity function"""

    def test_young_storm_prefers_environmental(self):
        """Test that young storms prefer environmental guidance"""
        weights = BlendingWeights(w_obs=0.5, w_mean=0.25, w_bunkers=0.25)
        
        # Young storm (1 sample), moderate characteristics
        result = adjust_weights_for_maturity(
            h_core=8.0, 
            track_history=1, 
            shear_magnitude=15.0, 
            base_weights=weights
        )
        
        # Should increase mean/bunkers, decrease observed
        assert result.w_mean > weights.w_mean
        assert result.w_obs < weights.w_obs

    def test_mature_storm_prefers_observed(self):
        """Test that mature storms prefer observed motion"""
        weights = BlendingWeights(w_obs=0.5, w_mean=0.25, w_bunkers=0.25)
        
        # Mature storm (20 samples)
        result = adjust_weights_for_maturity(
            h_core=8.0,
            track_history=20,
            shear_magnitude=15.0,
            base_weights=weights
        )
        
        # Should increase observed weight
        assert result.w_obs > weights.w_obs

    def test_weights_sum_to_one(self):
        """Test that adjusted weights still sum to approximately 1"""
        weights = BlendingWeights(w_obs=0.5, w_mean=0.25, w_bunkers=0.25)
        
        result = adjust_weights_for_maturity(
            h_core=8.0,
            track_history=5,
            shear_magnitude=15.0,
            base_weights=weights
        )
        
        total = result.w_obs + result.w_mean + result.w_bunkers
        assert total == pytest.approx(1.0, abs=0.01)

    def test_no_samples_uses_environmental(self):
        """Test that low history strongly favors environmental"""
        weights = BlendingWeights(w_obs=0.5, w_mean=0.25, w_bunkers=0.25)
        
        result = adjust_weights_for_maturity(
            h_core=8.0,
            track_history=0,
            shear_magnitude=15.0,
            base_weights=weights
        )
        
        # Should favor environmental components (mean + bunkers) > observed
        env_weight = result.w_mean + result.w_bunkers
        assert env_weight > result.w_obs
