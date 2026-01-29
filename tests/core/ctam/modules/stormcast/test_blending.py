"""
Tests for StormCast blending module
"""

import pytest
import math
from EdgeWARN.core.ctam.modules.StormCast.core.blending import (
    smooth_observed_motion,
    blend_motion,
    adjust_weights_for_maturity,
    _exponential_filter
)
from EdgeWARN.core.ctam.modules.StormCast.core.config import BlendingWeights


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
        history = [(10.0, 5.0)]
        
        with pytest.raises(ValueError):
            smooth_observed_motion(history, method="invalid")


class TestBlendMotion:
    """Tests for blend_motion function"""

    def test_blend_observed_and_environmental(self):
        """Test blending observed and environmental motion"""
        observed = (10.0, 5.0)  # 10 m/s east, 5 m/s north
        environmental = (15.0, 8.0)  # 15 m/s east, 8 m/s north
        weights = BlendingWeights(observed=0.6, environmental=0.4)
        
        result = blend_motion(observed, environmental, weights)
        
        # Expected: 0.6*10 + 0.4*15 = 12 for u
        # Expected: 0.6*5 + 0.4*8 = 6.2 for v
        assert result[0] == pytest.approx(12.0, abs=0.01)
        assert result[1] == pytest.approx(6.2, abs=0.01)

    def test_equal_weights(self):
        """Test with equal weights"""
        observed = (10.0, 10.0)
        environmental = (20.0, 20.0)
        weights = BlendingWeights(observed=0.5, environmental=0.5)
        
        result = blend_motion(observed, environmental, weights)
        
        assert result[0] == pytest.approx(15.0, abs=0.01)
        assert result[1] == pytest.approx(15.0, abs=0.01)

    def test_all_observed(self):
        """Test with 100% observed weight"""
        observed = (10.0, 5.0)
        environmental = (100.0, 100.0)
        weights = BlendingWeights(observed=1.0, environmental=0.0)
        
        result = blend_motion(observed, environmental, weights)
        
        assert result == observed

    def test_all_environmental(self):
        """Test with 100% environmental weight"""
        observed = (10.0, 5.0)
        environmental = (100.0, 100.0)
        weights = BlendingWeights(observed=0.0, environmental=1.0)
        
        result = blend_motion(observed, environmental, weights)
        
        assert result == environmental


class TestAdjustWeightsForMaturity:
    """Tests for adjust_weights_for_maturity function"""

    def test_young_storm_prefers_environmental(self):
        """Test that young storms prefer environmental guidance"""
        weights = BlendingWeights(observed=0.5, environmental=0.5)
        
        # Young storm (1 sample)
        result = adjust_weights_for_maturity(weights, n_samples=1)
        
        # Should increase environmental weight
        assert result.environmental > weights.environmental
        assert result.observed < weights.observed

    def test_mature_storm_prefers_observed(self):
        """Test that mature storms prefer observed motion"""
        weights = BlendingWeights(observed=0.5, environmental=0.5)
        
        # Mature storm (many samples)
        result = adjust_weights_for_maturity(weights, n_samples=20)
        
        # Should increase observed weight
        assert result.observed > weights.observed
        assert result.environmental < weights.environmental

    def test_weights_sum_to_one(self):
        """Test that adjusted weights still sum to approximately 1"""
        weights = BlendingWeights(observed=0.7, environmental=0.3)
        
        result = adjust_weights_for_maturity(weights, n_samples=5)
        
        total = result.observed + result.environmental
        assert total == pytest.approx(1.0, abs=0.01)

    def test_no_samples_uses_environmental(self):
        """Test with zero samples defaults to environmental"""
        weights = BlendingWeights(observed=0.5, environmental=0.5)
        
        result = adjust_weights_for_maturity(weights, n_samples=0)
        
        # Should heavily favor environmental
        assert result.environmental > result.observed
