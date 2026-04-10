import numpy as np

from EdgeWARN.ctam.modules.Mesocyclone.preprocess import preprocess_azshear_grid


def test_preprocess_applies_noise_floor():
    grid = np.array([[0.002, 0.004], [0.007, 0.001]], dtype=float)
    result = preprocess_azshear_grid(grid)
    assert result[0, 0] == 0.0
    assert result[1, 1] == 0.0
    assert result[1, 0] >= 0.0
