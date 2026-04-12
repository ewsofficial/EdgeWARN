import numpy as np

from EdgeWARN.ctam.modules.Mesocyclone import config as cfg
from EdgeWARN.ctam.modules.Mesocyclone.preprocess import preprocess_azshear_grid


def test_preprocess_applies_noise_floor():
    grid = np.array([[0.002, 0.004], [0.007, 0.001]], dtype=float)
    result = preprocess_azshear_grid(grid)
    assert result[0, 0] == 0.0
    assert result[1, 1] == 0.0
    assert result[1, 0] >= 0.0


def test_tiled_preprocess_matches_full_grid_path(monkeypatch):
    grid = np.zeros((12, 12), dtype=float)
    grid[3:7, 4:8] = 0.01
    grid[5, 6] = 0.02

    monkeypatch.setattr(cfg, "ENABLE_MORPH_CLEANUP", True)
    monkeypatch.setattr(cfg, "PREPROCESS_TILE_SIZE", 4)
    monkeypatch.setattr(cfg, "PREPROCESS_HALO_CELLS", 4)

    monkeypatch.setattr(cfg, "ENABLE_TILED_PREPROCESS", False)
    full = preprocess_azshear_grid(grid)

    monkeypatch.setattr(cfg, "ENABLE_TILED_PREPROCESS", True)
    tiled = preprocess_azshear_grid(grid)

    assert np.allclose(tiled, full, atol=1e-6)
