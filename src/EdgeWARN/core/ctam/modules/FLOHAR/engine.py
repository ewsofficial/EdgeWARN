"""
FLOHAR Scoring Engine

Vectorized numpy engine that computes per-pixel composite threat scores
(0-100) from seven FLASH indicator grids + RQI quality control.

Three pillars:
    1. Rainfall Extremity (0.40)  — ARI-based
    2. Hydrologic Response (0.35) — streamflow + soil saturation
    3. Guidance Exceedance (0.25) — QPE-to-FFG ratio
"""

import numpy as np
from typing import Tuple

from . import config as cfg


# ─────────────────────────────────────────────────────────────────────
# Internal normalisation helpers (all vectorized)
# ─────────────────────────────────────────────────────────────────────

def _mask_sentinels(arr: np.ndarray) -> np.ndarray:
    """Replace sentinel values with NaN."""
    out = arr.astype(np.float64, copy=True)
    for s in cfg.SENTINEL_VALUES:
        out[out == s] = np.nan
    return out


def _normalize_ari(ari: np.ndarray) -> np.ndarray:
    """
    Logarithmic ARI normalization.
    ARI of ARI_CEILING_YEARS → 1.0, ARI ≤ 1 → ~0.
    """
    safe = np.where(ari > 0, ari, 1.0)  # avoid log10(0)
    normed = np.log10(safe) / np.log10(cfg.ARI_CEILING_YEARS)
    return np.clip(normed, 0.0, 1.0)


def _sigmoid(x: np.ndarray, x0: float, k: float) -> np.ndarray:
    """Sigmoid normalization: 1 / (1 + exp(-k * (x - x0)))."""
    return 1.0 / (1.0 + np.exp(-k * (x - x0)))


def _normalize_soil_sat(soil: np.ndarray) -> np.ndarray:
    """Linear clamp: [SOIL_SAT_LOW, SOIL_SAT_HIGH] → [0, 1]."""
    span = cfg.SOIL_SAT_HIGH - cfg.SOIL_SAT_LOW
    if span <= 0:
        return np.zeros_like(soil)
    normed = (soil - cfg.SOIL_SAT_LOW) / span
    return np.clip(normed, 0.0, 1.0)


def _normalize_ffg(ratio: np.ndarray) -> np.ndarray:
    """
    Piecewise FFG ratio normalization:
        < 0.75 → 0
        0.75 → 1.0  → ramp 0 → 0.5
        1.0  → 2.0  → ramp 0.5 → 1.0
        ≥ 2.0       → 1.0
    """
    out = np.zeros_like(ratio, dtype=np.float64)

    # Segment 1: 0.75 ≤ ratio < 1.0  →  ramp 0→0.5
    mask1 = (ratio >= cfg.FFG_RAMP_START) & (ratio < cfg.FFG_RAMP_MID)
    span1 = cfg.FFG_RAMP_MID - cfg.FFG_RAMP_START
    if span1 > 0:
        out[mask1] = 0.5 * (ratio[mask1] - cfg.FFG_RAMP_START) / span1

    # Segment 2: 1.0 ≤ ratio < 2.0  →  ramp 0.5→1.0
    mask2 = (ratio >= cfg.FFG_RAMP_MID) & (ratio < cfg.FFG_RAMP_END)
    span2 = cfg.FFG_RAMP_END - cfg.FFG_RAMP_MID
    if span2 > 0:
        out[mask2] = 0.5 + 0.5 * (ratio[mask2] - cfg.FFG_RAMP_MID) / span2

    # Segment 3: ratio ≥ 2.0  →  1.0
    out[ratio >= cfg.FFG_RAMP_END] = 1.0

    return out


def _rqi_weight(rqi: np.ndarray) -> np.ndarray:
    """
    RQI quality-control weight:
        ≥ 0.8 → 1.0
        0.3–0.8 → linear ramp 0→1
        < 0.3 → 0.0 (hard mask)
    """
    out = np.zeros_like(rqi, dtype=np.float64)

    # Linear ramp band
    ramp_span = cfg.RQI_FULL_WEIGHT_THRESHOLD - cfg.RQI_MIN_WEIGHT_THRESHOLD
    mask_ramp = (rqi >= cfg.RQI_MIN_WEIGHT_THRESHOLD) & (rqi < cfg.RQI_FULL_WEIGHT_THRESHOLD)
    if ramp_span > 0:
        out[mask_ramp] = (rqi[mask_ramp] - cfg.RQI_MIN_WEIGHT_THRESHOLD) / ramp_span

    # Full weight
    out[rqi >= cfg.RQI_FULL_WEIGHT_THRESHOLD] = 1.0

    return out


def classify_severity(score: np.ndarray) -> np.ndarray:
    """
    Vectorized severity tier classification.
    Returns string array with 'none', 'advisory', 'warning', 'emergency'.
    """
    result = np.full(score.shape, "none", dtype="U10")
    # Iterate ascending so higher tiers overwrite lower ones
    for threshold, label in reversed(cfg.SEVERITY_TIERS):
        result = np.where(score >= threshold, label, result)
    return result


def classify_severity_scalar(score: int) -> str:
    """Classify a single score into a severity tier."""
    for threshold, label in cfg.SEVERITY_TIERS:
        if score >= threshold:
            return label
    return "none"


# ─────────────────────────────────────────────────────────────────────
# Main scoring function
# ─────────────────────────────────────────────────────────────────────

def compute_threat_grid(
    ari_max: np.ndarray,
    ari_30m: np.ndarray,
    ari_01h: np.ndarray,
    crest_streamflow: np.ndarray,
    hp_streamflow: np.ndarray,
    soil_sat: np.ndarray,
    ffg_ratio: np.ndarray,
    rqi: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute per-pixel threat scores from aligned FLASH grids.

    All inputs must be 2D arrays of the same shape. Sentinel values
    (-999, -9999) and NaN are handled gracefully — weights are
    redistributed among valid indicators.

    Args:
        ari_max:          Maximum ARI of QPE (all durations)
        ari_30m:          ARI for 30-minute QPE
        ari_01h:          ARI for 1-hour QPE
        crest_streamflow: CREST max unit streamflow
        hp_streamflow:    HP max unit streamflow
        soil_sat:         SAC max soil saturation fraction (0–1)
        ffg_ratio:        QPE-to-FFG ratio
        rqi:              Radar Quality Index (0–1)

    Returns:
        Tuple of:
            threat_grid:   2D int array (0–100)
            rainfall_grid: 2D float array — Pillar 1 score (0–1)
            hydro_grid:    2D float array — Pillar 2 score (0–1)
            ffg_grid:      2D float array — Pillar 3 score (0–1)
    """
    shape = ari_max.shape

    # ── Pillar 1: Rainfall Extremity ────────────────────────────────
    # Process ARI grids and free sentinel-masked copies immediately
    ari_max_c = _mask_sentinels(ari_max)
    ari_max_norm = _normalize_ari(ari_max_c)
    del ari_max_c

    ari_30m_c = _mask_sentinels(ari_30m)
    ari_30m_norm = _normalize_ari(ari_30m_c)
    del ari_30m_c

    ari_01h_c = _mask_sentinels(ari_01h)
    ari_01h_norm = _normalize_ari(ari_01h_c)
    del ari_01h_c

    indicators = [
        (ari_max_norm, cfg.ARI_SUB_WEIGHTS["ari_max"]),
        (ari_30m_norm, cfg.ARI_SUB_WEIGHTS["ari_30m"]),
        (ari_01h_norm, cfg.ARI_SUB_WEIGHTS["ari_01h"]),
    ]
    rainfall_grid = _weighted_nanmean(indicators, shape)
    del ari_max_norm, ari_30m_norm, ari_01h_norm

    # ── Pillar 2: Hydrologic Response ───────────────────────────────
    crest_c = _mask_sentinels(crest_streamflow)
    crest_norm = _sigmoid(crest_c, cfg.CREST_SIGMOID["x0"], cfg.CREST_SIGMOID["k"])
    del crest_c

    hp_c = _mask_sentinels(hp_streamflow)
    hp_norm = _sigmoid(hp_c, cfg.HP_SIGMOID["x0"], cfg.HP_SIGMOID["k"])
    del hp_c

    soil_c = _mask_sentinels(soil_sat)
    soil_norm = _normalize_soil_sat(soil_c)

    stream_indicators = [
        (crest_norm, cfg.CREST_SUB_WEIGHT),
        (hp_norm, cfg.HP_SUB_WEIGHT),
    ]
    streamflow_blend = _weighted_nanmean(stream_indicators, shape)
    del crest_norm, hp_norm

    hydro_grid = (
        streamflow_blend * cfg.HYDRO_STREAMFLOW_WEIGHT
        + soil_norm * cfg.HYDRO_SOIL_WEIGHT
    )
    del streamflow_blend, soil_norm

    # Soil saturation conditioning: boost when soil > 0.85
    soil_boost_mask = soil_c > cfg.SOIL_BOOST_THRESHOLD
    boost_range = 1.0 - cfg.SOIL_BOOST_THRESHOLD  # 0.15
    if boost_range > 0:
        boost_factor = np.where(
            soil_boost_mask,
            1.0 + cfg.SOIL_BOOST_MAX * ((soil_c - cfg.SOIL_BOOST_THRESHOLD) / boost_range),
            1.0,
        )
        boost_factor = np.where(np.isnan(boost_factor), 1.0, boost_factor)
        hydro_grid = hydro_grid * boost_factor
        del boost_factor
    del soil_c, soil_boost_mask

    hydro_grid = np.clip(hydro_grid, 0.0, 1.0)

    # ── Pillar 3: Guidance Exceedance ───────────────────────────────
    ffg_c = _mask_sentinels(ffg_ratio)
    ffg_grid = _normalize_ffg(np.nan_to_num(ffg_c, nan=0.0))
    del ffg_c

    # ── Replace NaN pillar scores with 0 ────────────────────────────
    rainfall_grid = np.nan_to_num(rainfall_grid, nan=0.0)
    hydro_grid = np.nan_to_num(hydro_grid, nan=0.0)

    # ── Composite score with pillar weights ─────────────────────────
    pw = cfg.PILLAR_WEIGHTS
    composite = (
        rainfall_grid * pw["rainfall"]
        + hydro_grid * pw["hydro"]
        + ffg_grid * pw["ffg"]
    )

    # ── Quality control: RQI weighting ──────────────────────────────
    rqi_c = _mask_sentinels(rqi)
    rqi_w = _rqi_weight(np.nan_to_num(rqi_c, nan=0.0))
    del rqi_c
    adjusted = composite * rqi_w
    del composite, rqi_w

    # ── Scale to 0–100 integer ──────────────────────────────────────
    threat_grid = np.clip(np.round(adjusted * 100).astype(int), 0, 100)
    del adjusted

    return threat_grid, rainfall_grid, hydro_grid, ffg_grid


# ─────────────────────────────────────────────────────────────────────
# Helper: weighted mean with NaN redistribution
# ─────────────────────────────────────────────────────────────────────

def _weighted_nanmean(
    indicators: list,
    shape: tuple,
) -> np.ndarray:
    """
    Compute weighted average over indicators, redistributing weights
    from NaN entries to valid entries on a per-pixel basis.

    Args:
        indicators: list of (array, weight) tuples
        shape: output array shape

    Returns:
        Weighted mean array (NaN where all indicators are NaN)
    """
    weighted_sum = np.zeros(shape, dtype=np.float64)
    weight_sum = np.zeros(shape, dtype=np.float64)

    for arr, weight in indicators:
        valid = ~np.isnan(arr)
        weighted_sum += np.where(valid, arr * weight, 0.0)
        weight_sum += np.where(valid, weight, 0.0)

    # Avoid division by zero
    result = np.where(weight_sum > 0, weighted_sum / weight_sum, np.nan)
    return result
