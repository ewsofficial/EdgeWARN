"""
FLOHAR Scoring Engine

Vectorized numpy engine that computes per-pixel composite threat scores
(0-100) from seven FLASH indicator grids + RQI quality control.

Three pillars:
    1. Rainfall Extremity (0.40)  — ARI-based
    2. Hydrologic Response (0.35) — streamflow + soil saturation
    3. Guidance Exceedance (0.25) — QPE-to-FFG ratio

Performance notes:
    - All intermediate arrays use float32 to halve memory vs float64
    - RQI hard mask is applied early to skip scoring on invalid pixels
    - _weighted_nanmean uses a fast path when no NaN values are present
    - Intermediates are freed with `del` immediately after use
"""

import numpy as np
from typing import Tuple

from . import config as cfg

# ── Pre-computed constants ──────────────────────────────────────────
_LOG10_ARI_CEILING = np.float32(np.log10(cfg.ARI_CEILING_YEARS))
_DTYPE = np.float32


# ─────────────────────────────────────────────────────────────────────
# Internal normalisation helpers (all vectorized, float32)
# ─────────────────────────────────────────────────────────────────────

def _mask_sentinels(arr: np.ndarray) -> np.ndarray:
    """Replace sentinel values with NaN. Uses float32."""
    out = arr.astype(_DTYPE, copy=True)
    for s in cfg.SENTINEL_VALUES:
        out[out == np.float32(s)] = np.nan
    return out


def _normalize_ari(ari: np.ndarray) -> np.ndarray:
    """
    Logarithmic ARI normalization.
    ARI of ARI_CEILING_YEARS → 1.0, ARI ≤ 1 → ~0.
    """
    safe = np.where(ari > 0, ari, np.float32(1.0))
    normed = np.log10(safe) / _LOG10_ARI_CEILING
    return np.clip(normed, 0.0, 1.0)


def _sigmoid(x: np.ndarray, x0: float, k: float) -> np.ndarray:
    """Sigmoid normalization: 1 / (1 + exp(-k * (x - x0)))."""
    return np.float32(1.0) / (np.float32(1.0) + np.exp(np.float32(-k) * (x - np.float32(x0))))


def _normalize_soil_sat(soil: np.ndarray) -> np.ndarray:
    """Linear clamp: [SOIL_SAT_LOW, SOIL_SAT_HIGH] → [0, 1]."""
    span = np.float32(cfg.SOIL_SAT_HIGH - cfg.SOIL_SAT_LOW)
    if span <= 0:
        return np.zeros_like(soil)
    normed = (soil - np.float32(cfg.SOIL_SAT_LOW)) / span
    return np.clip(normed, 0.0, 1.0)


def _normalize_ffg(ratio: np.ndarray) -> np.ndarray:
    """
    Piecewise FFG ratio normalization:
        < 0.75 → 0
        0.75 → 1.0  → ramp 0 → 0.5
        1.0  → 2.0  → ramp 0.5 → 1.0
        ≥ 2.0       → 1.0
    """
    out = np.zeros_like(ratio, dtype=_DTYPE)

    ramp_start = np.float32(cfg.FFG_RAMP_START)
    ramp_mid = np.float32(cfg.FFG_RAMP_MID)
    ramp_end = np.float32(cfg.FFG_RAMP_END)

    # Segment 1: 0.75 ≤ ratio < 1.0  →  ramp 0→0.5
    mask1 = (ratio >= ramp_start) & (ratio < ramp_mid)
    span1 = ramp_mid - ramp_start
    if span1 > 0:
        out[mask1] = np.float32(0.5) * (ratio[mask1] - ramp_start) / span1

    # Segment 2: 1.0 ≤ ratio < 2.0  →  ramp 0.5→1.0
    mask2 = (ratio >= ramp_mid) & (ratio < ramp_end)
    span2 = ramp_end - ramp_mid
    if span2 > 0:
        out[mask2] = np.float32(0.5) + np.float32(0.5) * (ratio[mask2] - ramp_mid) / span2

    # Segment 3: ratio ≥ 2.0  →  1.0
    out[ratio >= ramp_end] = np.float32(1.0)

    return out


def _rqi_weight(rqi: np.ndarray) -> np.ndarray:
    """
    RQI quality-control weight:
        ≥ RQI_MIN_WEIGHT_THRESHOLD → 1.0 (Valid, no score penalty)
        < RQI_MIN_WEIGHT_THRESHOLD → 0.0 (Hard mask, unreliable data)
        
    Note: RQI previously ramped the score down between min and full thresholds.
    This was removed per feedback because poor radar quality reduces confidence
    but should not artificially reduce the severity/threat score of what is detected.
    """
    out = np.zeros_like(rqi, dtype=_DTYPE)
    
    rqi_min = np.float32(cfg.RQI_MIN_WEIGHT_THRESHOLD)
    out[rqi >= rqi_min] = np.float32(1.0)
    
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

    Uses float32 throughout to halve memory usage vs float64.
    Applies RQI hard mask early to skip computation on invalid pixels.

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
            rainfall_grid: 2D float32 array — Pillar 1 score (0–1)
            hydro_grid:    2D float32 array — Pillar 2 score (0–1)
            ffg_grid:      2D float32 array — Pillar 3 score (0–1)
    """
    shape = ari_max.shape

    # ── Early RQI masking ───────────────────────────────────────────
    # Compute RQI weight first so we can identify pixels to skip.
    # Pixels with rqi < RQI_MIN_WEIGHT_THRESHOLD will always be 0.
    rqi_c = _mask_sentinels(rqi)
    rqi_w = _rqi_weight(np.nan_to_num(rqi_c, nan=np.float32(0.0)))
    del rqi_c

    # valid_mask: True where RQI is high enough to produce nonzero output
    valid_mask = rqi_w > 0

    # If no valid pixels, return zeros immediately
    if not np.any(valid_mask):
        zeros = np.zeros(shape, dtype=_DTYPE)
        return (
            np.zeros(shape, dtype=int),
            zeros.copy(), zeros.copy(), zeros.copy(),
        )

    # ── Pillar 1: Rainfall Extremity ────────────────────────────────
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
    # For streamflow, sentinels usually mean 0 flow (no data = no flow in MRMS).
    # If we mask them as NaN, the engine redistributes their 70% weight
    # to soil saturation, causing the hydro score to falsely max out
    # in areas with wet soil but zero flooding.
    crest_c = np.where(np.isin(crest_streamflow, cfg.SENTINEL_VALUES), np.float32(0.0), crest_streamflow)
    crest_norm = _sigmoid(crest_c, cfg.CREST_SIGMOID["x0"], cfg.CREST_SIGMOID["k"])
    del crest_c

    hp_c = np.where(np.isin(hp_streamflow, cfg.SENTINEL_VALUES), np.float32(0.0), hp_streamflow)
    hp_norm = _sigmoid(hp_c, cfg.HP_SIGMOID["x0"], cfg.HP_SIGMOID["k"])
    del hp_c

    # Convert soil saturation from raw percentage to fraction
    soil_c = _mask_sentinels(soil_sat) / np.float32(100.0)
    soil_norm = _normalize_soil_sat(soil_c)

    stream_indicators = [
        (crest_norm, cfg.CREST_SUB_WEIGHT),
        (hp_norm, cfg.HP_SUB_WEIGHT),
    ]
    streamflow_blend = _weighted_nanmean(stream_indicators, shape)
    del crest_norm, hp_norm

    hydro_grid = (
        streamflow_blend * np.float32(cfg.HYDRO_STREAMFLOW_WEIGHT)
        + soil_norm * np.float32(cfg.HYDRO_SOIL_WEIGHT)
    )
    del streamflow_blend, soil_norm

    # Soil saturation conditioning: boost when soil > 0.85
    soil_boost_threshold = np.float32(cfg.SOIL_BOOST_THRESHOLD)
    soil_boost_mask = soil_c > soil_boost_threshold
    boost_range = np.float32(1.0) - soil_boost_threshold
    if boost_range > 0:
        boost_factor = np.where(
            soil_boost_mask,
            np.float32(1.0) + np.float32(cfg.SOIL_BOOST_MAX) * ((soil_c - soil_boost_threshold) / boost_range),
            np.float32(1.0),
        )
        boost_factor = np.where(np.isnan(boost_factor), np.float32(1.0), boost_factor)
        hydro_grid = hydro_grid * boost_factor
        del boost_factor
    del soil_c, soil_boost_mask

    hydro_grid = np.clip(hydro_grid, 0.0, 1.0)

    # ── Pillar 3: Guidance Exceedance ───────────────────────────────
    # Convert FFG exceedance from raw percentage to ratio
    ffg_c = _mask_sentinels(ffg_ratio) / np.float32(100.0)
    ffg_grid = _normalize_ffg(np.nan_to_num(ffg_c, nan=np.float32(0.0)))
    del ffg_c

    # ── Replace NaN pillar scores with 0 ────────────────────────────
    rainfall_grid = np.nan_to_num(rainfall_grid, nan=np.float32(0.0))
    hydro_grid = np.nan_to_num(hydro_grid, nan=np.float32(0.0))

    # ── Composite score with pillar weights ─────────────────────────
    pw = cfg.PILLAR_WEIGHTS
    composite = (
        rainfall_grid * np.float32(pw["rainfall"])
        + hydro_grid * np.float32(pw["hydro"])
        + ffg_grid * np.float32(pw["ffg"])
    )

    # ── Apply RQI weighting (already computed early) ────────────────
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

    Includes a fast path that skips NaN checking when no NaN values
    are present, avoiding boolean mask allocation overhead.

    Args:
        indicators: list of (array, weight) tuples
        shape: output array shape

    Returns:
        Weighted mean array (NaN where all indicators are NaN)
    """
    # Fast path: check if any NaN exists across all indicators
    has_nan = False
    for arr, _ in indicators:
        if np.isnan(arr).any():
            has_nan = True
            break

    if not has_nan:
        # Direct weighted sum — no NaN redistribution needed
        total_weight = sum(w for _, w in indicators)
        if total_weight <= 0:
            return np.zeros(shape, dtype=_DTYPE)
        result = np.zeros(shape, dtype=_DTYPE)
        for arr, weight in indicators:
            result += arr * np.float32(weight)
        result /= np.float32(total_weight)
        return result

    # Slow path: per-pixel NaN weight redistribution
    weighted_sum = np.zeros(shape, dtype=_DTYPE)
    weight_sum = np.zeros(shape, dtype=_DTYPE)

    for arr, weight in indicators:
        valid = ~np.isnan(arr)
        weighted_sum += np.where(valid, arr * np.float32(weight), np.float32(0.0))
        weight_sum += np.where(valid, np.float32(weight), np.float32(0.0))

    result = np.where(weight_sum > 0, weighted_sum / weight_sum, np.nan)
    return result
