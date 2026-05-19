"""Shared xradar helpers for NEXRAD Level-II parsing."""
from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np


def open_partial_volume(path: str | Path):
    """Open a partial NEXRAD Level-II file with xradar, dropping incomplete sweeps."""
    try:
        import xradar as xd
    except ImportError as exc:
        raise RuntimeError("xradar is required for NEXRAD volume parsing") from exc

    opener = getattr(xd.io.backends.nexrad_level2, "open_nexradlevel2_datatree", None)
    if opener is None:
        raise RuntimeError("xradar nexrad Level-II DataTree opener is unavailable")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            return opener(str(path), loaddata=False, incomplete_sweep="drop")
        except TypeError:
            return opener(str(path))


def extract_sweep_timestamp(ds) -> str | None:
    """Extract sweep timestamp from xarray time coordinate."""
    try:
        values = np.asarray(ds["time"].values).reshape(-1)
        values = values[~np.isnat(values)]
        if len(values) == 0:
            return None
        return np.datetime_as_string(values.max(), unit="s", timezone="UTC")
    except Exception:
        return None


def extract_sweep_angle(ds) -> float | None:
    """Extract fixed angle from sweep dataset."""
    try:
        angle_var = ds.get("sweep_fixed_angle")
        if angle_var is None:
            return None
        return float(angle_var.values.item())
    except Exception:
        return None


def extract_waveform(node) -> str | None:
    """Extract waveform type from sweep node."""
    try:
        attrs = getattr(node, "attrs", {}) or {}
        dataset = node.ds if hasattr(node, "ds") else node.to_dataset()
        return (
            attrs.get("waveform_type")
            or dataset.attrs.get("waveform_type")
            or dataset.attrs.get("prt_mode")
            or dataset.attrs.get("sweep_mode")
        )
    except Exception:
        return None


def extract_azimuth_count(ds) -> int:
    """Extract azimuth count from sweep dataset."""
    try:
        return int(ds.sizes.get("azimuth", ds.sizes.get("time", 0)))
    except Exception:
        return 0
