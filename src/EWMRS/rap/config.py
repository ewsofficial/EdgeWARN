"""Configuration for EWMRS RAP Uint16Array conversion."""

from __future__ import annotations

import util.file as fs

UINT16_NODATA = 65535
UINT16_VALID_MAX = 65534
RAP_WIND_PRESSURE_LEVELS_MB = (925, 850, 700, 500, 250)


def _instability_and_shear_layers() -> list[dict]:
    return [
        {
            "name": "RAP_CAPE_Surface",
            "short_names": ["cape"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "J kg-1",
            "scale": {"min": 0.0, "max": 6000.0},
            "outdir": fs.GUI_RAP_DIR / "CAPE_Surface",
            "description": "RAP surface convective available potential energy",
        },
        {
            "name": "RAP_CIN_Surface",
            "short_names": ["cin"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "J kg-1",
            "scale": {"min": -1000.0, "max": 0.0},
            "outdir": fs.GUI_RAP_DIR / "CIN_Surface",
            "description": "RAP surface convective inhibition",
        },
        {
            "name": "RAP_MLCAPE",
            "short_names": ["cape"],
            "filter": {"typeOfLevel": "pressureFromGroundLayer", "level": 9000},
            "units": "J kg-1",
            "scale": {"min": 0.0, "max": 6000.0},
            "outdir": fs.GUI_RAP_DIR / "MLCAPE",
            "description": "RAP mixed-layer CAPE from the 90-0 mb above ground layer",
        },
        {
            "name": "RAP_MUCAPE",
            "short_names": ["cape"],
            "filter": {"typeOfLevel": "pressureFromGroundLayer", "level": 25500},
            "units": "J kg-1",
            "scale": {"min": 0.0, "max": 6000.0},
            "outdir": fs.GUI_RAP_DIR / "MUCAPE",
            "description": "RAP most-unstable CAPE from the 255-0 mb above ground layer",
        },
        {
            "name": "RAP_SRH_0_3km",
            "short_names": ["hlcy"],
            "filter": {"typeOfLevel": "heightAboveGroundLayer", "level": 3000},
            "units": "m2 s-2",
            "scale": {"min": -500.0, "max": 1000.0},
            "outdir": fs.GUI_RAP_DIR / "SRH_0-3km",
            "description": "RAP 0-3 km storm-relative helicity",
        },
        {
            "name": "RAP_SRH_0_1km",
            "short_names": ["hlcy"],
            "filter": {"typeOfLevel": "heightAboveGroundLayer", "level": 1000},
            "units": "m2 s-2",
            "scale": {"min": -500.0, "max": 1000.0},
            "outdir": fs.GUI_RAP_DIR / "SRH-0_1km",
            "description": "RAP 0-1 km storm-relative helicity",
        },
    ]


def _pressure_wind_layers() -> list[dict]:
    layers = []
    for level in RAP_WIND_PRESSURE_LEVELS_MB:
        for component, label in (("U", "u"), ("V", "v")):
            name = f"RAP_{component}Wind_{level}mb"
            layers.append({
                "name": name,
                "short_names": [label],
                "filter": {"typeOfLevel": "isobaricInhPa", "level": level},
                "units": "m s-1",
                "scale": {"min": -80.0, "max": 80.0},
                "outdir": fs.GUI_RAP_DIR / name,
                "description": f"RAP {level} mb {label}-component wind",
            })
    return layers


def get_rap_uint16_layers() -> list[dict]:
    """Return RAP layers converted to one Uint16Array-compatible file each."""
    return [
        {
            "name": "RAP_Temperature_2m",
            "short_names": ["2t", "t", "t2m"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
            "units": "K",
            "scale": {"min": 180.0, "max": 330.0},
            "outdir": fs.GUI_RAP_DIR / "RAP_Temperature_2m",
            "description": "RAP 2 meter temperature",
        },
        {
            "name": "RAP_Dewpoint_2m",
            "short_names": ["2d", "d2m", "dpt"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
            "units": "K",
            "scale": {"min": 180.0, "max": 330.0},
            "outdir": fs.GUI_RAP_DIR / "RAP_Dewpoint_2m",
            "description": "RAP 2 meter dewpoint temperature",
        },
        {
            "name": "RAP_UWind_10m",
            "short_names": ["10u", "u10", "u"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 10},
            "units": "m s-1",
            "scale": {"min": -80.0, "max": 80.0},
            "outdir": fs.GUI_RAP_DIR / "RAP_UWind_10m",
            "description": "RAP 10 meter u-component wind",
        },
        {
            "name": "RAP_VWind_10m",
            "short_names": ["10v", "v10", "v"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 10},
            "units": "m s-1",
            "scale": {"min": -80.0, "max": 80.0},
            "outdir": fs.GUI_RAP_DIR / "RAP_VWind_10m",
            "description": "RAP 10 meter v-component wind",
        },
        *_instability_and_shear_layers(),
        *_pressure_wind_layers(),
    ]
