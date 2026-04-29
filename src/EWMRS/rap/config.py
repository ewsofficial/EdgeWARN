"""Configuration for EWMRS RAP Uint16Array conversion."""

from __future__ import annotations

import util.file as fs

UINT16_NODATA = 65535
UINT16_VALID_MAX = 65534
RAP_WIND_PRESSURE_LEVELS_MB = (925, 850, 700, 500, 250)
RAP_THERMO_PRESSURE_LEVELS_MB = (925, 850, 700, 500, 250)


def _outdir(layer_name: str):
    return fs.GUI_RAP_DIR / layer_name.removeprefix("RAP_")


def _wind_colormap_key(name: str) -> str:
    if name in ("RAP_UWind_10m", "RAP_VWind_10m"):
        return "RAP_Wind_LL"

    for level in (925, 850):
        if name.endswith(f"_{level}mb"):
            return "RAP_Wind_LL"

    for level in (700, 500):
        if name.endswith(f"_{level}mb"):
            return "RAP_Wind_ML"

    return "RAP_Wind_HL"


def _temperature_colormap_key(name: str) -> str:
    # Low-level temperatures: surface, 2m, 925mb, 850mb, 700mb
    if name in ("RAP_Temperature_Surface", "RAP_Temperature_2m"):
        return "RAP_Temperature_LL"

    for level in (925, 850, 700):
        if name.endswith(f"_{level}mb"):
            return "RAP_Temperature_LL"

    # High-level temperatures: 500mb, 250mb
    for level in (500, 250):
        if name.endswith(f"_{level}mb"):
            return "RAP_Temperature_HL"

    return "RAP_Temperature_LL"


def _with_colormap_key(layer: dict) -> dict:
    # Map layers to their appropriate colormaps
    name = layer["name"]
    if name in ("RAP_CAPE_Surface", "RAP_MLCAPE", "RAP_MUCAPE", "RAP_CAPE_0_3km"):
        colormap_key = "RAP_CAPE"
    elif name in ("RAP_SRH_0-3km", "RAP_SRH_0-1km"):
        colormap_key = "RAP_SRH"
    elif name.startswith("RAP_Temperature_"):
        colormap_key = _temperature_colormap_key(name)
    elif name.startswith("RAP_RelativeHumidity_"):
        colormap_key = "RAP_RelativeHumidity"
    elif name.startswith("RAP_UWind_") or name.startswith("RAP_VWind_"):
        colormap_key = _wind_colormap_key(name)
    elif name == "RAP_MSLP_Surface":
        colormap_key = None
    else:
        colormap_key = name
    return {**layer, "colormap_key": colormap_key} if colormap_key is not None else layer


def _pressure_thermo_layers() -> list[dict]:
    layers = []
    for level in RAP_THERMO_PRESSURE_LEVELS_MB:
        for name_part, short_name, units, scale, description in (
            ("Temperature", "t", "K", {"min": 180.0, "max": 330.0}, "temperature"),
            ("RelativeHumidity", "r", "%", {"min": 0.0, "max": 100.0}, "relative humidity"),
        ):
            name = f"RAP_{name_part}_{level}mb"
            layers.append(_with_colormap_key({
                "name": name,
                "short_names": [short_name],
                "filter": {"typeOfLevel": "isobaricInhPa", "level": level},
                "units": units,
                "scale": scale,
                "outdir": _outdir(name),
                "description": f"RAP {level} mb {description}",
            }))
    return layers


def _surface_and_precip_layers() -> list[dict]:
     return [_with_colormap_key(layer) for layer in [
         {
             "name": "RAP_Temperature_Surface",
             "short_names": ["t"],
             "filter": {"typeOfLevel": "surface", "level": 0},
             "units": "K",
             "scale": {"min": 180.0, "max": 330.0},
             "outdir": _outdir("RAP_Temperature_Surface"),
             "description": "RAP surface temperature",
         },
         {
             "name": "RAP_RelativeHumidity_2m",
             "short_names": ["2r", "r"],
             "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
             "units": "%",
             "scale": {"min": 0.0, "max": 100.0},
             "outdir": _outdir("RAP_RelativeHumidity_2m"),
             "description": "RAP 2 meter relative humidity",
         },
         {
             "name": "RAP_ThetaE_Surface",
             "short_names": ["papt"],
             "filter": {"typeOfLevel": "surface", "level": 0},
             "units": "K",
             "scale": {"min": 250.0, "max": 390.0},
             "outdir": _outdir("RAP_ThetaE_Surface"),
             "description": "RAP surface pseudo-adiabatic potential temperature",
         },
         {
             "name": "RAP_MSLP_Surface",
             "short_names": ["prmsl"],
             "filter": {"typeOfLevel": "surface", "level": 0},
             "units": "Pa",
             "scale": {"min": 95000.0, "max": 105000.0},
             "outdir": _outdir("RAP_MSLP_Surface"),
             "description": "RAP surface mean sea level pressure",
         },
         {
             "name": "RAP_SnowWaterEquivalent_Surface",
             "short_names": ["sdwe"],
             "filter": {"typeOfLevel": "surface", "level": 0},
             "units": "kg m-2",
             "scale": {"min": 0.0, "max": 200.0},
             "outdir": _outdir("RAP_SnowWaterEquivalent_Surface"),
             "description": "RAP snow water equivalent",
         },
        {
            "name": "RAP_SnowDepth_Surface",
            "short_names": ["sde"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "m",
            "scale": {"min": 0.0, "max": 5.0},
            "outdir": _outdir("RAP_SnowDepth_Surface"),
            "description": "RAP snow depth",
        },
        {
            "name": "RAP_WetBulbZeroHeight",
            "short_names": ["gh"],
            "filter": {"typeOfLevel": "lowestLevelWetBulb0", "level": 0},
            "units": "gpm",
            "scale": {"min": 0.0, "max": 6000.0},
            "outdir": _outdir("RAP_WetBulbZeroHeight"),
            "description": "RAP wet bulb zero height",
        },
        {
            "name": "RAP_FreezingLevelHeight",
            "short_names": ["gh"],
            "filter": {"typeOfLevel": "isothermZero", "level": 0},
            "units": "gpm",
            "scale": {"min": 0.0, "max": 6000.0},
            "outdir": _outdir("RAP_FreezingLevelHeight"),
            "description": "RAP 0C isotherm height",
        },
    ]]


def _instability_and_shear_layers() -> list[dict]:
    return [_with_colormap_key(layer) for layer in [
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
            "name": "RAP_MLCIN",
            "short_names": ["cin"],
            "filter": {"typeOfLevel": "pressureFromGroundLayer", "level": 9000},
            "units": "J kg-1",
            "scale": {"min": -1000.0, "max": 0.0},
            "outdir": fs.GUI_RAP_DIR / "MLCIN",
            "description": "RAP mixed-layer CIN from the 90-0 mb above ground layer",
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
            "name": "RAP_MUCIN",
            "short_names": ["cin"],
            "filter": {"typeOfLevel": "pressureFromGroundLayer", "level": 25500},
            "units": "J kg-1",
            "scale": {"min": -1000.0, "max": 0.0},
            "outdir": fs.GUI_RAP_DIR / "MUCIN",
            "description": "RAP most-unstable CIN from the 255-0 mb above ground layer",
        },
        {
            "name": "RAP_CAPE_0_3km",
            "short_names": ["cape"],
            "filter": {"typeOfLevel": "heightAboveGroundLayer", "level": 0},
            "units": "J kg-1",
            "scale": {"min": 0.0, "max": 6000.0},
            "outdir": fs.GUI_RAP_DIR / "CAPE_0-3km",
            "description": "RAP 0-3 km CAPE",
        },
        {
            "name": "RAP_SRH_0-3km",
            "short_names": ["hlcy"],
            "filter": {"typeOfLevel": "heightAboveGroundLayer", "level": 3000},
            "units": "m2 s-2",
            "scale": {"min": -500.0, "max": 1000.0},
            "outdir": fs.GUI_RAP_DIR / "SRH_0-3km",
            "description": "RAP 0-3 km storm-relative helicity",
        },
        {
            "name": "RAP_SRH_0-1km",
            "short_names": ["hlcy"],
            "filter": {"typeOfLevel": "heightAboveGroundLayer", "level": 1000},
            "units": "m2 s-2",
            "scale": {"min": -500.0, "max": 1000.0},
            "outdir": fs.GUI_RAP_DIR / "SRH-0-1km",
            "description": "RAP 0-1 km storm-relative helicity",
        },
        {
            "name": "RAP_LiftedIndex_Surface_500_1000mb",
            "short_names": ["lftx"],
            "filter": {"typeOfLevel": "isobaricLayer", "level": 500},
            "units": "K",
            "scale": {"min": -15.0, "max": 15.0},
            "outdir": fs.GUI_RAP_DIR / "LiftedIndex_Surface_500-1000mb",
            "description": "RAP surface lifted index over the 500-1000 mb layer",
        },
        {
            "name": "RAP_AbsoluteVorticity_500mb",
            "short_names": ["absv"],
            "filter": {"typeOfLevel": "isobaricInhPa", "level": 500},
            "units": "s-1",
            "scale": {"min": -0.0002, "max": 0.0002},
            "outdir": fs.GUI_RAP_DIR / "AbsoluteVorticity_500mb",
            "description": "RAP 500 mb absolute vorticity",
        },
    ]]


def _pressure_wind_layers() -> list[dict]:
    layers = []
    for level in RAP_WIND_PRESSURE_LEVELS_MB:
        for component, label in (("U", "u"), ("V", "v")):
            name = f"RAP_{component}Wind_{level}mb"
            layers.append(_with_colormap_key({
                "name": name,
                "short_names": [label],
                "filter": {"typeOfLevel": "isobaricInhPa", "level": level},
                "units": "m s-1",
                "scale": {"min": -80.0, "max": 80.0},
                "outdir": _outdir(name),
                "description": f"RAP {level} mb {label}-component wind",
            }))
    return layers


def get_rap_uint16_layers() -> list[dict]:
    """Return RAP layers converted to one Uint16Array-compatible file each."""
    return [_with_colormap_key(layer) for layer in [
        {
            "name": "RAP_Temperature_2m",
            "short_names": ["2t", "t", "t2m"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
            "units": "K",
            "scale": {"min": 180.0, "max": 330.0},
            "outdir": _outdir("RAP_Temperature_2m"),
            "description": "RAP 2 meter temperature",
        },
        {
            "name": "RAP_Dewpoint_2m",
            "short_names": ["2d", "d2m", "dpt"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 2},
            "units": "K",
            "scale": {"min": 180.0, "max": 330.0},
            "outdir": _outdir("RAP_Dewpoint_2m"),
            "description": "RAP 2 meter dewpoint temperature",
        },
        {
            "name": "RAP_UWind_10m",
            "short_names": ["10u", "u10", "u"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 10},
            "units": "m s-1",
            "scale": {"min": -80.0, "max": 80.0},
            "outdir": _outdir("RAP_UWind_10m"),
            "description": "RAP 10 meter u-component wind",
        },
        {
            "name": "RAP_VWind_10m",
            "short_names": ["10v", "v10", "v"],
            "filter": {"typeOfLevel": "heightAboveGround", "level": 10},
            "units": "m s-1",
            "scale": {"min": -80.0, "max": 80.0},
            "outdir": _outdir("RAP_VWind_10m"),
            "description": "RAP 10 meter v-component wind",
        },
        *_surface_and_precip_layers(),
        *_instability_and_shear_layers(),
        *_pressure_thermo_layers(),
        *_pressure_wind_layers(),
    ]]
