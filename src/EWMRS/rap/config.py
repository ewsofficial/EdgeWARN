"""Configuration for EWMRS RAP Uint16Array conversion."""

from __future__ import annotations

import util.file as fs

UINT16_NODATA = 65535
UINT16_VALID_MAX = 65534
RAP_WIND_PRESSURE_LEVELS_MB = (925, 850, 700, 500, 250)
RAP_THERMO_PRESSURE_LEVELS_MB = (925, 850, 700, 500, 250)


def _outdir(layer_name: str):
    return fs.GUI_RAP_DIR / layer_name.removeprefix("RAP_")


def _pressure_thermo_layers() -> list[dict]:
    layers = []
    for level in RAP_THERMO_PRESSURE_LEVELS_MB:
        for name_part, short_name, units, scale, description in (
            ("Temperature", "t", "K", {"min": 180.0, "max": 330.0}, "temperature"),
            ("RelativeHumidity", "r", "%", {"min": 0.0, "max": 100.0}, "relative humidity"),
        ):
            name = f"RAP_{name_part}_{level}mb"
            layers.append({
                "name": name,
                "short_names": [short_name],
                "filter": {"typeOfLevel": "isobaricInhPa", "level": level},
                "units": units,
                "scale": scale,
                "outdir": _outdir(name),
                "description": f"RAP {level} mb {description}",
            })
    return layers


def _surface_and_precip_layers() -> list[dict]:
    return [
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
            "scale": {"min": 250.0, "max": 400.0},
            "outdir": _outdir("RAP_ThetaE_Surface"),
            "description": "RAP surface pseudo-adiabatic potential temperature",
        },
        {
            "name": "RAP_PrecipitationRate_Surface",
            "short_names": ["prate"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "kg m-2 s-1",
            "scale": {"min": 0.0, "max": 0.05},
            "outdir": _outdir("RAP_PrecipitationRate_Surface"),
            "description": "RAP surface precipitation rate",
        },
        {
            "name": "RAP_TotalPrecipitation_Surface",
            "short_names": ["tp"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "kg m-2",
            "scale": {"min": 0.0, "max": 100.0},
            "outdir": _outdir("RAP_TotalPrecipitation_Surface"),
            "description": "RAP total precipitation",
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
            "name": "RAP_FreezingRain_Surface",
            "short_names": ["frzr"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "kg m-2",
            "scale": {"min": 0.0, "max": 50.0},
            "outdir": _outdir("RAP_FreezingRain_Surface"),
            "description": "RAP freezing rain accumulation",
        },
        {
            "name": "RAP_CategoricalSnow_Surface",
            "short_names": ["csnow"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "category",
            "scale": {"min": 0.0, "max": 1.0},
            "outdir": _outdir("RAP_CategoricalSnow_Surface"),
            "description": "RAP categorical snow flag",
        },
        {
            "name": "RAP_CategoricalIcePellets_Surface",
            "short_names": ["cicep"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "category",
            "scale": {"min": 0.0, "max": 1.0},
            "outdir": _outdir("RAP_CategoricalIcePellets_Surface"),
            "description": "RAP categorical ice pellets flag",
        },
        {
            "name": "RAP_CategoricalFreezingRain_Surface",
            "short_names": ["cfrzr"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "category",
            "scale": {"min": 0.0, "max": 1.0},
            "outdir": _outdir("RAP_CategoricalFreezingRain_Surface"),
            "description": "RAP categorical freezing rain flag",
        },
        {
            "name": "RAP_CategoricalRain_Surface",
            "short_names": ["crain"],
            "filter": {"typeOfLevel": "surface", "level": 0},
            "units": "category",
            "scale": {"min": 0.0, "max": 1.0},
            "outdir": _outdir("RAP_CategoricalRain_Surface"),
            "description": "RAP categorical rain flag",
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
    ]


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
            "name": "RAP_BestLiftedIndex_180_0mbAGL",
            "short_names": ["4lftx"],
            "filter": {"typeOfLevel": "pressureFromGroundLayer", "level": 18000},
            "units": "K",
            "scale": {"min": -15.0, "max": 15.0},
            "outdir": fs.GUI_RAP_DIR / "BestLiftedIndex_180-0mbAGL",
            "description": "RAP best four-layer lifted index from the 180-0 mb above ground layer",
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
                "outdir": _outdir(name),
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
    ]
