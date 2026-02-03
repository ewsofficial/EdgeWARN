# RAP GRIB2 Data Structure Reference

This document details the structure of RAP (Rapid Refresh) GRIB2 files as used by EdgeWARN, based on inspection via `cfgrib.open_datasets()`.

---

## Overview

RAP files contain **39 distinct datasets**, each representing a different level type or layer. `cfgrib` automatically separates these when loading.

---

## Key Datasets for MorphoWind

### 1. Surface / 2m Data (Temperature & Dewpoint)

| Dataset | Level Type | Level Value | Variables |
|:--------|:-----------|:------------|:----------|
| **11** | `heightAboveGround` | `2.0` m | `t2m`, `d2m`, `sh2`, `r2`, `pt` |

- **`t2m`**: 2-meter Temperature (Kelvin)
- **`d2m`**: 2-meter Dewpoint (Kelvin)
- **Derived**: `dewpoint_depression = t2m - d2m` (Convert to °C first)

### 2. Isobaric Winds (Upper Air)

| Dataset | Level Type | Levels | Variables |
|:--------|:-----------|:-------|:----------|
| **19** | `isobaricInhPa` | 37 levels (1000–100 hPa) | `u`, `v`, `t`, `gh`, `r`, `w` |

- **`u`**: U-component of wind (m/s)
- **`v`**: V-component of wind (m/s)
- **Target Levels**: `850`, `700`, `500`, `250` hPa

### 3. Freezing Level (0°C Isotherm)

| Dataset | Level Type | Level Value | Variables |
|:--------|:-----------|:------------|:----------|
| **22** | `isothermZero` | Single | `gh`, `pres`, `r` |

- **`gh`**: Geopotential Height of 0°C isotherm (m, convert to km)

---

## Other Useful Datasets

| Dataset | Level Type | Variables | Description |
|:--------|:-----------|:----------|:------------|
| **13** | `heightAboveGround=10` | `u10`, `v10` | 10m winds |
| **15** | `heightAboveGroundLayer` | `cape`, `hlcy`, `ustm`, `vstm` | Storm motion, helicity |
| **25** | `lowestLevelWetBulb0` | `gh` | Wet bulb zero height |
| **31** | `pressureFromGroundLayer` | `cape`, `cin` | SBCAPE, MLCAPE, MUCAPE |
| **35** | `surface` | `gust`, `ltng`, `vis`, `prate` | Surface variables |

---

## Loading Example

```python
import cfgrib

# Load all datasets
datasets = cfgrib.open_datasets("rap_file.grib2")

# Get 2m Temperature/Dewpoint (Dataset 11)
ds_2m = cfgrib.open_datasets(
    "rap_file.grib2",
    filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2}
)[0]
t2m = ds_2m['t2m'].values  # Kelvin
d2m = ds_2m['d2m'].values  # Kelvin

# Get Winds at 850 hPa (Dataset 19)
ds_isobaric = cfgrib.open_datasets(
    "rap_file.grib2",
    filter_by_keys={'typeOfLevel': 'isobaricInhPa'}
)[0]
u850 = ds_isobaric['u'].sel(isobaricInhPa=850).values

# Get Freezing Level (Dataset 22)
ds_fl = cfgrib.open_datasets(
    "rap_file.grib2",
    filter_by_keys={'typeOfLevel': 'isothermZero'}
)[0]
freezing_level_m = ds_fl['gh'].values  # Geopotential meters
freezing_level_km = freezing_level_m / 1000.0
```

---

## Unit Conversions

| Variable | Raw Unit | Target Unit | Conversion |
|:---------|:---------|:------------|:-----------|
| `t2m`, `d2m` | Kelvin | Celsius | `- 273.15` |
| `gh` (Freezing Level) | m | km | `/ 1000` |
| `u`, `v` | m/s | m/s | None |
