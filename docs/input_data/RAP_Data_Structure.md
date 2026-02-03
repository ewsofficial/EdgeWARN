# RAP GRIB2 Data Structure Reference

This document details the complete structure of RAP (Rapid Refresh) GRIB2 files, based on inspection via `cfgrib.open_datasets()`.

---

## Overview

RAP files contain **39 distinct datasets**, each representing a different level type or layer. `cfgrib` automatically separates these when loading.

---

## Complete Dataset Reference

### Single-Level Surface Variables

| DS # | Level Type | Level Value | Variables | Description |
|:-----|:-----------|:------------|:----------|:------------|
| **0** | `surface` | — | `orog` | Model terrain / orography (m) |
| **1** | `surface` | — | `lsm` | Land-sea mask (0-1) |
| **2** | `surface` | — | `sp` | Surface pressure (Pa) |
| **3** | `surface` | — | `prmsl` | Mean sea level pressure (Pa) |
| **4** | `surface` | — | `gust` | Wind gust at surface (m/s) |
| **5** | `surface` | — | `ltng` | Lightning (flash count) |
| **6** | `surface` | — | `vis` | Visibility (m) |
| **7** | `surface` | — | `prate` | Precipitation rate (kg/m²/s) |
| **8** | `surface` | — | `crain`, `csnow`, `cfrzr`, `cicep` | Precip type flags (0-1) |
| **9** | `surface` | — | `cape`, `cin` | Surface-based CAPE/CIN (J/kg) |
| **10** | `surface` | — | `hpbl` | PBL height (m) |

---

### Height Above Ground (Single Level)

| DS # | Level Type | Level Value | Variables | Description |
|:-----|:-----------|:------------|:----------|:------------|
| **11** | `heightAboveGround` | `2` m | `t2m`, `d2m`, `sh2`, `r2`, `pt` | 2m temp, dewpoint, humidity |
| **12** | `heightAboveGround` | `2` m | `tmax`, `tmin` | 2m max/min temperature (K) |
| **13** | `heightAboveGround` | `10` m | `u10`, `v10` | 10m wind components (m/s) |
| **14** | `heightAboveGround` | `80` m | `u`, `v` | 80m wind components (m/s) |

---

### Height Above Ground (Layer Averages)

| DS # | Level Type | Level Range | Variables | Description |
|:-----|:-----------|:------------|:----------|:------------|
| **15** | `heightAboveGroundLayer` | `0-3000` m | `cape`, `hlcy` | 0-3km CAPE, Storm-relative helicity |
| **16** | `heightAboveGroundLayer` | `0-6000` m | `ustm`, `vstm`, `hlcy` | Storm motion, 0-6km helicity |
| **17** | `heightAboveGroundLayer` | `0-1000` m | `hlcy` | 0-1km helicity (m²/s²) |

---

### Isobaric Levels (Pressure Levels)

| DS # | Level Type | Levels | Variables | Description |
|:-----|:-----------|:-------|:----------|:------------|
| **18** | `isobaricInhPa` | 37 levels (1000–100 hPa) | `t`, `gh` | Temperature (K), Geopotential height (m) |
| **19** | `isobaricInhPa` | 37 levels (1000–100 hPa) | `u`, `v` | Wind components (m/s) |
| **20** | `isobaricInhPa` | 37 levels (1000–100 hPa) | `r`, `w` | Relative humidity (%), Vertical velocity (Pa/s) |
| **21** | `isobaricInhPa` | 37 levels (1000–100 hPa) | `absv` | Absolute vorticity (1/s) |

**Available pressure levels:**
`1000, 975, 950, 925, 900, 875, 850, 825, 800, 775, 750, 725, 700, 675, 650, 625, 600, 575, 550, 525, 500, 475, 450, 425, 400, 375, 350, 325, 300, 275, 250, 225, 200, 175, 150, 125, 100` hPa

---

### Special Atmospheric Levels

| DS # | Level Type | Description | Variables |
|:-----|:-----------|:------------|:----------|
| **22** | `isothermZero` | 0°C isotherm level | `gh`, `pres`, `r` |
| **23** | `tropopause` | Tropopause | `t`, `u`, `v`, `pres`, `gh` |
| **24** | `maxWind` | Maximum wind level | `u`, `v`, `pres` |
| **25** | `lowCloudBottom` | Low cloud base | `pres` |
| **26** | `lowCloudTop` | Low cloud top | `pres` |
| **27** | `middleCloudBottom` | Mid cloud base | `pres` |
| **28** | `middleCloudTop` | Mid cloud top | `pres` |
| **29** | `highCloudBottom` | High cloud base | `pres` |
| **30** | `highCloudTop` | High cloud top | `pres` |

---

### Pressure From Ground Layer (Parcel-Based)

| DS # | Level Type | Layer | Variables | Description |
|:-----|:-----------|:------|:----------|:------------|
| **31** | `pressureFromGroundLayer` | `0-255` hPa (Surface-based) | `cape`, `cin` | SBCAPE, SBCIN (J/kg) |
| **32** | `pressureFromGroundLayer` | `0-90` hPa (Mixed layer) | `cape`, `cin`, `plcl`, `epot` | MLCAPE, LCL pressure |
| **33** | `pressureFromGroundLayer` | `90-180` hPa (Most unstable) | `cape`, `cin` | MUCAPE, MUCIN (J/kg) |
| **34** | `pressureFromGroundLayer` | `0-180` hPa | `4lftx` | Best (4-layer) lifted index |

---

### Derived / Composite Fields

| DS # | Level Type | Variables | Description |
|:-----|:-----------|:----------|:------------|
| **35** | `cloudCeiling` | `ceil` | Cloud ceiling height (m) |
| **36** | `atmosphereSingleLayer` | `pwat`, `tcc`, `lcc`, `mcc`, `hcc` | Precipitable water, cloud cover (%) |
| **37** | `lowestLevelWetBulb0` | `gh` | Height of wet-bulb zero (m) |
| **38** | `equilibriumLevel` | `pres` | Equilibrium level pressure (Pa) |

---

## Variables Quick Reference

### Wind Variables
| Variable | Description | Units |
|:---------|:------------|:------|
| `u`, `u10` | U-component (east-west) | m/s |
| `v`, `v10` | V-component (north-south) | m/s |
| `gust` | Wind gust speed | m/s |
| `ustm`, `vstm` | Storm motion components | m/s |

### Temperature Variables
| Variable | Description | Units |
|:---------|:------------|:------|
| `t`, `t2m` | Temperature | K |
| `d2m` | 2m Dewpoint temperature | K |
| `tmax`, `tmin` | Max/Min temperature | K |
| `pt` | Potential temperature | K |

### Stability Variables
| Variable | Description | Units |
|:---------|:------------|:------|
| `cape` | Convective Available Potential Energy | J/kg |
| `cin` | Convective Inhibition | J/kg |
| `hlcy` | Storm-relative helicity | m²/s² |
| `4lftx` | Best lifted index | K |

### Moisture Variables
| Variable | Description | Units |
|:---------|:------------|:------|
| `r`, `r2` | Relative humidity | % |
| `sh2` | 2m Specific humidity | kg/kg |
| `pwat` | Precipitable water | kg/m² |

### Height / Pressure Variables
| Variable | Description | Units |
|:---------|:------------|:------|
| `gh` | Geopotential height | m |
| `pres` | Pressure | Pa |
| `sp` | Surface pressure | Pa |
| `prmsl` | Mean sea level pressure | Pa |
| `hpbl` | Planetary boundary layer height | m |
| `plcl` | LCL pressure | Pa |

### Precipitation Variables
| Variable | Description | Units |
|:---------|:------------|:------|
| `prate` | Precipitation rate | kg/m²/s |
| `crain` | Categorical rain (0/1) | — |
| `csnow` | Categorical snow (0/1) | — |
| `cfrzr` | Categorical freezing rain (0/1) | — |
| `cicep` | Categorical ice pellets (0/1) | — |

### Cloud Variables
| Variable | Description | Units |
|:---------|:------------|:------|
| `tcc` | Total cloud cover | % |
| `lcc` | Low cloud cover | % |
| `mcc` | Medium cloud cover | % |
| `hcc` | High cloud cover | % |
| `ceil` | Cloud ceiling height | m |

---

## Loading Example

```python
import cfgrib

# Load all datasets
datasets = cfgrib.open_datasets("rap_file.grib2")
print(f"Found {len(datasets)} datasets")

# Inspect a dataset
for i, ds in enumerate(datasets):
    print(f"DS {i}: {list(ds.data_vars)}")

# Get 2m Temperature/Dewpoint
ds_2m = cfgrib.open_datasets(
    "rap_file.grib2",
    filter_by_keys={'typeOfLevel': 'heightAboveGround', 'level': 2}
)[0]
t2m = ds_2m['t2m'].values  # Kelvin
d2m = ds_2m['d2m'].values  # Kelvin

# Get Winds at 850 hPa
ds_isobaric = cfgrib.open_datasets(
    "rap_file.grib2",
    filter_by_keys={'typeOfLevel': 'isobaricInhPa'}
)[0]
u850 = ds_isobaric['u'].sel(isobaricInhPa=850).values

# Get CAPE from surface parcel
ds_cape = cfgrib.open_datasets(
    "rap_file.grib2",
    filter_by_keys={'typeOfLevel': 'pressureFromGroundLayer'}
)[0]
sbcape = ds_cape['cape'].values
```

---

## Unit Conversions

| Variable | Raw Unit | Target Unit | Conversion |
|:---------|:---------|:------------|:-----------|
| `t2m`, `d2m`, `t` | Kelvin | Celsius | `- 273.15` |
| `gh` | m | km | `/ 1000` |
| `sp`, `pres`, `prmsl` | Pa | hPa | `/ 100` |
| `u`, `v` | m/s | kt | `* 1.944` |
| `prate` | kg/m²/s | mm/hr | `* 3600` |
