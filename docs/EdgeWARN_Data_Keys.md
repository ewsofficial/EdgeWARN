# EdgeWARN Data Keys

## Root JSON Structure (Storm Cells)

The storm cells JSON output follows this structure:

| Key              | Type           | Description                                      |
|------------------|----------------|--------------------------------------------------|
| source           | string         | Authors of the JSON                              |
| product          | string         | Product ID                                       |
| latest_timestamp | ISOFormat      | The latest timestamp of data included in this file |
| features         | List[Object]   | List of storm cell objects (see Primary Data Keys below) |

## Primary Data Keys (Feature Object)

| Key          | Units          | Description                                      |
|--------------|----------------|--------------------------------------------------|
| id          | N/A           | ProbSevere Cell ID                              |
| timestamp   | ISOFormat     | When the cell was last matched/updated (only present for active cells) |
| num_gates   | N/A           | Number of gates in the storm cell               |
| centroid    | Lat, Lon      | Latitude and Longitude of Cell Centroid (Lon is in 0-360 format) |
| bbox        | List of [lat, lon] | Bounding Box of storm cell (Lon is in 0-360 format) |
| hail_core   | List of [lat, lon] | Bounding box of storm's hail core (Lon is in 0-360 format) |
| max_refl    | dBZ           | Maximum reflectivity in the storm cell          |
| dx          | m             | X-displacement from previous scan centroid       |
| dy          | m             | Y-displacement from previous scan centroid       |
| dt          | s             | Time difference from previous scan              |
| properties  | Object        | Integrated meteorological data |
| modules     | Object        | CTAM module outputs (see Modules section below) |

## Data Keys in ``modules``

The `modules` object contains output from registered CTAM modules. Each module has its own key.

### ``modules.StormCast``

| Key              | Type           | Description                                      |
|------------------|----------------|--------------------------------------------------|
| status          | string        | "success", "skipped", or "error"                 |
| u               | m/s           | Predicted eastward motion component (if success) |
| v               | m/s           | Predicted northward motion component (if success) |
| forecast_cones  | List[Object]  | Forecast uncertainty cones at various lead times |
| reason          | string        | Explanation if status is "skipped"               |
| error           | string        | Error message if status is "error"               |

#### ``forecast_cones`` Object Structure

| Key        | Type          | Description                                           |
|------------|---------------|-------------------------------------------------------|
| center     | (lat, lon)    | Center of the uncertainty cone in lat/lon coordinates |
| x          | m             | X position in local meters (relative to storm origin) |
| y          | m             | Y position in local meters (relative to storm origin) |
| radius     | m             | Radius of the uncertainty cone at given confidence    |
| lead_time  | s             | Forecast lead time in seconds                         |

### ``modules.MorphoWind``

| Key              | Type           | Description                                      |
|------------------|----------------|--------------------------------------------------|
| risk_type       | string        | "QLCS", "Microburst", or "None"                  |
| confidence      | float         | 0.0 - 1.0 probability score                      |
| severity_index  | float         | Maximum of QLCS and Microburst scores            |
| physics_triggers | List[string] | Triggered physics flags (see below)              |
| scores          | Object        | `{qlcs: float, microburst: float}`               |
| physics         | Object        | Detailed metrics (see below)                     |

#### ``physics_triggers`` Values

| Trigger              | Description                                      |
|----------------------|--------------------------------------------------|
| `VIL_COLLAPSE`      | Rapid VIL drop detected (with pre-condition met) |
| `ET_COLLAPSE`       | Rapid Echo Top drop detected                     |
| `REAR_INFLOW_NOTCH` | Rear-sector convexity defect confirmed           |
| `BOOKEND_VORTEX`    | Linear structure with high shear at ends         |

#### ``physics`` Object Structure

| Key            | Units | Description                                    |
|----------------|-------|------------------------------------------------|
| vil_density   | g/m³  | VIL / EchoTop18                                |
| collapse_score | 0-1  | Combined VIL + ET collapse score               |
| vil_change    | kg/m² | VIL change from 2 scans ago                    |
| et_change     | km    | Echo Top change from 2 scans ago               |
| defect_bearing | °    | Bearing of deepest convexity defect            |
| linearity     | ratio | Skeleton linearity metric                      |

## Data Keys in ``properties``

| Key              | Units          | Description                                      |
|------------------|----------------|--------------------------------------------------|
| timestamp       | ISOFormat     | Storm history timestamp                         |
| bbox            | List of [lat, lon] | Bounding Box of storm cell at this timestamp (Lon is in 0-360 format) |
| max_refl        | dBZ           | Maximum reflectivity in the storm cell          |
| num_gates       | N/A           | Number of gates                                 |
| centroid        | Lat, Lon      | Latitude and longitude of storm cell (Lon is 0-360 format) |
| dx              | m             | X-difference between previous storm scan centroid |
| dy              | m             | Y-difference between previous storm scan centroid |
| dt              | s             | Time difference between previous storm scan     |
| p100CGFlashDensity | fl/km²/min   | Cloud-to-ground flash density (Max)             |
| p100EchoTop18   | km MSL        | Maximum 18 dBZ Echo Top                          |
| p95EchoTop18    | km MSL        | 95th percentile 18 dBZ Echo Top                  |
| p90EchoTop18    | km MSL        | 90th percentile 18 dBZ Echo Top                  |
| p100EchoTop30   | km MSL        | Maximum 30 dBZ Echo Top                          |
| p100PrecipRate  | mm            | Highest instantaneous precip rate                |
| p100VILDensity  | g/kg³         | Maximum VIL Density                              |
| p95VILDensity   | g/m³          | 95th percentile VIL Density                      |
| p90VILDensity   | g/m³          | 90th percentile VIL Density                      |
| p50VILDensity   | g/m³          | Median VIL Density                               |
| p100RALA        | dBZ           | Reflectivity at lowest altitude (Max)            |
| p100VII         | kg/m²         | Maximum Vertically Integrated Ice                |
| Ref0            | dBZ           | Maximum Reflectivity at 0°C                      |
| Ref5            | dBZ           | Maximum Reflectivity at -5°C                     |
| Ref15           | dBZ           | Maximum Reflectivity at -15°C                    |
| ProbSevere      | %             | Probability of severe weather (ProbSevere model) |
| ProbWind        | %             | Probability of severe wind                       |
| ProbHail        | %             | Probability of severe hail                       |
| ProbTor         | %             | Probability of tornado                           |
| vx              | m/s           | Eastward velocity component                      |
| vy              | m/s           | Southward velocity component                     |
| MLCAPE          | J/kg          | Mixed Layer CAPE                                |
| MUCAPE          | J/kg          | Most Unstable CAPE                              |
| MLCIN           | J/kg          | Mixed Layer CIN                                 |
| DCAPE           | J/kg          | Downdraft CAPE                                  |
| CAPE_M10M30     | J/kg          | CAPE from -10°C to -30°C                        |
| LCL             | m             | Lifted Condensation Level                       |
| Wetbulb_0C_Hgt  | kft           | Height of 0°C Dewpoint                          |
| LLLR            | °C/km         | Low-level lapse rate                            |
| MLLR            | °C/km         | Mid-level lapse rate                            |
| EBShear         | kt            | Effective Bulk Shear                            |
| SRH01km         | m²/s²         | 0-1 km Storm-Relative Helicity                  |
| SRH02km         | m²/s²         | 0-2 km Storm-Relative Helicity                  |
| SRW46km         | kt            | 4-6 km Storm-Relative Wind                      |
| MeanWind_1-3kmAGL | kt          | 1-3 km Mean Wind                                |
| LJA             | std           | Lightning Jump Algorithm                        |
| CompRef         | dBZ           | Composite Reflectivity                          |
| Ref10           | dBZ           | Max Reflectivity at -10°C (ProbSevere)          |
| Ref20           | dBZ           | Max Reflectivity at -20°C (ProbSevere)          |
| MESH            | in            | Maximum Expected Size of Hail                   |
| H50_Above_0C    | km            | Height of 50 dBZ echo above 0°C Isotherm        |
| EchoTop50       | km            | Maximum Height of 50 dBZ reflectivity           |
| VIL             | kg/m²         | Vertically Integrated Liquid (ProbSevere)       |
| p100VIL         | kg/m²         | Maximum VIL (MRMS Integration)                  |
| p95VIL          | kg/m²         | 95th percentile VIL (MRMS Integration)          |
| p90VIL          | kg/m²         | 90th percentile VIL (MRMS Integration)          |
| p50VIL          | kg/m²         | Median VIL (MRMS Integration)                   |
| MaxFED          | fl/km²/min    | Maximum Flash Extent Density                    |
| MaxFCD          | fl/km²/min    | Maximum Flash Centroid Density                  |
| AccumFCD        | fl/km²/min    | Accumulated Flash Centroid Density              |
| MinFlashArea    | km²           | Minimum Flash Area                              |
| TE@MaxFCD       | fJ            | Total Optical Energy at MaxFCD                  |
| FlashRate       | fl/min        | Total lightning flashes per minute (ENI)        |
| FlashDensity    | fl/km²/min    | ENI Lightning Flash Density                     |
| MaxLLAz         | 0.001 s⁻¹     | Maximum Low-level Azimuthal Shear               |
| p98LLAz         | 0.001 s⁻¹     | 98th percentile Low-Level Azimuthal Shear       |
| p98MLAz         | 0.001 s⁻¹     | 98th percentile Mid-Level Azimuthal Shear       |
| p100AzShearLow  | 10⁻³ s⁻¹      | Maximum Low-Level (0-2km) Azimuthal Shear       |
| p95AzShearLow   | 10⁻³ s⁻¹      | 95th percentile Low-Level Azimuthal Shear       |
| p100AzShearMid  | 10⁻³ s⁻¹      | Maximum Mid-Level (3-6km) Azimuthal Shear       |
| p95AzShearMid   | 10⁻³ s⁻¹      | 95th percentile Mid-Level Azimuthal Shear       |
| MaxRC_Emiss     | %/min         | Max rate of change in 11μm top-of-troposphere emissivity |
| ICP             | N/A           | Intense Convection Probability                  |
| PWAT            | in            | Precipitable Water                              |
| avg_beam_hgt    | km AGL        | Average Beam Height over storm cell             |
| GLM_FLASH_COUNT | fl/min        | GLM Flash Count within storm cell (GOES)        |
| GLM_TOTAL_ENERGY| J             | Total GLM Flash Energy within storm cell (GOES) |
| u{level}        | m/s           | U-wind component at isobaric level (100-1000hPa)|
| v{level}        | m/s           | V-wind component at isobaric level (100-1000hPa)|
| u10m            | m/s           | 10-meter U-wind component                       |
| v10m            | m/s           | 10-meter V-wind component                       |
| freezing_level_height | km      | Height of 0°C isotherm (Derived)                |
| freezing_level_m | m            | Height of 0°C isotherm (Raw RAP)                |
| dewpoint_depression | °C        | T - Td at 2m (dry air correction)               |
| temp_2m         | °C            | 2-meter temperature                              |
| dewpoint_2m     | °C            | 2-meter dewpoint temperature                     |
| morphology      | Object        | Geometric features (see below)                   |

### ``morphology`` Object

| Key              | Units          | Description                                      |
|------------------|----------------|--------------------------------------------------|
| solidity        | ratio         | Contour Area / Convex Hull Area                  |
| aspect_ratio    | ratio         | Major / Minor Axis of MinAreaRect                |
| defect_max_depth | pixels       | Depth of largest convexity defect               |
| defect_bearing  | degrees       | Bearing of defect from centroid (0-360°)         |
| linearity       | ratio         | Skeleton Length / Complexity                     |
| branching_factor | count        | Number of skeleton junctions                     |

---

## METAR Data Keys

METAR data is provided as a list of observation objects.

| Key              | Type           | Description                                      |
|------------------|----------------|--------------------------------------------------|
| observation_time | ISOFormat      | Time of observation (e.g. "2026/01/23 12:00")    |
| type             | string         | "METAR" or "SPECI"                               |
| station          | string         | Station ICAO code (e.g. "KJFK")                  |
| coordinates      | [lat, lon]     | Station coordinates                              |
| wind             | Object         | Wind data: `{direction, speed, gust}`            |
| visibility       | string         | Visibility (e.g. "10SM")                         |
| temperature      | string         | Temperature in °C                                |
| dewpoint         | string         | Dewpoint in °C                                   |
| pressure         | float          | Altimeter setting in inHg (e.g. 30.12)           |
| clouds           | List[Object]   | Cloud layers: `[{code, altitude, type}]`         |
| weather          | List[string]   | Weather phenomena (e.g. ["-RA", "BR"])           |
| remarks          | string         | Raw remarks section                              |

---

## NWS Alert Data Keys

NWS Alerts are provided as a GeoJSON FeatureCollection.

### Feature Properties

| Key          | Type           | Description                                      |
|--------------|----------------|--------------------------------------------------|
| event        | string         | Event name (e.g. "Severe Thunderstorm Warning")  |
| headline     | string         | NWS Headline                                     |
| description  | string         | Full text description                            |
| effective    | ISOFormat      | Effective time                                   |
| expires      | ISOFormat      | Expiration time                                  |
| severity     | string         | Severity (e.g. "Severe")                         |
| urgency      | string         | Urgency (e.g. "Immediate")                       |
| certainty    | string         | Certainty (e.g. "Observed")                      |
| areaDesc     | string         | Text description of the area                     |
| Polygon      | List[[x,y]]    | **Computed** exterior polygon of the alert area (union of zones) |

Note: Standard GeoJSON `geometry` is also present.
