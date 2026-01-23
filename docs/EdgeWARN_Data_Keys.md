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
| CGFlashDensity  | fl/km²/min   | Cloud-to-ground flash density                   |
| EchoTop18       | km MSL        | Highest level of 18 dBZ reflectivity            |
| EchoTop30       | km MSL        | Highest level of 30 dBZ reflectivity            |
| PrecipRate      | mm            | Highest instantaneous precip rate               |
| VILDensity      | g/kg³         | Highest VIL Density                             |
| RALA            | dBZ           | Reflectivity at lowest altitude                 |
| VII             | kg/m²         | Highest VII                                     |
| ProbSevere      | %             | Probability of severe weather                   |
| ProbWind        | %             | Probability of severe wind                      |
| ProbHail        | %             | Probability of severe hail                      |
| ProbTor         | %             | Probability of tornado                          |
| vx              | m/s           | Eastward velocity component                     |
| vy              | m/s           | Southward velocity component                    |
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
| Ref10           | dBZ           | Max Reflectivity at -10°C                       |
| Ref20           | dBZ           | Max Reflectivity at -20°C                       |
| MESH            | in            | Maximum Expected Size of Hail                   |
| H50_Above_0C    | km            | Height of 50 dBZ echo above 0°C Isotherm        |
| EchoTop50       | km            | Maximum Height of 50 dBZ reflectivity           |
| VIL             | kg/m²         | Vertically Integrated Liquid                    |
| MaxFED          | fl/km²/min    | Maximum Flash Extent Density                    |
| MaxFCD          | fl/km²/min    | Maximum Flash Centroid Density                  |
| AccumFCD        | fl/km²/min    | Accumulated Flash Centroid Density              |
| MinFlashArea    | km²           | Minimum Flash Area                              |
| TE@MaxFCD       | fJ            | Total Optical Energy at MaxFCD                  |
| FlashRate       | fl/min        | Total lightning flashes per minute              |
| FlashDensity    | fl/km²/min    | ENI Lightning Flash Density                     |
| MaxLLAz         | 0.001 s⁻¹     | Maximum Low-level Azimuthal Shear               |
| p98LLAz         | 0.001 s⁻¹     | 98th percentile Low-Level Azimuthal Shear       |
| p98MLAz         | 0.001 s⁻¹     | 98th percentile Mid-Level Azimuthal Shear       |
| MaxRC_Emiss     | %/min         | Max rate of change in 11μm top-of-troposphere emissivity |
| ICP             | N/A           | Intense Convection Probability                  |
| PWAT            | in            | Precipitable Water                              |
| avg_beam_hgt    | km AGL        | Average Beam Height over storm cell             |
| GLM_FLASH_COUNT | fl/min        | GLM Flash Count within storm cell               |
| GLM_TOTAL_ENERGY| J             | Total GLM Flash Energy within storm cell        |
| u850            | m/s           | 850-hPa u-component of wind                     |
| v850            | m/s           | 850-hPa v-component of wind                     |
| u700            | m/s           | 700-hPa u-component of wind                     |
| v700            | m/s           | 700-hPa v-component of wind                     |
| u500            | m/s           | 500-hPa u-component of wind                     |
| v500            | m/s           | 500-hPa v-component of wind                     |
| u250            | m/s           | 250-hPa u-component of wind                     |
| v250            | m/s           | 250-hPa v-component of wind                     |

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
