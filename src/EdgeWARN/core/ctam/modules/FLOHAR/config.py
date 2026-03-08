"""
Flash Flood Detection Algorithm Configuration

All algorithm constants: pillar weights, normalization parameters,
severity tiers, and region extraction settings.
"""

from pathlib import Path

# ── Pillar weights (must sum to 1.0) ────────────────────────────────
PILLAR_WEIGHTS = {
    "rainfall": 0.40,
    "hydro": 0.35,
    "ffg": 0.25,
}

# ── Pillar 1: Rainfall Extremity ────────────────────────────────────
# ARI log normalization ceiling (years)
ARI_CEILING_YEARS = 200

# Sub-weights within the rainfall pillar
ARI_SUB_WEIGHTS = {
    "ari_max": 0.50,
    "ari_30m": 0.25,
    "ari_01h": 0.25,
}

# ── Pillar 2: Hydrologic Response ───────────────────────────────────
# Streamflow sigmoid parameters — literature-based defaults.
# x0 = midpoint (m³/s/km²), k = steepness
CREST_SIGMOID = {"x0": 1.5, "k": 2.0}
HP_SIGMOID = {"x0": 1.5, "k": 2.0}

# Sub-weights within the hydrologic pillar (streamflow vs soil)
HYDRO_STREAMFLOW_WEIGHT = 0.70
HYDRO_SOIL_WEIGHT = 0.30

# Streamflow blend sub-weights (within the streamflow portion)
CREST_SUB_WEIGHT = 0.50
HP_SUB_WEIGHT = 0.50

# Soil saturation conditioning (confirmed 0–1 fraction from GRIB inspection)
SOIL_SAT_LOW = 0.40    # below → 0
SOIL_SAT_HIGH = 0.90   # above → 1
SOIL_BOOST_THRESHOLD = 0.85
SOIL_BOOST_MAX = 0.25  # +25% max boost when soil > threshold

# ── Pillar 3: Guidance Exceedance ───────────────────────────────────
# FFG ratio piecewise breakpoints
FFG_RAMP_START = 0.85   # previously 0.75
FFG_RAMP_MID = 1.25     # previously 1.0
FFG_RAMP_END = 2.0

# ── Quality Control (Radar Quality Index) ───────────────────────────
RQI_FULL_WEIGHT_THRESHOLD = 0.9   # previously 0.8
RQI_MIN_WEIGHT_THRESHOLD = 0.5    # previously 0.3

# ── Severity tiers (checked in descending order) ────────────────────
SEVERITY_TIERS = [
    (80, "emergency"),  # previously 75
    (55, "warning"),    # previously 50
    (30, "advisory"),   # previously 25
    (0, "none"),
]

# ── Region extraction ───────────────────────────────────────────────
THREAT_THRESHOLD = 30               # previously 25
MIN_REGION_AREA_KM2 = 10.0          # previously 4.0
POLYGON_SIMPLIFY_TOLERANCE = 0.005
CONNECTIVITY = 8
MAX_REGIONS = 1000

# ── Temporal persistence ────────────────────────────────────────────
MAX_HISTORY_LENGTH = 6  # number of cycles to track for persistence
PERSISTENCE_MULTIPLIERS = {
    1: 1.0,    # 0-1 active cycles
    3: 1.10,   # 2-3 active cycles
    5: 1.20,   # 4-5 active cycles
    6: 1.30,   # 6+ active cycles
}

# ── Sentinel values treated as missing data ─────────────────────────
SENTINEL_VALUES = [-999.0, -9999.0, -999.9, -9999.9]

# ── Grid key → FLASH product directory mapping ──────────────────────
# Maps algorithm grid keys to the corresponding fs.MRMS_FLASH_* directory attrs
GRID_DIR_MAP = {
    "crest_streamflow": "MRMS_FLASH_CREST_MAXUNIT_DIR",
    "ari_max": "MRMS_FLASH_ARIMAX_DIR",
    "ari_30m": "MRMS_FLASH_ARI30M_DIR",
    "ari_01h": "MRMS_FLASH_ARI01H_DIR",
    "hp_streamflow": "MRMS_FLASH_HP_MAXUNIT_DIR",
    "soil_sat": "MRMS_FLASH_SAC_MAXSOIL_DIR",
    "ffg_ratio": "MRMS_FLASH_FFGMAX_DIR",
    "rqi": "MRMS_RQI_DIR",
}
