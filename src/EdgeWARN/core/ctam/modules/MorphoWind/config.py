
# ==========================================
# MorphoWind Risk Thresholds (Gaussian Smoothing)
# P(risk) = 0.5 * (1 + erf((x - MEAN) / (SIGMA * sqrt(2))))
# ==========================================

# --- QLCS / Bow Echo Detection ---

# Solidity (Contour Area / Hull Area)
# Lower is riskier (Non-convex).
QLCS_SOLIDITY_MEAN = 0.75
QLCS_SOLIDITY_SIGMA = 0.1

# Aspect Ratio (Major Axis / Minor Axis)
# Higher is riskier (Linear).
QLCS_ASPECT_RATIO_MEAN = 3.0
QLCS_ASPECT_RATIO_SIGMA = 1.0

# AzShear (Low-Level 0-2km)
# Higher is riskier.
# Units: 10^-3 s^-1 (Integer values like 3, 4, 5...)
QLCS_SHEAR_MEAN = 4.0
QLCS_SHEAR_SIGMA = 2.0

# Rear Inflow Notch (Defect Depth)
# Higher is riskier.
QLCS_DEFECT_DEPTH_MEAN = 5.0
QLCS_DEFECT_DEPTH_SIGMA = 2.0



# --- Microburst / Downburst Detection ---

# VIL Density (g/m^3)
# Higher is riskier.
MB_VIL_DENSITY_MEAN = 3.5  # Increased from 2.5 based on "High Density" > 4.0
MB_VIL_DENSITY_SIGMA = 1.5

# Echo Top (18 dBZ) Height (km)
# Lower is riskier (Collapsing/Shallow core).
MB_ECHOTOP_MEAN = 6.0
MB_ECHOTOP_SIGMA = 2.0

# --- Temporal Collapse Thresholds (Physics Triggers) ---

# Rate of Change Thresholds (per 5 minutes approx)
# Values below these (negative) indicate rapid collapse.

# VIL Density Collapse Rate (g/m^3 per 5 min)
# Example: -1.5 means density dropping by 1.5 g/m^3
COLLAPSE_VIL_RATE_THRESHOLD = -1.0

# Echo Top Collapse Rate (km per 5 min)
# Example: -1.5 km drop in 5 mins
COLLAPSE_ET_RATE_THRESHOLD = -1.5

# --- Environmental Calibration ---

# Reference Freezing Level (km) - Standard Mid-Latitude
REF_FREEZING_LEVEL_KM = 4.0

# VIL Density Correction (g/m^3 per km of deviation)
# If Freezing Level is HIGHER (Warm), we LOWER the threshold (hail melts, wet microburst risk higher at lower density?)
# Actually, if FL is High, deep warm layer -> more melting -> less hail loading, BUT more evap potential?
# Standard practice: High FL -> Requires Higher VIL for *Hail*, but Density threshold might effectively shift.
# Amburn and Wolf: Density >= 3.5 is universal-ish.
# Let's use simple logic: For every 1km HIGHER freezing level, REDUCE threshold by 0.5 (easier to trigger).
VIL_DENSITY_CORRECTION_PER_KM = 0.5 

# --- Bookend Vortex Logic ---
BOOKEND_VORTEX_SHEAR_THRESHOLD = 5.0 # High shear requirement
BOOKEND_VORTEX_LINEARITY_THRESHOLD = 0.6 # Stricter Skeleton Linearity
BOOKEND_MAX_BRANCHING = 2 # Max junctions for a "clean" line

# --- Dry Air / Dewpoint Depression Logic ---
# If Dewpoint Depression (T - Td) is high, dry air in sub-cloud layer
# enhances evaporative cooling, increasing Microburst risk.
DEWPOINT_DEPRESSION_THRESHOLD = 15.0 # Celsius
# Correction to VIL Density Mean (Lower Mean = Easier to trigger)
DRY_AIR_VIL_CORRECTION = 0.75
