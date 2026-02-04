
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

# --- Environmental Calibration (Gaussian Smoothing) ---

# Freezing Level Correction
# Higher freezing level = deeper warm layer = more evaporative cooling potential
# We calculate a multiplier using Gaussian CDF that scales the VIL density mean
# P(correction) = 0.5 * (1 + erf((FL - FL_MEAN) / (FL_SIGMA * sqrt(2))))
FL_MEAN = 4.0    # Reference freezing level (km) - Standard Mid-Latitude
FL_SIGMA = 1.5   # Transition width (km)
FL_MAX_CORRECTION = 1.0  # Maximum VIL mean reduction at high FL (g/m³)

# Dewpoint Depression Correction (Dry Air Microburst Enhancement)
# Higher depression = drier sub-cloud layer = stronger evaporative cooling
# DD_MEAN: 50% correction applied at this value
# DD_SIGMA: Controls how quickly correction ramps up
DD_MEAN = 12.0   # Dewpoint depression (°C) where P(correction) = 0.5
DD_SIGMA = 5.0   # Transition width (°C)
DD_MAX_CORRECTION = 1.0  # Maximum VIL mean reduction for very dry air (g/m³) 

# --- Bookend Vortex Logic ---
BOOKEND_VORTEX_SHEAR_THRESHOLD = 5.0 # High shear requirement
BOOKEND_VORTEX_LINEARITY_THRESHOLD = 0.6 # Stricter Skeleton Linearity
BOOKEND_MAX_BRANCHING = 2 # Max junctions for a "clean" line

# (Deprecated: Old threshold-based constants removed)
# Environmental corrections now use Gaussian smoothing above
