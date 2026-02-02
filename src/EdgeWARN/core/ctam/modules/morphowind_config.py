
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
MB_VIL_DENSITY_MEAN = 2.5
MB_VIL_DENSITY_SIGMA = 1.0

# Echo Top (18 dBZ) Height (km)
# Lower is riskier (Collapsing/Shallow core).
MB_ECHOTOP_MEAN = 6.0
MB_ECHOTOP_SIGMA = 2.0

