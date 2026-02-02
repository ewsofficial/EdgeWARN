
# ==========================================
# MorphoWind Risk Thresholds
# ==========================================

# --- QLCS / Bow Echo Detection ---

# Solidity (Contour Area / Hull Area)
# Values below this indicate a non-convex, potentially linear or "bowed" shape.
QLCS_SOLIDITY_THRESHOLD = 0.75

# Aspect Ratio (Major Axis / Minor Axis)
# Values above this indicate an elongated, linear system.
QLCS_ASPECT_RATIO_THRESHOLD = 3.0

# AzShear (Low-Level 0-2km)
# 95th Percentile shear value (s^-1).
# 0.004 is a moderate shear signature.
QLCS_SHEAR_THRESHOLD = 0.004

# Rear Inflow Notch (Defect Depth)
# Normalized depth of convexity defects. 
# > 5.0 indicates a significant notch relative to cell size.
QLCS_DEFECT_DEPTH_THRESHOLD = 5.0


# --- Microburst / Downburst Detection ---

# VIL Density (g/m^3)
# 95th Percentile value.
# > 2.5 indicates high liquid water content/hail core, potential for loading.
MB_VIL_DENSITY_THRESHOLD = 2.5

# Echo Top (18 dBZ) Height (km)
# 95th Percentile value.
# < 6.0 km combined with high VIL density suggests a shallow, heavy core 
# or a core that has collapsed below the freezing level.
MB_ECHOTOP_THRESHOLD = 6.0
