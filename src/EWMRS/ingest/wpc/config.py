"""Configuration for WPC Surface Analysis ingestion."""

from util.ewmrs_file import BASE_DIR, WPC_DIR, WPC_SFC_DIR

# Base URL for WPC coded surface data
WPC_CODED_SFC_BASE_URL = "https://ftp.wpc.ncep.noaa.gov/coded_sfc"

# Ensure directories exist
WPC_DIR.mkdir(parents=True, exist_ok=True)
WPC_SFC_DIR.mkdir(parents=True, exist_ok=True)

# Update interval in hours (WPC updates every 3 hours)
UPDATE_INTERVAL_HOURS = 3

# Valid hours for surface analysis (every 3 hours UTC)
VALID_HOURS = [0, 3, 6, 9, 12, 15, 18, 21]

# Feature type definitions for styling reference
FEATURE_TYPES = {
    "COLD": {"name": "Cold Front", "color": "#0000FF"},
    "WARM": {"name": "Warm Front", "color": "#FF0000"},
    "STNRY": {"name": "Stationary Front", "color": "#800080"},
    "OCFNT": {"name": "Occluded Front", "color": "#800080"},
    "TROF": {"name": "Trough", "color": "#8B4513"},
    "HIGH": {"name": "High Pressure", "color": "#0000FF"},
    "LOW": {"name": "Low Pressure", "color": "#FF0000"},
}
