# Cell Alert Configuration

# Projection lead time (seconds)
# Default: 1800 (30 minutes)
ALERT_LEAD_TIME_SECONDS = 1800

# Safety buffer applied to the final alert polygon (degrees)
# ~0.01 is approximately 1.1km depending on latitude
ALERT_SAFETY_BUFFER_DEGREES = 0.01

# Scan interval for polygon updates
# The polygon will be recalculated every N scans to prevent rapid shifting
ALERT_UPDATE_INTERVAL = 3
