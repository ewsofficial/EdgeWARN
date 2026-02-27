
#!/usr/bin/env python3
"""Debug script to calculate confidence manually"""
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from EdgeWARN.core.process.detect.kalman import ConfidenceCalculator, TrackingConfig

# Initialize confidence calculator
config = TrackingConfig()
calc = ConfidenceCalculator(config=config)

# Debug information from previous run
scans_predicted = 1
time_predicted_seconds = 120.0
velocity_variance = (100.0, 100.0)  # var_u, var_v
position_uncertainty_km = (1.0, 1.0)  # std_lat_km, std_lon_km

print(f"=== CONFIDENCE CALCULATION ===")
print(f"Configuration:")
print(f"  Confidence decay factor: {config.confidence_decay_factor}")
print(f"  Confidence threshold: {config.confidence_threshold}")
print(f"  Max prediction time: {config.max_prediction_time_minutes} minutes")
print()
print(f"Inputs:")
print(f"  Scans predicted: {scans_predicted}")
print(f"  Time predicted: {time_predicted_seconds} seconds")
print(f"  Velocity variance: {velocity_variance}")
print(f"  Position uncertainty: {position_uncertainty_km}")
print()

# Calculate scan confidence
scan_confidence = 1.0 * (config.confidence_decay_factor ** scans_predicted)
print(f"Scan confidence: {scan_confidence:.4f}")

# Calculate time factor
max_time_seconds = config.max_prediction_time_minutes * 60
time_factor = max(0.0, 1.0 - (time_predicted_seconds / max_time_seconds) * 0.3)
print(f"Time factor: {time_factor:.4f}")

# Calculate motion factor
var_u, var_v = velocity_variance
total_var = var_u + var_v
motion_factor = 1.0
if total_var > 0:
    motion_factor = max(0.5, 1.0 - total_var / 500.0)
print(f"Motion factor: {motion_factor:.4f}")

# Calculate position factor
std_lat, std_lon = position_uncertainty_km
avg_std = (std_lat + std_lon) / 2
position_factor = 1.0
if avg_std > 5.0:
    position_factor = max(0.5, 1.0 - (avg_std - 5.0) / 20.0)
print(f"Position factor: {position_factor:.4f}")

# Calculate total confidence
total = scan_confidence * time_factor * motion_factor * position_factor
print(f"Total confidence: {total:.4f}")

# Check if should terminate
should_terminate, reason = calc.should_terminate(total, time_predicted_seconds, scans_predicted)
print(f"Should terminate: {should_terminate}")
if reason:
    print(f"Reason: {reason}")
