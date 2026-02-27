#!/usr/bin/env python3
"""Debug script to compute height weights for h_core=6.0 km"""
import sys
sys.path.insert(0, '/home/yuchenwei/Projects/EdgeWARN-Core')

from EdgeWARN.core.ctam.modules.StormCast.core.diagnostics import compute_height_weights
from EdgeWARN.core.ctam.modules.StormCast.core.config import LEVEL_HEIGHTS, PRESSURE_LEVELS

h_core = 6.0
weights = compute_height_weights(h_core)

# Find the pressure levels with significant weights
print("Pressure levels and their weights for h_core=6.0 km:")
for level in sorted(PRESSURE_LEVELS):
    weight = weights[level]
    if weight > 0.01:
        height = LEVEL_HEIGHTS[level]
        print(f"Level: {level} mb, Height: {height:.2f} km, Weight: {weight:.4f}")

print(f"\nTotal weight: {sum(weights.values()):.4f}")

# Check 850, 700, 500, 250 levels
for level in [850, 700, 500, 250]:
    if level in weights:
        height = LEVEL_HEIGHTS[level]
        print(f"\nLevel {level} mb:")
        print(f"Height: {height:.2f} km")
        print(f"Weight: {weights[level]:.4f}")
