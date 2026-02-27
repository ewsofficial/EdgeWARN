
#!/usr/bin/env python3
"""Debug script to track the issue with Kalman tracking tests"""
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

import copy
from datetime import datetime
from unittest.mock import Mock

from EdgeWARN.core.process.detect.track import StormCellTracker
from EdgeWARN.core.process.detect.kalman import TrackingConfig

# Create mock IO manager
class MockIOManager:
    def __init__(self):
        self.messages = []
    
    def write_info(self, msg):
        print(f"INFO: {msg}")
        self.messages.append(('info', msg))
    
    def write_debug(self, msg):
        print(f"DEBUG: {msg}")
        self.messages.append(('debug', msg))
    
    def write_warning(self, msg):
        print(f"WARNING: {msg}")
        self.messages.append(('warning', msg))
    
    def write_error(self, msg):
        print(f"ERROR: {msg}")
        self.messages.append(('error', msg))

# Test data from the test case
tracker = StormCellTracker(
    ps_old=None,
    ps_new=None,
    io_manager=MockIOManager(),
    tracking_config=TrackingConfig()
)

active_cells = [
    {
        'id': 1001,
        'centroid': [35.0, -97.0],
        'num_gates': 150,
        'max_refl': 55.0,
        'bbox': [[34.9, -97.1], [35.1, -96.9]],
        'tracking_mode': 'active',
        'prediction_count': 0,
        'confidence': 1.0,
        'dx': 1200.0,
        'dy': 600.0,
        'dt': 120.0,
        'modules': {
            'StormCast': {
                'status': 'success',
                'u': 10.0,
                'v': 5.0
            }
        }
    },
    {
        'id': 1002,
        'centroid': [36.0, -96.0],
        'num_gates': 200,
        'max_refl': 60.0,
        'bbox': [[35.9, -96.1], [36.1, -95.9]],
        'tracking_mode': 'active',
        'prediction_count': 0,
        'confidence': 1.0,
        'dx': 800.0,
        'dy': 400.0,
        'dt': 120.0
    }
]

updated_data = [
    {
        'id': 1002,
        'centroid': [36.01, -96.01],
        'num_gates': 210,
        'max_refl': 61.0,
        'bbox': [[35.91, -96.11], [36.11, -95.91]]
    }
]

# Print tracker state before update
print("=== TRACKER STATE BEFORE UPDATE ===")
print(f"Kalman filters: {list(tracker._kalman_filters.keys())}")
print(f"Prediction states: {list(tracker._prediction_states.keys())}")
print()

# Initialize Kalman filters for both cells before update
print("\n=== MANUALLY INITIALIZING KALMAN FILTERS ===")
for cell in active_cells:
    cell_id = int(cell['id'])
    tracker._update_kalman_with_observation(cell, cell_id)
    print(f"Cell {cell_id}: Kalman filter initialized")
    if cell_id in tracker._kalman_filters:
        kf = tracker._kalman_filters[cell_id]
        state = kf.get_state_dict()
        print(f"  State: {state}")
        vel_var = kf.covariance.get_velocity_variance()
        pos_unc = kf.covariance.get_position_std_km(kf.ref_lat)
        print(f"  Velocity variance: {vel_var}")
        print(f"  Position uncertainty: {pos_unc}")

# Perform update
try:
    result = tracker.update_cells(
        entries=active_cells,
        updated_data=updated_data,
        timestamp='2026-01-01T00:02:00',
        dt_seconds=120.0
    )
    
    # Print result
    print("\n=== UPDATE RESULT ===")
    print(f"Number of cells: {len(result)}")
    for cell in result:
        print(f"Cell {cell['id']}: {cell['tracking_mode']}")
    
    # Print tracker state after update
    print("\n=== TRACKER STATE AFTER UPDATE ===")
    print(f"Kalman filters: {list(tracker._kalman_filters.keys())}")
    print(f"Prediction states: {list(tracker._prediction_states.keys())}")
    if 1001 in tracker._kalman_filters:
        print(f"1001 Kalman state: {tracker._kalman_filters[1001].get_state_dict()}")
    if 1001 in tracker._prediction_states:
        print(f"1001 Prediction state: {tracker._prediction_states[1001]}")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
