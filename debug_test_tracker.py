
#!/usr/bin/env python3
"""Debug script to track the issue with Kalman tracking tests"""
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from datetime import datetime
from unittest.mock import Mock
from math import cos, radians

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
config = TrackingConfig()
config.confidence_decay_factor = 0.8
config.confidence_threshold = 0.35

# Set custom Kalman configuration
from EdgeWARN.core.process.detect.kalman import KalmanConfig
kalman_config = KalmanConfig()
kalman_config.process_noise_acceleration = 1e-12
kalman_config.process_noise_velocity = 0.0001
kalman_config.process_noise_position = 0.00001

tracker = StormCellTracker(
    ps_old=None,
    ps_new=None,
    io_manager=MockIOManager(),
    tracking_config=config,
    kalman_config=kalman_config
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

# Try to manually initialize Kalman filters
print("=== TRYING TO INITIALIZE KALMAN FILTERS ===")
from EdgeWARN.core.process.detect.kalman import KalmanFilter
for cell in active_cells:
    cell_id = int(cell['id'])
    kf = KalmanFilter(config=kalman_config)
    
    centroid = cell.get('centroid', [0, 0])
    lat, lon = centroid[0], centroid[1]
    
    u, v = 0.0, 0.0
    dx = cell.get('dx')
    dy = cell.get('dy')
    dt = cell.get('dt')
    
    if dx is not None and dy is not None and dt is not None and dt > 0:
        u = dx / dt  # m/s
        v = dy / dt  # m/s
    
    modules = cell.get('modules', {})
    stormcast = modules.get('StormCast', {})
    if stormcast.get('status') == 'success':
        sc_u = stormcast.get('u')
        sc_v = stormcast.get('v')
        if sc_u is not None and sc_v is not None:
            u, v = sc_u, sc_v
    
    kf.initialize(lat, lon, u, v)
    # Directly set reasonable covariance values to prevent explosion
    from EdgeWARN.core.process.detect.kalman.state import CovarianceMatrix
    kf.covariance = CovarianceMatrix.from_diagonal([
        (1/111.0)**2,  # 1km position uncertainty
        (1/(111.0*cos(radians(lat))))**2,
        1.0,  # 1 m/s velocity uncertainty
        1.0,
        0.1,  # 0.1 m/s² acceleration uncertainty
        0.1
    ])
    tracker._kalman_filters[cell_id] = kf
    print(f"Cell {cell_id} Kalman filter initialized: {cell_id in tracker._kalman_filters}")
    
    if cell_id in tracker._kalman_filters:
        state = kf.get_state_dict()
        vel_var = kf.covariance.get_velocity_variance()
        pos_unc = kf.covariance.get_position_std_km(kf.ref_lat)
        print(f"Cell {cell_id} velocity variance: {vel_var}")
        print(f"Cell {cell_id} position uncertainty: {pos_unc}")

# Perform first update (this is what the test is doing)
print("\n=== FIRST UPDATE ===")
result1 = tracker.update_cells(
    entries=active_cells,
    updated_data=updated_data,
    timestamp='2026-01-01T00:02:00',
    dt_seconds=120.0
)

print("\n=== UPDATE RESULT ===")
print(f"Number of cells: {len(result1)}")
for cell in result1:
    print(f"Cell {cell['id']} ({cell['tracking_mode']}):")
    print(f"  Centroid: {cell['centroid']}")
    print(f"  Confidence: {cell['confidence']:.4f}")
    print(f"  Prediction count: {cell['prediction_count']}")

# Print tracker state after update
print("\n=== TRACKER STATE AFTER UPDATE ===")
print(f"Kalman filters: {list(tracker._kalman_filters.keys())}")
print(f"Prediction states: {list(tracker._prediction_states.keys())}")

if 1001 in tracker._kalman_filters:
    print(f"1001 Kalman state: {tracker._kalman_filters[1001].get_state_dict()}")

if 1001 in tracker._prediction_states:
    print(f"1001 Prediction state: {tracker._prediction_states[1001]}")
