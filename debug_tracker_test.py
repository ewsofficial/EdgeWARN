from datetime import datetime
from unittest.mock import Mock

from EdgeWARN.core.process.detect.track import StormCellTracker
from EdgeWARN.core.process.detect.kalman import TrackingConfig, KalmanConfig


class MockIOManager:
    def __init__(self):
        self.messages = []

    def write_info(self, msg):
        self.messages.append(('info', msg))

    def write_debug(self, msg):
        self.messages.append(('debug', msg))

    def write_warning(self, msg):
        self.messages.append(('warning', msg))

    def write_error(self, msg):
        self.messages.append(('error', msg))


def test_debug():
    io_manager = MockIOManager()
    config = TrackingConfig()
    config.confidence_decay_factor = 0.8
    config.confidence_threshold = 0.25

    kalman_config = KalmanConfig()
    kalman_config.process_noise_acceleration = 1e-12
    kalman_config.process_noise_velocity = 0.0001
    kalman_config.process_noise_position = 0.00001

    tracker = StormCellTracker(
        ps_old=None,
        ps_new=None,
        io_manager=io_manager,
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

    updated_data_1 = [
        {
            'id': 1002,
            'centroid': [36.01, -96.01],
            'num_gates': 210,
            'max_refl': 61.0,
            'bbox': [[35.91, -96.11], [36.11, -95.91]]
        }
    ]

    print("Calling update_cells...")
    result_1 = tracker.update_cells(
        entries=active_cells,
        updated_data=updated_data_1,
        timestamp='2026-01-01T00:02:00',
        dt_seconds=120.0
    )
    print(f"Result: {len(result_1)} cells")
    for cell in result_1:
        print(f"  Cell ID {cell['id']}, Mode {cell['tracking_mode']}, Prediction Count {cell['prediction_count']}")

    print("\nKalman filters:")
    for cell_id, kf in tracker._kalman_filters.items():
        print(f"  Cell {cell_id} has KF")

    print("\nPrediction states:")
    for cell_id, state in tracker._prediction_states.items():
        print(f"  Cell {cell_id} has prediction state: {state}")


if __name__ == "__main__":
    test_debug()