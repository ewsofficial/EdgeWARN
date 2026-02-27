import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from EdgeWARN.core.process.detect.track import StormCellTracker
from EdgeWARN.core.process.detect.lineage import LineageResult, LineageEvent, MergeEvent

class MockIOManager:
    def __init__(self):
        self.messages = []
        
    def write_info(self, msg):
        self.messages.append(('info', msg))
        
    def write_error(self, msg):
        self.messages.append(('error', msg))
        
    def write_warning(self, msg):
        self.messages.append(('warning', msg))


def _make_cell(cell_id, lat=35.0, lon=-97.0, refl=50):
    return {
        'id': cell_id,
        'centroid': [lat, lon],
        'max_refl': refl,
        'bbox': [[lat - 0.01, lon - 0.01], [lat + 0.01, lon + 0.01]],
        'parent_ids': [],
        'split_from': None,
        'tracking_mode': 'active',
        'prediction_count': 0,
        'confidence': 1.0
    }

def test_merge_kf_migration():
    io_manager = MockIOManager()
    tracker = StormCellTracker(None, None, io_manager)
    
    # Setup:
    old_100 = _make_cell(100, refl=60)
    old_101 = _make_cell(101, refl=45)
    entries = [old_100, old_101]
    new_200 = _make_cell(200, refl=62)
    
    print("BEFORE update_cells:")
    print(f"_kalman_filters: {list(tracker._kalman_filters.keys())}")
    
    lineage = LineageResult()
    lineage.merges.append(MergeEvent(
        child_id=200,
        parent_ids=[100, 101],
        dominant_parent=100,
    ))
    
    lineage.cell_events[200] = LineageEvent.MERGE
    
    result = tracker.update_cells(
        entries, [new_200],
        timestamp="2024-01-01T00:02:00",
        lineage=lineage,
    )
    
    print("\nAFTER update_cells:")
    print(f"_kalman_filters: {list(tracker._kalman_filters.keys())}")
    print("Result:")
    for cell in result:
        print(f"Cell {cell['id']}")

test_merge_kf_migration()
