# Split Detection and Metadata Investigation Plan

## Problem Statement
Investigate why the split logic isn't working properly and check if metadata is being correctly written.

## Current Understanding

### Key Files and Components:
- **[lineage/detector.py](/home/yuchenwei/Projects/EdgeWARN-Core/src/EdgeWARN/core/process/detect/lineage/detector.py)**: Core split/merge detection logic
- **[lineage/buffer.py](/home/yuchenwei/Projects/EdgeWARN-Core/src/EdgeWARN/core/process/detect/lineage/buffer.py)**: Hysteresis buffer for confirming events
- **[track.py](/home/yuchenwei/Projects/EdgeWARN-Core/src/EdgeWARN/core/process/detect/track.py)**: Storm cell tracker that applies lineage updates
- **[tools/save.py](/home/yuchenwei/Projects/EdgeWARN-Core/src/EdgeWARN/core/process/detect/tools/save.py)**: Cell data saver for writing JSON output

### Detection Flow:
1. LineageDetector identifies potential split events by checking if a single old cell overlaps with multiple new cells
2. Potential events are confirmed by LineageBuffer (requires consecutive detections)
3. Confirmed events are processed by StormCellTracker.update_cells()
4. Results are saved to JSON by CellDataSaver

## Investigation Steps

### 1. Verify Lineage Detection is Finding Splits
Run existing tests to confirm split detection works:
```bash
pytest tests/unit/test_lineage.py::TestSplitDetection -v
```
**Expected**: Test `test_two_children_split_detected` should pass

### 2. Test Tracker Updates
Run tests to verify tracker is applying split information:
```bash
pytest tests/unit/test_high_fixes.py::TestH1SplitKFMigration -v
```
**Expected**: All split-related tests should pass

### 3. Check Metadata Writing
Run tests for CellDataSaver:
```bash
pytest tests/core/process/detect/test_save.py -v
```

### 4. Create Integration Test
Create a script to run a complete end-to-end test:
```python
#!/usr/bin/env python3
import sys
import os
sys.path.append('/home/yuchenwei/Projects/EdgeWARN-Core/src')

from EdgeWARN.core.process.detect.lineage.detector import LineageDetector
from EdgeWARN.core.process.detect.lineage.buffer import LineageBuffer
from EdgeWARN.core.process.detect.track import StormCellTracker
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from unittest.mock import MagicMock

# Test data
old_cells = [
    {
        'id': 1,
        'bbox': [[35.0, 262.0], [35.0, 262.4], [35.4, 262.4], [35.4, 262.0]],
        'centroid': [35.2, 262.2],
        'max_refl': 60.0,
        'num_gates': 200,
        'event_type': 'ACTIVE',
        'parent_ids': [],
        'split_from': None,
        'tracking_mode': 'active'
    }
]

new_cells = [
    {
        'id': 10,
        'bbox': [[35.0, 262.0], [35.0, 262.2], [35.2, 262.2], [35.2, 262.0]],
        'centroid': [35.1, 262.1],
        'max_refl': 55.0,
        'num_gates': 100,
        'event_type': 'ACTIVE',
        'parent_ids': [],
        'split_from': None,
        'tracking_mode': 'active'
    },
    {
        'id': 20,
        'bbox': [[35.2, 262.2], [35.2, 262.4], [35.4, 262.4], [35.4, 262.2]],
        'centroid': [35.3, 262.3],
        'max_refl': 50.0,
        'num_gates': 80,
        'event_type': 'ACTIVE',
        'parent_ids': [],
        'split_from': None,
        'tracking_mode': 'active'
    }
]

# Test lineage detection
buffer = LineageBuffer(min_confirmations=1)
detector = LineageDetector(buffer=buffer, overlap_threshold=0.1)
lineage = detector.detect(old_cells, new_cells)
print(f"Splits detected: {len(lineage.splits)}")

# Test tracker
tracker = StormCellTracker(ps_old=None, ps_new=None, io_manager=MagicMock())
updated = tracker.update_cells(old_cells, new_cells, timestamp="2024-01-01T00:02:00", dt_seconds=120.0, lineage=lineage)
for cell in updated:
    print(f"Cell {cell['id']}: event_type={cell['event_type']}, split_from={cell['split_from']}")

# Test save
saver = CellDataSaver(None, None, None, None, None, None)
json_data = saver.create_json_structure("2024-01-01T00:02:00", updated)
print(f"JSON features: {len(json_data['features'])}")
```
Save as `test_split_integration.py` and run.

### 5. Check Configuration
Review default configuration values:
- `DEFAULT_OVERLAP_THRESHOLD`: 0.15 (15% overlap required)
- `min_confirmations`: 2 (requires 2 consecutive detections by default)

These settings may affect split detection.

## Potential Issues to Look For

1. **Overlap calculation**: Are the split overlaps being calculated correctly using old cell area as denominator?
2. **Hysteresis buffer**: Is `min_confirmations` set to 1 for testing, or requiring multiple scans?
3. **Tracker processing**: Is `update_cells()` correctly applying split_from and event_type?
4. **Saver**: Is `create_json_structure()` correctly passing through all metadata?
5. **Main flow**: Is `main.py` correctly connecting all components together?

## Action Plan

1. Run existing tests to identify failing cases
2. Create integration test to reproduce issue
3. Analyze results to pinpoint problem area
4. Fix identified issues
5. Re-run tests to verify fixes
6. Document findings

---

*Created: 2026-03-12*
