import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from EdgeWARN.core.process.detect.lineage.spatial import calculate_overlap_ratio


def create_mock_cell(cell_id, lat, lon, size=0.2, max_refl=55.0, num_gates=100):
    """Create a mock storm cell dictionary for testing."""
    bbox = [
        [lat, lon],
        [lat, lon + size],
        [lat + size, lon + size],
        [lat + size, lon],
    ]
    return {
        'id': cell_id,
        'bbox': bbox,
        'centroid': [lat + size/2, lon + size/2],
        'max_refl': max_refl,
        'num_gates': num_gates,
    }

# Test_three_way_split:
entries_old = [
    create_mock_cell(1, 35.0, 262.0, size=0.6, max_refl=60.0),
]

entries_new = [
    create_mock_cell(10, 35.0, 262.0, size=0.2, max_refl=55.0),
    create_mock_cell(20, 35.0, 262.1, size=0.2, max_refl=50.0),
    create_mock_cell(30, 35.1, 262.0, size=0.2, max_refl=45.0),
]


old_bbox = entries_old[0]['bbox']
print("old_bbox:", old_bbox)

for cell in entries_new:
    overlap = calculate_overlap_ratio(old_bbox, cell['bbox'])
    print(f"Cell {cell['id']} overlap with old: {overlap:.2f}")