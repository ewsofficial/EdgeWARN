import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from EdgeWARN.core.process.detect.lineage.spatial import calculate_overlap_ratio

def create_mock_cell(cell_id, lat, lon, size=0.2, max_refl=55.0, num_gates=100):
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

print("Merge scenario:")
base_lat = 35.0
base_lon = 262.0

old1 = create_mock_cell(1, base_lat, base_lon, size=0.2)
old2 = create_mock_cell(2, base_lat, base_lon + 0.25, size=0.2)
new = create_mock_cell(100, base_lat, base_lon, size=0.2)

print(f"Old1 bbox: {old1['bbox']}")
print(f"Old2 bbox: {old2['bbox']}")
print(f"New bbox: {new['bbox']}")

overlap1 = calculate_overlap_ratio(new['bbox'], old1['bbox'])
overlap2 = calculate_overlap_ratio(new['bbox'], old2['bbox'])

print(f"Overlap new→old1 (new as denominator): {overlap1:.2f}")
print(f"Overlap new→old2 (new as denominator): {overlap2:.2f}")

print("\nSplit scenario:")
base_lat = 40.0
base_lon = 262.0

old = create_mock_cell(200, base_lat, base_lon, size=0.2)
new1 = create_mock_cell(300, base_lat, base_lon, size=0.2)
new2 = create_mock_cell(301, base_lat + 0.2, base_lon + 0.2, size=0.2)

print(f"Old bbox: {old['bbox']}")
print(f"New1 bbox: {new1['bbox']}")
print(f"New2 bbox: {new2['bbox']}")

overlap1 = calculate_overlap_ratio(old['bbox'], new1['bbox'])
overlap2 = calculate_overlap_ratio(old['bbox'], new2['bbox'])

print(f"Overlap old→new1 (old as denominator): {overlap1:.2f}")
print(f"Overlap old→new2 (old as denominator): {overlap2:.2f}")