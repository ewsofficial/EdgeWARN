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

# Old cell
old = create_mock_cell(1, 35.0, 262.0, size=0.2)
old_bbox = old['bbox']

# New cells (split from old cell)
new1 = create_mock_cell(10, 35.0, 262.0, size=0.2)
new2 = create_mock_cell(20, 35.1, 262.1, size=0.2)

print("Old cell bbox:", old_bbox)
print("New1 cell bbox:", new1['bbox'])
print("New2 cell bbox:", new2['bbox'])

# Calculate overlap between old and new1
overlap1 = calculate_overlap_ratio(old_bbox, new1['bbox'])
print("Overlap old->new1 ratio (old as denominator):", overlap1)

# Calculate overlap between old and new2
overlap2 = calculate_overlap_ratio(old_bbox, new2['bbox'])
print("Overlap old->new2 ratio (old as denominator):", overlap2)

print(f"Overlap ratios with threshold 0.15: {overlap1:.2f}, {overlap2:.2f}")
