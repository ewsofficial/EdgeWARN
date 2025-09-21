from shapely.geometry import Polygon
from shapely.ops import unary_union
from typing import Tuple, List, Dict
import numpy as np
from .utils import GeoUtils


class CellTerminator:
    """
    Utilities to decide whether storm cells should be terminated/removed.
    Provides polygon overlap calculations and simple heuristics.
    """

    @staticmethod
    def terminate_highly_covered_cells(cells: List[Dict], coverage_threshold: float = 67.0) -> List[Dict]:
        """
        Remove cells that are highly covered (>= coverage_threshold%) by larger cells.
        
        Args:
            cells: List of storm cell dictionaries
            coverage_threshold: Percentage threshold for removal (default: 67%)
            
        Returns:
            List of cells with highly covered ones removed
        """
        if len(cells) <= 1:
            return cells
        
        # Ensure all cells have area calculated
        for cell in cells:
            polygon = cell.get('alpha_shape') or cell.get('convex_hull')
            if polygon and len(polygon) >= 3:
                cell['area_km2'] = GeoUtils.polygon_area_km2(polygon)
            else:
                cell['area_km2'] = 0.0
        
        # Sort by area descending (largest first)
        cells_sorted = sorted(cells, key=lambda x: x.get('num_gates', 0), reverse=True)
        
        cells_to_remove = set()
        
        # Compare each cell with all larger cells
        for i, smaller_cell in enumerate(cells_sorted):
            smaller_id = smaller_cell['id']
            if smaller_id in cells_to_remove:
                continue
                
            for larger_cell in cells_sorted[:i]:  # Only check larger cells (earlier in list)
                larger_id = larger_cell['id']
                if larger_id in cells_to_remove:
                    continue
                    
                # Calculate how much the smaller cell is covered by the larger cell
                overlap_area, overlap_pct_smaller, _ = CellTerminator.polygon_overlap(
                    smaller_cell, larger_cell
                )
                
                # If smaller cell is highly covered by larger cell, mark for removal
                if overlap_pct_smaller >= coverage_threshold:
                    cells_to_remove.add(smaller_id)
                    print(f"Terminating cell {smaller_id} ({smaller_cell['area_km2']:.1f} km²): "
                          f"{overlap_pct_smaller:.1f}% covered by cell {larger_id} ({larger_cell['area_km2']:.1f} km²)")
                    break  # No need to check other larger cells
        
        # Filter out the cells to remove
        filtered_cells = [cell for cell in cells if cell['id'] not in cells_to_remove]
        
        print(f"Terminated {len(cells_to_remove)} cells highly covered by larger cells")
        return filtered_cells

    @staticmethod
    def polygon_overlap(cell1: Dict, cell2: Dict) -> Tuple[float, float, float]:
        """
        Calculate overlap between two storm cells.
        
        Args:
            cell1: First storm cell dictionary
            cell2: Second storm cell dictionary
            
        Returns:
            (intersection_area_km2, overlap_pct_cell1, overlap_pct_cell2)
        """
        # Get polygons from cells
        poly1_points = cell1.get('alpha_shape', []) or cell1.get('convex_hull', [])
        poly2_points = cell2.get('alpha_shape', []) or cell2.get('convex_hull', [])
        
        if len(poly1_points) < 3 or len(poly2_points) < 3:
            return 0.0, 0.0, 0.0
        
        try:
            # Create Shapely Polygon objects
            poly1 = Polygon(poly1_points)
            poly2 = Polygon(poly2_points)
            
            # Fix invalid geometries if needed
            if not poly1.is_valid:
                poly1 = poly1.buffer(0)
            if not poly2.is_valid:
                poly2 = poly2.buffer(0)
            
            if poly1.is_empty or poly2.is_empty:
                return 0.0, 0.0, 0.0
            
            # Calculate intersection
            intersection = poly1.intersection(poly2)
            
            if intersection.is_empty:
                return 0.0, 0.0, 0.0
            
            # Calculate areas using our existing method for consistency
            area1 = GeoUtils.polygon_area_km2(poly1_points)
            area2 = GeoUtils.polygon_area_km2(poly2_points)
            
            if hasattr(intersection, 'exterior'):
                intersection_area = GeoUtils.polygon_area_km2(list(intersection.exterior.coords))
            elif hasattr(intersection, 'geoms'):
                # MultiPolygon case - sum areas of all polygons
                intersection_area = sum(GeoUtils.polygon_area_km2(list(geom.exterior.coords)) 
                                      for geom in intersection.geoms)
            else:
                intersection_area = 0.0
            
            # Calculate overlap percentages
            overlap_pct1 = (intersection_area / area1 * 100) if area1 > 0 else 0.0
            overlap_pct2 = (intersection_area / area2 * 100) if area2 > 0 else 0.0
            
            return intersection_area, overlap_pct1, overlap_pct2
            
        except Exception as e:
            print(f"Warning: Error calculating cell overlap: {e}")
            return 0.0, 0.0, 0.0

    @staticmethod
    def is_highly_overlapped(cell_a: dict, cell_b: dict, threshold_pct: float = 67.0) -> bool:
        """
        Return True if cell_a is overlapped by cell_b by more than `threshold_pct` percent.
        """
        _, overlap_pct_a, _ = CellTerminator.polygon_overlap(cell_a, cell_b)
        return overlap_pct_a >= threshold_pct