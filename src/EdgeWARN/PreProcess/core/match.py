import numpy as np
from scipy.optimize import linear_sum_assignment

PENALTY_COST = 1000.0

class StormCellTerminator:
    def __init__():
        """
        Initializes Class
        """
    
class CellMatcher:
    @staticmethod

    # TO DO: Need to treat old cells that are >75% covered by a new cell as terminated if not matched to the new cell.

    def match_cells(cells0, cells1, weights=None):
        if weights is None:
            weights = {
                'distance': 0.5,
                'num_gates': 0.3,
                'max_reflectivity': 0.2
            }
        # Quick guards for empty inputs
        n0, n1 = len(cells0), len(cells1)
        if n0 == 0 or n1 == 0:
            print(f"DEBUG: No cells to match (n0={n0}, n1={n1})")
            return []

        # Safely compute max values (fall back to 1 if necessary to avoid division by zero)
        max_num_gates = 1.0
        max_reflect = 1.0
        
        # Combine both cell sets to find global max values
        all_cells = cells0 + cells1
        if all_cells:
            try:
                max_num_gates = max(max_num_gates, max(cell.get('num_gates', 0) for cell in all_cells))
                max_reflect = max(max_reflect, max(cell.get('max_reflectivity_dbz', 0) for cell in all_cells))
            except (ValueError, KeyError):
                # Handle cases where keys might be missing
                pass

        max_vals = {
            'num_gates': max_num_gates,
            'max_reflectivity_dbz': max_reflect
        }

        # Build cost matrix and check feasibility before assignment
        cost_matrix = np.full((n0, n1), np.inf)
        for i, c0 in enumerate(cells0):
            for j, c1 in enumerate(cells1):
                # Calculate distance between centroids first
                lat1, lon1 = c0.get('centroid', [0, 0])
                lat2, lon2 = c1.get('centroid', [0, 0])
                
                # Calculate dx and dy in km (approximate conversion)
                # 1° latitude ≈ 111 km, 1° longitude ≈ 111 km * cos(latitude)
                dx_km = abs(lon1 - lon2) * 111.0 * np.cos(np.radians((lat1 + lat2) / 2))
                dy_km = abs(lat1 - lat2) * 111.0
                
                # Check if either dx or dy exceeds 10 km
                if dx_km > 10.0 or dy_km > 10.0:
                    cost_matrix[i, j] = np.inf  # Disallow this match
                else:
                    cost_matrix[i, j] = CellMatcher.compute_cost(c0, c1, max_vals, weights)

        # If there are no costs below the penalty threshold, there are no reasonable matches
        if not (cost_matrix < PENALTY_COST).any():
            print(f"DEBUG: No candidate pairs with cost < PENALTY_COST (n0={n0}, n1={n1}); no feasible matches.")
            return []

        # Try the Hungarian algorithm first; if it fails (infeasible), fall back to a greedy matcher
        try:
            row_ind, col_ind = linear_sum_assignment(cost_matrix)
            matches = []
            for i, j in zip(row_ind, col_ind):
                if np.isfinite(cost_matrix[i, j]) and cost_matrix[i, j] < PENALTY_COST:
                    matches.append((i, j, float(cost_matrix[i, j])))
            return matches
        except Exception as e:
            print(f"DEBUG: linear_sum_assignment failed: {e}; falling back to greedy matching.")
            
            # Debug: list cost-matrix info before greedy fallback
            try:
                print(f"DEBUG: cost_matrix shape: {cost_matrix.shape}")
                finite_pairs = [(i, j, float(cost_matrix[i, j]))
                                for i in range(n0) for j in range(n1) 
                                if np.isfinite(cost_matrix[i, j]) and cost_matrix[i, j] < PENALTY_COST]
                print(f"DEBUG: finite pairs found: {len(finite_pairs)}")
                
                if finite_pairs:
                    finite_pairs.sort(key=lambda x: x[2])
                    for idx, (i, j, c) in enumerate(finite_pairs[:30]):
                        print(f"DEBUG: candidate {idx+1}: row={i}, col={j}, cost={c:.6f}")
                else:
                    print("DEBUG: No finite pairs found below penalty threshold.")
                    
            except Exception as dbg_e:
                print(f"DEBUG: failed to print cost matrix details: {dbg_e}")

            # Greedy matching: sort all finite pairs by cost and take the lowest-cost disjoint pairs
            finite_pairs = [(i, j, float(cost_matrix[i, j]))
                            for i in range(n0) for j in range(n1) 
                            if np.isfinite(cost_matrix[i, j]) and cost_matrix[i, j] < PENALTY_COST]
            finite_pairs.sort(key=lambda x: x[2])
            
            used_rows = set()
            used_cols = set()
            greedy_matches = []
            for i, j, c in finite_pairs:
                if i in used_rows or j in used_cols:
                    continue
                used_rows.add(i)
                used_cols.add(j)
                greedy_matches.append((i, j, c))
                
            return greedy_matches

    @staticmethod
    def compute_cost(cell0, cell1, max_vals, weights):
        """
        Compute cost between two cells based on distance, num_gates, and reflectivity
        """

        # Guard against empty inputs
        n0, n1 = len(cell0), len(cell1)
        if n0 == 0 or n1 == 0:
            print("Error: No cells detected in input")
            return []
        # Extract values with defaults
        # Calculate distance between centroids
        lat1, lon1 = cell0.get('centroid', [0, 0])
        lat2, lon2 = cell1.get('centroid', [0, 0])
        dist = np.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)
        
        num_gates0 = cell0.get('num_gates', 0)
        num_gates1 = cell1.get('num_gates', 0)
        
        reflect0 = cell0.get('max_reflectivity_dbz', 0)
        reflect1 = cell1.get('max_reflectivity_dbz', 0)
        
        # Normalize differences (0-1 range)
        norm_dist = min(dist / 10.0, 1.0)  # Adjust scaling as needed (10 degrees max distance)
        norm_gates_diff = abs(num_gates0 - num_gates1) / max_vals['num_gates']
        norm_reflect_diff = abs(reflect0 - reflect1) / max_vals['max_reflectivity_dbz']
        
        # Weighted cost
        cost = (weights['distance'] * norm_dist +
                weights['num_gates'] * norm_gates_diff +
                weights['max_reflectivity'] * norm_reflect_diff)
        
        return cost