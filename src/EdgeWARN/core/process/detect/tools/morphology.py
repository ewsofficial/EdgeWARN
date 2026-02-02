import cv2
import numpy as np
import scipy.ndimage
from skimage.morphology import skeletonize

class MorphologyEngine:
    @staticmethod
    def process_cell(mask_slice, refl_slice):
        """
        Calculate scalar morphological metrics for a single cell.
        
        Args:
            mask_slice (np.ndarray): Boolean mask of the storm object (within the slice).
            refl_slice (np.ndarray): Reflectivity values (same shape as mask).
            
        Returns:
            dict: Dictionary of scalar metrics.
        """
        if not np.any(mask_slice):
            return {}
            
        metrics = {}
        
        # === 1. Geometry & Solidity ===
        # Convert mask to uint8 for OpenCV
        # Ensure contiguous array for OpenCV C++ bindings
        mask_u8 = mask_slice.astype(np.uint8) * 255
        
        # Find contours (external only)
        # Note: We use CHAIN_APPROX_SIMPLE to save points, but for accurate area we might want NONE?
        # Actually cv2.contourArea is accurate even with SIMPLE.
        contours, _ = cv2.findContours(mask_u8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        if contours:
            # Take the largest contour by area
            cnt = max(contours, key=cv2.contourArea)
            
            area = cv2.contourArea(cnt)
            if area > 0:
                hull = cv2.convexHull(cnt)
                hull_area = cv2.contourArea(hull)
                
                # Solidity = Area / Hull_Area
                if hull_area > 0:
                    metrics['solidity'] = round(area / hull_area, 3)
                else:
                    metrics['solidity'] = 1.0 # Should ideally use 1.0 for single point/line?
                    
                # Convexity Defects (The "Notch" Detector)
                # hull indices are needed for convexityDefects
                hull_indices = cv2.convexHull(cnt, returnPoints=False)
                if hull_indices is not None and len(hull_indices) > 3: # Defects require >3 points
                    try:
                        defects = cv2.convexityDefects(cnt, hull_indices)
                        if defects is not None:
                            # defects shape: [N, 1, 4] -> (start_idx, end_idx, farthest_pt_idx, fixpt_depth_approx)
                            # depth is roughly distance * 256
                            max_depth = 0
                            for i in range(defects.shape[0]):
                                _, _, _, d = defects[i, 0]
                                if d > max_depth:
                                    max_depth = d
                            
                            # Normalize depth (approx pixels)
                            metrics['defect_max_depth'] = round(max_depth / 256.0, 1)
                        else:
                            metrics['defect_max_depth'] = 0.0
                    except Exception:
                         metrics['defect_max_depth'] = 0.0
                         
                # Aspect Ratio
                if w > 0 and h > 0:
                    aspect_ratio = float(w) / h
                    # Normalize to be >= 1 for "elongation"
                    if aspect_ratio < 1.0:
                        aspect_ratio = 1.0 / aspect_ratio
                    metrics['aspect_ratio'] = round(aspect_ratio, 2)
                    
        return metrics
