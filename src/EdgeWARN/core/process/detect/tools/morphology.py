"""
Optimized morphology processing for storm cell analysis.
"""
import cv2
import numpy as np
import math
from scipy.ndimage import convolve
from skimage.morphology import skeletonize

# Pre-allocated kernel for skeleton analysis (module-level, created once)
_SKELETON_KERNEL = np.array([[1, 1, 1], [1, 10, 1], [1, 1, 1]], dtype=np.uint8)

# Minimum cell size for full analysis (pixels)
_MIN_PIXELS_FULL_ANALYSIS = 25


class MorphologyEngine:
    """Optimized morphology calculations for storm cells."""
    
    @staticmethod
    def process_cell(mask_slice, refl_slice):
        """
        Calculate scalar morphological metrics for a single cell.
        Optimized version with early bailout and reduced overhead.
        """
        pixel_count = np.count_nonzero(mask_slice)
        if pixel_count == 0:
            return {}
        
        metrics = {}
        
        # === Early bailout for tiny cells ===
        if pixel_count < _MIN_PIXELS_FULL_ANALYSIS:
            # Return defaults for small cells (not enough data for meaningful analysis)
            return {
                'linearity': 0.0,
                'branching_factor': 0,
                'solidity': 1.0,
                'defect_max_depth': 0.0,
                'defect_bearing': 0.0,
                'aspect_ratio': 1.0
            }
        
        # === Skeleton Analysis (Linearity) ===
        skeleton = skeletonize(mask_slice)
        skel_len = np.count_nonzero(skeleton)
        
        if skel_len > 0:
            filtered = convolve(skeleton.astype(np.uint8), _SKELETON_KERNEL, mode='constant', cval=0)
            skel_pixels = filtered[skeleton]
            n_endpoints = np.count_nonzero(skel_pixels == 11)
            n_junctions = np.count_nonzero(skel_pixels >= 13)
            
            complexity = n_endpoints + n_junctions
            if complexity > 0:
                metrics['linearity'] = round(skel_len / (complexity * 5.0), 2)
            else:
                metrics['linearity'] = 1.0
            metrics['branching_factor'] = int(n_junctions)
        else:
            metrics['linearity'] = 0.0
            metrics['branching_factor'] = 0
        
        # === Contour Analysis ===
        mask_u8 = mask_slice.astype(np.uint8) * 255
        contours, _ = cv2.findContours(mask_u8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        # Default values
        metrics['solidity'] = 1.0
        metrics['defect_max_depth'] = 0.0
        metrics['defect_bearing'] = 0.0
        metrics['aspect_ratio'] = 1.0
        
        if not contours:
            return metrics
            
        cnt = max(contours, key=cv2.contourArea)
        area = cv2.contourArea(cnt)
        
        if area <= 0:
            return metrics
        
        # Solidity
        hull = cv2.convexHull(cnt)
        hull_area = cv2.contourArea(hull)
        if hull_area > 0:
            metrics['solidity'] = round(area / hull_area, 3)
        
        # Aspect Ratio (fast - just minAreaRect)
        rect = cv2.minAreaRect(cnt)
        (_, _), (w, h), _ = rect
        if w > 0 and h > 0:
            ar = max(w, h) / min(w, h)
            metrics['aspect_ratio'] = round(ar, 2)
        
        # Convexity Defects (only for larger cells with complex hulls)
        if len(cnt) < 10:  # Skip defect analysis for simple shapes
            return metrics
            
        hull_indices = cv2.convexHull(cnt, returnPoints=False)
        if hull_indices is None or len(hull_indices) <= 3:
            return metrics
        
        try:
            defects = cv2.convexityDefects(cnt, hull_indices)
            if defects is not None and defects.shape[0] > 0:
                # Find max depth defect
                depths = defects[:, 0, 3]
                max_idx_in_defects = np.argmax(depths)
                max_depth = depths[max_idx_in_defects]
                f_idx = defects[max_idx_in_defects, 0, 2]
                
                metrics['defect_max_depth'] = round(max_depth / 256.0, 1)
                
                # Calculate bearing
                defect_pt = cnt[f_idx][0]
                M = cv2.moments(cnt)
                if M["m00"] != 0:
                    cX = int(M["m10"] / M["m00"])
                    cY = int(M["m01"] / M["m00"])
                    dy = cY - defect_pt[1]
                    dx = defect_pt[0] - cX
                    angle = math.degrees(math.atan2(dy, dx))
                    metrics['defect_bearing'] = round((angle + 360) % 360, 1)
        except Exception:
            pass
        
        return metrics
