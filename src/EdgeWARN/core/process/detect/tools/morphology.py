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
        mask_u8 = mask_slice.astype(np.uint8) * 255
        
        # Skeletonization (Linearity Check)
        # Convert to boolean for skimage
        # skeletonize works on boolean True/False
        skeleton = skeletonize(mask_slice)
        
        # Analyze Skeleton structure
        # A pure line has 2 endpoints and 0 junctions.
        # A cluster has many endpoints and junctions.
        
        # Count pixels in skeleton (Length)
        skel_len = np.sum(skeleton)
        
        if skel_len > 0:
            # Convolution to find neighbors
            # 3x3 kernel of ones. The center pixel value in the convolved image (where skeleton is True)
            # gives number of neighbors + 1 (itself).
            from scipy.ndimage import convolve
            kernel = np.array([[1,1,1],[1,10,1],[1,1,1]], dtype=np.uint8)
            filtered = convolve(skeleton.astype(np.uint8), kernel, mode='constant', cval=0)
            
            # Select only skeleton pixels
            # Values in filtered:
            # Endpoints: 1 neighbor -> 10 + 1 = 11
            # Line body: 2 neighbors -> 10 + 2 = 12
            # Junction: 3+ neighbors -> 10 + 3 = 13+
            
            skel_pixels = filtered[skeleton]
            n_endpoints = np.sum(skel_pixels == 11)
            n_junctions = np.sum(skel_pixels >= 13)
            
            # Linearity Metric: Length / Complexity
            # High for simple lines, Low for complex/star shapes
            complexity = n_endpoints + n_junctions
            if complexity > 0:
                metrics['linearity'] = round(skel_len / (complexity * 5.0), 2) # Normalized approx
            else:
                 metrics['linearity'] = 1.0 # Single point or loop
                 
            metrics['branching_factor'] = int(n_junctions)
        else:
            metrics['linearity'] = 0.0
            metrics['branching_factor'] = 0

        
        contours, _ = cv2.findContours(mask_u8, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        if contours:
            cnt = max(contours, key=cv2.contourArea)
            area = cv2.contourArea(cnt)
            
            if area > 0:
                hull = cv2.convexHull(cnt)
                hull_area = cv2.contourArea(hull)
                
                # Solidity
                if hull_area > 0:
                    metrics['solidity'] = round(area / hull_area, 3)
                else:
                    metrics['solidity'] = 1.0
                    
                # Convexity Defects (Notch)
                hull_indices = cv2.convexHull(cnt, returnPoints=False)
                
                # Default values
                metrics['defect_max_depth'] = 0.0
                metrics['defect_bearing'] = 0.0

                if hull_indices is not None and len(hull_indices) > 3:
                     try:
                        defects = cv2.convexityDefects(cnt, hull_indices)
                        if defects is not None:
                            max_depth = 0
                            max_idx = -1
                            
                            for i in range(defects.shape[0]):
                                _, _, f_idx, d = defects[i, 0]
                                if d > max_depth:
                                    max_depth = d
                                    max_idx = f_idx
                            
                            metrics['defect_max_depth'] = round(max_depth / 256.0, 1)

                            if max_idx != -1:
                                defect_pt = cnt[max_idx][0]
                                M = cv2.moments(cnt)
                                if M["m00"] != 0:
                                    cX = int(M["m10"] / M["m00"])
                                    cY = int(M["m01"] / M["m00"])
                                    
                                    # Invert Y for Geo coords
                                    dy = cY - defect_pt[1]
                                    dx = defect_pt[0] - cX
                                    import math
                                    angle = math.degrees(math.atan2(dy, dx))
                                    metrics['defect_bearing'] = round((angle + 360) % 360, 1)
                     except Exception:
                         pass
                
                # Aspect Ratio (Rotated Rect)
                rect = cv2.minAreaRect(cnt)
                (x, y), (w, h), angle = rect
                
                if w > 0 and h > 0:
                    ar = w / h
                    # Normalize to be >= 1
                    if ar < 1.0:
                        ar = 1.0 / ar
                    metrics['aspect_ratio'] = round(ar, 2)
                else:
                    metrics['aspect_ratio'] = 1.0

        return metrics
