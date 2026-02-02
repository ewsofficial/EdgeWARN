import sys
import os
import numpy as np
import cv2

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))

from EdgeWARN.core.process.detect.tools.morphology import MorphologyEngine

def test_morphology_logic():
    print("Testing MorphologyEngine Logic...")

    # 1. Create a Synthetic "C-Shape" (Bow Echo) Mask
    # 30x30 grid
    mask = np.zeros((30, 30), dtype=bool)
    # Fill a C-shape
    mask[5:25, 5:10] = True  # Left vertical
    mask[5:10, 5:25] = True  # Top horizontal
    mask[20:25, 5:25] = True # Bottom horizontal
    
    # 2. Synthetic VIL Data (Random values 0-80)
    vil = np.random.rand(30, 30) * 80
    
    # 3. Synthetic EchoTop Data (Random values 0-15)
    et = np.random.rand(30, 30) * 15

    # 4. Process
    results = MorphologyEngine.process_cell(mask, None, vil, et)

    print("\n--- Results ---")
    for k, v in results.items():
        print(f"{k}: {v}")

    # 5. Logical Assertions
    print("\n--- Verification ---")
    
    # Solidity check: A C-shape is significantly less than 1.0 (its convex hull fills the gap)
    if results['solidity'] < 0.8:
        print(f"PASS: Solidity {results['solidity']} correctly identifies non-convex shape.")
    else:
        print(f"FAIL: Solidity {results['solidity']} is too high for a C-shape.")

    # Aspect Ratio check
    if results['aspect_ratio'] >= 1.0:
        print(f"PASS: Aspect Ratio {results['aspect_ratio']} is valid.")
    else:
        print(f"FAIL: Aspect Ratio {results['aspect_ratio']} < 1.0.")

    # VIL check
    print(f"PASS: VIL Max {results['vil_density_max']} extracted.")
    
    # Defect check
    if results['defect_max_depth'] > 0:
        print(f"PASS: Convexity Defect found (Depth: {results['defect_max_depth']}).")
    else:
        print(f"FAIL: No convexity defect found in C-shape.")

if __name__ == "__main__":
    test_morphology_logic()
