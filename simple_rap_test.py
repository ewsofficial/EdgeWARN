#!/usr/bin/env python3
"""
Simple test to verify RAP integration improvements
"""

import sys
from pathlib import Path

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent / "src"))

def test_coordinate_handling():
    """Test the coordinate system handling logic"""
    import numpy as np
    
    # Test longitude conversion
    lon_0_360 = np.array([0, 90, 180, 270, 359])
    lon_needs_conversion = lon_0_360.max() > 180
    print(f"Longitude needs conversion: {lon_needs_conversion}")
    
    if lon_needs_conversion:
        lon_converted = np.where(lon_0_360 > 180, lon_0_360 - 360, lon_0_360)
        print(f"Original: {lon_0_360}")
        print(f"Converted: {lon_converted}")
    
    # Test polygon bounds
    poly_bounds = (-85.0, 35.0, -84.0, 36.0)  # minx, miny, maxx, maxy
    print(f"Polygon bounds: {poly_bounds}")
    
    # Test data bounds
    data_bounds = (lon_converted.min(), 34.0, lon_converted.max(), 37.0) if lon_needs_conversion else (-85.0, 34.0, -84.0, 37.0)
    print(f"Data bounds: {data_bounds}")
    
    # Test bounding box mask logic
    minx, miny, maxx, maxy = poly_bounds
    data_lat = np.array([34.5, 35.0, 35.5, 36.0, 36.5])
    data_lon = lon_converted if lon_needs_conversion else np.array([-85.0, -84.5, -84.0, -83.5, -83.0])
    
    bbox_mask = (data_lat >= miny) & (data_lat <= maxy) & (data_lon >= minx) & (data_lon <= maxx)
    print(f"Bounding box mask: {bbox_mask}")
    print(f"Masked lat: {data_lat[bbox_mask]}")
    print(f"Masked lon: {data_lon[bbox_mask]}")
    
    return True

def test_variable_mapping():
    """Test variable name mapping"""
    u_candidates = ['u', 'UGRD', 'u-component_of_wind_isobaric', 'wind_u']
    v_candidates = ['v', 'VGRD', 'v-component_of_wind_isobaric', 'wind_v']
    
    available_vars = ['UGRD', 'VGRD']
    
    for var in available_vars:
        if var in u_candidates:
            output_var = 'u'
            print(f"{var} maps to {output_var}")
        elif var in v_candidates:
            output_var = 'v'
            print(f"{var} maps to {output_var}")