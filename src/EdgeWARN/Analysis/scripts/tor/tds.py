# Pseudocode Implementation for TDS
import numpy as np
import xarray as xr
from pathlib import Path
import gc
import re

class TornadoDetector:
    def __init__(self, rhohv_threshold, llrt_threshold, coverage_threshold):
        self.rhohv_threshold = rhohv_threshold
        self.llrt_threshold = llrt_threshold
        self.coverage_threshold = coverage_threshold

    def _detect_timestamp(self, dataset):
        patterns = [
            r"\d{8}[_-]\d{6}",
        ]
        
        match = re.search(patterns, dataset)
        if match:
            return match.group(0)
        else:
            return "unknown"

    def create_tds_ds(self, rhohv_path, llr_path, outpath: Path):
        """
        Creates Tornado Debris Signature dataset using RhoHV and Low-Level Rotation Track
        Returns 1 where RhoHV < 0.85 and rotation track > 0.006 s^-1, 0 otherwise
        """
        print(f"DEBUG: Loading RhoHV data from {rhohv_path}")
        print(f"DEBUG: Loading LLR data from {llr_path}")
        
        try:
            # Load datasets
            rhohv_ds = xr.open_dataset(rhohv_path)
            llr_ds = xr.open_dataset(llr_path)
            
            # Find variable names automatically
            rhohv_vars = [var for var in rhohv_ds.data_vars if not var.startswith('_')]
            llr_vars = [var for var in llr_ds.data_vars if not var.startswith('_')]
            
            if not rhohv_vars:
                print("ERROR: No variables found in RhoHV dataset")
                return None
            if not llr_vars:
                print("ERROR: No variables found in LLR dataset")
                return None
                
            rhohv_var = "unknown"
            llr_var = "unknown"
            
            print(f"DEBUG: Using RhoHV variable: {rhohv_var}")
            print(f"DEBUG: Using LLR variable: {llr_var}")
            
            # Extract data arrays
            rhohv_data = rhohv_ds[rhohv_var]
            llr_data = llr_ds[llr_var]
            
            # Apply thresholds
            print("DEBUG: Applying thresholds...")
            rhohv_mask = rhohv_data < self.rhohv_threshold
            llr_mask = llr_data > self.llrt_threshold
            
            # Combine masks
            tds_mask = rhohv_mask & llr_mask

            # Convert to 1 where thresholds met, -1 otherwise
            tds_binary = xr.where(tds_mask, 1, -1).astype(np.int8)
            
            # Create output dataset
            print("DEBUG: Creating output dataset...")
            tds_ds = xr.Dataset({
                'tds': tds_binary
            })
            
            # Copy coordinates from source dataset
            for coord in rhohv_ds.coords:
                if coord in rhohv_ds.coords:
                    tds_ds.coords[coord] = rhohv_ds.coords[coord]
            
            # Generate output filename with timestamp
            timestamp = TornadoDetector._detect_timestamp(rhohv_path)
            
            filename = f"EdgeWARN_TDS_{timestamp}.nc"
            full_path = outpath / filename
            
            print(f"DEBUG: Saving to {full_path}")
            tds_ds.to_netcdf(full_path)
            
            # Cleanup
            rhohv_ds.close()
            llr_ds.close()
            
            print("DEBUG: TDS detection completed successfully")
            return full_path
            
        except Exception as e:
            print(f"DEBUG: Error in TDS detection: {e}")
            return None
        
        finally:
            del rhohv_ds, rhohv_data, rhohv_mask
            del llr_ds, llr_data, llr_mask
            gc.collect()

    def calculate_tds_index(self, tds_path, storm_cells):
        """
        Calculates TDS index from EdgeWARN TDS dataset
        Inputs:
         - tds_path: EdgeWARN TDS dataset
         - storm_cells: List of storm cell dicts
        """
        # Implementation
        # 1 - Get storm cell polygon
        # 2 - Fetch TDS data inside polygon
        # 3 - Calculate TDS Index by: Flagged Values / Total Values
        # Where Flagged Values - Values where TDS = 1, Total Values - Total number of valid data points
        # 4 - Write to analysis under latest storm cell history entry

            
            

            
            
            