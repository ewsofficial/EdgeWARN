import xarray as xr
import json
from util.io import IOManager
from pathlib import Path

io_manager = IOManager("[GUI]")

class TransformUtils:
    @staticmethod
    def load_ds(ds_path: Path, lat_limits=None, lon_limits=None):
        """
        Loads .grib2/.nc datasets
        Args:
         - ds_path: Pathlib Path() object of grib2 or netCDF dataset
         - lat_limits, lon_limits: 0-360 format of lat/lon limits (Only works on netCDF)
        
        Returns:
         - ds: Loaded dataset
        """
        
        io_manager.write_debug(f"Opening file: {ds_path} ...")

        try:
            if str(ds_path).endswith(".grib2") or str(ds_path).endswith(".grib"):
                if lat_limits or lon_limits:
                    IOManager.write_warning("lat/lon limits not supported with GRIB files, skipping ... ")
                
                ds = xr.open_dataset(ds_path, decode_timedelta=True)
                io_manager.write_debug(f"Successfully loaded dataset: {ds_path}")
                return ds
        
            if str(ds_path).endswith(".nc"):
                ds = xr.open_dataset(ds_path, decode_timedelta=True)

                if lat_limits and lon_limits:
                    # Latitude/Longitude variables: 'latitude', 'longitude'
                    ds = ds.sel(
                        latitude=slice(lat_limits[0], lat_limits[1]),
                        longitude=slice(lon_limits[0], lon_limits[1])
                    )
                    io_manager.write_debug(f"Loaded dataset subset with lat {lat_limits}, lon {lon_limits}")

                else:
                    io_manager.write_warning("lat/lon coordinates not specified, loading full dataset")

                io_manager.write_debug("Successfully loaded full dataset")
                return ds
        
        except Exception as e:
            IOManager.write_error(f"Failed to load dataset - {e}")
            return