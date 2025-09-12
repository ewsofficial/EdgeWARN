import xarray as xr

filename = r"C:\Users\weiyu\Downloads\rap.t00z.awp130pgrbf00.grib2"

# Open only CAPE/CIN/PLPL
ds = xr.open_dataset(
    filename,
    engine="cfgrib",
    backend_kwargs={
        "filter_by_keys": {
            "typeOfLevel": "pressureFromGroundLayer",
            "shortName": "cape"  # change to 'cin', 'plpl' for others
        }
    }
)

print(ds)
