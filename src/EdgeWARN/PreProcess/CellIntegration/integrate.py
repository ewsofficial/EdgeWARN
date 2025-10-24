from .utils import StormIntegrationUtils
import xarray as xr
import numpy as np
import gc

class StormCellIntegrator:
    def __init__(self):
        pass

    def integrate_ds(self, dataset_path, storm_cells, output_key):
        """
        Integrate a dataset over storm cells, storing the result in each cell's storm_history.
        Handles both 1D and 2D lat/lon coordinates.
        Fully loads dataset into memory, no subsetting.
        """

        print(f"[CellIntegration] DEBUG: Integrating dataset for {len(storm_cells)} storm cells")

        # Step 1: Load dataset directly (no subsetting)
        try:
            if dataset_path.endswith(".grib2"):
                ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)

            ds.load()  # load entire dataset
            print(f"[CellIntegration] DEBUG: Dataset loaded successfully with shape {list(ds.sizes.values())}")

            # Identify coordinate names
            lat_name = "latitude" if "latitude" in ds.coords else "lat"
            lon_name = "longitude" if "longitude" in ds.coords else "lon"

            # Check if dataset is empty
            if ds.sizes[lat_name] == 0 or ds.sizes[lon_name] == 0:
                print("[CellIntegration] WARN: Dataset empty")
                for cell in storm_cells:
                    if cell.get("storm_history"):
                        cell["storm_history"][-1][output_key] = "EMPTY_DATASET"
                ds.close()
                return storm_cells

        except MemoryError:
            print("[CellIntegration] ERROR: Dataset too large to load into memory")
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "MEMORY_ERROR"
            return storm_cells
        except Exception as e:
            print(f"[CellIntegration] ERROR: Failed to load dataset: {e}")
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "DATASET_LOAD_ERROR"
            return storm_cells

        # Step 2: Select variable
        var = ds.get("unknown")
        if var is None:
            print("[CellIntegration] ERROR: Variable 'unknown' not found in dataset")
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "VAR_NOT_FOUND"
            ds.close()
            return storm_cells

        # Step 3: Get coordinates (can be 1D or 2D)
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values

        # Step 4: Process storm cells
        for cell in storm_cells:
            if not cell.get("storm_history"):
                continue

            latest = cell["storm_history"][-1]
            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                latest[output_key] = "N/A"
                continue

            try:
                if lat_vals.ndim == 1 and lon_vals.ndim == 1:
                    mask = np.logical_and.outer(
                        (lat_vals >= poly.bounds[1]) & (lat_vals <= poly.bounds[3]),
                        (lon_vals >= poly.bounds[0]) & (lon_vals <= poly.bounds[2])
                    )
                else:
                    mask = (
                        (lat_vals >= poly.bounds[1]) & (lat_vals <= poly.bounds[3]) &
                        (lon_vals >= poly.bounds[0]) & (lon_vals <= poly.bounds[2])
                    )

                subset_vals = var.where(mask & (var >= 0))
                if subset_vals.size == 0 or np.all(np.isnan(subset_vals)):
                    latest[output_key] = "N/A"
                else:
                    latest[output_key] = float(np.nanmax(subset_vals))

            except Exception as e:
                print(f"[CellIntegration] ERROR: Processing cell {cell.get('id', 'unknown')}: {e}")
                latest[output_key] = "PROCESSING_ERROR"

            finally:
                try:
                    del subset_vals, mask, poly
                except Exception:
                    pass
                gc.collect()

        # Step 5: Cleanup
        ds.close()
        del var, ds
        gc.collect()

        return storm_cells

    def integrate_probsevere(self, probsevere_data, storm_cells):
        """
        Integrate ProbSevere probability data with storm cells by matching IDs.
        """
        if not isinstance(probsevere_data, dict) or 'features' not in probsevere_data:
            return storm_cells

        features = probsevere_data['features']

        for cell in storm_cells:
            if not cell.get("storm_history"):
                continue

            entry = cell["storm_history"][-1]
            cell_id = cell.get('id')
            if 'centroid' not in entry or len(entry['centroid']) < 2:
                continue

            # Match by ID
            match = None
            for feature in features:
                feature_id = feature.get('id') or feature.get('properties', {}).get('ID')
                if feature_id == cell_id:
                    match = feature.get('properties', {})
                    break

            if match:
                entry['probsevere_details'] = {
                    'MLCAPE': float(match.get('MLCAPE', 0)),
                    'MUCAPE': float(match.get('MUCAPE', 0)),
                    'MLCIN': float(match.get('MLCIN', 0)),
                    'DCAPE': float(match.get('DCAPE', 0)),
                    'CAPE_M10M30': float(match.get('CAPE_M10M30', 0)),
                    'LCL': float(match.get('LCL', 0)),
                    'Wetbulb_0C_Hgt': float(match.get('WETBULB_0C_HGT', 0)),
                    'LLLR': float(match.get('LLLR', 0)),
                    'MLLR': float(match.get('MLLR', 0)),
                    'EBShear': float(match.get('EBSHEAR', 0)),
                    'SRH01km': float(match.get('SRH01KM', 0)),
                    'SRH02km': float(match.get('SRW02KM', 0)),
                    'SRW46km': float(match.get('SRW46KM', 0)),
                    'MeanWind_1-3kmAGL': float(match.get('MEANWIND_1-3kmAGL', 0)),
                    'LJA': float(match.get('LJA', 0)),
                    'CompRef': float(match.get('COMPREF', 0)),
                    'Ref10': float(match.get('REF10', 0)),
                    'Ref20': float(match.get('REF20', 0)),
                    'MESH': float(match.get('MESH', 0)),
                    'H50_Above_0C': float(match.get('H50_Above_0C', 0)),
                    'EchoTop50': float(match.get('EchoTop_50', 0)),
                    'VIL': float(match.get('VIL', 0)),
                    'MaxFED': float(match.get('MaxFED', 0)),
                    'MaxFCD': float(match.get('MaxFCD', 0)),
                    'AccumFCD': float(match.get('AccumFCD', 0)),
                    'MinFlashArea': float(match.get('MinFlashArea', 0)),
                    'TE@MaxFCD': float(match.get('TE@MaxFCD', 0)),
                    'FlashRate': float(match.get('FLASH_RATE', 0)),
                    'FlashDensity': float(match.get('FLASH_DENSITY', 0)),
                    'MaxLLAz': float(match.get('MAXLLAZ', 0)),
                    'p98LLAz': float(match.get('P98LLAZ', 0)),
                    'p98MLAz': float(match.get('P98MLAZ', 0)),
                    'MaxRC_Emiss': float(match.get('MAXRC_EMISS', 0)),
                    'ICP': float(match.get('ICP', 0)),
                    'PWAT': float(match.get('PWAT', 0)),
                    'avg_beam_hgt': float(match.get('AVG_BEAM_HGT', 0))
                }

        return storm_cells
