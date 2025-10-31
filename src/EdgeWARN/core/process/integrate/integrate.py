from .utils import StormIntegrationUtils
import xarray as xr
import numpy as np
import gc

class StormCellIntegrator:
    def __init__(self):
        pass

    def integrate_ds_via_max(self, dataset_path, storm_cells, output_key):
        """
        Integrate a dataset over storm cells, storing the result in each cell's storm_history.
        Saves maximum value of the dataset in each storm cell.
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
                latest[output_key] = 0
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
                    latest[output_key] = 0
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
        Flattens all ProbSevere variables directly into each storm history entry.
        """
        if not isinstance(probsevere_data, dict) or 'features' not in probsevere_data:
            print(f"[CellIntegration] ERROR: Failed to integrate ProbSevere data - Invalid Data Format")
            return storm_cells

        features = probsevere_data['features']

        # Pre-index features by their ID for O(1) lookups
        feature_lookup = {
            str(f.get('id') or f.get('properties', {}).get('ID')): f.get('properties', {})
            for f in features
        }

        # Variable mappings (key: target name, value: source property)
        field_map = {
            'MLCAPE': 'MLCAPE',
            'MUCAPE': 'MUCAPE',
            'MLCIN': 'MLCIN',
            'DCAPE': 'DCAPE',
            'CAPE_M10M30': 'CAPE_M10M30',
            'LCL': 'LCL',
            'Wetbulb_0C_Hgt': 'WETBULB_0C_HGT',
            'LLLR': 'LLLR',
            'MLLR': 'MLLR',
            'EBShear': 'EBSHEAR',
            'SRH01km': 'SRH01KM',
            'SRH02km': 'SRW02KM',
            'SRW46km': 'SRW46KM',
            'MeanWind_1-3kmAGL': 'MEANWIND_1-3kmAGL',
            'LJA': 'LJA',
            'CompRef': 'COMPREF',
            'Ref10': 'REF10',
            'Ref20': 'REF20',
            'MESH': 'MESH',
            'H50_Above_0C': 'H50_Above_0C',
            'EchoTop50': 'EchoTop_50',
            'VIL': 'VIL',
            'MaxFED': 'MaxFED',
            'MaxFCD': 'MaxFCD',
            'AccumFCD': 'AccumFCD',
            'MinFlashArea': 'MinFlashArea',
            'TE@MaxFCD': 'TE@MaxFCD',
            'FlashRate': 'FLASH_RATE',
            'FlashDensity': 'FLASH_DENSITY',
            'MaxLLAz': 'MAXLLAZ',
            'p98LLAz': 'P98LLAZ',
            'p98MLAz': 'P98MLAZ',
            'MaxRC_Emiss': 'MAXRC_EMISS',
            'ICP': 'ICP',
            'PWAT': 'PWAT',
            'avg_beam_hgt': 'AVG_BEAM_HGT'
        }

        for cell in storm_cells:
            if not cell.get("storm_history"):
                continue

            entry = cell["storm_history"][-1]
            cell_id = str(cell.get('id'))
            if 'centroid' not in entry or len(entry['centroid']) < 2:
                continue

            match = feature_lookup.get(cell_id)
            if not match:
                continue

            # Flatten values directly into the entry
            for target_key, source_key in field_map.items():
                try:
                    entry[target_key] = float(match.get(source_key, 0))
                except (TypeError, ValueError):
                    entry[target_key] = "MATCH_ERROR"

        return storm_cells
