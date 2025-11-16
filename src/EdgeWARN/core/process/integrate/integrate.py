from .utils import StormIntegrationUtils
import xarray as xr
import numpy as np
import gc

class StormCellIntegrator:
    def __init__(self, io_manager):
        self.io_manager = io_manager

    def integrate_ds_via_max(self, dataset_path, storm_cells, output_key, variable_name="unknown"):
        """
        Integrate a dataset over storm cells, storing the max value in each cell's storm_history.
        Optimized for large grids using precomputed coordinate grids and NumPy indexing.
        
        Args:
            dataset_path (str): Path to the dataset file (GRIB2 or NetCDF)
            storm_cells (list): List of storm cell dicts
            output_key (str): Key under which to store max value in storm_history
            variable_name (str): Variable to extract from dataset
        """
        self.io_manager.write_debug(f"Integrating dataset for {len(storm_cells)} storm cells: {dataset_path}")

        # Step 1: Load dataset fully
        try:
            if dataset_path.endswith(".grib2"):
                ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)
            ds.load()
            self.io_manager.write_debug(f"Dataset loaded successfully with shape {list(ds.sizes.values())}")
        except MemoryError:
            self.io_manager.write_error("Dataset too large to load into memory")
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "MEMORY_ERROR"
            return storm_cells
        except Exception as e:
            self.io_manager.write_error(f"Failed to load dataset: {e}")
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "DATASET_LOAD_ERROR"
            return storm_cells

        # Step 2: Select variable
        var = ds.get(variable_name)
        if var is None:
            self.io_manager.write_error(f"Variable '{variable_name}' not found in dataset")
            for cell in storm_cells:
                if cell.get("storm_history"):
                    cell["storm_history"][-1][output_key] = "VAR_NOT_FOUND"
            ds.close()
            return storm_cells

        # Step 3: Create coordinate grids once
        try:
            lat_grid, lon_grid = StormIntegrationUtils.create_coordinate_grids(ds)
            self.io_manager.write_debug(f"Created coordinate grids with dimensions: [{len(lat_grid)}, {len(lon_grid)}]")
        except Exception as e:
            self.io_manager.write_error(f"Failed to create coordinate grids: {e}")
            ds.close()
            return storm_cells

        lat_vals = lat_grid[:, 0] if lat_grid.ndim == 2 else lat_grid
        lon_vals = lon_grid[0, :] if lon_grid.ndim == 2 else lon_grid

        # Step 4: Process each storm cell
        for cell in storm_cells:
            if not cell.get("storm_history"):
                continue

            latest = cell["storm_history"][-1]
            polygon = StormIntegrationUtils.create_cell_polygon(cell)

            if polygon is None:
                latest[output_key] = 0
                continue

            try:
                # Fast bounding box approximation for 1D coordinates
                if lat_vals.ndim == 1 and lon_vals.ndim == 1:
                    minx, miny, maxx, maxy = polygon.bounds
                    lat_inds = np.searchsorted(lat_vals, [miny, maxy])
                    lon_inds = np.searchsorted(lon_vals, [minx, maxx])

                    lat_slice = slice(lat_inds[0], lat_inds[1]+1)
                    lon_slice = slice(lon_inds[0], lon_inds[1]+1)
                    subset_vals = var.values[lat_slice, lon_slice]
                    subset_vals = subset_vals[subset_vals >= 0]

                    latest[output_key] = float(np.nanmax(subset_vals)) if subset_vals.size > 0 else 0

                # Full 2D coordinates: rasterize polygon
                else:
                    mask = StormIntegrationUtils.create_polygon_mask(polygon, lat_grid, lon_grid)
                    subset_vals = var.values[mask & (var.values >= 0)]
                    latest[output_key] = float(np.nanmax(subset_vals)) if subset_vals.size > 0 else 0

            except Exception as e:
                self.io_manager.write_error(f"Processing cell {cell.get('id', 'unknown')}: {e}")
                latest[output_key] = "PROCESSING_ERROR"
            finally:
                # Clean up temporaries
                try:
                    del subset_vals, mask, polygon
                except Exception:
                    pass

        # Step 5: Cleanup
        ds.close()
        del var, ds, lat_grid, lon_grid
        gc.collect()

        return storm_cells

    def integrate_probsevere(self, probsevere_data, storm_cells):
        """
        Integrate ProbSevere probability data with storm cells by matching IDs.
        Flattens all ProbSevere variables directly into each storm history entry.
        """
        if not isinstance(probsevere_data, dict) or 'features' not in probsevere_data:
            self.io_manager.write_error(f"Failed to integrate ProbSevere data - Invalid Data Format")
            return storm_cells

        features = probsevere_data['features']

        # Pre-index features by their ID for O(1) lookups
        feature_lookup = {
            str(f.get('id') or f.get('properties', {}).get('ID')): f.get('properties', {})
            for f in features
        }

        # Variable mappings (key: target name, value: source property)
        field_map = {
            'ProbSevere': 'ProbSevere',
            'ProbWind': 'ProbWind',
            'ProbHail': 'ProbHail',
            'ProbTor': 'ProbTor',
            'vx': 'MOTION_EAST',
            'vy': 'MOTION_SOUTH',
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
