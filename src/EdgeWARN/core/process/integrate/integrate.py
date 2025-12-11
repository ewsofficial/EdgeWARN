from .utils import StormIntegrationUtils
from EdgeWARN.core.gui_pipelines.transform.render import GUILayerRenderer
from EdgeWARN.core.gui_pipelines.transform.tools import TransformUtils
import xarray as xr
import numpy as np
import shapely.vectorized as sv
import gc

class StormCellIntegrator:
    def __init__(self, io_manager):
        self.io_manager = io_manager

    def integrate_ds_via_max(self, dataset_path, storm_cells, output_key, render_config=None):

        # Load dataset
        try:
            if dataset_path.endswith(".grib2"):
                ds = xr.open_dataset(dataset_path, engine="cfgrib", decode_timedelta=True)
            else:
                ds = xr.open_dataset(dataset_path, decode_timedelta=True)

            ds.load()
        except Exception as e:
            self.io_manager.write_error(f"Load error: {e}")
            return storm_cells

        # Coordinates
        lat_name = "latitude" if "latitude" in ds.coords else "lat"
        lon_name = "longitude" if "longitude" in ds.coords else "lon"
        lat_vals = ds[lat_name].values
        lon_vals = ds[lon_name].values

        var = ds.get("unknown")
        if var is None:
            self.io_manager.write_error("Variable 'unknown' not found")
            return storm_cells

        latest_ts = max(
            (
                cell["storm_history"][-1]["timestamp"]
                for cell in storm_cells
                if cell.get("storm_history")
            ),
            default=None,
        )

        if latest_ts is None:
            ds.close()
            return storm_cells

        active_cells = [
            cell for cell in storm_cells if cell.get("storm_history") and cell["storm_history"][-1]["timestamp"] == latest_ts
        ]
        self.io_manager.write_info(f"Integrating {output_key} data for {len(active_cells)} cells")

        var_values = var.values

        for cell in active_cells:
            latest = cell["storm_history"][-1]

            poly = StormIntegrationUtils.create_cell_polygon(cell)
            if poly is None:
                latest[output_key] = 0
                continue

            try:
                minx, miny, maxx, maxy = poly.bounds

                lat_mask = (lat_vals >= miny) & (lat_vals <= maxy)
                lon_mask = (lon_vals >= minx) & (lon_vals <= maxx)

                lat_subset = lat_vals[lat_mask]
                lon_subset = lon_vals[lon_mask]

                if lat_subset.size == 0 or lon_subset.size == 0:
                    latest[output_key] = 0
                    continue

                sub_var = var_values[np.ix_(lat_mask, lon_mask)]
                sub_lon, sub_lat = np.meshgrid(lon_subset, lat_subset)

                if sub_var.size == 0:
                    latest[output_key] = 0
                    continue

                inside = sv.contains(poly, sub_lon, sub_lat)

                masked_vals = sub_var[inside]
                masked_vals = masked_vals[masked_vals >= 0]

                if masked_vals.size == 0:
                    latest[output_key] = 0
                else:
                    latest[output_key] = float(np.nanmax(masked_vals))

            except Exception as e:
                self.io_manager.write_error(f"Process cell {cell.get('id')}: {e}")
                latest[output_key] = "PROCESSING_ERROR"

        if render_config:
            try:
                # ds is already open
                ts_str = TransformUtils.find_timestamp(dataset_path)
                renderer = GUILayerRenderer(ds, render_config['outdir'], render_config['colormap_key'], render_config['file_name'], ts_str)
                renderer.convert_to_png()
                self.io_manager.write_debug(f"Rendered {render_config['file_name']} successfully")
            except Exception as e:
                self.io_manager.write_error(f"Failed to render {render_config['file_name']}: {e}")

        ds.close()
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
