import numpy as np


class RAPFileHandler:
    """Handles RAP GRIB2 files specifically using cfgrib.open_datasets."""

    def __init__(self, io_manager):
        self.io_manager = io_manager

    def get_isobaric_dataset(self, filepath):
        """Finds the dataset containing isobaricInhPa levels with u and v."""
        try:
            import cfgrib

            self.io_manager.write_debug(f"Opening RAP file {filepath} with isobaric filter")

            try:
                datasets = cfgrib.open_datasets(
                    filepath,
                    filter_by_keys={"typeOfLevel": "isobaricInhPa"},
                )

                self.io_manager.write_debug(f"Found {len(datasets)} datasets with isobaricInhPa type")

                for ds in datasets:
                    if "u" in ds.data_vars and "v" in ds.data_vars and "isobaricInhPa" in ds.coords:
                        return ds

                self.io_manager.write_debug("Filtered approach didn't find suitable dataset, trying general approach")

            except Exception as filter_error:
                self.io_manager.write_debug(f"Filtered approach failed: {filter_error}, trying general approach")

            datasets = cfgrib.open_datasets(filepath)
            self.io_manager.write_debug(f"Scanning all {len(datasets)} datasets as fallback")

            best_dataset = None
            best_score = 0

            for ds in datasets:
                if "isobaricInhPa" not in ds.coords:
                    continue

                u_var = None
                v_var = None

                for var in ds.data_vars:
                    if u_var is None and var in ["u", "UGRD", "u-component_of_wind_isobaric", "wind_u"]:
                        u_var = var
                    elif v_var is None and var in ["v", "VGRD", "v-component_of_wind_isobaric", "wind_v"]:
                        v_var = var

                score = 0
                if u_var is not None:
                    score += 1
                if v_var is not None:
                    score += 1

                try:
                    levels = ds.isobaricInhPa.values
                    target_levels = [850, 700, 500, 250]
                    available_target_levels = [l for l in target_levels if l in levels]
                    score += len(available_target_levels) / 4.0
                except Exception:
                    pass

                if score > best_score and u_var is not None and v_var is not None:
                    best_score = score
                    best_dataset = ds

            if best_dataset is not None:
                self.io_manager.write_debug(f"Selected fallback dataset with score {best_score}")
                return best_dataset

            self.io_manager.write_error(f"Could not find suitable isobaric U/V dataset in {filepath}")
            return None

        except Exception as e:
            self.io_manager.write_error(f"Error opening RAP file {filepath}: {e}")
            import traceback

            self.io_manager.write_error(traceback.format_exc())
            return None
