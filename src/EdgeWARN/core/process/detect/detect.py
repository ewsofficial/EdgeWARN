from EdgeWARN.core.process.detect.tools.utils import DetectionDataHandler
from EdgeWARN.core.process.detect.tools.gatemapper import GateMapper
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from util.io import IOManager
import util.file as fs
import gc


def detect_cells(
    radar_path,
    ps_path,
    preciptype_path,
    io_manager,
    lat_min,
    lat_max,
    lon_min,
    lon_max,
    *,
    return_probsevere=False,
    vil_path=None,  # NEW
    et_path=None,   # NEW
):
    handler = DetectionDataHandler(
        radar_path,
        ps_path,
        preciptype_path,
        io_manager,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
    )

    # Use load_subset directly to avoid loading full metadata if possible/cleaner
    radar_ds = handler.load_subset()
    
    if radar_ds is None:
        io_manager.write_error(f"Failed to load/subset radar data from {radar_path}")
        if return_probsevere:
            return [], None
        return []

    ps_ds = handler.load_probsevere()
    
    # === Consolidate Loading of Physics Grids (MorphoWind) ===
    # We load them here so they are available for scalar extraction
    vil_ds = None
    if vil_path:
        try:
            vil_ds = handler.load_vil(vil_path)
            io_manager.write_debug(f"Loaded VIL Density from {vil_path}")
        except Exception as e:
            io_manager.write_warning(f"Failed to load VIL: {e}")

    et_ds = None
    if et_path:
        try:
            et_ds = handler.load_echotop(et_path)
            io_manager.write_debug(f"Loaded EchoTop form {et_path}")
        except Exception as e:
            io_manager.write_warning(f"Failed to load EchoTop: {e}")


    mapper = GateMapper(radar_ds, ps_ds, io_manager, refl_threshold=40.0)
    mapped_ds = mapper.map_gates_to_polygons()
    expanded_ds = mapper.expand_gates(mapped_ds)
    bboxes = mapper.draw_bbox(expanded_ds, step=8)

    # === Load PrecipType now, just before saving ===
    preciptype_ds = handler.load_preciptype()
    if preciptype_ds is None:
         io_manager.write_warning("Failed to load precipitation type data, hail core detection will be disabled")

    saver = CellDataSaver(
        bboxes,
        radar_ds,
        mapped_ds,
        expanded_ds,
        ps_ds,
        preciptype_ds,
    )

    # Pass physics grids to create_entry for scalar extraction
    entries = saver.create_entry(vil_ds=vil_ds, et_ds=et_ds)


    if return_probsevere:
        return entries, ps_ds

    return entries


if __name__ == "__main__":
    radar_path = fs.latest_files(fs.MRMS_COMPOSITE_DIR, 1)[-1]
    ps_path = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1]
    pt_path = fs.latest_files(fs.MRMS_PRECIPTYP_DIR, 1)[-1]
    io_manager = IOManager("[CellDetection]")
    lat_min, lat_max = 35.0, 38.0
    lon_min, lon_max = 283.0, 285.0

    detect_cells(radar_path, ps_path, pt_path, io_manager, lat_min, lat_max, lon_min, lon_max)
