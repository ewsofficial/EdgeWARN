from EdgeWARN.core.process.detect.tools.utils import DetectionDataHandler
from EdgeWARN.core.process.detect.tools.gatemapper import GateMapper
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from util.io import IOManager
import util.file as fs
import gc
from util.performance import tracker as perf_tracker


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
    radar_obj=None,
    ps_obj=None,
    preciptype_obj=None,
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
        radar_obj=radar_obj,
        ps_obj=ps_obj,
        preciptype_obj=preciptype_obj,
    )

    # Use load_subset directly to avoid loading full metadata if possible/cleaner
    perf_tracker.start("Detection - Load Radar")
    radar_ds = handler.load_subset()
    perf_tracker.stop("Detection - Load Radar")
    
    if radar_ds is None:
        io_manager.write_error(f"Failed to load/subset radar data from {radar_path}")
        if return_probsevere:
            return [], None
        return []

    perf_tracker.start("Detection - Load ProbSevere")
    ps_ds = handler.load_probsevere()
    perf_tracker.stop("Detection - Load ProbSevere")

    # Load PrecipType early for discrimination logic
    perf_tracker.start("Detection - Load PrecipType")
    preciptype_ds = handler.load_preciptype()
    perf_tracker.stop("Detection - Load PrecipType")

    if preciptype_ds is None:
         io_manager.write_warning("Failed to load precipitation type data, stratiform discrimination will be limited")

    mapper = GateMapper(radar_ds, ps_ds, io_manager, refl_threshold=37.5, min_seed_percentage=0.001)
    
    perf_tracker.start("Detection - Map Gates")
    mapped_ds = mapper.map_gates_to_polygons()
    perf_tracker.stop("Detection - Map Gates")
    
    perf_tracker.start("Detection - Expand Gates")
    expanded_ds = mapper.expand_gates(mapped_ds)
    perf_tracker.stop("Detection - Expand Gates")

    perf_tracker.start("Detection - BBox")
    bboxes = mapper.draw_bbox(expanded_ds, step=8)
    perf_tracker.stop("Detection - BBox")

    saver = CellDataSaver(
        bboxes,
        radar_ds,
        mapped_ds,
        expanded_ds,
        ps_ds,
        preciptype_ds,
    )

    # Pass physics grids to create_entry for scalar extraction
    perf_tracker.start("Detection - Create Entry")
    entries = saver.create_entry()
    perf_tracker.stop("Detection - Create Entry")


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
