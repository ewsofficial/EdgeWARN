from EdgeWARN.core.process.detect.tools.utils import DetectionDataHandler
from EdgeWARN.core.gui_pipelines.transform.render import GUILayerRenderer
from EdgeWARN.core.process.detect.tools.gatemapper import GateMapper
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from util.io import IOManager
import util.file as fs


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
    render=True,
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

    radar_ds = handler.load_radar_full()
    ps_ds = handler.load_probsevere()
    preciptype_ds = handler.load_preciptype()

    mapper = GateMapper(radar_ds, ps_ds, io_manager, refl_threshold=40.0)
    mapped_ds = mapper.map_gates_to_polygons()
    expanded_ds = mapper.expand_gates(mapped_ds)
    bboxes = mapper.draw_bbox(expanded_ds, step=8)

    saver = CellDataSaver(
        bboxes,
        radar_ds,
        mapped_ds,
        expanded_ds,
        ps_ds,
        preciptype_ds,
    )

    entries = saver.create_entry()
    entries = saver.append_storm_history(entries, radar_path)

    # Render Composite Reflectivity
    if render:
        try:
            ts_str = DetectionDataHandler.find_timestamp(radar_path)
            renderer = GUILayerRenderer(radar_ds_full, fs.GUI_COMPOSITE_DIR, "NWS_Reflectivity", "MRMS_MergedReflectivityQC", ts_str)
            renderer.convert_to_png()
            io_manager.write_debug("Rendered MRMS_MergedReflectivityQC successfully")
        except Exception as e:
            io_manager.write_error(f"Failed to render MRMS_MergedReflectivityQC: {e}")

    radar_ds = handler.subset_radar(radar_ds)

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
