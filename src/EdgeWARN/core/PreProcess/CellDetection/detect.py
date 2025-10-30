from EdgeWARN.core.PreProcess.CellDetection.tools.utils import DetectionDataHandler
from EdgeWARN.core.PreProcess.CellDetection.tools.gatemapper import GateMapper
from EdgeWARN.core.PreProcess.CellDetection.tools.save import CellDataSaver
import util.file as fs

def detect_cells(radar_path, ps_path, precipflag_path, lat_min, lat_max, lon_min, lon_max):
    handler = DetectionDataHandler(
        radar_path,
        ps_path,
        lat_min, lat_max,
        lon_min, lon_max
    )

    radar_ds = handler.load_subset()
    ps_ds = handler.load_probsevere()

    mapper = GateMapper(radar_ds, ps_ds, None, refl_threshold=40.0)
    mapped_ds = mapper.map_gates_to_polygons()
    expanded_ds = mapper.expand_gates(mapped_ds)
    bboxes = mapper.draw_bbox(expanded_ds, step=8)

    saver = CellDataSaver(
        bboxes,
        radar_path, radar_ds, mapped_ds,
        ps_path, ps_ds
    )

    saver.create_entry()
    entries = saver.create_entry()
    entries = saver.append_storm_history(entries, radar_path)

    return entries

if __name__ == "__main__":
    radar_path = fs.latest_files(fs.MRMS_3D_DIR, 1)[-1]
    ps_path = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1]
    lat_min, lat_max = 35.0, 38.0
    lon_min, lon_max = 283.0, 285.0
    
    detect_cells(radar_path, ps_path, None, lat_min, lat_max, lon_min, lon_max)