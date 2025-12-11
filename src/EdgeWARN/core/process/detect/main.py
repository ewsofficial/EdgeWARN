from EdgeWARN.core.process.detect.tools.utils import DetectionDataHandler
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from EdgeWARN.core.process.detect.tools.vecmath import StormVectorCalculator
from EdgeWARN.core.process.detect.track import StormCellTracker
from EdgeWARN.core.process.detect.detect import detect_cells
from util.io import IOManager
import util.file as fs
import json as js
from datetime import datetime

io_manager = IOManager("[CellDetection]")


def _detect_with_optional_probsevere(
    radar_path,
    ps_path,
    pt_path,
    lat_min,
    lat_max,
    lon_min,
    lon_max,
    need_probsevere,
    render_files=True,
):
    if need_probsevere:
        return detect_cells(
            radar_path,
            ps_path,
            pt_path,
            io_manager,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            return_probsevere=True,
            render=render_files,
        )

    return detect_cells(
        radar_path,
        ps_path,
        pt_path,
        io_manager,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        render=render_files,
    ), None


def main(radar_old, radar_new, ps_old, ps_new, pt_old, pt_new, lat_bounds: tuple, lon_bounds: tuple, json_output, render_files=True):
    lat_min, lat_max = lat_bounds
    lon_min, lon_max = lon_bounds

    # === Single-frame fallback ===
    single_frame = radar_new is None or ps_new is None or pt_new is None
    if single_frame:
        io_manager.write_debug("No new scan specified — running single-frame detection mode")

    ps_old_data = None

    # === Load or create previous entries ===
    if json_output.exists() and json_output.stat().st_size > 0:
        try:
            with open(json_output, 'r') as f:
                data_old = js.load(f)

            if isinstance(data_old, dict) and "features" in data_old:
                entries_old = data_old["features"]
            elif isinstance(data_old, list):
                entries_old = data_old
            else:
                raise ValueError("Invalid JSON structure")

            if not entries_old:
                raise ValueError("Empty features list")

            io_manager.write_debug(f"Loaded {len(entries_old)} cells from {json_output}")
        except (js.JSONDecodeError, KeyError, IndexError, ValueError) as e:
            io_manager.write_error(f"Failed to load existing data: {e}. Redetecting from old scan ...")
            entries_old, ps_old_data = _detect_with_optional_probsevere(
                radar_old,
                ps_old,
                pt_old,
                lat_min,
                lat_max,
                lon_min,
                lon_max,
                need_probsevere=not single_frame,
                render_files=render_files,
            )
            io_manager.write_debug(f"Detected {len(entries_old)} cells in old scan.")
    else:
        io_manager.write_info("JSON output doesn't exist, detecting from old scan ...")
        entries_old, ps_old_data = _detect_with_optional_probsevere(
            radar_old,
            ps_old,
            pt_old,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            need_probsevere=not single_frame,
            render_files=render_files,
        )
        io_manager.write_debug(f"Detected {len(entries_old)} cells in old scan.")

    # === If single-frame mode, just update/save ===
    if single_frame:
        io_manager.write_info("Saving single-frame detection results (no tracking possible).")
        saver = CellDataSaver(None, radar_old, None, None, ps_old, None)
        entries = saver.append_storm_history(entries_old, radar_old)
        entries = StormVectorCalculator.calculate_vectors(entries)

        ts_str = DetectionDataHandler.find_timestamp(radar_old)
        try:
            dt = datetime.fromisoformat(ts_str)
            dt = dt.replace(second=0, microsecond=0)
            final_ts = dt.isoformat()
        except ValueError:
            final_ts = ts_str

        output_data = saver.create_json_structure(final_ts, entries)
        with open(json_output, 'w') as f:
            js.dump(output_data, f, indent=2, default=str)
        return

    # === Dual-frame mode ===
    io_manager.write_debug("Detecting cells in new scan ...")
    entries_new, ps_new_data = _detect_with_optional_probsevere(
        radar_new,
        ps_new,
        pt_new,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        need_probsevere=True,
        render_files=render_files,
    )
    io_manager.write_debug(f"Detected {len(entries_new)} cells in new scan")

    io_manager.write_info("Matching and updating cell data")
    if ps_old_data is None:
        ps_old_data = DetectionDataHandler(
            radar_old,
            ps_old,
            pt_old,
            io_manager,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
        ).load_probsevere()

    tracker = StormCellTracker(ps_old_data, ps_new_data, io_manager)
    saver = CellDataSaver(None, radar_new, None, None, ps_new_data, None)
    entries = tracker.update_cells(entries_old, entries_new)
    entries = saver.append_storm_history(entries, radar_new)
    entries = StormVectorCalculator.calculate_vectors(entries)

    ts_str = DetectionDataHandler.find_timestamp(radar_new)
    try:
        dt = datetime.fromisoformat(ts_str)
        dt = dt.replace(second=0, microsecond=0)
        final_ts = dt.isoformat()
    except ValueError:
        final_ts = ts_str

    output_data = saver.create_json_structure(final_ts, entries)
    with open(json_output, 'w') as f:
        js.dump(output_data, f, indent=2, default=str)