import json
from collections import defaultdict

import util.file as fs
from util.io import IOManager
from util.performance import tracker as perf_tracker

from EdgeWARN.process.detect.tools.save import CellDataSaver
from EdgeWARN.process.integrate.config import get_datasets_config
from EdgeWARN.process.integrate.integrate import StormCellIntegrator
from EdgeWARN.process.integrate.integrate_glm import integrate_glm
from EdgeWARN.process.integrate.integrate_rap import integrate_rap
from EdgeWARN.process.integrate.utils import StatFileHandler

io_manager = IOManager("[CellIntegration]")


def _run_step(step_name, action):
    try:
        perf_tracker.start(step_name)
        result = action()
        perf_tracker.stop(step_name)
        return result
    except Exception:
        if step_name in perf_tracker.active_timers:
            perf_tracker.stop(step_name)
        raise


def _integrate_dataset_groups(integrator, cells):
    grouped_configs = defaultdict(list)
    for config in get_datasets_config():
        grouped_configs[config["filepath"]].append(config)

    result_cells = cells
    for filepath, group_list in grouped_configs.items():
        name_list = [c["name"] for c in group_list]
        name_str = ", ".join(name_list)

        try:
            latest_files = fs.latest_files(filepath, 1)
            if not latest_files:
                io_manager.write_warning(f"No files found for {name_str} at {filepath}, skipping")
                continue

            latest_file = latest_files[-1]
            io_manager.write_debug(f"Using latest file for {name_str}: {latest_file}")

            result_cells = _run_step(
                f"Integration - {name_str}",
                lambda: integrator.integrate_multi_stats(latest_file, result_cells, group_list),
            )
            io_manager.write_debug(f"Integration completed for {name_str}")

        except Exception as e:
            io_manager.write_error(f"Failed to integrate {name_str} data: {e}")

    return result_cells


def _integrate_azshear(integrator, cells):
    try:
        latest_low_files = fs.latest_files(fs.MRMS_AZSHEARLOW_DIR, 1)
        latest_mid_files = fs.latest_files(fs.MRMS_AZSHEARMID_DIR, 1)
        if latest_low_files and latest_mid_files:
            io_manager.write_info(f"Integrating AzShear support features for {len(cells)} cells")
            return _run_step(
                "Integration - AzShear Features",
                lambda: integrator.integrate_azshear_features(
                    latest_low_files[-1],
                    latest_mid_files[-1],
                    cells,
                ),
            )

        io_manager.write_warning("AzShear feature extraction skipped due to missing low/mid AzShear files")
    except Exception as e:
        io_manager.write_error(f"Failed to integrate AzShear support features: {e}")

    return cells


def _integrate_probsevere(integrator, cells):
    try:
        latest_files = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)
        if not latest_files:
            io_manager.write_warning("No ProbSevere files found, skipping ProbSevere integration")
            return cells

        latest_file = latest_files[-1]
        with open(latest_file, "r") as f:
            probsevere_data = json.load(f)
        io_manager.write_debug(f"Using latest ProbSevere file: {latest_file}")

        cells = _run_step(
            "Integration - ProbSevere",
            lambda: integrator.integrate_probsevere(probsevere_data, cells),
        )
        io_manager.write_debug("Successfully integrated ProbSevere data")
    except Exception as e:
        io_manager.write_error(f"Failed to integrate ProbSevere data: {e}")

    return cells


def _integrate_glm(cells):
    try:
        io_manager.write_info(f"Integrating GLM data for {len(cells)} cells")
        latest_glm_files = fs.latest_files(fs.GOES_GLM_DIR, 1)
        if latest_glm_files:
            latest_file = latest_glm_files[-1]
            io_manager.write_debug(f"Using latest GLM file: {latest_file}")
            cells = _run_step("Integration - GLM", lambda: integrate_glm(cells, latest_file))
            io_manager.write_debug("Successfully integrated GLM data")
        else:
            io_manager.write_warning("No GLM files found, skipping GLM integration")

    except Exception as e:
        io_manager.write_error(f"Failed to integrate GLM data: {e}")

    return cells


def _integrate_rap(cells):
    try:
        io_manager.write_info(f"Integrating RAP data for {len(cells)} cells")
        latest_rap_files = fs.latest_files(fs.RAP_DIR, 1)
        if latest_rap_files:
            latest_file = latest_rap_files[-1]
            io_manager.write_debug(f"Using latest RAP file: {latest_file}")
            cells = _run_step("Integration - RAP", lambda: integrate_rap(cells, latest_file, io_manager))
            io_manager.write_debug("Successfully integrated RAP data")
        else:
            io_manager.write_warning("No RAP files found, skipping RAP integration")
    except Exception as e:
        io_manager.write_error(f"Failed to integrate RAP data: {e}")

    return cells


def _run_ctam_if_enabled(cells, timestamp, disable_ctam):
    if disable_ctam:
        io_manager.write_info("CTAM module execution disabled via command-line flag")
        return cells

    try:
        from EdgeWARN.ctam.run import run_ctam

        io_manager.write_info(f"Running CTAM modules for {len(cells)} cells")
        cells = _run_step("Integration - CTAM", lambda: run_ctam(cells, timestamp=timestamp))
        io_manager.write_debug("CTAM module execution completed successfully")
    except Exception as e:
        io_manager.write_error(f"Failed to run CTAM modules: {e}")

    return cells


def _save_cells(handler, timestamp, cells, json_path):
    data = CellDataSaver(None, None, None, None, None, None).create_json_structure(timestamp, cells)
    handler.write_json(data, json_path)


def _update_history(cells, timestamp):
    try:
        from .history import CellHistoryManager

        history_manager = CellHistoryManager(io_manager)
        _run_step("Integration - History", lambda: history_manager.update_cell_histories(cells, timestamp))
    except Exception as e:
        io_manager.write_error(f"Failed to update cell history: {e}")


def _update_api_indexes(cells, remove_old_cells):
    try:
        from EdgeWARN.api_integration.index_manager import APIIndexManager

        api_index = APIIndexManager(io_manager, remove_old_cells=remove_old_cells)

        active_cell_ids = [cell["id"] for cell in cells if "timestamp" in cell]

        def _update():
            api_index.update_cell_index(active_cell_ids)
            api_index.cleanup_inactive_cells()

        _run_step("Integration - API Index", _update)
    except Exception as e:
        io_manager.write_error(f"Failed to update API indexes: {e}")


def main(json_path=None, remove_old_cells=True, disable_ctam=False):
    handler = StatFileHandler(io_manager)
    integrator = StormCellIntegrator(io_manager)

    if json_path is None:
        raise ValueError("json_path must be provided to integration.main")

    cells, timestamp = handler.load_json(json_path)
    result_cells = cells

    result_cells = _integrate_dataset_groups(integrator, result_cells)
    result_cells = _integrate_azshear(integrator, result_cells)
    result_cells = _integrate_probsevere(integrator, result_cells)
    result_cells = _integrate_glm(result_cells)
    result_cells = _integrate_rap(result_cells)
    result_cells = _run_ctam_if_enabled(result_cells, timestamp, disable_ctam)

    _run_step("Integration - Save", lambda: _save_cells(handler, timestamp, result_cells, json_path))
    _update_history(result_cells, timestamp)
    _update_api_indexes(result_cells, remove_old_cells)
