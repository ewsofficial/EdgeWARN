import util.file as fs
from EdgeWARN.core.process.integrate.integrate import StormCellIntegrator
from EdgeWARN.core.process.integrate.integrate_glm import integrate_glm
from EdgeWARN.core.process.integrate.integrate_rap import integrate_rap_winds
from EdgeWARN.core.process.integrate.utils import StatFileHandler
from EdgeWARN.core.process.detect.tools.save import CellDataSaver
from util.io import IOManager
import json
from EdgeWARN.core.process.integrate.config import get_datasets_config
io_manager = IOManager("[CellIntegration]")

def main(json_path=None, remove_old_cells=True):
    handler = StatFileHandler(io_manager)
    integrator = StormCellIntegrator(io_manager)
    
    if json_path is None:
        raise ValueError("json_path must be provided to integration.main")
    
    # Check if json_path is a file or just a string, handle strictly as requested
    # The new pipeline passes a Path object.
    
    cells, timestamp = handler.load_json(json_path) # _ is latest_timestamp, we don't need this

    result_cells = cells

    # Integrate datasets
    for dataset_config in get_datasets_config():
        name = dataset_config["name"]
        outdir = dataset_config["filepath"]
        key = dataset_config["key"]
        
        try:
            latest_file = fs.latest_files(outdir, 1)[-1]
            io_manager.write_debug(f"Using latest {name} file: {latest_file}")

            method = dataset_config.get("method", "max")
            percentile = dataset_config.get("percentile", 90)

            result_cells = integrator.integrate_ds_by_percentile(
                latest_file, 
                result_cells, 
                key, 
                method=method, 
                percentile=percentile
            )
            io_manager.write_debug(f"{name} integration completed successfully!")
        
        except Exception as e:
            io_manager.write_error(f"Failed to integrate {name} data: {e}")

    # Integrate ProbSevere
    try:
        io_manager.write_info(f"Integrating ProbSevere data for {len(cells)} cells")
        latest_file = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1]
        with open(latest_file, 'r') as f:
            probsevere_data = json.load(f)
        io_manager.write_debug(f"Using latest ProbSevere file: {latest_file}")

        result_cells = integrator.integrate_probsevere(probsevere_data, result_cells)
        io_manager.write_debug(f"Successfully integrated ProbSevere data")
    
    except Exception as e:
        io_manager.write_error(f"Failed to integrate ProbSevere data: {e}")
    
    # Integrate GLM
    try:
        io_manager.write_info(f"Integrating GLM data for {len(result_cells)} cells")
        latest_glm_files = fs.latest_files(fs.GOES_GLM_DIR, 1)
        if latest_glm_files:
            latest_file = latest_glm_files[-1]
            io_manager.write_debug(f"Using latest GLM file: {latest_file}")
            result_cells = integrate_glm(result_cells, latest_file)
            io_manager.write_debug(f"Successfully integrated GLM data")
        else:
            io_manager.write_warning("No GLM files found, skipping GLM integration")

    except Exception as e:
        io_manager.write_error(f"Failed to integrate GLM data: {e}")
    
    # Integrate RAP Winds
    try:
        io_manager.write_info(f"Integrating RAP wind data for {len(result_cells)} cells")
        latest_rap_files = fs.latest_files(fs.RAP_DIR, 1)
        if latest_rap_files:
            latest_file = latest_rap_files[-1]
            io_manager.write_debug(f"Using latest RAP file: {latest_file}")
            result_cells = integrate_rap_winds(result_cells, latest_file, io_manager)
            io_manager.write_debug(f"Successfully integrated RAP wind data")
        else:
            io_manager.write_warning("No RAP files found, skipping RAP integration")
    except Exception as e:
        io_manager.write_error(f"Failed to integrate RAP data: {e}")
    
    # Run CTAM modules (StormCast, etc.)
    try:
        from EdgeWARN.core.ctam.run import run_ctam
        io_manager.write_info(f"Running CTAM modules for {len(result_cells)} cells")
        result_cells = run_ctam(result_cells)
        io_manager.write_debug("CTAM module execution completed successfully")
    except Exception as e:
        io_manager.write_error(f"Failed to run CTAM modules: {e}")
    
    # Save data
    data = CellDataSaver(None, None, None, None, None, None).create_json_structure(timestamp, result_cells)
    handler.write_json(data, json_path)

    # Update per-cell history
    try:
        from .history import CellHistoryManager
        history_manager = CellHistoryManager(io_manager)
        history_manager.update_cell_histories(result_cells, timestamp)
    except Exception as e:
        io_manager.write_error(f"Failed to update cell history: {e}")
    
    # Update API indexes
    try:
        from EdgeWARN.core.api_integration.index_manager import APIIndexManager
        api_index = APIIndexManager(io_manager, remove_old_cells=remove_old_cells)
        
        # Update cell index with active cells (those that have timestamps)
        active_cell_ids = [cell["id"] for cell in result_cells if "timestamp" in cell]
        api_index.update_cell_index(active_cell_ids)
        
        # Cleanup inactive cells from index
        api_index.cleanup_inactive_cells()
    except Exception as e:
        io_manager.write_error(f"Failed to update API indexes: {e}")