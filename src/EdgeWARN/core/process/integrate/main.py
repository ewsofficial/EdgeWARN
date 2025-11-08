import util.file as fs
from EdgeWARN.core.process.integrate.integrate import StormCellIntegrator
from EdgeWARN.core.process.integrate.utils import StatFileHandler
from util.io import IOManager

# ------------------------------
# MRMS dataset list
# ------------------------------
datasets = [
    ("NLDN", fs.MRMS_NLDN_DIR, "CGFlashDensity"),
    ("EchoTop18", fs.MRMS_ECHOTOP18_DIR, "EchoTop18"),
    ("EchoTop30", fs.MRMS_ECHOTOP30_DIR, "EchoTop30"),
    ("PrecipRate", fs.MRMS_PRECIPRATE_DIR, "PrecipRate"),
    ("VIL Density", fs.MRMS_VIL_DIR, "VILDensity"),
    ("Reflectivity at Lowest Altitude", fs.MRMS_RALA_DIR, "RALA"),
    ("VII", fs.MRMS_VII_DIR, "VII")
]

io_manager = IOManager("[CellIntegration]")

def main():
    handler = StatFileHandler(io_manager)
    integrator = StormCellIntegrator(io_manager)
    json_path = "stormcell_test.json"
    cells = handler.load_json(json_path)

    result_cells = cells

    # Integrate datasets
    for name, outdir, key in datasets:
        try:
            io_manager.write_debug(f"Integrating {name} data for {len(cells)} cells")
            latest_file = fs.latest_files(outdir, 1)[-1]
            io_manager.write_debug(f"Using latest {name} file: {latest_file}")

            result_cells = integrator.integrate_ds_via_max(latest_file, result_cells, key)
            io_manager.write_debug(f"{name} integration completed successfully!")
        
        except Exception as e:
            io_manager.write_error(f"Failed to integrate {name} data: {e}")

    # Integrate ProbSevere
    try:
        io_manager.write_debug(f"Integrating ProbSevere data for {len(cells)} cells")
        latest_file = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)[-1]
        probsevere_data = handler.load_json(latest_file)
        io_manager.write_debug(f"Using latest ProbSevere file: {latest_file}")

        result_cells = integrator.integrate_probsevere(probsevere_data, result_cells)
        io_manager.write_debug(f"Successfully integrated ProbSevere data")
    
    except Exception as e:
        io_manager.write_error(f"Failed to integrate ProbSevere data: {e}")
    
    # Save data
    io_manager.write_debug(f"Saving final data to {json_path}")
    handler.write_json(result_cells, json_path)

if __name__ == "__main__":
    main()

