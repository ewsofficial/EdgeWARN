import util.core.file as fs
from datetime import datetime
from EdgeWARN.PreProcess.CellIntegration.utils import StormIntegrationUtils, StatFileHandler
from EdgeWARN.PreProcess.CellIntegration.integrator import StormCellIntegrator

def main():
    """
    Main function to run integration of all data types with storm cells.
    """
    handler = StatFileHandler()
    
    # Load storm cells
    storm_json_path = "stormcell_test.json"
    print(f"Loading storm cells from {storm_json_path}")
    cells = handler.load_json(storm_json_path)
    if cells is None:
        print("No storm cells loaded; aborting.")
        return
    
    integrator = StormCellIntegrator()
    result_cells = cells
    
    # 1. Integrate NLDN lightning data
    print("\n" + "="*50)
    print("INTEGRATING NLDN LIGHTNING DATA")
    print("="*50)
    nldn_list = fs.latest_files(fs.MRMS_NLDN_DIR, 1)
    if nldn_list:
        nldn_path = nldn_list[-1]
        print(f"Using NLDN file: {nldn_path}")
        
        nldn_timestamp = handler.find_timestamp(nldn_path)
        if nldn_timestamp:
            try:
                # FIXED: Pass filepath instead of loaded dataset
                result_cells = integrator.integrate_ds(nldn_path, result_cells, nldn_timestamp, "flash_rate")
                print("NLDN integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate NLDN data: {e}")
        else:
            print("Could not determine timestamp from NLDN file")
    else:
        print("No NLDN files found")
    
    # 2. Integrate EchoTop data
    print("\n" + "="*50)
    print("INTEGRATING ECHOTOP DATA")
    print("="*50)
    echotop_list = fs.latest_files(fs.MRMS_ECHOTOP18_DIR, 1)
    if echotop_list:
        echotop_path = echotop_list[-1]
        print(f"Using EchoTop file: {echotop_path}")
        
        echotop_timestamp = handler.find_timestamp(echotop_path)
        if echotop_timestamp:
            try:
                # FIXED: Pass filepath instead of loaded dataset
                result_cells = integrator.integrate_ds(echotop_path, result_cells, echotop_timestamp, "echotop_km")
                print("EchoTop integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate EchoTop data: {e}")
        else:
            print("Could not determine timestamp from EchoTop file")
    else:
        print("No EchoTop files found")
    
    # 3. Integrate ProbSevere data
    print("\n" + "="*50)
    print("INTEGRATING PROBSEVERE DATA")
    print("="*50)
    probsevere_list = fs.latest_files(fs.MRMS_PROBSEVERE_DIR, 1)
    if probsevere_list:
        probsevere_path = probsevere_list[-1]
        print(f"Using ProbSevere file: {probsevere_path}")
        
        # Load raw ProbSevere JSON
        probsevere_data = handler.load_json(probsevere_path)
        if probsevere_data:
            # Extract timestamp from ProbSevere data
            probsevere_timestamp_str = probsevere_data.get('validTime', '').replace('_', ' ')
            try:
                probsevere_timestamp = datetime.strptime(probsevere_timestamp_str, "%Y%m%d %H%M%S UTC")
                # Pass the full JSON dict instead of just features
                result_cells = integrator.integrate_probsevere(probsevere_data, result_cells, probsevere_timestamp)
                print("ProbSevere integration completed successfully")
                
                # Create visualization of ProbSevere and storm cell polygons
                print("\nCreating visualization of ProbSevere and storm cell polygons...")
                # graph_probsevere_stormcells(probsevere_data, result_cells)
                
            except ValueError:
                print(f"Could not parse timestamp from ProbSevere data: {probsevere_timestamp_str}")
        else:
            print("Failed to load ProbSevere JSON data")
    else:
        print("No ProbSevere files found")
    
    # 4. Integrate MRMS PrecipRate data
    print("\n" + "="*50)
    print("INTEGRATING MRMS PRECIPRATE DATA")
    print("="*50)
    preciprate_list = fs.latest_files(fs.MRMS_PRECIPRATE_DIR, 1)
    if preciprate_list:
        preciprate_path = preciprate_list[-1]
        print(f"Using PrecipRate file: {preciprate_path}")
        
        preciprate_timestamp = handler.find_timestamp(preciprate_path)
        if preciprate_timestamp:
            try:
                # FIXED: Pass filepath instead of loaded dataset
                result_cells = integrator.integrate_ds(preciprate_path, result_cells, preciprate_timestamp, "preciprate")
                print("MRMS PrecipRate integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate PrecipRate data: {e}")
        else:
            print("Could not determine timestamp from PrecipRate file")
    else:
        print("No PrecipRate files found")

    # 5. Integrate MRMS VIL Density data
    print("\n" + "="*50)
    print("INTEGRATING MRMS VIL DENSITY DATA")
    print("="*50)
    vil_density_list = fs.latest_files(fs.MRMS_VIL_DIR, 1)
    if vil_density_list:
        vil_density_path = vil_density_list[-1]
        print(f"Using VIL Density file: {vil_density_path}")
        
        vil_density_timestamp = handler.find_timestamp(vil_density_path)
        if vil_density_timestamp:
            try:
                # FIXED: Pass filepath instead of loaded dataset
                result_cells = integrator.integrate_ds(vil_density_path, result_cells, vil_density_timestamp, "vil_density")
                print("MRMS VIL Density integration completed successfully")
            except Exception as e:
                print(f"Failed to integrate VIL Density data: {e}")
        else:
            print("Could not determine timestamp from VIL Density file")
    else:
        print("No VIL Density files found")
    
    # Save final integrated results
    output_json = storm_json_path.replace('.json', '_fully_integrated.json')
    print(f"\nSaving fully integrated results to {output_json}")
    handler.write_json(result_cells, output_json)
    
    # Print comprehensive summary
    print("\n" + "="*60)
    print("INTEGRATION SUMMARY")
    print("="*60)
    
    # Update these variable declarations:
    cells_with_nldn = 0
    cells_with_echotop = 0
    cells_with_probsevere = 0
    cells_with_preciprate = 0
    cells_with_vil_density = 0 
    total_entries = 0

    # Count number of cells with valid data
    for cell in result_cells:
        if 'storm_history' in cell:
            for entry in cell['storm_history']:
                total_entries += 1
                if isinstance(entry.get('flash_rate', "N/A"), (int, float)):
                    cells_with_nldn += 1
                if isinstance(entry.get('echotop_km', "N/A"), (int, float)):
                    cells_with_echotop += 1
                if isinstance(entry.get('prob_severe', "N/A"), (int, float)):
                    cells_with_probsevere += 1
                if isinstance(entry.get('preciprate', "N/A"), (int, float)):
                    cells_with_preciprate += 1
                if isinstance(entry.get('vil_density', "N/A"), (int, float)): 
                    cells_with_vil_density += 1

    # Print Summary
    print(f"Entries with NLDN data: {cells_with_nldn}/{total_entries}")
    print(f"Entries with EchoTop data: {cells_with_echotop}/{total_entries}")
    print(f"Entries with ProbSevere data: {cells_with_probsevere}/{total_entries}")
    print(f"Entries with PrecipRate data: {cells_with_preciprate}/{total_entries}")
    print(f"Entries with VIL Density data: {cells_with_vil_density}/{total_entries}") 

if __name__ == "__main__":
    main()
