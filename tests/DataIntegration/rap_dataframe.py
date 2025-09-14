import subprocess
import re

def list_grib_variables_cli_simple(file_path):
    """Use wgrib2 command-line tool to list all variables."""
    
    try:
        # Try wgrib2 first (more reliable for complex GRIB files)
        result = subprocess.run(['wgrib2', file_path, '-s'], 
                              capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("Variables from wgrib2:")
            print("=" * 50)
            
            # Extract variable names (short names)
            variables = set()
            for line in result.stdout.split('\n'):
                if ':' in line:
                    parts = line.split(':')
                    if len(parts) > 2:
                        var_name = parts[2].strip()
                        variables.add(var_name)
            
            print(f"All variables: {sorted(list(variables))}")
            
        else:
            print("wgrib2 not available, trying cfgrib CLI...")
            # Fall back to cfgrib
            result = subprocess.run(['cfgrib', 'dump', file_path], 
                                  capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                variables = set()
                for line in result.stdout.split('\n'):
                    if 'shortName=' in line:
                        match = re.search(r"shortName='([^']+)'", line)
                        if match:
                            variables.add(match.group(1))
                
                print(f"All variables: {sorted(list(variables))}")
                
    except FileNotFoundError:
        print("Neither wgrib2 nor cfgrib command-line tools found.")
    except Exception as e:
        print(f"Error with CLI method: {e}")

# Usage
list_grib_variables_cli_simple(r"C:\Users\weiyu\Downloads\rap.t00z.awp130pgrbf00.grib2")