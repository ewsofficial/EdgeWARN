from EdgeWARN.ctam.utils import DataHandler
from util.io import IOManager

io_manager = IOManager("[CTAM]")

class RadarIntensityIndiceCalculator:
    def __init__(self, stormcells):
        self.stormcells = stormcells
        self.data_handler = DataHandler(self.stormcells)
    
    def calculate_composite_et(self, key='CompET'):
        """
        Calculates Composite Echo Tops from stormcell data
        Formula:
            CompET = 0.1 * EchoTop18 + 0.3 * EchoTop30 + 0.6 * EchoTop50
            or 0.3 * EchoTop18 + 0.7 * EchoTop30 if EchoTop50 == 0
        Skips entries with non-numeric EchoTop values.
        """
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping CompET for cell {cell['id']}: No history")
                continue  # skip if no history available

            et18 = latest_entry.get('EchoTop18')
            et30 = latest_entry.get('EchoTop30')
            et50 = latest_entry.get('EchoTop50')

            # Skip if any EchoTop value is missing or not numeric
            if not all(isinstance(v, (int, float)) for v in [et18, et30, et50]):
                latest_entry[key] = 0

            # Compute CompET with logic for zero EchoTop50
            if et50 == 0:
                comp_et = 0.3 * et18 + 0.7 * et30
            else:
                comp_et = 0.1 * et18 + 0.3 * et30 + 0.6 * et50

            latest_entry[key] = comp_et
    
    def calculate_thl(self, thl_key='THL', thld_key='THLDensity'):
        """
        Calculates Total Hydrometer Load (THL) and Density (THLD)
        Formula:
            THL = VIL + VII
            THLD = THL / EchoTop18
        """
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping THL, THLD for cell {cell['id']}: No history")
                continue  # skip if no history available

            vil = latest_entry.get('VIL')
            vii = latest_entry.get('VII')
            et18 = latest_entry.get('EchoTop18')

            # Skip if any EchoTop value is missing or not numeric
            if not all(isinstance(v, (int, float)) for v in [vil, vii, et18]):
                print(f"Skipped THL, THLD for {cell['id']}")
                latest_entry[thl_key] = 0
                latest_entry[thld_key] = 0
                continue
            
            # Calculate and append THL, THLD
            latest_entry[thl_key] = vil + vii
            latest_entry[thld_key] = (vil + vii) / et18
            print(f"Integrated THL, THLD for {cell['id']}")
    
    def return_results(self):
        """
        Returns results of stormcell comp indice calculation for RadarIntensityIndiceCalculator
        ONLY RUN AFTER RUNNING ALL CALCULATIONS
        """
        return self.stormcells

if __name__ == "__main__":
    import json
    with open("stormcell_test.json", 'r') as f:
        stormcells = json.load(f)
    
    calculator = RadarIntensityIndiceCalculator(stormcells)
    calculator.calculate_composite_et()
    calculator.calculate_thl()
    stormcells = calculator.return_results()
    with open("stormcell_test.json", 'w') as f:
        json.dump(stormcells, f, indent=2)

