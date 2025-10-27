from EdgeWARN.CTAM.utils import DataHandler, DataLoader
from util.io import IOManager
from math import tanh

io_manager = IOManager("[CTAM]")

class RadarGrowthIndiceCalculator:
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
                continue  # skip if no history available

            et18 = latest_entry.get('EchoTop18')
            et30 = latest_entry.get('EchoTop30')
            et50 = latest_entry.get('EchoTop50')

            # Skip if any EchoTop value is missing or not numeric
            if not all(isinstance(v, (int, float)) for v in [et18, et30, et50]):
                latest_entry[key] = "N/A"

            # Compute CompET with logic for zero EchoTop50
            if et50 == 0:
                comp_et = 0.3 * et18 + 0.7 * et30
            else:
                comp_et = 0.1 * et18 + 0.3 * et30 + 0.6 * et50

            latest_entry[key] = comp_et
            return

    def calculate_total_hydrometer_load(self, thl_key='THL', thld_key='THLD'):
        """
        Calculates total hydrometer load (THL) and its density (THLD)
        Formula:
            THL = VIL + VII
            THLD = THL / EchoTop18
        Skips entries with non-numeric values
        """
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                continue

            vil = latest_entry.get('VIL')
            vii = latest_entry.get('VII')
            et18 = latest_entry.get('EchoTop18')

            if not all(isinstance(v, (int, float)) for v in [vil, vii, et18]):
                latest_entry[thl_key], latest_entry[thld_key] = None, None
            
            # Append THL and THLD to data
            latest_entry[thl_key] = vil + vii
            latest_entry[thld_key] = (vil + vii) / et18

    def calculate_pii(self, key='PrecipInt'):
        """
        Calculates Precipitation Intensity Index (PII)
        Formula:
            PII = [0.5 * tanh(0.05293 * RALA) + 0.5 * tanh(0.06617 * PrecipRate)] * tanh(MESH / 25)
        """
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                continue

            rala = latest_entry.get('RALA')
            preciprate = latest_entry.get('PrecipRate')
            mesh = latest_entry.get('MESH')

            if not all(isinstance(v, (int, float)) for v in [rala, preciprate, mesh]):
                continue
        
            # Calculate and append PII
            latest_entry[key] = (0.5 * tanh(0.05293 * rala) + 0.5 * tanh(0.06617 * preciprate)) * tanh(mesh/25)

    def return_results(self):
        """
        Returns results of composite indice calculations in this class
        Only call after all indices are computed
        """
        return self.stormcells

class SatelliteGrowthIndiceCalculator:
    def __init__(self, stormcells):
        self.stormcells = stormcells
        self.data_handler = DataHandler(self.stormcells)
    
    def calculate_flash_area_ratio(self, key='FlashAreaRatio'):
        """
        Calculates flash area ratio (FAR)
        Formula:
            FAR = MinFlashArea / StormArea
        """
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                continue

            area = cell.get('num_gates') * (1.11 ** 2)
            mfa = cell.get('MinFlashArea')

            if not all(isinstance(v, (int, float)) for v in [area, mfa]):
                continue

            # Calculate and append FAR
            latest_entry[key] = mfa / area
    
    def return_results(self):
        """
        Returns results of all composite indice calculations in this class
        Only call after all indices are computed
        """
        return self.stormcells