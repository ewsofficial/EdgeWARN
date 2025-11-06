from EdgeWARN.ctam.utils import DataHandler, default_norm
from util.io import IOManager

io_manager = IOManager("[CTAM]")

class IntensityIndiceCalculator:
    """
    Calculator for storm cell intensity indices with normalization support.

    This class computes several meteorological indices for storm cells, including:
        - Composite Echo Tops (CompET)
        - Total Hydrometer Load (THL) and THL Density (THLD)
        - Vertically Integrated Ice Density (VIIDensity)
        - Precipitation Intensity Index (PII)
        - Total Reflectivity Load (TRL)
        - Deep Convection Strength (DCS)
        - Upper Level Reflectivity Ratio (UpperLevelRefRatio)
        - Upper Level Intensity (ULInt)
        - Lower Level Intensity (LLInt)
        - Flash Area Ratio (FlashAreaRatio)
        - Flash Ratio (FlashRatio)
        - Normalized Lightning Intensity (NLI)
        - Flash Compactness Index (FlashCompactIndex)
    """

    def __init__(self, stormcells, norm_values=default_norm):
        self.stormcells = stormcells
        self.data_handler = DataHandler(self.stormcells)

        # Break the class if norm_values is malformed
        self.data_handler.verify_norm_values(norm_values, default_norm)
        self.norm_values = norm_values

    def calculate_composite_et(self, key='CompET', precision=2):
        """
        CompET = 0.1 * EchoTop18 + 0.3 * EchoTop30 + 0.6 * EchoTop50
        or 0.3 * EchoTop18 + 0.7 * EchoTop30 if EchoTop50 == 0
        """
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping CompET for cell {cell.get('id')} - No history")
                continue

            et18 = latest_entry.get('EchoTop18')
            et30 = latest_entry.get('EchoTop30')
            et50 = latest_entry.get('EchoTop50')

            if not all(isinstance(v, (int, float)) for v in [et18, et30, et50]):
                latest_entry[key] = 0
                continue

            comp_et = 0.3 * et18 + 0.7 * et30 if et50 == 0 else 0.1 * et18 + 0.3 * et30 + 0.6 * et50
            latest_entry[key] = round(comp_et, precision)

    def calculate_thl(self, thl_key='THL', thld_key='THLDensity', precision=2):
        """THL = VIL + VII; THLD = THL / EchoTop18"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping THL, THLD for cell {cell.get('id')} - No history")
                continue

            vil = latest_entry.get('VIL')
            vii = latest_entry.get('VII')
            et18 = latest_entry.get('EchoTop18')

            if not all(isinstance(v, (int, float)) for v in [vil, vii, et18]):
                latest_entry[thl_key] = 0
                latest_entry[thld_key] = 0
                continue

            latest_entry[thl_key] = round(vil + vii, precision)
            latest_entry[thld_key] = round((vil + vii) / et18, precision)

    def calculate_vii_density(self, key='VIIDensity', precision=2):
        """VIIDensity = VII / EchoTop18"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping VIIDensity for {cell.get('id')} - No history")
                continue

            vii = latest_entry.get('VII')
            et18 = latest_entry.get('EchoTop18')

            if not all(isinstance(v, (int, float)) for v in [vii, et18]) or et18 == 0:
                latest_entry[key] = 0
                continue

            latest_entry[key] = round(vii / et18, precision)

    def calculate_pii(self, key='PII', precision=2):
        """PII = (RALA / rala_norm) + (PrecipRate / preciprate_norm) + (MESH / mesh_norm)"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping PII for {cell.get('id')} - No history")
                continue

            rala = latest_entry.get('RALA')
            rala_norm = self.norm_values.get('RALA')
            preciprate = latest_entry.get('PrecipRate')
            preciprate_norm = self.norm_values.get('PrecipRate')
            mesh = latest_entry.get('MESH')
            mesh_norm = self.norm_values.get('MESH')

            if not all(isinstance(v, (int, float)) for v in [rala, preciprate, mesh]):
                latest_entry[key] = 0
                continue

            latest_entry[key] = round((rala / rala_norm) + (preciprate / preciprate_norm) + (mesh / mesh_norm), precision)

    def calculate_trl(self, key='TRL', precision=2):
        """TRL = (VIL / vil_norm) * (max_refl / maxref_norm)"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping TRL for {cell.get('id')} - No history")
                continue

            vil = latest_entry.get('VIL')
            vil_norm = self.norm_values.get('VIL')
            maxref = latest_entry.get('max_refl')
            maxref_norm = self.norm_values.get('MaxRef')

            if not all(isinstance(v, (int, float)) for v in [vil, maxref]):
                latest_entry[key] = 0
                continue

            latest_entry[key] = round((vil / vil_norm) * (maxref / maxref_norm), precision)

    def calculate_dcs(self, key='DCS', precision=2):
        """DCS = (maxref / maxref_norm) * (et50 / et50_norm)"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping DCS for {cell.get('id')} - No history")
                continue

            maxref = latest_entry.get('max_refl')
            maxref_norm = self.norm_values.get('MaxRef')
            et50 = latest_entry.get('EchoTop50')
            et50_norm = self.norm_values.get('EchoTop50')

            if not all(isinstance(v, (int, float)) for v in [maxref, et50]):
                latest_entry[key] = 0
                continue

            latest_entry[key] = round((maxref / maxref_norm) * (et50 / et50_norm), precision)

    def calculate_upper_ref_ratio(self, key='UpperLevelRefRatio', precision=2):
        """UpperLevelRefRatio = Ref20 / Ref10"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping UpperLevelRefRatio for {cell.get('id')} - No history")
                continue

            ref10 = latest_entry.get('Ref10')
            ref20 = latest_entry.get('Ref20')

            if not all(isinstance(v, (int, float)) for v in [ref10, ref20]) or ref10 == 0:
                latest_entry[key] = 0
                continue

            latest_entry[key] = round(ref20 / ref10, precision)

    def calculate_llint(self, key='LLInt', precision=2):
        """LLInt = (RALA / maxref) * EchoTop30"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping LLInt for {cell.get('id')} - No history")
                continue

            rala = latest_entry.get('RALA')
            maxref = latest_entry.get('max_refl')
            et30 = latest_entry.get('EchoTop30')

            if not all(isinstance(v, (float, int)) for v in [rala, maxref, et30]):
                latest_entry[key] = 0
                continue

            latest_entry[key] = round((rala / maxref) * et30, precision)

    def calculate_ulint(self, key='ULInt', precision=2):
        """ULInt = (max(ref10, ref20) / refupper_norm) * (1 + H50_Above_0C)"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping ULInt for {cell.get('id')} - No history")
                continue

            ref10 = latest_entry.get('Ref10')
            ref20 = latest_entry.get('Ref20')
            refupper_norm = self.norm_values.get('RefUpper')
            h50 = latest_entry.get('H50_Above_0C')

            if not all(isinstance(v, (int, float)) for v in [ref10, ref20, h50]):
                latest_entry[key] = 0
                continue

            latest_entry[key] = round((max(ref10, ref20) / refupper_norm) * (1 + h50), precision)

    def calculate_flash_area_ratio(self, key='FlashAreaRatio', precision=2):
        """FlashAreaRatio = MinFlashArea / (num_gates * 1.11^2)"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping FlashAreaRatio for {cell.get('id')} - No history")
                continue

            num_gates = latest_entry.get('num_gates')
            minflasharea = latest_entry.get('MinFlashArea')

            if not all(isinstance(v, (int, float)) for v in [num_gates, minflasharea]):
                latest_entry[key] = 0
                continue

            storm_area = num_gates * (1.11 ** 2)
            latest_entry[key] = round(minflasharea / storm_area, precision)

    def calculate_flash_ratio(self, key='FlashRatio', precision=2):
        """FlashRatio = CGFlashDensity / FlashDensity"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping FlashRatio for {cell.get('id')} - No history")
                continue

            cg_flash = latest_entry.get('CGFlashDensity')
            flash = latest_entry.get('FlashDensity')

            if not all(isinstance(v, (int, float)) for v in [cg_flash, flash]):
                latest_entry[key] = 0
                continue

            latest_entry[key] = 9999 if flash == 0 else round(cg_flash / flash, precision)

    def calculate_nli(self, key='NLI', precision=2):
        """NLI = FlashRate / StormArea; StormArea = num_gates * 1.11^2"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping NLI for {cell.get('id')} - No history")
                continue

            flashrate = latest_entry.get('FlashRate')
            num_gates = latest_entry.get('num_gates')

            if not all(isinstance(v, (int, float)) for v in [flashrate, num_gates]) or num_gates == 0:
                latest_entry[key] = 0
                continue

            storm_area = num_gates * (1.11 ** 2)
            latest_entry[key] = round(flashrate / storm_area, precision)

    def calculate_flash_compact_index(self, key='FlashCompactIndex', precision=2):
        """FlashCompactIndex = MaxFCD / MaxFED"""
        for cell in self.stormcells:
            latest_entry = cell.get('storm_history', [])[-1] if cell.get('storm_history') else None
            if not latest_entry:
                io_manager.write_warning(f"Skipping FlashCompactIndex for {cell.get('id')} - No history")
                continue

            maxfcd = latest_entry.get('MaxFCD')
            maxfed = latest_entry.get('MaxFED')

            if not all(isinstance(v, (int, float)) for v in [maxfcd, maxfed]) or maxfed == 0:
                latest_entry[key] = 0
                continue

            latest_entry[key] = round(maxfcd / maxfed, precision)

    def return_results(self):
        """Returns the storm cells with all calculated indices."""
        return self.stormcells

if __name__ == "__main__":
    import json
    with open("stormcell_test.json", 'r') as f:
        stormcells = json.load(f)
    
    calculator = IntensityIndiceCalculator(stormcells)
    calculator.calculate_composite_et()
    calculator.calculate_thl()
    calculator.calculate_vii_density()
    calculator.calculate_pii()
    calculator.calculate_trl()
    calculator.calculate_dcs()
    calculator.calculate_upper_ref_ratio()
    calculator.calculate_ulint()
    calculator.calculate_llint()
    calculator.calculate_flash_area_ratio()
    calculator.calculate_flash_ratio()
    calculator.calculate_nli()
    calculator.calculate_flash_compact_index()

    stormcells = calculator.return_results()
    with open("stormcell_test.json", 'w') as f:
        json.dump(stormcells, f, indent=2)
