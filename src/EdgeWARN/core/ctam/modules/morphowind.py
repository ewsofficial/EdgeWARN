from ..interface import AnalysisModule
from . import morphowind_config as cfg

class MorphoWindModule(AnalysisModule):
    """
    CTAM Module for Morphological Wind Risk Assessment.
    Detects QLCS/Bow Echoes and Microburst potential using geometric and microphysical features.
    """

    @property
    def name(self):
        return "MorphoWind"

    def run(self, storm_entry, environment=None):
        """
        Process storm cells and inject 'morphowind' risk object.
        """
        props = storm_entry.get("properties", {})
        morphology = props.get("morphology", {})
        
        # --- Extract Features ---
        # Geometry
        solidity = morphology.get("solidity", 1.0)
        aspect_ratio = morphology.get("aspect_ratio", 1.0)
        defect_depth = morphology.get("defect_max_depth", 0.0)
        
        # Physics (using p95 as robust metric)
        azshear_low = props.get("p95AzShearLow", 0.0)
        vil_density = props.get("p95VILDensity", 0.0)
        echotop_18 = props.get("p95EchoTop18", 0.0)
        
        # --- Logic ---
        
        # 1. QLCS Detection
        is_linear_shape = (solidity < cfg.QLCS_SOLIDITY_THRESHOLD) or (aspect_ratio > cfg.QLCS_ASPECT_RATIO_THRESHOLD)
        has_shear = azshear_low > cfg.QLCS_SHEAR_THRESHOLD
        has_notch = defect_depth > cfg.QLCS_DEFECT_DEPTH_THRESHOLD
        
        qlcs_risk = 0.0
        qlcs_flags = []
        
        if is_linear_shape:
            qlcs_risk += 0.4
            qlcs_flags.append("linear_shape")
            
            if has_shear:
                qlcs_risk += 0.4
                qlcs_flags.append("shear_detected")
                
            if has_notch:
                qlcs_risk += 0.2
                qlcs_flags.append("rear_inflow_notch")
        
        
        # 2. Microburst Detection
        # High VIL Density + Low/Mid Echo Top -> Core Collapsing or Shallow Heavy Core
        is_heavy_core = vil_density > cfg.MB_VIL_DENSITY_THRESHOLD
        is_shallow_or_collapsed = (echotop_18 > 0) and (echotop_18 < cfg.MB_ECHOTOP_THRESHOLD)
        
        mb_risk = 0.0
        mb_flags = []
        
        if is_heavy_core:
            mb_risk += 0.5
            mb_flags.append("heavy_core")
            
            if is_shallow_or_collapsed:
                # Classic downburst signature (heavy mass, low top)
                mb_risk += 0.4
                mb_flags.append("shallow_core")
        
        # --- Result ---
        result = {
            "risk_type": "None",
            "confidence": 0.0,
            "flags": []
        }
        
        # Simple priority: QLCS > Microburst if both high, or separate?
        # For now, pick highest risk.
        if qlcs_risk > 0.6 and qlcs_risk >= mb_risk:
            result["risk_type"] = "QLCS"
            result["confidence"] = round(qlcs_risk, 2)
            result["flags"] = qlcs_flags
        elif mb_risk > 0.6:
            result["risk_type"] = "Microburst"
            result["confidence"] = round(mb_risk, 2)
            result["flags"] = mb_flags
        
        # Inject result into properties (for UI/client)
        props["morphowind"] = result
        
        # Inject into modules (for CTAM data consistency)
        if "modules" not in storm_entry:
            storm_entry["modules"] = {}
        storm_entry["modules"][self.name] = result
