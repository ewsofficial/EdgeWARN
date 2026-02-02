import math
from ..interface import AnalysisModule
from . import morphowind_config as cfg

class MorphoWindModule(AnalysisModule):
    """
    CTAM Module for Morphological Wind Risk Assessment.
    Detects QLCS/Bow Echoes and Microburst potential using data-driven fuzzy logic.
    """

    @property
    def name(self):
        return "MorphoWind"

    def _gaussian_score(self, value, mean, sigma, invert=False):
        """
        Calculate probability score (0.0 - 1.0) using Gaussian CDF.
        
        Args:
            value: The observed value.
            mean: The value where probability is 0.5 (inflection point).
            sigma: The standard deviation controlling the width of the transition.
            invert: If True, lower values are riskier (e.g. Solidity).
                    If False, higher values are riskier (e.g. VIL).
        """
        if invert:
            z = (mean - value) / (sigma * math.sqrt(2))
        else:
            z = (value - mean) / (sigma * math.sqrt(2))
            
        return 0.5 * (1 + math.erf(z))

    def run(self, storm_entry, environment=None):
        """
        Process storm cells and inject 'morphowind' risk object.
        """
        props = storm_entry.get("properties", {})
        morphology = props.get("morphology", {})
        
        # --- Extract Features ---
        solidity = morphology.get("solidity", 1.0)
        aspect_ratio = morphology.get("aspect_ratio", 1.0)
        defect_depth = morphology.get("defect_max_depth", 0.0)
        
        azshear_low = props.get("p95AzShearLow", 0.0)
        vil_density = props.get("p95VILDensity", 0.0)
        echotop_18 = props.get("p95EchoTop18", 0.0)
        
        # --- Scoring (Gaussian Smoothing) ---
        
        # QLCS Features
        score_solidity = self._gaussian_score(solidity, cfg.QLCS_SOLIDITY_MEAN, cfg.QLCS_SOLIDITY_SIGMA, invert=True)
        score_aspect = self._gaussian_score(aspect_ratio, cfg.QLCS_ASPECT_RATIO_MEAN, cfg.QLCS_ASPECT_RATIO_SIGMA)
        score_shear = self._gaussian_score(azshear_low, cfg.QLCS_SHEAR_MEAN, cfg.QLCS_SHEAR_SIGMA)
        score_notch = self._gaussian_score(defect_depth, cfg.QLCS_DEFECT_DEPTH_MEAN, cfg.QLCS_DEFECT_DEPTH_SIGMA)
        
        # Microburst Features
        score_vil = self._gaussian_score(vil_density, cfg.MB_VIL_DENSITY_MEAN, cfg.MB_VIL_DENSITY_SIGMA)
        score_et = self._gaussian_score(echotop_18, cfg.MB_ECHOTOP_MEAN, cfg.MB_ECHOTOP_SIGMA, invert=True)
        
        # --- Aggregation ---
        
        # QLCS Risk: Average of indicators
        qlcs_risk = (score_solidity + score_aspect + score_shear + score_notch) / 4.0
        
        # Microburst Risk: Average of indicators
        mb_risk = (score_vil + score_et) / 2.0
        
        # --- Flags (for UI context) ---
        qlcs_flags = []
        if score_solidity > 0.6: qlcs_flags.append("non_convex")
        if score_aspect > 0.6: qlcs_flags.append("linear")
        if score_shear > 0.6: qlcs_flags.append("high_shear")
        if score_notch > 0.6: qlcs_flags.append("rear_inflow_notch")
        
        mb_flags = []
        if score_vil > 0.6: mb_flags.append("heavy_core")
        if score_et > 0.6: mb_flags.append("shallow_core")
        
        # --- Result ---
        result = {
            "risk_type": "None",
            "confidence": 0.0,
            "flags": [],
            "scores": {
                "qlcs": round(qlcs_risk, 2),
                "microburst": round(mb_risk, 2)
            }
        }
        
        # Classification
        # Require a minimum confidence to classify (e.g. 0.5)
        if qlcs_risk > 0.5 and qlcs_risk >= mb_risk:
            result["risk_type"] = "QLCS"
            result["confidence"] = round(qlcs_risk, 2)
            result["flags"] = qlcs_flags
        elif mb_risk > 0.5:
            result["risk_type"] = "Microburst"
            result["confidence"] = round(mb_risk, 2)
            result["flags"] = mb_flags
            
        # Inject result
        props["morphowind"] = result
        
        if "modules" not in storm_entry:
            storm_entry["modules"] = {}
        storm_entry["modules"][self.name] = result
