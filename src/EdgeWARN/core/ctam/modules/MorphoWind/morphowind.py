import math
from ...interface import AnalysisModule
from . import config as cfg

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
        from ...util.history import get_cell_history
        
        props = storm_entry.get("properties", {})
        morphology = props.get("morphology", {})
        cell_id = storm_entry.get("id")
        
        # --- Extract Features ---
        solidity = morphology.get("solidity", 1.0)
        aspect_ratio = morphology.get("aspect_ratio", 1.0)
        defect_depth = morphology.get("defect_max_depth", 0.0)
        defect_bearing = morphology.get("defect_bearing", 0.0)
        
        azshear_low = props.get("p95AzShearLow", 0.0)
        vil = props.get("p95VIL", 0.0) # Assume p95VIL exists (standard metric)
        echotop_18 = props.get("p95EchoTop18", 0.0)
        
        # --- Physics Calculations ---
        
        # 1. VIL Density (g/m^3)
        vil_density = 0.0
        if echotop_18 > 0:
            vil_density = vil / echotop_18
            
        # Freezing Level Correction (Gaussian Smoothed)
        # Higher FL = deeper warm layer = more evaporative potential = lower VIL threshold
        fl_correction = 0.0
        fl_height = None
        if environment and "freezing_level_height" in environment:
            fl_height = environment["freezing_level_height"]
        elif "freezing_level_height" in props:
            fl_height = props["freezing_level_height"]
        
        if fl_height is not None:
            # Use Gaussian CDF to smoothly scale correction from 0 to FL_MAX_CORRECTION
            fl_factor = self._gaussian_score(fl_height, cfg.FL_MEAN, cfg.FL_SIGMA)
            fl_correction = fl_factor * cfg.FL_MAX_CORRECTION
        
        # Dewpoint Depression Correction (Gaussian Smoothed)
        # Higher DD = drier sub-cloud air = stronger evaporative cooling
        dp_correction = 0.0
        dd = None
        if environment and "dewpoint_depression" in environment:
            dd = environment["dewpoint_depression"]
        elif "dewpoint_depression" in props:
            dd = props["dewpoint_depression"]
        
        if dd is not None:
            # Use Gaussian CDF to smoothly scale correction from 0 to DD_MAX_CORRECTION
            dd_factor = self._gaussian_score(dd, cfg.DD_MEAN, cfg.DD_SIGMA)
            dp_correction = dd_factor * cfg.DD_MAX_CORRECTION
            
        # 2. Collapse Detection (Temporal Physics)
        history = get_cell_history(cell_id, limit=5)
        
        max_historical_vil_density = 0.0
        # Pre-condition check loop
        for h_entry in history:
            h_props = h_entry.get("properties", {})
            h_vil = h_props.get("p95VIL", 0.0)
            h_et = h_props.get("p95EchoTop18", 0.0)
            if h_et > 0:
                d = h_vil / h_et
                if d > max_historical_vil_density:
                    max_historical_vil_density = d
        
        # We need to have had a "Heavy Core" (Density > 3.0 approx) at some point
        pre_condition_met = max_historical_vil_density >= 3.0
        
        collapse_score = 0.0
        physics_triggers = []
        vil_rate = 0.0
        et_rate = 0.0

        if len(history) >= 2:
            past_idx = min(len(history) - 1, 2)
            past_entry = history[past_idx]
            past_props = past_entry.get("properties", {})
            
            past_vil = past_props.get("p95VIL", 0.0)
            past_et = past_props.get("p95EchoTop18", 0.0)
            
            vil_rate = vil - past_vil
            et_rate = echotop_18 - past_et
            
            if pre_condition_met:
                if vil_rate < cfg.COLLAPSE_VIL_RATE_THRESHOLD:
                    collapse_score += 0.5
                    physics_triggers.append("VIL_COLLAPSE")
                if et_rate < cfg.COLLAPSE_ET_RATE_THRESHOLD:
                    collapse_score += 0.5
                    physics_triggers.append("ET_COLLAPSE")
            else:
                # If we never had a core, a "collapse" is just dissipation
                pass

        # 3. Rear Inflow Notch Verification (Kinematic Physics)
        notch_score = 0.0
        dx = storm_entry.get("dx", 0.0)
        dy = storm_entry.get("dy", 0.0)
        motion_mag = math.sqrt(dx*dx + dy*dy)
        
        if motion_mag > 0.1 and defect_depth > 1.0:
            motion_angle = math.degrees(math.atan2(dy, dx))
            motion_bearing = (motion_angle + 360) % 360
            diff = abs(motion_bearing - defect_bearing)
            if diff > 180: diff = 360 - diff
                
            if diff > 135:
                notch_score = self._gaussian_score(defect_depth, cfg.QLCS_DEFECT_DEPTH_MEAN, cfg.QLCS_DEFECT_DEPTH_SIGMA)
                if notch_score > 0.5: physics_triggers.append("REAR_INFLOW_NOTCH")
            elif diff < 45:
                 notch_score = 0.0
            else:
                 notch_score = self._gaussian_score(defect_depth, cfg.QLCS_DEFECT_DEPTH_MEAN, cfg.QLCS_DEFECT_DEPTH_SIGMA) * 0.5
        elif defect_depth > 0:
            notch_score = self._gaussian_score(defect_depth, cfg.QLCS_DEFECT_DEPTH_MEAN, cfg.QLCS_DEFECT_DEPTH_SIGMA) * 0.7

        # 4. Bookend Vortex Check
        linearity = morphology.get("linearity", 0.0)
        branching_factor = morphology.get("branching_factor", 0)
        
        # A "clean" line is linear and has few branches (junctions)
        is_linear = linearity > cfg.BOOKEND_VORTEX_LINEARITY_THRESHOLD or aspect_ratio > 3.0
        is_simple = branching_factor <= cfg.BOOKEND_MAX_BRANCHING
        is_high_shear = azshear_low > cfg.BOOKEND_VORTEX_SHEAR_THRESHOLD
        
        bookend_bonus = 0.0
        if is_linear and is_simple and is_high_shear:
            bookend_bonus = 0.5
            physics_triggers.append("BOOKEND_VORTEX")

        # --- Scoring (Gaussian Smoothing) ---
        score_solidity = self._gaussian_score(solidity, cfg.QLCS_SOLIDITY_MEAN, cfg.QLCS_SOLIDITY_SIGMA, invert=True)
        score_aspect = self._gaussian_score(aspect_ratio, cfg.QLCS_ASPECT_RATIO_MEAN, cfg.QLCS_ASPECT_RATIO_SIGMA)
        score_shear = self._gaussian_score(azshear_low, cfg.QLCS_SHEAR_MEAN, cfg.QLCS_SHEAR_SIGMA)
        
        # Apply Freezing Level and Dry Air corrections to Mean
        adjusted_vil_mean = cfg.MB_VIL_DENSITY_MEAN - fl_correction - dp_correction
        score_vil_density = self._gaussian_score(vil_density, adjusted_vil_mean, cfg.MB_VIL_DENSITY_SIGMA)
        score_et = self._gaussian_score(echotop_18, cfg.MB_ECHOTOP_MEAN, cfg.MB_ECHOTOP_SIGMA, invert=True)
        
        # --- Aggregation ---
        qlcs_risk = (score_solidity + score_aspect + score_shear + (notch_score * 1.5)) / 4.5
        qlcs_risk = min(1.0, qlcs_risk + bookend_bonus)
        
        mb_risk = ((score_vil_density + score_et) / 2.0) * 0.7 + (collapse_score * 0.3)
        if collapse_score > 0.8: mb_risk = max(mb_risk, 0.9)
        
        result = {
            "risk_type": "None",
            "confidence": 0.0,
            "physics_triggers": physics_triggers, 
            "severity_index": 0.0,
            "scores": {
                "qlcs": round(qlcs_risk, 2),
                "microburst": round(mb_risk, 2)
            },
            "physics": {
                "vil_density": round(vil_density, 2),
                "collapse_score": round(collapse_score, 2),
                "vil_change": round(vil_rate, 1),
                "et_change": round(et_rate, 1),
                "defect_bearing": defect_bearing,
                "linearity": linearity
            }
        }
        
        result["severity_index"] = round(max(qlcs_risk, mb_risk), 2)
        
        # Classification
        # Require a minimum confidence to classify (e.g. 0.5)
        if qlcs_risk > 0.5 and qlcs_risk >= mb_risk:
            result["risk_type"] = "QLCS"
            result["confidence"] = round(qlcs_risk, 2)
        elif mb_risk > 0.5:
            result["risk_type"] = "Microburst"
            result["confidence"] = round(mb_risk, 2)
            
        # Inject result
        
        if "modules" not in storm_entry:
            storm_entry["modules"] = {}
        storm_entry["modules"][self.name] = result
