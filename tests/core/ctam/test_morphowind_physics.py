import sys
import os
import unittest
from unittest.mock import patch
sys.path.append(os.path.join(os.getcwd(), 'src'))

from EdgeWARN.core.ctam.modules.morphowind import MorphoWindModule 
from EdgeWARN.core.ctam.modules import morphowind_config as cfg

class TestMorphoWindPhysics(unittest.TestCase):
    def setUp(self):
        self.module = MorphoWindModule()

    @patch('EdgeWARN.core.ctam.util.history.get_cell_history')
    def test_microburst_collapse_logic(self, mock_get_history):
        """
        Test that a rapid drop in VIL/EchoTop triggers the collapse flag.
        Pre-condition: Must have had heavy core (Density > 3.0) previously.
        """
        current_storm = {
            "id": "Cell_MB",
            "properties": {
                "p95VIL": 10.0,
                "p95EchoTop18": 5.0,
                "morphology": {
                    "solidity": 0.9,
                    "aspect_ratio": 1.1,
                    "defect_max_depth": 0.0
                }
            }
        }
        
        # History (High VIL, High ET - Pre Collapse)
        # T-2 Entry: VIL=40, ET=10 -> Density=4.0 (>3.0 Threshold ✅)
        history_state = [
            {}, 
            {},
            {
                "properties": {
                    "p95VIL": 40.0,  
                    "p95EchoTop18": 10.0 
                }
            }
        ]
        mock_get_history.return_value = history_state

        self.module.run(current_storm)
        
        result = current_storm['properties']['morphowind']
        
        print("\n--- Microburst Collapse Test ---")
        print(f"Triggers: {result['physics_triggers']}")
        
        self.assertIn("VIL_COLLAPSE", result['physics_triggers'])
        self.assertEqual(result['risk_type'], "Microburst")

    @patch('EdgeWARN.core.ctam.util.history.get_cell_history')
    def test_rear_inflow_notch(self, mock_get_history):
        """
        Test that a convexity defect aligned with the REAR of the motion vector triggers REAR_INFLOW_NOTCH.
        """
        mock_get_history.return_value = [] 
        
        current_storm = {
            "id": "Cell_QLCS",
            "dx": 10.0, # Moving East
            "dy": 0.0,
            "properties": {
                "p95VIL": 20.0,
                "p95EchoTop18": 8.0,
                "p95AzShearLow": 5.0, # High Shear
                "morphology": {
                    "solidity": 0.5, 
                    "aspect_ratio": 4.0, 
                    "defect_max_depth": 10.0, 
                    "defect_bearing": 180.0 # Rear (West)
                }
            }
        }
        
        self.module.run(current_storm)
        result = current_storm['properties']['morphowind']
        
        print("\n--- Rear Inflow Notch Test ---")
        print(f"Triggers: {result['physics_triggers']}")
        
        self.assertIn("REAR_INFLOW_NOTCH", result['physics_triggers'])
        self.assertEqual(result['risk_type'], "QLCS")

    def test_bookend_vortex_logic(self):
        """
        Test that Linear Morphology + High Shear + Low Branching triggers BOOKEND_VORTEX.
        """
        current_storm = {
            "id": "Cell_Bookend",
            "properties": {
                "p95VIL": 20.0,
                "p95EchoTop18": 8.0,
                "p95AzShearLow": 6.0, # Very High Shear (> 5.0)
                "morphology": {
                    "solidity": 0.5,
                    "aspect_ratio": 4.0, # Linear (> 3.0)
                    "linearity": 0.8, # Linear Skeleton
                    "branching_factor": 1, # Clean line (< 2)
                    "defect_max_depth": 0.0
                }
            }
        }
        
        # Mocking history to empty for this test
        with patch('EdgeWARN.core.ctam.util.history.get_cell_history', return_value=[]):
            self.module.run(current_storm)
            
        result = current_storm['properties']['morphowind']
        print("\n--- Bookend Vortex Test ---")
        print(f"Triggers: {result['physics_triggers']}")
        
        self.assertIn("BOOKEND_VORTEX", result['physics_triggers'])
        self.assertEqual(result['risk_type'], "QLCS")

    def test_dry_air_enhancement(self):
        """
        Test that high Dewpoint Depression increases Microburst sensitivity.
        A storm with moderate VIL Density that would normally score lower,
        SHOULD score higher when environment has high DD.
        """
        # VIL=24, ET=6 -> Density = 4.0 (Just above baseline mean of 3.5)
        # WITH Dry Air correction (-0.75), effective mean becomes 2.75.
        # Score for 4.0 vs Mean 2.75 should be significantly higher.
        current_storm = {
            "id": "Cell_DryAir",
            "properties": {
                "p95VIL": 24.0,
                "p95EchoTop18": 6.0, # VIL Density = 4.0
                "morphology": {
                    "solidity": 0.9,
                    "aspect_ratio": 1.1,
                    "defect_max_depth": 0.0
                }
            }
        }
        
        environment = {
            "dewpoint_depression": 20.0 # High DD -> Dry Air
        }
        
        with patch('EdgeWARN.core.ctam.util.history.get_cell_history', return_value=[]):
            self.module.run(current_storm, environment=environment)
            
        result = current_storm['properties']['morphowind']
        print("\n--- Dry Air Enhancement Test ---")
        print(f"VIL Density: {result['physics']['vil_density']}, Risk: {result['risk_type']}, MB Score: {result['scores']['microburst']}")
        
        # With Dry Air correction, the adjusted mean is lower, so this should score higher
        # The key is that the score is elevated above a baseline (no correction) case.
        self.assertGreater(result['scores']['microburst'], 0.4)

    def test_complex_cluster_ignored(self):
        """
        Test that a complex cluster (high branching) does NOT trigger BOOKEND_VORTEX,
        even if it has high shear and appears linear by Aspect Ratio.
        """
        current_storm = {
            "id": "Cell_Cluster",
            "properties": {
                "p95VIL": 20.0,
                "p95EchoTop18": 8.0,
                "p95AzShearLow": 6.0, # High Shear
                "morphology": {
                    "solidity": 0.6,
                    "aspect_ratio": 3.5, # Appears linear
                    "linearity": 0.4, # Low Linearity (Messy skeleton)
                    "branching_factor": 5, # High Branching (Complex/Cluster)
                    "defect_max_depth": 0.0
                }
            }
        }
        
        with patch('EdgeWARN.core.ctam.util.history.get_cell_history', return_value=[]):
            self.module.run(current_storm)
            
        result = current_storm['properties']['morphowind']
        print("\n--- Complex Cluster Test ---")
        print(f"Triggers: {result['physics_triggers']}")
        
        self.assertNotIn("BOOKEND_VORTEX", result['physics_triggers'])

    @patch('EdgeWARN.core.ctam.util.history.get_cell_history')
    def test_front_notch_ignored(self, mock_get_history):
        mock_get_history.return_value = []
        current_storm = {
            "id": "Cell_Safe",
            "dx": 10.0,
            "dy": 0.0,
            "properties": {
                "p95VIL": 20.0,
                "p95EchoTop18": 8.0,
                "morphology": {
                    "solidity": 0.6, 
                    "aspect_ratio": 3.0,
                    "defect_max_depth": 10.0, 
                    "defect_bearing": 90.0 # Front (East)
                }
            }
        }
        self.module.run(current_storm)
        result = current_storm['properties']['morphowind']
        self.assertNotIn("REAR_INFLOW_NOTCH", result['physics_triggers'])

if __name__ == '__main__':
    unittest.main()
