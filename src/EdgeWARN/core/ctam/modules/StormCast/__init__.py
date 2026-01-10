"""
StormCast CTAM Module

Adapter for integrating StormCast core into the CTAM framework.
"""

from typing import Dict, Any, Optional
from ...interface import AnalysisModule

# Re-export core components for external use
from .core import (
    StormCastEngine,
    ForecastResult,
    StormState,
    EnvironmentProfile,
    ForecastPoint,
)

class StormCastModule(AnalysisModule):
    """
    CTAM adapter for StormCast forecasting.
    
    This module wraps the StormCast core engine to run forecasts
    on storm cell entries. Results from StormCast.core are stored
    directly into the storm_entry['modules']['StormCast'] dict.
    """
    
    @property
    def name(self) -> str:
        return "StormCast"

    def run(self, storm_entry: Dict[str, Any], environment: Optional[Dict[str, Any]] = None) -> None:
        """
        Run StormCast on a storm entry.
        
        The core library generates the full JSON output, which is
        placed directly into the modules dict under 'StormCast'.
        
        Args:
            storm_entry: dict containing 'properties' and 'modules'.
            environment: Optional environmental data (e.g. RAP winds).
        """
        # Placeholder: Core logic integration will be added when
        # the exact input/output format is finalized.
        # For now, just mark as executed.
        storm_entry["modules"][self.name] = {
            "status": "pending_integration",
            "message": "StormCast core logic integration pending."
        }
