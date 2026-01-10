from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class AnalysisModule(ABC):
    """
    Abstract base class for all CTAM analysis modules.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """
        The unique name of the module. 
        This will be used as the key in the 'modules' dictionary.
        """
        pass

    @abstractmethod
    def run(self, storm_entry: Dict[str, Any], environment: Optional[Dict[str, Any]] = None) -> None:
        """
        Perform analysis on a single storm entry.

        Parameters:
        - storm_entry: dict containing 'properties' and 'modules'.
                       This dictionary is modified in-place to add results.
        - environment: optional external data (e.g. RAP wind profile, thermodynamics).

        Module writes its results into:
        storm_entry['modules'][self.name]

        Results can include arbitrary parameters or metadata.
        """
        pass
