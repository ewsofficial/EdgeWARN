from EdgeWARN.ctam.utils import DataHandler
from util.io import IOManager

io_manager = IOManager("[CTAM]")

class GrowthIndiceCalculator:
    
    def __init__(self, stormcells):
        """
        Initializes the calculator with a list of storm cells.
        
        Args:
            stormcells (list[dict]): List of storm cell dictionaries, each containing a 'storm_history'.
        """
        self.stormcells = stormcells
        self.data_handler = DataHandler(self.stormcells)
    
    
    
    
        