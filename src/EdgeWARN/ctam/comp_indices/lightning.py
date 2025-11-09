from EdgeWARN.ctam.utils import DataHandler, DataLoader

class LtngIndiceCalculator:
    def __init__(self, stormcells):
        self.stormcells = stormcells
        self.data_handler = DataHandler(self.stormcells)

    def return_results(self):
        """
        Returns results of composite indice calculations
        Only call after all indices are computed
        """
        return self.stormcells