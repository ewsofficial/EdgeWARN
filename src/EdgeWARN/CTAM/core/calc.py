import math
import json

class CompositeIndiceCalculator:
    def __init__():
        """
        Initializes CompositeIndiceCalculator
        """
    
    @staticmethod
    def calculate_scp(mucape, srh, ebwd):
        """
        Calculates Supercell Composite Parameter
        Formula: (MUCAPE/1000) x (SRH/50) x (EBWD/20)
        Source: https://www.spc.noaa.gov/exper/soundings/help/scp.html
        Inputs:
         - MUCAPE: Most-Unstable CAPE
         - SRH: 0-1 km Storm-Relative Helicity
         - EBWD: Effective-layer Bulk Wind Difference / Shear
        """
        return (mucape/1000) * (srh/50) * (ebwd/20)

    @staticmethod    
    def calculate_stp(mlcape, srh, ebwd, lcl, mlcin):
        """
        Calculates Significant Tornado Parameter
        Formula: (MLCAPE/1500) * (SRH/150) * (EBWD/12) * ((2000 - mlLCL)/1000) * ((MLCINH + 200)/150)
        Source: https://www.spc.noaa.gov/exper/soundings/help/stp.html
        Inputs:
         - MLCAPE: Mixed-layer CAPE
         - SRH: 0-1 km Storm-Relative Helicity
         - EBWD: Effective-layer Bulk Wind Difference / Shear 
         - mlLCL: Mixed-layer Lifting Condensation Level
         - MLCINH: Mixed-layer Convective Inhibition
        """
        return (mlcape/1500) * (srh/150) * (ebwd/12) * ((2000-lcl)/1000) * ((mlcin+200)/150)
    
    @staticmethod
    def calculate_dcp(dcape, mucape, ebwd, wind):
        """
        Calculates Derecho Composite Parameter
        Formula: (DCAPE/980) x (MUCAPE/2000) x (EBWD/20) x (WIND/16)
        Inputs:
         - DCAPE: Downdraft CAPE
         - MUCAPE: Most-Unstable CAPE
         - EBWD: 0-6 km Wind Shear
         - WIND: 0-6 km Mean Wind
        """
        return (dcape/980) * (mucape/2000) * (ebwd/20) * (wind/16)
    
    @staticmethod
    def calculate_sigsvr(mlcape, shr):
        """
        Calculates Craven-Brooks Significant Severe Parameter
        Formula: MLCAPE * SHEAR
        Inputs:
         - MLCAPE: Mixed-Layer CAPE
         - SHEAR: 0-6 km Wind Shear in m/s
        """
        return mlcape * shr
    
    @staticmethod
    def calculate_ehi(mlcape, srh):
        """
        Calculates Energy Helicity Index
        Formula: MLCAPE x SRH / 160,000
        Inputs:
         - MLCAPE: Mixed-Layer CAPE
         - SRH: 0-3 km Storm Relative Helicity
        """
        return mlcape * srh * 1.6e-5

class VectorManipulator:
    def __init__(self, dx, dy, dt):
        """
        Initializes VectorManipulator Class
        """
        self.dx = dx
        self.dy = dy
        self.dt = dt
    
    @staticmethod
    def get_vector_data(json_file, cell_id):
        """
        Fetches vector data found in EdgeWARN storm cell json files
        Inputs:
         - Valid EdgeWARN-formatted storm cell JSON file
        Outputs:
         - List of (dx, dy, dt) tuples
        """

        # Load JSON file
        with open(json_file, 'r') as f:
            data = json.load(f)

        # Retrieve vector information and return as a list of tuples
        vectors = []

        for cell in data:
            if cell['id'] == cell_id:
                if 'storm_history' in cell:
                    for entry in cell['storm_history']:
                        if all(key in entry for key in ['dx', 'dy', 'dt']):
                            vectors.append((
                                entry['dx'],
                                entry['dy'],
                                entry['dt']
                            ))
                break
        
        return vectors
    
    def scale_vector(self, scale_magnitude):
        vector = [self.dx, self.dy, self.dt]
        return [i * scale_magnitude for i in vector]
    
    def magnitude(self):
        return math.sqrt(self.dx ** 2 + self.dy ** 2)
    
    def direction(self):
        return math.atan(self.dy/self.dx)
    
    def speed(self):
        return (math.sqrt(self.dx ** 2 + self.dy ** 2)/self.dt)
    

    


        


    
    
    

    