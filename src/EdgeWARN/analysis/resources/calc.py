import math

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
    def calculate_ehi(mlcape, srh):
        """
        Calculates Energy Helicity Index
        Formula: MLCAPE x SRH / 160,000
        Inputs:
         - MLCAPE: Mixed-Layer CAPE
         - SRH: 0-3 km Storm Relative Helicity
        """
        return mlcape * srh * 1.6e-5
    