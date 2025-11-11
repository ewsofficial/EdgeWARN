class CoordinateUtils:
    def __init__(self):
        pass

    @staticmethod
    def convert_lon(lon):
        """Convert longitude from 0-360 to -180-180 format."""
        return lon - 360 if lon > 180 else lon