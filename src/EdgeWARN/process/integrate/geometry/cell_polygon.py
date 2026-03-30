import numpy as np
from shapely.geometry import Polygon


class StormIntegrationUtils:
    @staticmethod
    def normalize_longitude(lon):
        """Normalize longitude into the [-180, 180] range."""
        normalized = float(lon)
        while normalized > 180.0:
            normalized -= 360.0
        while normalized <= -180.0:
            normalized += 360.0
        return normalized

    @staticmethod
    def create_coordinate_grids(dataset):
        """
        Extract and create 2D latitude/longitude grids from any dataset.
        """
        lat_coord = None
        lon_coord = None

        for coord_name in dataset.coords:
            if coord_name.lower() in ["lat", "latitude", "y"]:
                lat_coord = dataset[coord_name].values
            elif coord_name.lower() in ["lon", "longitude", "x"]:
                lon_coord = dataset[coord_name].values

        if lat_coord is None or lon_coord is None:
            raise ValueError("[CellIntegration] ERROR: Could not find latitude and longitude coordinates in dataset")

        if lat_coord.ndim == 1 and lon_coord.ndim == 1:
            lon_grid, lat_grid = np.meshgrid(lon_coord, lat_coord)
        else:
            lat_grid, lon_grid = lat_coord, lon_coord

        return lat_grid, lon_grid

    @staticmethod
    def create_cell_polygon(cell, min_size=0.0):
        """
        Return a valid Polygon for the storm cell.
        Ensures at least 4 coordinates for LinearRing.
        Fallbacks:
            - bbox if available and has >= 3 points
            - small box around centroid
            - None if both fail
        """
        polygon = None

        if "bbox" in cell and cell["bbox"] and len(cell["bbox"]) >= 3:
            coords = [
                (StormIntegrationUtils.normalize_longitude(pt[1]), pt[0])
                for pt in cell["bbox"]
            ]
            polygon = Polygon(coords)

        if polygon is None or not polygon.is_valid or polygon.is_empty:
            if "centroid" in cell and len(cell["centroid"]) >= 2:
                lat, lon = cell["centroid"][0], StormIntegrationUtils.normalize_longitude(cell["centroid"][1])
                d = max(min_size, 0.01)
                coords = [
                    (lon - d, lat - d),
                    (lon - d, lat + d),
                    (lon + d, lat + d),
                    (lon + d, lat - d),
                    (lon - d, lat - d),
                ]
                polygon = Polygon(coords)

        if polygon is not None and polygon.is_valid and not polygon.is_empty:
            return polygon

        return None

    @staticmethod
    def create_polygon_mask(polygon, lat_grid, lon_grid):
        """
        Create a boolean mask using polygon bounds (min/max coordinates).
        Fast and efficient for rectangular approximation.
        """
        if polygon is None:
            return None

        minx, miny, maxx, maxy = polygon.bounds
        mask = (lon_grid >= minx) & (lon_grid <= maxx) & (lat_grid >= miny) & (lat_grid <= maxy)

        return mask
