import folium
from .coordinate_utils import CoordinateUtils
from util.io import IOManager

class MapUtils:
    def __init__(self, io_manager=None):
        self.io_manager = io_manager or IOManager("[MapUtils]")

    def create_map(self):
        return folium.Map(location=[39.5, -98.35], zoom_start=5, no_cdn=True)

    def add_storm_cells(self, m, stormcells):
        for cell in stormcells:
            cell_id = cell['id']
            centroid = cell['centroid']
            bbox = cell['bbox']
            max_refl = cell['max_refl']
            num_gates = cell['num_gates']

            # Convert coordinates
            centroid_lat = centroid[0]
            centroid_lon = CoordinateUtils.convert_lon(centroid[1])

            # Convert bbox points
            bbox_converted = [[point[0], CoordinateUtils.convert_lon(point[1])] for point in bbox]

            # Add polygon for bbox
            folium.Polygon(
                locations=bbox_converted,
                color='red',
                weight=2,
                fill=True,
                fill_color='red',
                fill_opacity=0.3,
                tooltip=f"Storm Cell {cell_id}<br>Max Refl: {max_refl}<br>Gates: {num_gates}"
            ).add_to(m)