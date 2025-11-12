import folium
import json
import os
import webbrowser

def convert_lon(lon):
    """Convert longitude from 0-360 to -180-180 format."""
    return lon - 360 if lon > 180 else lon

def main():
    # Load overlay manifest
    manifest_path = "overlay_manifest.json"
    if not os.path.exists(manifest_path):
        print("overlay_manifest.json not found. Please run the transform process first.")
        return

    with open(manifest_path, 'r') as f:
        layers = json.load(f)

    if not layers:
        print("No layers found in manifest.")
        return

    # Get bounds from first layer (assuming all layers have same bounds)
    bounds = layers[0]['bounds']
    south, north = bounds['south'], bounds['north']
    west = convert_lon(bounds['west'])
    east = convert_lon(bounds['east'])

    # Create folium map centered on the bounds
    center_lat = (south + north) / 2
    center_lon = (west + east) / 2
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5, no_cdn=True)

    # Add each layer as toggleable overlay
    for layer in layers:
        layer_bounds = layer['bounds']
        layer_south, layer_north = layer_bounds['south'], layer_bounds['north']
        layer_west = convert_lon(layer_bounds['west'])
        layer_east = convert_lon(layer_bounds['east'])

        # Create feature group for the layer
        fg = folium.FeatureGroup(name=layer['name'], show=False)

        # Add image overlay
        folium.raster_layers.ImageOverlay(
            image=layer['latest_image'],
            bounds=[[layer_south, layer_west], [layer_north, layer_east]],
            opacity=0.7
        ).add_to(fg)

        fg.add_to(m)

    # Add layer control
    folium.LayerControl().add_to(m)

    # Save the map
    output_path = os.path.join(os.path.dirname(__file__), 'interactive_map.html')
    m.save(output_path)
    print(f"Map saved to {output_path}.")
    # Open in browser
    webbrowser.open(output_path)
    print("Opened interactive map in browser.")

if __name__ == "__main__":
    main()