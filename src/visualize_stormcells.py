import json
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon
from matplotlib.widgets import Slider
import numpy as np

# Load the storm cell data
with open('stormcell_test.json', 'r') as f:
    data = json.load(f)

# Collect all unique timestamps
timestamps = sorted(set(h['timestamp'] for cell in data['features'] for h in cell['storm_history']))

# No global limits, will set per timestamp

# Create a figure and axis
fig, ax = plt.subplots(figsize=(10, 8))
plt.subplots_adjust(bottom=0.1)

# Function to update the plot
def update(val):
    idx = int(val)
    ts = timestamps[idx]
    ax.clear()

    current_lons = []
    current_lats = []

    for cell in data['features']:
        cell_id = cell['id']
        bbox = cell['bbox']
        if not bbox:
            continue

        # Prepare bbox points: [lon, lat], normalized lon
        bbox_points = np.array([[(p[1] + 360) % 360, p[0]] for p in bbox])
        
        # Get all history up to this timestamp
        hist_list = [h for h in cell['storm_history'] if h['timestamp'] <= ts]
        hist_list.sort(key=lambda x: x['timestamp'])

        if hist_list:
            # Plot historical centroids path
            lons_hist = [(h['centroid'][1] + 360) % 360 for h in hist_list]
            lats_hist = [h['centroid'][0] for h in hist_list]
            ax.plot(lons_hist, lats_hist, '-', color='orange', label=f'Cell {cell_id} Path' if len(hist_list) > 1 else "")

            # Plot historical bboxes
            for h in hist_list[:-1]:  # Exclude current
                if 'bbox' in h:
                    hist_bbox_points = np.array([[(p[1] + 360) % 360, p[0]] for p in h['bbox']])
                    poly = Polygon(hist_bbox_points, closed=True, fill=False, edgecolor='cyan', alpha=0.3, linewidth=1)
                    ax.add_patch(poly)
                    # Collect for limits
                    current_lons.extend(hist_bbox_points[:, 0])
                    current_lats.extend(hist_bbox_points[:, 1])

            # Plot arrows for motion
            for i in range(len(hist_list) - 1):
                lon1, lat1 = lons_hist[i], lats_hist[i]
                lon2, lat2 = lons_hist[i+1], lats_hist[i+1]
                dx = lon2 - lon1
                dy = lat2 - lat1
                ax.arrow(lon1, lat1, dx, dy, head_width=0.002, head_length=0.002, fc='purple', ec='purple', alpha=0.7)

            # Current entry
            hist_entry = hist_list[-1] if hist_list else None
            if hist_entry and hist_entry['timestamp'] == ts:
                c = hist_entry['centroid']
                lon = (c[1] + 360) % 360
                lat = c[0]

                current_lons.append(lon)
                current_lats.append(lat)

                # Plot current centroid
                ax.plot(lon, lat, 'o', markersize=8, label=f'Cell {cell_id}')

                # Plot current bbox directly
                if 'bbox' in hist_entry:
                    current_bbox_points = np.array([[(p[1] + 360) % 360, p[0]] for p in hist_entry['bbox']])
                else:
                    current_bbox_points = bbox_points  # cell's bbox
                poly = Polygon(current_bbox_points, closed=True, fill=False, edgecolor='blue', alpha=0.5, linewidth=2)
                ax.add_patch(poly)

                # Collect bbox points for limits
                current_lons.extend(current_bbox_points[:, 0])
                current_lats.extend(current_bbox_points[:, 1])

                # Plot velocity arrows
                # vx vy
                vx = hist_entry.get('vx', 0)
                vy = hist_entry.get('vy', 0)
                ax.quiver(lon, lat, vx, vy, angles='xy', scale_units='xy', scale=80, color='red', width=0.003, label='vx vy' if cell_id == data['features'][0]['id'] else "")

                # Computed averaged dx/dt dy/dt over previous 4 entries
                comp_vels = []
                for h in hist_list:
                    if 'dx' in h and 'dy' in h and 'dt' in h and h['dt'] != 0:
                        comp_vels.append((h['dx'] / h['dt'], h['dy'] / h['dt']))
                if comp_vels:
                    # Average the last min(4, len) velocities
                    num_to_avg = min(4, len(comp_vels))
                    avg_vx = sum(v[0] for v in comp_vels[-num_to_avg:]) / num_to_avg
                    avg_vy = sum(v[1] for v in comp_vels[-num_to_avg:]) / num_to_avg
                    ax.quiver(lon, lat, avg_vx, avg_vy, angles='xy', scale_units='xy', scale=80, color='green', width=0.003, label='avg dx/dt dy/dt' if cell_id == data['features'][0]['id'] else "")

    if current_lons and current_lats:
        ax.set_xlim(min(current_lons) - 0.1, max(current_lons) + 0.1)
        ax.set_ylim(min(current_lats) - 0.1, max(current_lats) + 0.1)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title(f'Storm Cell Visualization at {ts}')

    ax.grid(True)
    fig.canvas.draw_idle()

# Add slider
ax_slider = plt.axes([0.1, 0.02, 0.8, 0.03])
slider = Slider(ax_slider, 'Time Index', 0, len(timestamps)-1, valinit=0, valstep=1)
slider.on_changed(update)

# Initial plot
update(0)

# Show the plot
plt.show()