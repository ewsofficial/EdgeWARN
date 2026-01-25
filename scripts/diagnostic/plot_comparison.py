
import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def plot_scans(timestamps):
    base_dir = Path("/home/yuchenwei/EdgeWARN_input/data")
    
    fig, axes = plt.subplots(1, len(timestamps), figsize=(6 * len(timestamps), 6))
    if len(timestamps) == 1:
        axes = [axes]
    
    for i, ts in enumerate(timestamps):
        ew_ts = ts.replace("_", "-")
        ew_file = base_dir / "stormcells" / f"stormcells_{ew_ts}.json"
        ps_file = base_dir / "ProbSevere" / f"MRMS_PROBSEVERE_{ts}.json"
        
        ax = axes[i]
        ax.set_title(f"Scan {ts}")
        
        # Load ProbSevere
        if ps_file.exists():
            with open(ps_file, 'r') as f:
                ps_data = json.load(f)
            for feat in ps_data['features']:
                coords = np.array(feat['geometry']['coordinates'][0])
                # Convert lon -180 to 0-360 if needed, but let's just plot as they are
                # ps is usually -180...180
                lons = coords[:, 0]
                lats = coords[:, 1]
                ax.plot(lons, lats, color='blue', alpha=0.5, linestyle='--', label='ProbSevere' if i==0 else "")
                
        # Load EdgeWARN
        if ew_file.exists():
            with open(ew_file, 'r') as f:
                ew_data = json.load(f)
            for feat in ew_data['features']:
                if 'bbox' in feat:
                    coords = np.array(feat['bbox'])
                    # EdgeWARN is [lat, lon] with lon 0-360
                    lats = coords[:, 0]
                    lons = coords[:, 1]
                    # Normalize lons to -180...180 for overlap with ps
                    lons = np.where(lons > 180, lons - 360, lons)
                    ax.plot(lons, lats, color='red', linewidth=2, label='EdgeWARN' if i==0 else "")
                    
                    if 'centroid' in feat:
                        c_lat, c_lon = feat['centroid']
                        c_lon = c_lon - 360 if c_lon > 180 else c_lon
                        ax.scatter(c_lon, c_lat, color='black', s=10)

        # zoom into a region with storms
        # Let's find bounds from the data
        all_lats = []
        all_lons = []
        if ps_file.exists():
            for feat in ps_data['features']:
                coords = np.array(feat['geometry']['coordinates'][0])
                all_lons.extend(coords[:, 0])
                all_lats.extend(coords[:, 1])
        
        if all_lats:
             ax.set_xlim(min(all_lons) - 0.5, max(all_lons) + 0.5)
             ax.set_ylim(min(all_lats) - 0.5, max(all_lats) + 0.5)

        ax.legend()
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

    plt.tight_layout()
    output_path = "/home/yuchenwei/Projects/EdgeWARN-Core/scripts/comparison_plot.png"
    plt.savefig(output_path)
    print(f"Plot saved to {output_path}")

if __name__ == "__main__":
    import sys
    ts_list = ["20260125_004843", "20260125_005038", "20260125_005238"]
    plot_scans(ts_list)
