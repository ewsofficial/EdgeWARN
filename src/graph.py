import json
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path

# Target timestamp
TARGET_TS = datetime.fromisoformat("2025-10-11T23:14:38")

def plot_specific_timestamp(json_path: Path, target_ts: datetime):
    with open(json_path, "r") as f:
        cells = json.load(f)

    plt.figure(figsize=(8, 6))
    found_any = False

    for cell in cells:
        history = cell.get("storm_history", [])
        if not history:
            continue

        # Find entry with exact timestamp
        entry = next((h for h in history if datetime.fromisoformat(h["timestamp"]) == target_ts), None)
        if not entry:
            continue

        found_any = True
        bbox = cell.get("bbox", [])
        centroid = entry.get("centroid", [])
        cell_id = cell.get("id", "N/A")
        refl = entry.get("max_refl", cell.get("max_refl", None))

        if not bbox:
            continue

        # Extract lat/lon for polygon
        lats = [p[0] for p in bbox] + [bbox[0][0]]
        lons = [p[1] for p in bbox] + [bbox[0][1]]

        # Plot polygon
        plt.plot(lons, lats, "-", linewidth=2, label=f"Cell {cell_id} (Refl {refl})")

        # Plot centroid
        if centroid:
            plt.scatter(centroid[1], centroid[0], c="red", s=40, zorder=5)
            plt.text(centroid[1], centroid[0], f"{cell_id}", fontsize=8, ha="left", va="bottom")

    if not found_any:
        print(f"No storm entries found for timestamp {target_ts.isoformat()}")
        return

    plt.title(f"Storm Cells at {target_ts.isoformat()}")
    plt.xlabel("Longitude (°)")
    plt.ylabel("Latitude (°)")
    plt.legend(loc="best")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    json_path = Path("stormcell_test.json")
    plot_specific_timestamp(json_path, TARGET_TS)
