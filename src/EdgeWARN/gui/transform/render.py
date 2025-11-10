from pathlib import Path
import json
import numpy as np
from PIL import Image
from .tools import TransformUtils
import util.file as fs
from util.io import IOManager
from datetime import datetime

io_manager = IOManager("[Transform]")

class GUILayerRenderer:
    def __init__(self, filepath: Path, outdir: Path, colormap_key, file_name):
        """
        Args:
            filepath (Path): Path of the dataset being converted to GUI png
            outdir (Path): Output directory of the converted png file
            colormap_key (str): Key of the color map as stored under colormaps.json
            file_name (str): Key of .png file name
        """
        self.filepath = filepath
        self.outdir = outdir
        self.colormap_key = colormap_key
        self.file_name = file_name

    def _get_cmap(self):
        """
        Returns:
            thresholds (np.ndarray): array of dBZ or value thresholds
            colors (np.ndarray): array of RGB colors corresponding to thresholds
        """
        with open("colormaps.json", 'r') as f:
            cmaps_json = json.load(f)

        # Iterate through all colormaps to find the matching key
        for source in cmaps_json:
            for cmap in source.get("colormaps", []):
                if cmap.get("name") == self.colormap_key:
                    thresholds = [t["value"] for t in cmap["thresholds"]]
                    colors = [t["rgb"] for t in cmap["thresholds"]]
                    return np.array(thresholds), np.array(colors, dtype=np.float32)
        
        # If key not found, raise an error
        raise ValueError(f"Colormap '{self.colormap_key}' not found in colormaps.json")

    def convert_to_png(self):
        """
        Converts dataset to a png file and then saves it to outdir
        """
        # Step 1: Load the file
        latest_file = fs.latest_files(self.filepath, 1)[-1]
        ds = TransformUtils.load_ds(latest_file)
        data = ds['unknown'].values

        # Step 2: Get colormap
        thresholds, colors = self._get_cmap()

        # Step 2.5: Interpolate colors
        flat_data = data.flatten()
        r = np.interp(flat_data, thresholds, colors[:, 0])
        g = np.interp(flat_data, thresholds, colors[:, 1])
        b = np.interp(flat_data, thresholds, colors[:, 2])
        a = np.where(flat_data < 0, 0, 255)  # transparent for values < 0

        # Reshape to original grid for 1:1 pixel correspondence
        rgba = np.stack([r, g, b, a], axis=1).reshape((data.shape[0], data.shape[1], 4)).astype(np.uint8)

        # Step 3: Generate and save
        # Find timestamp
        timestamp = TransformUtils.find_timestamp(latest_file)
        dt = datetime.fromisoformat(timestamp)
        timestamp = dt.strftime(r"%Y%m%d-%H%M%S")

        # Ensure the output directory exists
        self.outdir.mkdir(parents=True, exist_ok=True)

        # Define the full file path
        png_file = self.outdir / f"{self.file_name}_{timestamp}.png"

        # Create the image and save
        img = Image.fromarray(rgba, mode="RGBA")
        img.save(png_file, optimize=True)

        io_manager.write_debug(f"Saved {self.file_name} PNG file to {png_file}")
