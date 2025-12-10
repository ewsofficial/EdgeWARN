from . import config
from . import render
from .tools import OverlayManifestUtils
import util.file as fs
import time

if __name__ == "__main__":
    start = time.time()
    manifest = OverlayManifestUtils()
    for entry in config.file_list:
        name = entry.get('name')
        colormap_key = entry.get('colormap_key')
        filepath = entry.get('filepath')
        outdir = entry.get('outdir')
        renderer = render.GUILayerRenderer(filepath, outdir, colormap_key, name)
        png_file, timestamp = renderer.convert_to_png()
        manifest.add_layer(name, colormap_key, str(png_file), timestamp)
    manifest.save_to_json(fs.GUI_MANIFEST_JSON)
    end = time.time()
    print(f"Total time: {end - start}")