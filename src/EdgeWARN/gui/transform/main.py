from . import config
from . import render
from .tools import OverlayManifestUtils
import util.file as fs

if __name__ == "__main__":
    manifest = OverlayManifestUtils()
    for entry in config.file_list:
        name = entry.get('name')
        colormap_key = entry.get('colormap_key')
        filepath = entry.get('filepath')
        outdir = entry.get('outdir')
        renderer = render.GUILayerRenderer(filepath, outdir, colormap_key, name)
        png_file = renderer.convert_to_png()
        manifest.add_layer(name, colormap_key, str(png_file))
    manifest.save_to_json("overlay_manifest.json")