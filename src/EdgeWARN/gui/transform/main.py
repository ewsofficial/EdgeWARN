from . import config
from . import render
import util.file as fs

if __name__ == "__main__":
    for entry in config.file_list:
        name = entry.get('name')
        colormap_key = entry.get('colormap_key')
        filepath = entry.get('filepath')
        outdir = entry.get('outdir')
        renderer = render.GUILayerRenderer(filepath, outdir, colormap_key, name)
        renderer.convert_to_png()