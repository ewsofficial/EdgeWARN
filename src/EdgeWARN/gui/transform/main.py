import EdgeWARN.gui.transform.config
import EdgeWARN.gui.transform.render
import util.file as fs

if __name__ == "__main__":
    for entry in EdgeWARN.gui.transform.config.file_list:
        name = entry.get('name')
        colormap_key = entry.get('colormap_key')
        filepath = entry.get('filepath')
        outdir = entry.get('outdir')
        renderer = EdgeWARN.gui.transform.render.GUILayerRenderer(filepath, outdir, colormap_key, name)
        renderer.convert_to_png()