import webbrowser
from pathlib import Path
from util.io import IOManager
import util.file as fs

class OutputUtils:
    def __init__(self, io_manager=None):
        self.io_manager = io_manager or IOManager("[OutputUtils]")

    def save_and_open_map(self, m):
        # Ensure the directory structure exists
        fs.GUI_MAP_DIR.mkdir(parents=True, exist_ok=True)
        
        output_path = fs.GUI_MAP_DIR / 'basemap.html'
        m.save(str(output_path))
        self.io_manager.write_debug(f"Basemap saved to {output_path}")

        webbrowser.open(str(output_path))
        self.io_manager.write_debug("Opened basemap in browser.")