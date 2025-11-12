import webbrowser
from pathlib import Path
from datetime import datetime
from util.io import IOManager
import util.file as fs

class OutputUtils:
    def __init__(self, io_manager=None):
        self.io_manager = io_manager or IOManager("[OutputUtils]")

    def save_and_open_map(self, m, timestamp=None):
        """
        Save the map to HTML file and open it in browser.
        
        Args:
            m: Folium map object
            timestamp: datetime object for timestamping the output file.
                      If None, uses current time.
        """
        # Ensure the directory structure exists
        fs.GUI_MAP_DIR.mkdir(parents=True, exist_ok=True)
        
        # Generate timestamped filename
        if timestamp is None:
            timestamp = datetime.now()
        
        # Format timestamp as YYYYMMDD-HHMMSS
        timestamp_str = timestamp.strftime("%Y%m%d-%H%M%S")
        filename = f"basemap_{timestamp_str}.html"
        output_path = fs.GUI_MAP_DIR / filename
        
        m.save(str(output_path))
        self.io_manager.write_debug(f"Basemap saved to {output_path}")

        webbrowser.open(str(output_path))
        self.io_manager.write_debug("Opened basemap in browser.")