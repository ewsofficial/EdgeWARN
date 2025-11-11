import webbrowser
from pathlib import Path

class OutputUtils:
    def save_and_open_map(self, m):
        output_path = Path(__file__).parent / 'basemap.html'
        m.save(str(output_path))
        print(f"Basemap saved to {output_path}")

        webbrowser.open(str(output_path))
        print("Opened basemap in browser.")