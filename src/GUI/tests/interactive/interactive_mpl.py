import sys
import requests
from PySide6.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget
from PySide6.QtWebEngineWidgets import QWebEngineView
import plotly.graph_objects as go
import tempfile
import os

# -------------------------
# Load US states GeoJSON
# -------------------------
states_url = "https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json"
states = requests.get(states_url).json()

# -------------------------
# Create Plotly figure
# -------------------------
fig = go.Figure()

for feature in states["features"]:
    geom_type = feature["geometry"]["type"]
    state_name = feature["properties"]["name"]
    coords = feature["geometry"]["coordinates"]

    if geom_type == "Polygon":
        coords = [coords]

    for polygon in coords:
        lons, lats = zip(*polygon[0])
        fig.add_trace(go.Scattergeo(
            lon=lons,
            lat=lats,
            mode="lines",
            line=dict(color="black", width=2),
            fill="none",
            hoverinfo="text",
            hovertext=state_name,
            showlegend=False
        ))

fig.update_layout(
    title_text="Interactive US Map - States Only",
    geo=dict(
        scope="usa",
        projection_type="albers usa",
        showland=True,
        landcolor="lightgray",
        showlakes=True,
        lakecolor="lightblue"
    ),
    dragmode="pan"
)

# -------------------------
# Save figure to HTML with Plotly JS embedded
# -------------------------
tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
fig.write_html(tmp_file.name, include_plotlyjs=True, full_html=True)  # <-- embed JS
tmp_file.close()

# -------------------------
# Native Desktop GUI
# -------------------------
class MapWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("US Map GUI - States Only")
        self.setGeometry(100, 100, 1200, 800)

        central_widget = QWidget()
        layout = QVBoxLayout()
        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

        web_view = QWebEngineView()
        web_view.load(f"file://{os.path.abspath(tmp_file.name)}")  # absolute path
        layout.addWidget(web_view)

# -------------------------
# Run the app
# -------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MapWindow()
    window.show()
    sys.exit(app.exec())
