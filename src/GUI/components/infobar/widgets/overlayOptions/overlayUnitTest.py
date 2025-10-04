import sys
import os
from PyQt6.QtWidgets import QApplication, QMainWindow
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtCore import QUrl

class SettingsOverlay(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("HTML Viewer")
        self.setGeometry(100, 100, 800, 600)

        # Web view
        self.web_view = QWebEngineView()
        self.setCentralWidget(self.web_view)

        # HTML file in the same folder as the script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        html_file_path = os.path.join(current_dir, "overlay.html")

        # Load the HTML file
        self.web_view.load(QUrl.fromLocalFile(html_file_path))

if __name__ == "__main__":
    app = QApplication(sys.argv)
    viewer = SettingsOverlay()
    viewer.show()
    sys.exit(app.exec())
