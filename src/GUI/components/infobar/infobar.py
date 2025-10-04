import os
from PyQt6.QtWidgets import QWidget, QVBoxLayout
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtCore import QUrl

class InfoBar(QWidget):
    """
    A QWidget that renders a specific HTML file in a QWebEngineView.
    Can be embedded into another PyQt window.
    """

    # Hardcoded HTML file path
    HTML_FILE = "GUI/components/infobar/infobar.html"

    def __init__(self, parent=None):
        super().__init__(parent)

        # Layout for the web view
        self.layout = QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(self.layout)

        # Web view widget
        self.browser = QWebEngineView()
        file_path = os.path.abspath(self.HTML_FILE)
        self.browser.setUrl(QUrl.fromLocalFile(file_path))
        self.layout.addWidget(self.browser)
