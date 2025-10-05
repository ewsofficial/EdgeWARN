import os
from PyQt6.QtWidgets import QWidget, QVBoxLayout
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtCore import QUrl, Qt
from PyQt6.QtWebChannel import QWebChannel

class SettingsOverlayWidget(QWidget):
    """Floating HTML overlay widget with no whitespace."""
    def __init__(self, html_path=None, width=300, height=114, parent=None):
        super().__init__(parent)
        self.setFixedSize(width, height)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_OpaquePaintEvent, False)

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        self.setLayout(layout)

        self.web_view = QWebEngineView()
        self.web_view.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.web_view.setAttribute(Qt.WidgetAttribute.WA_OpaquePaintEvent, False)
        self.web_view.setStyleSheet("background: transparent;")
        layout.addWidget(self.web_view)

        if html_path is None:
            html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "overlay.html")
        else:
            html_path = os.path.abspath(html_path)

        if not os.path.exists(html_path):
            raise FileNotFoundError(f"HTML file not found: {html_path}")

        self.web_view.load(QUrl.fromLocalFile(html_path))
