import sys
import os
from PyQt6.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtGui import QIcon
from PyQt6.QtCore import QUrl
from GUI.components.infobar.widgets.overlayOptions.overlay import SettingsOverlayWidget

class HtmlGui(QMainWindow):
    def __init__(self, html_file, icon_path):
        super().__init__()
        self.setWindowTitle("EdgeWARN GUI Test")
        self.setGeometry(200, 100, 1000, 700)
        self.setWindowIcon(QIcon(icon_path))

        # Central container widget
        central_container = QWidget()
        self.setCentralWidget(central_container)

        self.layout = QVBoxLayout()
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)
        central_container.setLayout(self.layout)

        # Main browser
        self.browser = QWebEngineView()
        self.layout.addWidget(self.browser)

        file_path = os.path.abspath(html_file)
        self.browser.setUrl(QUrl.fromLocalFile(file_path))
        self.browser.loadFinished.connect(lambda ok: self.update_overlay_position())

        # Overlay widget
        overlay_html_path = os.path.join("GUI", "widgets", "overlayOptions", "overlay.html")
        self.overlay_widget = SettingsOverlayWidget(
            html_path=overlay_html_path,
            width=300,
            height=114,
            parent=central_container
        )
        self.overlay_widget.show()
        self.overlay_widget.raise_()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.update_overlay_position()

    def update_overlay_position(self):
        """Align overlay flush below topbar and right-aligned dynamically."""
        js_code = """
            (function() {
                const topbar = document.querySelector('.top-bar');
                if (!topbar) return null;
                const rect = topbar.getBoundingClientRect();
                return {height: rect.height, right: rect.right};
            })();
        """

        def on_topbar_rect(result):
            if not result:
                return
            topbar_height = result['height']
            topbar_right = result['right']

            overlay_x = int(topbar_right - self.overlay_widget.width())
            overlay_y = int(topbar_height)  # flush below topbar

            # Map coordinates relative to QWebEngineView widget
            self.overlay_widget.move(overlay_x, overlay_y)

        self.browser.page().runJavaScript(js_code, on_topbar_rect)


if __name__ == "__main__":
    icon_path = os.path.abspath("GUI/assets/EdgeWARN_logo.png")

    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(icon_path))

    window = HtmlGui("GUI/components/infobar/infobar.html", icon_path)
    window.show()

    sys.exit(app.exec())
