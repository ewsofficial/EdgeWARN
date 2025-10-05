import sys
import os
from PyQt6.QtWidgets import QWidget, QVBoxLayout, QApplication
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtCore import QUrl
from PyQt6.QtGui import QIcon
from PyQt6.QtWebChannel import QWebChannel

from GUI.components.infobar.actions.optionsClicked import HamburgerBridge
from GUI.components.infobar.widgets.overlayOptions.overlay import SettingsOverlayWidget

class InfoBar(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("EdgeWARN GUI Widget")
        self.setMinimumSize(1000, 700)

        # Hard-coded paths
        self.icon_path = os.path.abspath("GUI/assets/EdgeWARN_logo.png")
        self.html_file = os.path.abspath("GUI/components/infobar/infobar.html")

        # Layout
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

        # Main browser
        self.browser = QWebEngineView()
        self.browser.setUrl(QUrl.fromLocalFile(self.html_file))
        self.browser.loadFinished.connect(self.on_page_load)
        layout.addWidget(self.browser)

        # WebChannel Bridge
        self.bridge = HamburgerBridge()
        self.channel = QWebChannel()
        self.channel.registerObject("bridge", self.bridge)
        self.browser.page().setWebChannel(self.channel)

        # Overlay widget (initially hidden)
        self.overlay_widget = SettingsOverlayWidget(width=300, height=114, parent=self)
        self.overlay_widget.hide()

        # Connect bridge signal to toggle overlay
        self.bridge.hamburgerClickedSignal.connect(self.toggle_overlay)

    def on_page_load(self, ok):
        if not ok:
            print("Failed to load HTML page.")

    def toggle_overlay(self):
        if self.overlay_widget.isVisible():
            self.overlay_widget.hide()
        else:
            js_code = """
                (function() {
                    const topbar = document.querySelector('.top-bar');
                    if (!topbar) return null;
                    const rect = topbar.getBoundingClientRect();
                    return {bottom: rect.bottom, right: rect.right};
                })();
            """

            def on_topbar_rect(rect):
                if not rect:
                    return

                # Map JS page coordinates to global screen coordinates
                page_bottom = rect['bottom']
                page_right = rect['right']
                view_pos = self.browser.mapToGlobal(self.browser.rect().topLeft())

                overlay_x = int(view_pos.x() + page_right - self.overlay_widget.width())
                overlay_y = int(view_pos.y() + page_bottom)

                self.overlay_widget.move(overlay_x, overlay_y)
                self.overlay_widget.show()
                self.overlay_widget.raise_()

            self.browser.page().runJavaScript(js_code, on_topbar_rect)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self.overlay_widget.isVisible():
            self.overlay_widget.hide()  # hide overlay on resize

    def moveEvent(self, event):
        super().moveEvent(event)
        if self.overlay_widget.isVisible():
            self.overlay_widget.hide()  # hide overlay on move


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(os.path.abspath("GUI/assets/EdgeWARN_logo.png")))

    widget = InfoBar()
    widget.show()

    sys.exit(app.exec())
