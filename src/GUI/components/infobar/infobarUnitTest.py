import sys
import os
from PyQt6.QtWidgets import QApplication, QMainWindow
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtCore import QUrl
from PyQt6.QtGui import QIcon
from PyQt6.QtWebChannel import QWebChannel

from GUI.components.infobar.actions.optionsClicked import Bridge
from GUI.components.infobar.widgets.overlayOptions.overlay import SettingsOverlayWidget

class HtmlGui(QMainWindow):
    def __init__(self, html_file, icon_path):
        super().__init__()
        self.setWindowTitle("EdgeWARN GUI Test")
        self.setGeometry(200, 100, 1000, 700)
        self.setWindowIcon(QIcon(icon_path))

        # Main browser
        self.browser = QWebEngineView()
        self.setCentralWidget(self.browser)
        self.browser.loadFinished.connect(self.on_page_load)
        self.browser.setUrl(QUrl.fromLocalFile(os.path.abspath(html_file)))

        # WebChannel Bridge
        self.bridge = Bridge()
        self.channel = QWebChannel()
        self.channel.registerObject("bridge", self.bridge)
        self.browser.page().setWebChannel(self.channel)

        # Overlay widget (initially hidden)
        overlay_html_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "widgets", "overlayOptions", "overlay.html")
        )

        self.overlay_widget = SettingsOverlayWidget(
            html_path=overlay_html_path,
            width=300,
            height=114,
            parent=self
        )
        self.overlay_widget.hide()

        # Connect the bridge signal to toggle overlay
        self.bridge.hamburgerClickedSignal.connect(self.toggle_overlay)

    def on_page_load(self, ok):
        if not ok:
            print("Failed to load infobar HTML page.")

    def toggle_overlay(self):
        if self.overlay_widget.isVisible():
            self.overlay_widget.hide()
        else:
            # Detect topbar size dynamically
            def on_topbar_rect(result):
                if not result:
                    return
                # result should be a dict with .bottom and .right from JS
                page_bottom = result['bottom']
                page_right = result['right']

                # Map page coordinates to the widget coordinates
                overlay_x = int(page_right - self.overlay_widget.width())
                overlay_y = int(page_bottom)  # top edge flush with bottom of topbar

                self.overlay_widget.move(overlay_x, overlay_y)
                self.overlay_widget.show()
                self.overlay_widget.raise_()

            # JS to get the topbar's bounding rect
            js_code = """
                (function() {
                    const topbar = document.querySelector('.top-bar');
                    if (!topbar) return null;
                    const rect = topbar.getBoundingClientRect();
                    return {bottom: rect.bottom, right: rect.right};
                })();
            """

            self.browser.page().runJavaScript(js_code, on_topbar_rect)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if self.overlay_widget.isVisible():
            self.overlay_widget.hide()  # retract overlay on resize

    def moveEvent(self, event):
        super().moveEvent(event)
        if self.overlay_widget.isVisible():
            self.overlay_widget.hide()  # retract overlay on move

if __name__ == "__main__":
    icon_path = os.path.abspath("GUI/assets/EdgeWARN_logo.png")
    html_file = os.path.abspath("GUI/components/infobar/infobar.html")

    app = QApplication(sys.argv)
    app.setWindowIcon(QIcon(icon_path))

    window = HtmlGui(html_file, icon_path)
    window.show()

    sys.exit(app.exec())
