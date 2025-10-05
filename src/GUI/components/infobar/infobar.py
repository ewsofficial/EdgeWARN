import os
from PyQt6.QtWidgets import QWidget, QVBoxLayout
from PyQt6.QtWebEngineWidgets import QWebEngineView
from PyQt6.QtWebChannel import QWebChannel
from PyQt6.QtCore import QUrl
from GUI.components.infobar.actions.optionsClicked import Bridge  # import your bridge

class InfoBar(QWidget):
    HTML_FILE = "GUI/components/infobar/infobar.html"

    def __init__(self, parent=None):
        super().__init__(parent)

        layout = QVBoxLayout()
        layout.setContentsMargins(0,0,0,0)
        self.setLayout(layout)

        self.browser = QWebEngineView()
        layout.addWidget(self.browser)

        file_path = os.path.abspath(self.HTML_FILE)
        self.browser.setUrl(QUrl.fromLocalFile(file_path))

        # Setup WebChannel bridge
        self.bridge = Bridge()
        self.channel = QWebChannel()
        self.channel.registerObject("bridge", self.bridge)
        self.browser.page().setWebChannel(self.channel)
