from PyQt6.QtWidgets import QWidget, QLabel, QVBoxLayout
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QPixmap
import os

class LogoStripe(QWidget):
    def __init__(self, logo_width=60, parent=None):
        super().__init__(parent)
        self.logo_width = logo_width
        self.setFixedWidth(self.logo_width)
        self.setStyleSheet("background-color: #194771ff;")  # Vertical stripe color

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Path to logo
        logo_path = os.path.join("GUI", "assets", "EdgeWARN_logo.png")
        if os.path.exists(logo_path):
            self.logo_label = QLabel(self)
            pixmap = QPixmap(logo_path)
            pixmap = pixmap.scaledToWidth(self.logo_width, Qt.TransformationMode.SmoothTransformation)
            self.logo_label.setPixmap(pixmap)
            layout.addWidget(self.logo_label)
        else:
            # Placeholder if logo not found
            label = QLabel("LOGO", self)
            label.setStyleSheet("color: white; font-weight: bold; font-size: 20px;")
            label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            layout.addWidget(label)
