import sys
from PyQt6.QtWidgets import QMainWindow, QApplication, QWidget, QLabel, QHBoxLayout, QVBoxLayout
from PyQt6.QtCore import Qt, QTimer, QTime, QDate
from PyQt6.QtGui import QPixmap

from GUI.components.infobar.actions.optionsClicked import HamburgerBridge
from GUI.components.infobar.widgets.overlayOptions.overlay import SettingsOverlayWidget


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("EdgeWARN GUI")
        self.setGeometry(200, 100, 1000, 700)

        central = QWidget()
        self.setCentralWidget(central)
        self.main_layout = QVBoxLayout(central)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # Top bar
        self.top_bar_height = 60
        self.top_bar = QWidget(self)
        self.top_bar.setFixedHeight(self.top_bar_height)
        self.top_bar.setStyleSheet("background-color: #8fb3e3; color: white;")
        self.top_layout = QHBoxLayout(self.top_bar)
        self.top_layout.setContentsMargins(0, 0, 10, 0)
        self.top_layout.setSpacing(10)

        # Left: logo
        self.logo_label = QLabel()
        pixmap = QPixmap("GUI/assets/EdgeWARN_logo.png")
        pixmap = pixmap.scaledToHeight(self.top_bar_height, Qt.TransformationMode.SmoothTransformation)
        self.logo_label.setPixmap(pixmap)
        self.top_layout.addWidget(self.logo_label, alignment=Qt.AlignmentFlag.AlignLeft)

        # Center: time/date
        self.time_label = QLabel("--:-- UTC")
        self.time_label.setStyleSheet("font-size: 24px; font-weight: bold;")
        self.date_label = QLabel("YYYY/MM/DD")
        self.date_label.setStyleSheet("font-size: 12px;")
        center_layout = QVBoxLayout()
        center_layout.addWidget(self.time_label, alignment=Qt.AlignmentFlag.AlignCenter)
        center_layout.addWidget(self.date_label, alignment=Qt.AlignmentFlag.AlignCenter)
        center_widget = QWidget()
        center_widget.setLayout(center_layout)
        self.top_layout.addWidget(center_widget, alignment=Qt.AlignmentFlag.AlignHCenter)

        # Right: hamburger using HamburgerBridge
        self.bridge = HamburgerBridge()
        self.hamburger_btn = QLabel("☰", self.top_bar)  # simple clickable label
        self.hamburger_btn.setStyleSheet("font-size: 18px;")
        self.hamburger_btn.setFixedSize(30, 25)
        self.hamburger_btn.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.top_layout.addWidget(self.hamburger_btn, alignment=Qt.AlignmentFlag.AlignRight)
        # Call the bridge method so its debug print runs, then it emits the signal
        self.hamburger_btn.mousePressEvent = lambda event: self.bridge.hamburgerClicked()

        # Overlay
        self.overlay = SettingsOverlayWidget(parent=self)
        self.overlay.hide()
        self.bridge.hamburgerClickedSignal.connect(self.toggle_overlay)

        # Timer for time/date
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_time)
        self.timer.start(1000)
        self.update_time()

        # Dummy central content
        self.main_content = QLabel("Main Application Content Goes Here")
        self.main_content.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.main_layout.addWidget(self.main_content)

        # Initial positioning
        self.update_top_bar_geometry()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.update_top_bar_geometry()
        if self.overlay.isVisible():
            self.overlay.hide()

    def moveEvent(self, event):
        super().moveEvent(event)
        if self.overlay.isVisible():
            self.overlay.hide()

    def update_top_bar_geometry(self):
        # 71% of window width, flush to left
        width = int(self.width() * 0.71)
        self.top_bar.setGeometry(0, 0, width, self.top_bar_height)

    def update_time(self):
        current_time = QTime.currentTime().toString("HH:mm")
        current_date = QDate.currentDate().toString("yyyy/MM/dd")
        self.time_label.setText(f"{current_time} UTC")
        self.date_label.setText(current_date)

    def toggle_overlay(self):
        if self.overlay.isVisible():
            self.overlay.hide()
        else:
            top_bar_pos = self.top_bar.mapToGlobal(self.top_bar.rect().topLeft())
            overlay_x = top_bar_pos.x() + self.top_bar.width() - self.overlay.width()
            overlay_y = top_bar_pos.y() + self.top_bar.height()
            self.overlay.move(overlay_x, overlay_y)
            self.overlay.show()
            self.overlay.raise_()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
