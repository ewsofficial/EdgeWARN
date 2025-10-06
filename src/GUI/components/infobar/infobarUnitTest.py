import sys
from PyQt6.QtWidgets import QMainWindow, QApplication, QWidget, QLabel, QHBoxLayout, QVBoxLayout, QPushButton
from PyQt6.QtCore import Qt, QTimer, QTime, QDate
from PyQt6.QtGui import QPixmap

from GUI.components.infobar.widgets.overlayOptions.overlay import SettingsOverlayWidget


class InfoBar(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("EdgeWARN GUI")
        self.setGeometry(200, 100, 1000, 700)
        print("InfoBar: Initializing...")

        # Central widget
        central = QWidget()
        self.setCentralWidget(central)
        self.main_layout = QVBoxLayout(central)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # --- Top bar ---
        self.top_bar_height = 60
        self.top_bar = QWidget(self)
        self.top_bar.setFixedHeight(self.top_bar_height)
        self.top_bar.setStyleSheet("background-color: #8fb3e3; color: white;")
        self.top_layout = QHBoxLayout(self.top_bar)
        self.top_layout.setContentsMargins(10, 0, 10, 0)
        self.top_layout.setSpacing(10)

        # Left: logo
        self.logo_label = QLabel()
        pixmap = QPixmap("GUI/assets/EdgeWARN_logo.png")
        if not pixmap.isNull():
            pixmap = pixmap.scaledToHeight(self.top_bar_height, Qt.TransformationMode.SmoothTransformation)
            self.logo_label.setPixmap(pixmap)
        else:
            self.logo_label.setText("LOGO")
            print("InfoBar: Logo not found!")
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

        # Right: hamburger button
        self.hamburger_btn = QPushButton("☰")
        self.hamburger_btn.setFixedSize(30, 25)
        self.hamburger_btn.setStyleSheet(
            "font-size: 18px; background: transparent; border: none; color: white;"
        )
        self.hamburger_btn.clicked.connect(self.on_hamburger_clicked)
        self.top_layout.addWidget(self.hamburger_btn, alignment=Qt.AlignmentFlag.AlignRight)
        print("InfoBar: Hamburger button created")

        # Add top bar to main layout
        self.main_layout.addWidget(self.top_bar)

        # --- Overlay ---
        self.overlay = SettingsOverlayWidget(parent=self)
        self.overlay.hide()
        print("InfoBar: Overlay created, visible:", self.overlay.isVisible())

        # Timer for updating time/date
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_time)
        self.timer.start(1000)
        self.update_time()

        # Dummy central content
        self.main_content = QLabel("Main Application Content Goes Here")
        self.main_content.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.main_content.setStyleSheet("font-size: 16px; padding: 20px;")
        self.main_layout.addWidget(self.main_content)

        print("InfoBar: Initialization complete")

    def on_hamburger_clicked(self):
        print("InfoBar: Hamburger button clicked")
        self.toggle_overlay()

    def toggle_overlay(self):
        if self.overlay.isVisible():
            print("InfoBar: Hiding overlay")
            self.overlay.hide()
        else:
            print("InfoBar: Showing overlay")

            # Get top bar global position
            top_bar_global = self.top_bar.mapToGlobal(self.top_bar.rect().topLeft())

            # Position overlay so that top is flush with bottom of top bar
            # and right edges are aligned
            overlay_x = top_bar_global.x() + self.top_bar.width() - self.overlay.width()
            overlay_y = top_bar_global.y() + self.top_bar.height()

            self.overlay.move(overlay_x, overlay_y)
            self.overlay.show()
            self.overlay.raise_()
            self.overlay.activateWindow()
            print(f"InfoBar: Overlay shown at ({overlay_x}, {overlay_y})")

    def resizeEvent(self, event):
        super().resizeEvent(event)
        width = int(self.width() * 0.71)
        self.top_bar.setFixedWidth(width)
        if self.overlay.isVisible():
            print("InfoBar: resizeEvent - hiding overlay")
            self.overlay.hide()

    def moveEvent(self, event):
        super().moveEvent(event)
        if self.overlay.isVisible():
            print("InfoBar: moveEvent - hiding overlay")
            self.overlay.hide()

    def update_time(self):
        current_time = QTime.currentTime().toString("HH:mm")
        current_date = QDate.currentDate().toString("yyyy/MM/dd")
        self.time_label.setText(f"{current_time} UTC")
        self.date_label.setText(current_date)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    print("Main: Creating InfoBar window")
    window = InfoBar()
    window.show()
    sys.exit(app.exec())
