import sys
from PyQt6.QtWidgets import QWidget, QLabel, QHBoxLayout, QVBoxLayout
from PyQt6.QtCore import Qt, QTimer, QTime, QDate

class InfoBar(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        # --- Main layout ---
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)

        # --- Top bar ---
        self.top_bar = QWidget(self)
        self.top_bar.setFixedHeight(60)
        self.top_bar.setStyleSheet("background-color: #8fb3e3; color: white;")

        top_layout = QHBoxLayout(self.top_bar)
        top_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.setSpacing(0)

        # Centered time/date
        self.time_label = QLabel("--:-- UTC")
        self.time_label.setStyleSheet("font-size: 24px; font-weight: bold;")
        self.date_label = QLabel("YYYY/MM/DD")
        self.date_label.setStyleSheet("font-size: 12px")

        center_layout = QVBoxLayout()
        center_layout.setContentsMargins(0, 0, 0, 0)
        center_layout.setSpacing(0)
        center_layout.addWidget(self.time_label, alignment=Qt.AlignmentFlag.AlignCenter)
        center_layout.addWidget(self.date_label, alignment=Qt.AlignmentFlag.AlignCenter)

        center_widget = QWidget()
        center_widget.setLayout(center_layout)
        top_layout.addWidget(center_widget, stretch=1, alignment=Qt.AlignmentFlag.AlignCenter)

        self.main_layout.addWidget(self.top_bar, alignment=Qt.AlignmentFlag.AlignTop)

        # --- Timer for updating time/date ---
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_time)
        self.timer.start(1000)
        self.update_time()

    def update_time(self):
        current_time = QTime.currentTime().toString("HH:mm")
        current_date = QDate.currentDate().toString("yyyy/MM/dd")
        self.time_label.setText(f"{current_time} UTC")
        self.date_label.setText(current_date)
