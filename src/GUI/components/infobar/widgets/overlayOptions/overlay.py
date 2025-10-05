import sys
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton
)
from PyQt6.QtCore import Qt

class SettingsOverlayWidget(QWidget):
    """Floating overlay widget with buttons, no HTML/CSS."""
    def __init__(self, width=300, height=114, parent=None):
        super().__init__(parent)
        self.setFixedSize(width, height)
        self.setWindowFlags(Qt.WindowType.FramelessWindowHint | Qt.WindowType.Tool)
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        
        # Layout
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        
        # Common button style
        button_style = """
            QPushButton {
                background-color: #2a73cc;
                color: white;
                font-weight: 600;
                border: 2px solid black;
                height: 20px;
            }
            QPushButton:hover {
                background-color: #3b82f6;
            }
        """
        
        # Buttons
        self.change_overlay_btn = QPushButton("Change Map Overlay")
        self.change_overlay_btn.setStyleSheet(button_style)
        self.change_overlay_btn.clicked.connect(self.on_change_overlay)
        layout.addWidget(self.change_overlay_btn)
        
        self.settings_btn = QPushButton("Settings")
        self.settings_btn.setStyleSheet(button_style)
        self.settings_btn.clicked.connect(self.on_settings)
        layout.addWidget(self.settings_btn)
        
        self.output_terminal_btn = QPushButton("Open Output Terminal")
        self.output_terminal_btn.setStyleSheet(button_style)
        self.output_terminal_btn.clicked.connect(self.on_output_terminal)
        layout.addWidget(self.output_terminal_btn)

    # Button slots
    def on_change_overlay(self):
        print("Change Map Overlay clicked")

    def on_settings(self):
        print("Settings clicked")

    def on_output_terminal(self):
        print("Open Output Terminal clicked")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    overlay = SettingsOverlayWidget()
    overlay.show()
    sys.exit(app.exec())
