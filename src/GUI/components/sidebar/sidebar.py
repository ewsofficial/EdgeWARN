import sys
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QGridLayout, QScrollArea, QFrame, QSizePolicy
)
from PyQt6.QtGui import QFont, QColor, QPalette
from PyQt6.QtCore import Qt

class SidebarWidget(QWidget):
    """
    Main widget for the EdgeWARN sidebar, featuring a static title
    and a scrollable content area below.
    """
    def __init__(self):
        super().__init__()
        self.setStyleSheet("background-color: #B3C6E5;") # Overall background color
        self.setup_ui()

    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        # 1. Static Top Title (EdgeWARN)
        title_widget = QLabel("EdgeWARN")
        title_widget.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignTop)
        title_widget.setFont(QFont("Inter", 24, QFont.Weight.Bold))
        title_widget.setStyleSheet("""
            background-color: #1F4E79;
            color: white;
            padding: 10px 15px 10px 10px;
            border-top-right-radius: 12px;
        """)
        title_widget.setFixedHeight(50)
        main_layout.addWidget(title_widget)

        # 2. Scrollable Content Area
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        scroll_area.setStyleSheet("background-color: #B3C6E5;") # Match main background
        
        # Container for all content inside the scroll area
        content_widget = QWidget()
        content_layout = QVBoxLayout(content_widget)
        content_layout.setSpacing(8)
        content_layout.setContentsMargins(10, 10, 10, 10) # Padding around the blocks

        # --- Content Blocks ---
        
        # A. Cell ID Block
        cell_id_label = QLabel("Cell ID - #1574390")
        cell_id_label.setFont(QFont("Inter", 16, QFont.Weight.ExtraBold))
        cell_id_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        cell_id_label.setStyleSheet("""
            background-color: #1F4E79;
            color: #FF0000; /* Red text for Cell ID */
            padding: 10px;
            border-radius: 8px;
        """)
        cell_id_label.setFixedHeight(50)
        content_layout.addWidget(cell_id_label)

        # B. Tornado Threat Block (with multiple rows)
        tornado_data = {
            "Location": "38.745 N, 74.053 W",
            "Motion": "NE at 46 mph",
            "Max Reflectivity": "59.5 dBZ",
            "ProbWind": "76.0",
            "ProbHail": "81.0",
            "ProbTor": "67.0",
        }
        tornado_block = self._create_threat_block("TORNADO", tornado_data)
        content_layout.addWidget(tornado_block)
        
        # C. Wind Threat Block (with placeholder data based on the image structure)
        wind_data = {
            "Max Shear": "45 kt",
            "Vortex Tilt": "Low",
            "ProbTor": "67.0", # Value from image
        }
        wind_block = self._create_threat_block("WIND", wind_data)
        content_layout.addWidget(wind_block)

        # D. Flood Threat Block
        flood_data = {
            "FLDCHAR": "21.8",
        }
        flood_block = self._create_threat_block("FLOOD", flood_data)
        content_layout.addWidget(flood_block)
        
        # E. Placeholder Blank Blocks (to mimic the image's empty lower sections)
        # Note: These are necessary to push the content to the top.
        placeholder_block_1 = self._create_placeholder_block()
        content_layout.addWidget(placeholder_block_1)
        
        placeholder_block_2 = self._create_placeholder_block()
        content_layout.addWidget(placeholder_block_2)

        # Add a final spacer to push all blocks to the top of the scroll area
        content_layout.addStretch(1)

        scroll_area.setWidget(content_widget)
        main_layout.addWidget(scroll_area)
        
    def _create_threat_block(self, threat_name, data):
        """Helper function to create a standardized threat level information block."""
        block_widget = QWidget()
        block_layout = QVBoxLayout(block_widget)
        block_layout.setContentsMargins(10, 10, 10, 10)
        block_layout.setSpacing(5)

        # Header Label (e.g., THREAT LEVEL TORNADO)
        header_label = QLabel(f"THREAT LEVEL\n{threat_name}")
        header_label.setFont(QFont("Inter", 8, QFont.Weight.DemiBold))
        header_label.setAlignment(Qt.AlignmentFlag.AlignTop | Qt.AlignmentFlag.AlignLeft)
        header_label.setStyleSheet("color: white; border: none; padding: 0;")
        
        # Grid Layout for key/value pairs
        grid_layout = QGridLayout()
        grid_layout.setSpacing(5)
        
        # Place header on the left side of the block
        grid_layout.addWidget(header_label, 0, 0, len(data) + 1, 1) # Span all rows

        row = 0
        for key, value in data.items():
            # Label (Key)
            key_label = QLabel(key)
            key_label.setFont(QFont("Inter", 9, QFont.Weight.Normal))
            key_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
            key_label.setStyleSheet("color: #D3D3D3;") # Lighter gray for keys

            # Value (Value)
            value_label = QLabel(value)
            value_label.setFont(QFont("Inter", 9, QFont.Weight.DemiBold))
            value_label.setAlignment(Qt.AlignmentFlag.AlignRight)
            value_label.setStyleSheet("color: white;")

            # Add to grid layout
            grid_layout.addWidget(key_label, row, 1)
            grid_layout.addWidget(value_label, row, 2)
            row += 1

        # Use the grid layout for the content
        block_layout.addLayout(grid_layout)
        
        # Apply dark blue styling and rounded corners to the entire block
        block_widget.setStyleSheet("""
            QWidget {
                background-color: #1F4E79;
                border-radius: 8px;
            }
        """)
        
        return block_widget

    def _create_placeholder_block(self):
        """Creates a blank, dark blue block to match the original image."""
        block = QWidget()
        block.setFixedHeight(80) # Fixed height to resemble the blank sections
        block.setStyleSheet("""
            background-color: #1F4E79;
            border-radius: 8px;
        """)
        return block


class MainWindow(QMainWindow):
    """Main application window to host the sidebar."""
    def __init__(self):
        super().__init__()
        self.setWindowTitle("EdgeWARN Sidebar")
        self.setGeometry(100, 100, 400, 800) # Initial size, but it is resizable

        sidebar = SidebarWidget()
        
        # Set the sidebar as the central widget
        self.setCentralWidget(sidebar)


if __name__ == '__main__':
    # Initialize the application
    app = QApplication(sys.argv)
    
    # Set a consistent default font
    app.setFont(QFont("Inter", 10))

    # Create and show the main window
    window = MainWindow()
    window.show()

    # Start the event loop
    sys.exit(app.exec())
