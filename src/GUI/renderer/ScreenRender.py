from PyQt6.Widgets import QApplication, QMainWindow, QPushButton
from PyQt6.QtCore import Qt
from datetime import datetime

class Initialize:
  def __init__(self):
    super().__init__()

    # Configuration
    self.setWindowTitle("EdgeWARN Weather Hazards Nowcasting")
    self.showFullScreen()
    
