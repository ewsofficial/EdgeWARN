import sys
from PyQt6.QtWidgets import QApplication, QWidget

def main():
    app = QApplication(sys.argv)

    # Create a blank window
    window = QWidget()
    window.setWindowTitle("EdgeWARN")  # Title in the taskbar
    window.resize(960, 540)            # Window size

    window.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()
