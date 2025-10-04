import sys
from PyQt6.QtWidgets import QApplication, QMainWindow
from GUI.components.infobar.infobar import InfoBar  # import your HtmlGui module

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("EdgeWARN Main Window")
        self.setGeometry(100, 100, 800, 600)

        # Embed the HTML GUI widget
        self.html_widget = InfoBar(parent=self)
        self.setCentralWidget(self.html_widget)  # put it in the main window

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
