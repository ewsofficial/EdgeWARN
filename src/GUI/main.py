import sys
from PyQt6.QtWidgets import QApplication, QMainWindow, QWidget
from GUI.components.infobar.infobar import InfoBar
from GUI.components.sidebar.sidebar import LogoStripe

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("EdgeWARN Main Window")
        self.setGeometry(100, 100, 1000, 600)

        # Central container widget
        self.central_widget = QWidget(self)
        self.setCentralWidget(self.central_widget)

        # --- InfoBar (top-left) ---
        self.info_bar = InfoBar(parent=self.central_widget)
        self.info_bar.setFixedHeight(60)  # only fix height, width will be dynamic
        self.info_bar.show()

        # --- LogoStripe (top-right) ---
        self.logo_stripe = LogoStripe(parent=self.central_widget)
        self.logo_stripe.setFixedHeight(self.info_bar.top_bar.height())
        self.logo_stripe.show()

    def resizeEvent(self, event):
        # Make InfoBar 70% of central widget width
        self.info_bar.setFixedWidth(int(self.central_widget.width() * 0.70))
        self.info_bar.move(0, 0)

        # LogoStripe flush top-right
        self.logo_stripe.move(self.central_widget.width() - self.logo_stripe.width(), 0)
        self.logo_stripe.setFixedHeight(self.info_bar.top_bar.height())

        super().resizeEvent(event)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
