from PyQt6.QtCore import QObject, pyqtSlot, pyqtSignal
from GUI.components.infobar.widgets.overlayOptions.overlay import SettingsOverlayWidget

class Bridge(QObject):
    """Bridge object for JavaScript communication."""
    hamburgerClickedSignal = pyqtSignal()

    @pyqtSlot()
    def hamburgerClicked(self):
        """Called from JS when the hamburger icon is clicked."""
        print("[GUI] DEBUG: Hamburger clicked!")
        self.hamburgerClickedSignal.emit()
        
