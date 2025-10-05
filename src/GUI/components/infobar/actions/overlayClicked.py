from PyQt6.QtCore import QObject, pyqtSlot, pyqtSignal
from GUI.components.infobar.widgets.overlayOptions.overlay import SettingsOverlayWidget

class overlayBridge(QObject):
    """Bridge object for JavaScript communication."""
    overlayButtonSignal = pyqtSignal()
    settingsButtonSignal = pyqtSignal()
    outputButtonSignal = pyqtSignal()

    @pyqtSlot()
    def overlayButtonClicked(self):
        """Called from JS when change overlay button is clicked"""
        print("[GUI] DEBUG: Change overlay button clicked!")
        self.overlayButtonSignal.emit()

    @pyqtSlot()
    def settingsButtonClicked(self):
        """Called from JS when settings button is clicked"""
        print("[GUI] DEBUG: Settings button clicked!")
        self.settingsButtonSignal.emit()
    
    @pyqtSlot
    def outputButtonClicked(self):
        """Called from JS when show output button is clicked"""
        print("[GUI] DEBUG: Show output button clicked!")
        self.outputButtonSignal.emit()


        
