import json

class CellInformationRetriever:
    def __init__(self):
        """
        Initializes CellInformationRetriever
        """
    
    @staticmethod
    def load_storm_json(path):
        try:
            print(f"Attempting to open {path}")
            with open(path, 'r') as f:
                data = json.load(f)
            return data
        except Exception as e:
            print(f"Failed to retrieve JSON data: {e}")
            return None
        
