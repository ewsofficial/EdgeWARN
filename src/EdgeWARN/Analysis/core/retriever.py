import json
from datetime import datetime

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
        
    @staticmethod
    def get_storm_cells(data):
        """
        Retrieves all unique storm cell IDs from the JSON data
        Returns:
        - list of all storm cell IDs
        """
        try:
            cell_ids = [cell['id'] for cell in data]
            unique_cell_ids = list(set(cell_ids))  # Remove duplicates
            print(f"Found {len(unique_cell_ids)} unique storm cells")
            return sorted(unique_cell_ids)  # Return sorted list
        except Exception as e:
            print(f"Error retrieving storm cell IDs: {e}")
            return []
        
    @staticmethod
    def get_storm_history(data, cell_id):
        try:
            for cell in data:
                if cell['id'] == cell_id:
                    storm_history = cell['storm_history']
                    print(f"Successfully retrieved storm cell history for cell number {cell_id}")
                    return storm_history
        except Exception as e:
            print(f"Error retrieving storm cell history for cell number {cell_id}: {e} ")
            return None

    @staticmethod
    def get_storm_data(storm_history, variable_name):
        """
        Retrieves specific data from storm history entries
        Inputs:
        - storm_history: list of storm history entries
        - variable_name: name of data being retrieved 
        Returns:
        - list of tuples: (value, datetime_object) for each entry where the variable exists
        """
        data = []
        try:
            for entry in storm_history:
                if variable_name in entry:
                    value = entry[variable_name]
                    ts = entry.get('timestamp')
                    if ts:
                        # Convert timestamp from ISO format back to datetime object
                        timestamp = datetime.fromisoformat(ts)
                        data.append((value, timestamp))
            return data
        except Exception as e:
            print(f"Error retrieving {variable_name} from storm history: {e}")
            return []

class CellInformationSaver:
    def __init__():
        """
        Initializes CellInformationSaver class
        """
    
    @staticmethod
    def save_json(path, data):
        try:
            print(f"Attempting to save JSON to {path}")
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"Saved JSON data to {path}")
        except Exception as e:
            print(f"Failed to save JSON to {path}: {e}")

    @staticmethod
    def create_analysis_dict(data):
        """
        Add an empty 'analysis' dictionary to each storm history entry.
        
        Args:
            storm_data: List of storm cell dictionaries
            
        Returns:
            Modified storm data with empty analysis dicts added to each history entry
        """
        for storm_cell in data:
            if "storm_history" in storm_cell:
                for history_entry in storm_cell["storm_history"]:
                    # Add empty analysis dictionary if it doesn't exist
                    if "analysis" not in history_entry:
                        history_entry["analysis"] = {}
        
        return data