import json
from pathlib import Path
from typing import Union, List, Any, Dict, Optional
import re
import util.file as fs

class CTAMJsonManager:
    """
    Utility class for loading, managing, and saving JSON files for STORMCELL and CELL data.
    """

    def __init__(self, identifier: Union[str, int]):
        """
        Initialize the manager with a specific identifier.
        Loads existing properties if the file exists.
        
        Args:
            identifier: A string timestamp (YYYYMMDD-HHMMSS) or a cell ID (int or numeric string).
        """
        self.identifier = identifier
        self.properties: Dict[str, Any] = self.load_json(identifier) or {}

    @staticmethod
    def get_json_path(identifier: Union[str, int]) -> Path:
        """
        Determines the file path based on the identifier type.

        Args:
            identifier: A string timestamp (YYYYMMDD-HHMMSS) or a cell ID (int or numeric string).

        Returns:
            Path object to the target JSON file.
        """
        # Check if identifier is a cell ID (integer or numeric string)
        is_cell_id = False
        try:
            int(identifier)
            # If it's a pure number, it's a cell ID. 
            # Note: Timestamps have dashes, so int() would fail.
            is_cell_id = True
        except ValueError:
            pass

        if is_cell_id:
            return fs.CELL_DIR / f"{identifier}.json"
        else:
            # Assume it's a timestamp if it's not a simple integer
            # Validate format just to be safe (YYYYMMDD-HHMMSS)
            if not re.match(r"^\d{8}-\d{6}$", str(identifier)):
                # If it doesn't match the timestamp format, we might want to log a warning
                # but for now we follow original logic and assume it's a stormcell file
                pass 
            
            return fs.STORMCELL_DIR / f"stormcells_{identifier}.json"

    @staticmethod
    def load_json(identifier: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        Smartly detects if the input is a timestamp or cell ID and loads the corresponding file.

        Args:
            identifier: A string timestamp (YYYYMMDD-HHMMSS) or a cell ID (int or numeric string).

        Returns:
            The loaded JSON data as a dictionary, or None if the file is not found or invalid.
        """
        target_path = CTAMJsonManager.get_json_path(identifier)

        if not target_path.exists():
            return None

        try:
            with open(target_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return None

    def update_keys(self, new_data: Dict[str, Any]) -> None:
        """
        Updates the properties with new data and saves to the JSON file.
        
        Args:
            new_data: A dictionary of new key-value pairs to update.
        """
        self.properties.update(new_data)
        
        target_path = self.get_json_path(self.identifier)
        
        # Ensure parent directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(target_path, 'w') as f:
            json.dump(self.properties, f, indent=4)

    @staticmethod
    def extract_keys(data: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
        """
        Extract specific keys from a dictionary. Supports dot notation for nested keys.

        Args:
            data: The source dictionary.
            keys: A list of keys to extract. Key can be "top_level" or "nested.key".

        Returns:
            A new dictionary containing only the found keys.
        """
        result = {}
        for key in keys:
            parts = key.split('.')
            value = data
            found = True
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    found = False
                    break
            
            if found:
                result[key] = value
        
        return result
