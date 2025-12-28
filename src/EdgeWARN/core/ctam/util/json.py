import json
from pathlib import Path
from typing import Union, List, Any, Dict, Optional
import re
import util.file as fs

class CTAMJsonManager:
    """
    Utility class for loading and processing JSON files from STORMCELL_DIR and CELL_DIR.
    """

    @staticmethod
    def load_json(identifier: Union[str, int]) -> Optional[Dict[str, Any]]:
        """
        Smartly detects if the input is a timestamp or cell ID and loads the corresponding file.

        Args:
            identifier: A string timestamp (YYYYMMDD-HHMMSS) or a cell ID (int or numeric string).

        Returns:
            The loaded JSON data as a dictionary, or None if the file is not found or invalid.
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
            target_path = fs.CELL_DIR / f"{identifier}.json"
        else:
            # Assume it's a timestamp if it's not a simple integer
            # Validate format just to be safe (YYYYMMDD-HHMMSS)
            if not re.match(r"^\d{8}-\d{6}$", str(identifier)):
                # If it doesn't match the timestamp format, we can't load it
                # But maybe it's some other numeric format represented as string?
                # For now, strict check on timestamp format.
                pass 
            
            target_path = fs.STORMCELL_DIR / f"stormcells_{identifier}.json"

        if not target_path.exists():
            return None

        try:
            with open(target_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def extract_keys(data: Dict[str, Any], keys: List[str]) -> Dict[str, Any]:
        """
        Extract specific keys from a dictionary. Supports dot notation for nested keys.

        Args:
            data: The source dictionary.
            keys: A list of keys to extract. Key can be "top_level" or "nested.key".

        Returns:
            A new dictionary containing only the extracted keys. 
            The structure is flattened? Or preserved?
            Let's preserve the structure relative to the requested key name?
            Actually, commonly this returns a flat dict with the requested keys, 
            or a dict where the key is the requested string.
            
            Implementation: Returns a dict where keys are the input key strings, and values are the found values.
            If a key is missing, it is omitted from the result (or could be None).
            Let's omit missing keys to be clean.
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
