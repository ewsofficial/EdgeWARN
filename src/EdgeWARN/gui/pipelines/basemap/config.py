"""
Configuration for Storm Cell Tooltips

This module defines what parameters to display in storm cell tooltips.
You can customize which fields to show, their labels, and formatting here.
"""

# Define the tooltip content structure
# Each section will be displayed in the tooltip with its fields

TOOLTIP_CONTENT = [
    {
        # Basic storm cell information
        "section": "basic",
        "title": "Storm Cell Information",
        "fields": [
            {"name": "cell_id", "label": "Storm Cell", "format": "string"},
            {"name": "max_refl", "label": "Max Reflectivity", "format": "number", "unit": "dBZ"},
            {"name": "num_gates", "label": "Gates", "format": "string"},
            {"name": "timestamp", "label": "Time", "format": "timestamp"}
        ]
    },
    {
        # Storm intensity metrics
        "section": "intensity", 
        "title": "Storm Intensity",
        "fields": [
            {"name": "EchoTop18", "label": "Echo Top 18 dBZ", "format": "number", "unit": "km"},
            {"name": "EchoTop30", "label": "Echo Top 30 dBZ", "format": "number", "unit": "km"},
            {"name": "EchoTop50", "label": "Echo Top 50 dBZ", "format": "number", "unit": "km"},
            {"name": "VIL", "label": "VIL", "format": "number", "unit": "kg/m²"},
            {"name": "VILDensity", "label": "VIL Density", "format": "number"},
            {"name": "PrecipRate", "label": "Precip Rate", "format": "number", "unit": "mm/hr"}
        ]
    },
    {
        # Atmospheric parameters
        "section": "atmospheric",
        "title": "Atmospheric Parameters", 
        "fields": [
            {"name": "MLCAPE", "label": "MLCAPE", "format": "number", "unit": "J/kg"},
            {"name": "MUCAPE", "label": "MUCAPE", "format": "number", "unit": "J/kg"},
            {"name": "DCAPE", "label": "DCAPE", "format": "number", "unit": "J/kg"},
            {"name": "LCL", "label": "LCL", "format": "number", "unit": "m"},
            {"name": "MLCIN", "label": "MLCIN", "format": "number", "unit": "J/kg"}
        ]
    },
    {
        # Wind shear parameters
        "section": "wind",
        "title": "Wind Shear",
        "fields": [
            {"name": "EBShear", "label": "EB Shear", "format": "number", "unit": "m/s"},
            {"name": "SRH01km", "label": "SRH 0-1km", "format": "number", "unit": "m²/s²"},
            {"name": "SRH02km", "label": "SRH 0-2km", "format": "number", "unit": "m²/s²"},
            {"name": "MeanWind_1-3kmAGL", "label": "Mean Wind 1-3km", "format": "number", "unit": "m/s"},
            {"name": "LLLR", "label": "LLLR", "format": "number", "unit": "K/km"},
            {"name": "MLLR", "label": "MLLR", "format": "number", "unit": "K/km"}
        ]
    },
    {
        # Lightning and hazard parameters
        "section": "lightning",
        "title": "Lightning/Hazards",
        "fields": [
            {"name": "MaxFED", "label": "Max FED", "format": "number"},
            {"name": "MaxFCD", "label": "Max FCD", "format": "number"},
            {"name": "AccumFCD", "label": "Accum FCD", "format": "number"},
            {"name": "CGFlashDensity", "label": "CG Flash Density", "format": "number", "unit": "flashes/km²/min"},
            {"name": "MESH", "label": "MESH", "format": "number", "unit": "in"}
        ]
    }
]

# Display options
DISPLAY_OPTIONS = {
    "hide_zero_values": True,
    "hide_na_values": True,
    "number_format": {
        "decimal_places": 2,
        "show_integer_without_decimals": True
    },
    "timestamp_format": "%b %d, %Y %H:%M UTC"
}

# Field to data mapping
FIELD_MAPPINGS = {
    "cell_id": "id",
    "max_refl": "max_refl", 
    "num_gates": "num_gates",
    "timestamp": "storm_history[0].timestamp"
}

# How to get each field from storm cell data
def get_field_value(cell_data, field_name):
    """
    Get the value for a field from storm cell data.
    
    Args:
        cell_data: Storm cell dictionary
        field_name: Name of the field to get
        
    Returns:
        The field value or None if not found
    """
    # Handle basic fields with custom mappings
    if field_name in FIELD_MAPPINGS:
        mapping = FIELD_MAPPINGS[field_name]
        if mapping == "id":
            return cell_data.get("id")
        elif mapping == "max_refl":
            return cell_data.get("max_refl")
        elif mapping == "num_gates":
            return cell_data.get("num_gates")
        elif "storm_history" in mapping:
            storm_history = cell_data.get("storm_history", [])
            if storm_history:
                if mapping == "storm_history[0].timestamp":
                    return storm_history[0].get("timestamp")
                else:
                    # Extract field name from mapping
                    field = mapping.split(".")[-1]
                    return storm_history[0].get(field)
    
    # Default: get from storm_history[0]
    storm_history = cell_data.get("storm_history", [])
    if storm_history:
        return storm_history[0].get(field_name)
    
    return None