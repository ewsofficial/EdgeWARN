from datetime import datetime
from util.io import IOManager

# Import configuration
from .config import TOOLTIP_CONTENT, DISPLAY_OPTIONS, get_field_value

class TooltipUtils:
    def __init__(self, io_manager=None):
        self.io_manager = io_manager or IOManager("[TooltipUtils]")
    
    def _format_number(self, value):
        """Format a number based on display options."""
        if value is None:
            return None
            
        try:
            num = float(value)
            
            # Hide zero values if configured
            if DISPLAY_OPTIONS.get('hide_zero_values') and num == 0:
                return None
                
            # Hide N/A values if configured  
            if DISPLAY_OPTIONS.get('hide_na_values') and (value == 'N/A' or value == 'n/a'):
                return None
            
            # Format number
            number_format = DISPLAY_OPTIONS.get('number_format', {})
            decimal_places = number_format.get('decimal_places', 2)
            
            if num.is_integer() and number_format.get('show_integer_without_decimals', True):
                return str(int(num))
            else:
                return f"{num:.{decimal_places}f}"
        except (ValueError, TypeError):
            return value
    
    def _format_value(self, value, field_config):
        """Format a value based on its field configuration."""
        if value is None:
            return None
            
        format_type = field_config.get('format', 'string')
        
        if format_type == 'number':
            formatted_value = self._format_number(value)
            if formatted_value is None:
                return None
                
            unit = field_config.get('unit')
            if unit:
                return f"{formatted_value} {unit}"
            return formatted_value
            
        elif format_type == 'timestamp':
            if isinstance(value, str):
                try:
                    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                    format_str = DISPLAY_OPTIONS.get('timestamp_format', "%b %d, %Y %H:%M UTC")
                    return dt.strftime(format_str)
                except (ValueError, TypeError):
                    return value
            return value
            
        else:  # string format
            return str(value)
    
    def create_storm_cell_tooltip(self, cell):
        """
        Create HTML tooltip content for a storm cell.
        Parses parameters from config.py and adds them to tooltip content.
        
        Args:
            cell (dict): Storm cell data dictionary
            
        Returns:
            str: HTML formatted tooltip content
        """
        tooltip_content = []
        
        # Process each section defined in config
        for section_config in TOOLTIP_CONTENT:
            section_title = section_config.get('title', 'Information')
            tooltip_content.append(f"<b>{section_title}:</b>")
            
            # Process each field in the section
            for field_config in section_config.get('fields', []):
                field_name = field_config.get('name')
                field_label = field_config.get('label', field_name)
                
                # Get value from storm cell data using config function
                value = get_field_value(cell, field_name)
                
                # Skip if no value found
                if value is None:
                    continue
                
                # Format the value
                formatted_value = self._format_value(value, field_config)
                
                # Skip if formatted value is None (e.g., zero values when hiding zeros)
                if formatted_value is None:
                    continue
                
                # Special handling for cell_id
                if field_name == 'cell_id':
                    tooltip_content.append(f"<b>Storm Cell {formatted_value}</b>")
                else:
                    tooltip_content.append(f"{field_label}: {formatted_value}")
            
            # Add spacing after section
            tooltip_content.append("")
        
        return "<br>".join(tooltip_content)
    
    def create_simple_tooltip(self, cell):
        """Create a simple tooltip with just basic information."""
        basic_section = None
        for section in TOOLTIP_CONTENT:
            if section['section'] == 'basic':
                basic_section = section
                break
        
        if not basic_section:
            return ""
            
        tooltip_content = []
        tooltip_content.append(f"<b>{basic_section['title']}:</b>")
        
        for field_config in basic_section.get('fields', []):
            field_name = field_config.get('name')
            field_label = field_config.get('label', field_name)
            
            value = get_field_value(cell, field_name)
            if value is None:
                continue
                
            formatted_value = self._format_value(value, field_config)
            if formatted_value is None:
                continue
            
            if field_name == 'cell_id':
                tooltip_content.append(f"<b>Storm Cell {formatted_value}</b>")
            else:
                tooltip_content.append(f"{field_label}: {formatted_value}")
        
        return "<br>".join(tooltip_content)
    
    def create_custom_tooltip(self, cell, sections=None):
        """Create a custom tooltip with specified sections."""
        if sections is None:
            return self.create_storm_cell_tooltip(cell)
        
        tooltip_content = []
        
        # Process only requested sections
        for section_config in TOOLTIP_CONTENT:
            if section_config['section'] not in sections:
                continue
                
            section_title = section_config.get('title', 'Information')
            tooltip_content.append(f"<b>{section_title}:</b>")
            
            for field_config in section_config.get('fields', []):
                field_name = field_config.get('name')
                field_label = field_config.get('label', field_name)
                
                value = get_field_value(cell, field_name)
                if value is None:
                    continue
                    
                formatted_value = self._format_value(value, field_config)
                if formatted_value is None:
                    continue
                
                if field_name == 'cell_id':
                    tooltip_content.append(f"<b>Storm Cell {formatted_value}</b>")
                else:
                    tooltip_content.append(f"{field_label}: {formatted_value}")
            
            tooltip_content.append("")
        
        return "<br>".join(tooltip_content)
    
    def get_available_sections(self):
        """Get list of all available tooltip sections."""
        return [section['section'] for section in TOOLTIP_CONTENT]
    
    def get_section_fields(self, section_name):
        """Get list of field names for a specific section."""
        for section in TOOLTIP_CONTENT:
            if section['section'] == section_name:
                return [field['name'] for field in section.get('fields', [])]
        return []