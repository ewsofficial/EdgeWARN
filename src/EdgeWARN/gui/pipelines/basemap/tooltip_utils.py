from datetime import datetime
from util.io import IOManager

class TooltipUtils:
    def __init__(self, io_manager=None):
        self.io_manager = io_manager or IOManager("[TooltipUtils]")
    
    def _format_number(self, value):
        """
        Format a number to 2 decimal places.
        
        Args:
            value: The value to format
            
        Returns:
            str: Formatted number string or original value if not a number
        """
        if value is None or value == 'N/A':
            return value
        
        # Try to convert to float and format to 2 decimal places
        try:
            num = float(value)
            if num.is_integer():
                # If it's a whole number, don't show decimal places for integers
                return str(int(num))
            else:
                # Show 2 decimal places for floating point numbers
                return f"{num:.2f}"
        except (ValueError, TypeError):
            # Return original value if it can't be converted to a number
            return value
    
    def create_storm_cell_tooltip(self, cell):
        """
        Create HTML tooltip content for a storm cell.
        
        Args:
            cell (dict): Storm cell data dictionary
            
        Returns:
            str: HTML formatted tooltip content
        """
        cell_id = cell['id']
        max_refl = cell['max_refl']
        num_gates = cell['num_gates']
        
        # Get additional data from storm_history if available
        storm_history = cell.get('storm_history', [])
        history_data = storm_history[0] if storm_history else {}
        
        # Basic information (always shown)
        tooltip_content = [
            f"<b>Storm Cell {cell_id}</b>",
            f"Max Refl: {self._format_number(max_refl)} dBZ",
            f"Gates: {num_gates}",
            ""
        ]
        
        # Add timestamp if available
        if 'timestamp' in history_data:
            timestamp_str = history_data['timestamp']
            try:
                # Parse ISO timestamp and format it to a readable format
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                formatted_time = dt.strftime("%b %d, %Y %H:%M UTC")
                tooltip_content.append(f"Time: {formatted_time}")
            except (ValueError, TypeError):
                # Fall back to original format if parsing fails
                tooltip_content.append(f"Time: {timestamp_str}")
            tooltip_content.append("")
        
        # Storm intensity metrics
        intensity_metrics = [
            ("Echo Top 18 dBZ", history_data.get('EchoTop18', 'N/A')),
            ("Echo Top 30 dBZ", history_data.get('EchoTop30', 'N/A')),
            ("Echo Top 50 dBZ", history_data.get('EchoTop50', 'N/A')),
            ("VIL", history_data.get('VIL', 'N/A')),
            ("VIL Density", history_data.get('VILDensity', 'N/A')),
            ("Precip Rate", history_data.get('PrecipRate', 'N/A')),
        ]
        
        tooltip_content.append("<b>Intensity:</b>")
        for label, value in intensity_metrics:
            if value != 'N/A' and value != 0:
                formatted_value = self._format_number(value)
                tooltip_content.append(f"{label}: {formatted_value}")
        tooltip_content.append("")
        
        # Atmospheric parameters
        atm_params = [
            ("MLCAPE", history_data.get('MLCAPE', 'N/A')),
            ("MUCAPE", history_data.get('MUCAPE', 'N/A')),
            ("DCAPE", history_data.get('DCAPE', 'N/A')),
            ("LCL", history_data.get('LCL', 'N/A')),
            ("MLCIN", history_data.get('MLCIN', 'N/A')),
        ]
        
        tooltip_content.append("<b>Atmospheric:</b>")
        for label, value in atm_params:
            if value != 'N/A' and value != 0:
                formatted_value = self._format_number(value)
                tooltip_content.append(f"{label}: {formatted_value}")
        tooltip_content.append("")
        
        # Wind shear and hodograph parameters
        wind_params = [
            ("EB Shear", history_data.get('EBShear', 'N/A')),
            ("SRH 0-1km", history_data.get('SRH01km', 'N/A')),
            ("SRH 0-2km", history_data.get('SRH02km', 'N/A')),
            ("Mean Wind 1-3km", history_data.get('MeanWind_1-3kmAGL', 'N/A')),
            ("LLLR", history_data.get('LLLR', 'N/A')),
            ("MLLR", history_data.get('MLLR', 'N/A')),
        ]
        
        tooltip_content.append("<b>Wind Shear:</b>")
        for label, value in wind_params:
            if value != 'N/A' and value != 0:
                formatted_value = self._format_number(value)
                tooltip_content.append(f"{label}: {formatted_value}")
        tooltip_content.append("")
        
        # Lightning and hazard parameters
        lightning_params = [
            ("Max FED", history_data.get('MaxFED', 'N/A')),
            ("Max FCD", history_data.get('MaxFCD', 'N/A')),
            ("Accum FCD", history_data.get('AccumFCD', 'N/A')),
            ("CG Flash Density", history_data.get('CGFlashDensity', 'N/A')),
            ("MESH", history_data.get('MESH', 'N/A')),
        ]
        
        tooltip_content.append("<b>Lightning/Hazards:</b>")
        for label, value in lightning_params:
            if value != 'N/A' and value != 0:
                formatted_value = self._format_number(value)
                tooltip_content.append(f"{label}: {formatted_value}")
        
        # Join all content with line breaks
        return "<br>".join(tooltip_content)
    
    def create_simple_tooltip(self, cell):
        """
        Create a simple tooltip with just basic information.
        This maintains compatibility with the original implementation.
        
        Args:
            cell (dict): Storm cell data dictionary
            
        Returns:
            str: Simple HTML formatted tooltip content
        """
        cell_id = cell['id']
        max_refl = cell['max_refl']
        num_gates = cell['num_gates']
        
        return f"Storm Cell {cell_id}<br>Max Refl: {self._format_number(max_refl)}<br>Gates: {num_gates}"
    
    def create_detailed_tooltip(self, cell):
        """
        Create a comprehensive tooltip with all available storm cell information.
        
        Args:
            cell (dict): Storm cell data dictionary
            
        Returns:
            str: Detailed HTML formatted tooltip content
        """
        return self.create_storm_cell_tooltip(cell)
    
    def create_custom_tooltip(self, cell, include_sections=None):
        """
        Create a custom tooltip with specified sections.
        
        Args:
            cell (dict): Storm cell data dictionary
            include_sections (list): List of section names to include.
                                   Options: 'basic', 'intensity', 'atmospheric', 'wind', 'lightning'
                                   If None, includes all sections.
        
        Returns:
            str: Custom HTML formatted tooltip content
        """
        if include_sections is None:
            return self.create_storm_cell_tooltip(cell)
        
        cell_id = cell['id']
        max_refl = cell['max_refl']
        num_gates = cell['num_gates']
        
        storm_history = cell.get('storm_history', [])
        history_data = storm_history[0] if storm_history else {}
        
        tooltip_content = []
        
        # Always include basic info if requested
        if 'basic' in include_sections:
            tooltip_content.extend([
                f"<b>Storm Cell {cell_id}</b>",
                f"Max Refl: {self._format_number(max_refl)} dBZ",
                f"Gates: {num_gates}",
                ""
            ])
            
            if 'timestamp' in history_data:
                timestamp_str = history_data['timestamp']
                try:
                    # Parse ISO timestamp and format it to a readable format
                    dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    formatted_time = dt.strftime("%b %d, %Y %H:%M UTC")
                    tooltip_content.append(f"Time: {formatted_time}")
                except (ValueError, TypeError):
                    # Fall back to original format if parsing fails
                    tooltip_content.append(f"Time: {timestamp_str}")
                tooltip_content.append("")
        
        # Add requested sections
        section_configs = {
            'intensity': [
                ("Echo Top 18 dBZ", history_data.get('EchoTop18')),
                ("Echo Top 30 dBZ", history_data.get('EchoTop30')),
                ("Echo Top 50 dBZ", history_data.get('EchoTop50')),
                ("VIL", history_data.get('VIL')),
                ("VIL Density", history_data.get('VILDensity')),
                ("Precip Rate", history_data.get('PrecipRate')),
            ],
            'atmospheric': [
                ("MLCAPE", history_data.get('MLCAPE')),
                ("MUCAPE", history_data.get('MUCAPE')),
                ("DCAPE", history_data.get('DCAPE')),
                ("LCL", history_data.get('LCL')),
                ("MLCIN", history_data.get('MLCIN')),
            ],
            'wind': [
                ("EB Shear", history_data.get('EBShear')),
                ("SRH 0-1km", history_data.get('SRH01km')),
                ("SRH 0-2km", history_data.get('SRH02km')),
                ("Mean Wind 1-3km", history_data.get('MeanWind_1-3kmAGL')),
                ("LLLR", history_data.get('LLLR')),
                ("MLLR", history_data.get('MLLR')),
            ],
            'lightning': [
                ("Max FED", history_data.get('MaxFED')),
                ("Max FCD", history_data.get('MaxFCD')),
                ("Accum FCD", history_data.get('AccumFCD')),
                ("CG Flash Density", history_data.get('CGFlashDensity')),
                ("MESH", history_data.get('MESH')),
            ]
        }
        
        for section in include_sections:
            if section in section_configs:
                tooltip_content.append(f"<b>{section.title()}:</b>")
                for label, value in section_configs[section]:
                    if value is not None and value != 0 and value != 'N/A':
                        formatted_value = self._format_number(value)
                        tooltip_content.append(f"{label}: {formatted_value}")
                tooltip_content.append("")
        
        return "<br>".join(tooltip_content)