class StormCellTracker:
    def __init__(self, ps_old, ps_new, io_manager):
        self.ps_old = ps_old
        self.ps_new = ps_new
        self.io_manager = io_manager

    def update_cells(self, entries, updated_data):
        """
        Updates main fields in entries from updated_data without modifying storm_history.
        Removes cells that are not present in updated_data.
        
        entries: list of cell dicts
        updated_data: list of dicts with updated 'num_gates', 'centroid', 'max_refl', etc.
        """
        # Map updated_data by cell id for faster lookup
        updated_map = {int(cell['id']): cell for cell in updated_data}

        used_ids = set()
        updated_entries = []

        for cell in entries:
            cell_id = int(cell['id'])
            if cell_id in updated_map:
                updated = updated_map[cell_id]

                # Update only main fields, leave storm_history untouched
                cell['id'] = updated.get('id', cell['id'])
                cell['num_gates'] = updated.get('num_gates', cell['num_gates'])
                cell['centroid'] = updated.get('centroid', cell['centroid'])
                cell['max_refl'] = updated.get('max_refl', cell['max_refl'])
                cell['bbox'] = updated.get('bbox', cell['bbox'])

                used_ids.add(cell_id)
                updated_entries.append(cell)
                self.io_manager.write_debug(f"Updated cell {cell_id}")
            else:
                # Cell not found in updated_data - mark for deletion
                self.io_manager.write_debug(f"Removing cell {cell_id} (not found in new scan)")

        # Add NEW cells
        for cell in updated_data:
            cell_id = int(cell['id'])
            if cell_id not in used_ids:
                updated_entries.append(cell)
                self.io_manager.write_debug(f"Added new cell {cell_id}")
        
        # Return the filtered list (only cells that exist in updated_data)
        return updated_entries