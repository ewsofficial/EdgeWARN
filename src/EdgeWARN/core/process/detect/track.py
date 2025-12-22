class StormCellTracker:
    def __init__(self, ps_old, ps_new, io_manager):
        self.ps_old = ps_old
        self.ps_new = ps_new
        self.io_manager = io_manager

    def update_cells(self, entries, updated_data, timestamp=None):
        """
        Updates main fields in entries from updated_data without modifying storm_history.
        Removes cells that are not present in updated_data.
        
        entries: list of cell dicts
        updated_data: list of dicts with updated 'num_gates', 'centroid', 'max_refl', etc.
        timestamp: current scan timestamp (optional, if provided, updates matched cells)
        """
        # Map updated_data by cell id for faster lookup
        updated_map = {int(cell['id']): cell for cell in updated_data}

        used_ids = set()
        updated_entries = []

        unused_ids = 0
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
                
                # Update timestamp if provided (mark as active/current)
                if timestamp:
                    cell['timestamp'] = timestamp

                used_ids.add(cell_id)
                updated_entries.append(cell)
            else:
                # Cell not found in updated_data
                # Do NOT update timestamp (remains old/missing)
                unused_ids += 1
                updated_entries.append(cell)

        self.io_manager.write_info(f"Updated data for {len(updated_entries)} cells")
        self.io_manager.write_info(f"{unused_ids} cells not matched to new cells")

        # Add NEW cells
        new_cells = 0
        for cell in updated_data:
            cell_id = int(cell['id'])
            if cell_id not in used_ids:
                if timestamp:
                    cell['timestamp'] = timestamp
                updated_entries.append(cell)
                new_cells += 1

        self.io_manager.write_info(f"Added {new_cells} new cells to data")
        
        # Return the filtered list (only cells that exist in updated_data)
        return updated_entries
