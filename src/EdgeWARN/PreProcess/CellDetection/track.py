class StormCellTracker:
    def __init__(self, ps_old, ps_new):
        self.ps_old=  ps_old
        self.ps_new = ps_new

    @staticmethod
    def update_cells(entries, updated_data):
        """
        Updates main fields in entries from updated_data without modifying storm_history.
        
        entries: list of cell dicts
        updated_data: list of dicts with updated 'num_gates', 'centroid', 'max_refl', etc.
        """
        # Map updated_data by cell id for faster lookup
        updated_map = {int(cell['id']): cell for cell in updated_data}

        used_ids = set()

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
                print(f"[CellDetection] DEBUG: Updated cell {cell_id}")

        # Add NEW cells
        for cell in updated_data:
            cell_id = int(cell['id'])
            if cell_id not in used_ids:
                entries.append(cell)
                print(f"[CellDetection] DEBUG: Added new cell {cell_id}")
        
        return entries



