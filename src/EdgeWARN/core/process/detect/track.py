"""
Storm Cell Tracker with Lineage Detection.

This module provides the StormCellTracker class for tracking storm cells
across scans, including merge and split event detection.
"""

from typing import List, Dict, Any, Optional
from pathlib import Path

from .lineage import (
    LineageEvent,
    LineageResult,
    LineageBuffer,
    LineageDetector,
    detect_lineage_events,
)


class StormCellTracker:
    """
    Tracks storm cells across scans with lineage event detection.
    
    This class handles:
    - 1-to-1 cell ID matching and field updates
    - Merge detection (multiple parents -> single child)
    - Split detection (single parent -> multiple children)
    - Hysteresis buffering for false positive prevention
    
    Attributes:
        ps_old: Previous scan ProbSevere data
        ps_new: Current scan ProbSevere data
        io_manager: IO manager for logging
        lineage_buffer: Buffer for hysteresis filtering
    """
    
    def __init__(
        self,
        ps_old: Any,
        ps_new: Any,
        io_manager: Any,
        lineage_buffer: Optional[LineageBuffer] = None,
        overlap_threshold: float = 0.30,
    ):
        """
        Initialize the storm cell tracker.
        
        Args:
            ps_old: Previous scan ProbSevere data
            ps_new: Current scan ProbSevere data
            io_manager: IO manager for logging
            lineage_buffer: Optional pre-loaded LineageBuffer (loaded from disk if None)
            overlap_threshold: Minimum overlap ratio for merge/split detection
        """
        self.ps_old = ps_old
        self.ps_new = ps_new
        self.io_manager = io_manager
        self.overlap_threshold = overlap_threshold
        
        # Lineage buffer will be loaded from disk in detect_lineage_events
        self._lineage_buffer = lineage_buffer
    
    def detect_lineage_events(
        self,
        old_cells: List[Dict[str, Any]],
        new_cells: List[Dict[str, Any]],
        stormcell_dir: Optional[Path] = None,
    ) -> LineageResult:
        """
        Detect merge and split events between old and new cell sets.
        
        This method performs lineage detection with hysteresis buffering
        to prevent false positives from ProbSevere ID instability.
        
        Args:
            old_cells: List of cell dicts from previous scan
            new_cells: List of cell dicts from current scan
            stormcell_dir: Directory for buffer persistence (required if no buffer)
            
        Returns:
            LineageResult containing all detected events and classifications
            
        Example:
            >>> tracker = StormCellTracker(ps_old, ps_new, io_manager)
            >>> lineage = tracker.detect_lineage_events(entries_old, entries_new)
            >>> entries = tracker.update_cells(entries_old, entries_new, lineage=lineage)
        """
        # Load buffer from disk if not provided
        if self._lineage_buffer is None:
            if stormcell_dir is not None:
                self._lineage_buffer = LineageBuffer.load(stormcell_dir)
            else:
                self._lineage_buffer = LineageBuffer()
        
        # Create detector with the buffer
        detector = LineageDetector(
            buffer=self._lineage_buffer,
            overlap_threshold=self.overlap_threshold,
            io_manager=self.io_manager,
        )
        
        # Detect lineage events
        result = detector.detect(old_cells, new_cells)
        
        return result
    
    def update_cells(
        self,
        entries: List[Dict[str, Any]],
        updated_data: List[Dict[str, Any]],
        timestamp: Optional[str] = None,
        lineage: Optional[LineageResult] = None,
    ) -> List[Dict[str, Any]]:
        """
        Updates main fields in entries from updated_data without modifying storm_history.
        Removes cells that are not present in updated_data.
        Applies lineage event information to cell entries.
        
        Args:
            entries: List of cell dicts from previous scan
            updated_data: List of dicts with updated 'num_gates', 'centroid', 'max_refl', etc.
            timestamp: Current scan timestamp (optional, if provided, updates matched cells)
            lineage: LineageResult from detect_lineage_events() (optional)
            
        Returns:
            Updated list of cell entries with lineage information applied
        """
        # Map updated_data by cell id for faster lookup
        updated_map = {int(cell['id']): cell for cell in updated_data}
        
        # Get ID sets for fast path optimization
        old_ids = {int(c['id']) for c in entries}
        new_ids = set(updated_map.keys())
        
        # Fast path: No ID changes AND no lineage events
        if old_ids == new_ids and (lineage is None or lineage.is_empty()):
            return self._simple_update(entries, updated_map, timestamp)
        
        # Slow path: ID mismatch or active lineage events
        return self._apply_lineage_updates(
            entries, updated_map, lineage, timestamp
        )
    
    def _simple_update(
        self,
        entries: List[Dict[str, Any]],
        updated_map: Dict[int, Dict[str, Any]],
        timestamp: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Simple 1-to-1 update when no lineage events are detected.
        
        Args:
            entries: List of cell dicts
            updated_map: Dict mapping cell ID to updated data
            timestamp: Optional timestamp to apply
            
        Returns:
            List of updated cell entries
        """
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
                
                # Ensure lineage fields have defaults
                cell.setdefault('event_type', LineageEvent.ACTIVE.value)
                cell.setdefault('parent_ids', [])
                cell.setdefault('split_from', None)
                
                # Update timestamp if provided
                if timestamp:
                    cell['timestamp'] = timestamp
                
                updated_entries.append(cell)
        
        self.io_manager.write_info(f"Updated data for {len(updated_entries)} cells")
        
        return updated_entries
    
    def _apply_lineage_updates(
        self,
        entries: List[Dict[str, Any]],
        updated_map: Dict[int, Dict[str, Any]],
        lineage: Optional[LineageResult],
        timestamp: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Apply updates with lineage event handling.
        
        This method handles:
        - Normal 1-to-1 cell updates
        - Merge events (multiple parents -> single child)
        - Split events (single parent -> multiple children)
        - Dissipated cells
        - New cells
        
        Args:
            entries: List of cell dicts from previous scan
            updated_map: Dict mapping cell ID to updated data
            lineage: LineageResult from detect_lineage_events()
            timestamp: Optional timestamp to apply
            
        Returns:
            List of updated cell entries with lineage information
        """
        used_new_ids = set()
        updated_entries = []
        
        # Track statistics
        cells_updated = 0
        cells_dissipated = 0
        merge_children = 0
        split_children = 0
        
        # Build map of old entries by ID
        old_entries_map = {int(c['id']): c for c in entries}
        
        # Process merge events first
        if lineage:
            for merge in lineage.merges:
                child_id = merge.child_id
                if child_id not in updated_map:
                    continue
                
                # Get the child cell data
                child_data = updated_map[child_id]
                
                # Start with dominant parent's entry (to preserve history)
                dominant_entry = old_entries_map.get(merge.dominant_parent, {}).copy()
                
                # Update with child data
                dominant_entry['id'] = child_id
                dominant_entry['num_gates'] = child_data.get('num_gates', 0)
                dominant_entry['centroid'] = child_data.get('centroid')
                dominant_entry['max_refl'] = child_data.get('max_refl')
                dominant_entry['bbox'] = child_data.get('bbox')
                
                # Set lineage fields
                dominant_entry['event_type'] = LineageEvent.MERGE.value
                dominant_entry['parent_ids'] = merge.parent_ids
                dominant_entry['split_from'] = None
                
                if timestamp:
                    dominant_entry['timestamp'] = timestamp
                
                updated_entries.append(dominant_entry)
                used_new_ids.add(child_id)
                merge_children += 1
        
        # Process split events
        if lineage:
            for split in lineage.splits:
                parent_entry = old_entries_map.get(split.parent_id, {})
                
                for child_id in split.child_ids:
                    if child_id not in updated_map:
                        continue
                    
                    child_data = updated_map[child_id]
                    
                    if child_id == split.dominant_child:
                        # Dominant child inherits parent's entry (preserves history)
                        new_entry = parent_entry.copy()
                        new_entry['id'] = child_id
                        new_entry['event_type'] = LineageEvent.ACTIVE.value
                        new_entry['split_from'] = split.parent_id
                    else:
                        # Secondary children get new entries
                        new_entry = child_data.copy()
                        new_entry['event_type'] = LineageEvent.SPLIT.value
                        new_entry['split_from'] = split.parent_id
                        new_entry['parent_ids'] = []
                    
                    # Update fields from child data
                    new_entry['num_gates'] = child_data.get('num_gates', 0)
                    new_entry['centroid'] = child_data.get('centroid')
                    new_entry['max_refl'] = child_data.get('max_refl')
                    new_entry['bbox'] = child_data.get('bbox')
                    
                    if timestamp:
                        new_entry['timestamp'] = timestamp
                    
                    updated_entries.append(new_entry)
                    used_new_ids.add(child_id)
                    split_children += 1
        
        # Process normal 1-to-1 matches
        for cell in entries:
            cell_id = int(cell['id'])
            
            # Skip if already processed in merge/split
            if lineage and cell_id in {p for m in lineage.merges for p in m.parent_ids}:
                continue
            if lineage and cell_id in {s.parent_id for s in lineage.splits}:
                continue
            
            if cell_id in updated_map:
                updated = updated_map[cell_id]
                
                # Update only main fields
                cell['id'] = updated.get('id', cell['id'])
                cell['num_gates'] = updated.get('num_gates', cell['num_gates'])
                cell['centroid'] = updated.get('centroid', cell['centroid'])
                cell['max_refl'] = updated.get('max_refl', cell['max_refl'])
                cell['bbox'] = updated.get('bbox', cell['bbox'])
                
                # Set default lineage fields
                cell['event_type'] = LineageEvent.ACTIVE.value
                cell['parent_ids'] = []
                cell['split_from'] = None
                
                if timestamp:
                    cell['timestamp'] = timestamp
                
                used_new_ids.add(cell_id)
                updated_entries.append(cell)
                cells_updated += 1
            else:
                # Cell not found in updated_data - dissipated
                cells_dissipated += 1
                # Do NOT append to updated_entries (cell is removed)
        
        # Add truly new cells (not from merge/split)
        new_cells = 0
        for cell_id, cell_data in updated_map.items():
            if cell_id not in used_new_ids:
                # This is a truly new cell
                new_entry = cell_data.copy()
                new_entry['event_type'] = LineageEvent.ACTIVE.value
                new_entry['parent_ids'] = []
                new_entry['split_from'] = None
                
                if timestamp:
                    new_entry['timestamp'] = timestamp
                
                updated_entries.append(new_entry)
                new_cells += 1
        
        # Log statistics
        self.io_manager.write_info(f"Updated data for {cells_updated} cells")
        self.io_manager.write_info(f"{cells_dissipated} cells dissipated")
        self.io_manager.write_info(f"{new_cells} new cells added")
        if merge_children > 0:
            self.io_manager.write_info(f"{merge_children} merge children processed")
        if split_children > 0:
            self.io_manager.write_info(f"{split_children} split children processed")
        
        return updated_entries
    
    def get_lineage_buffer(self) -> Optional[LineageBuffer]:
        """Get the current lineage buffer instance."""
        return self._lineage_buffer
    
    def save_lineage_buffer(self, stormcell_dir: Path) -> bool:
        """
        Save the lineage buffer to disk.
        
        Args:
            stormcell_dir: Directory to save the buffer file
            
        Returns:
            True if save successful, False otherwise
        """
        if self._lineage_buffer is None:
            return False
        
        # Clear confirmed events before saving
        self._lineage_buffer.clear_confirmed_events()
        
        # End scan processing (prune, save)
        self._lineage_buffer.end_scan(stormcell_dir)
        
        return True
