"""
Lineage detection algorithm implementation.

This module provides the core lineage detection logic that identifies
merge and split events between old and new cell sets.
"""

from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from .events import (
    LineageEvent,
    LineageResult,
    MergeEvent,
    SplitEvent,
)
from .spatial import (
    build_spatial_index,
    find_overlapping_cells,
    bounds_overlap,
    select_dominant_parent,
    select_dominant_child,
    calculate_overlap_ratio,
)
from .buffer import LineageBuffer


# Default configuration constants
DEFAULT_OVERLAP_THRESHOLD = 0.15  # 15% overlap required for merge/split detection


class LineageDetector:
    """
    Detects merge and split events between old and new storm cell sets.
    
    This class implements the lineage detection algorithm specified in the
    Implementation Plan, including:
    - Merge detection (multiple parents -> single child)
    - Split detection (single parent -> multiple children)
    - Hysteresis buffering for false positive prevention
    
    Example:
        >>> detector = LineageDetector(buffer=LineageBuffer())
        >>> result = detector.detect(old_cells, new_cells)
        >>> for merge in result.merges:
        ...     print(f"Merge: {merge.parent_ids} -> {merge.child_id}")
    """
    
    def __init__(
        self,
        buffer: Optional[LineageBuffer] = None,
        overlap_threshold: float = DEFAULT_OVERLAP_THRESHOLD,
        io_manager: Optional[Any] = None,
    ):
        """
        Initialize the lineage detector.
        
        Args:
            buffer: LineageBuffer for hysteresis (created if None)
            overlap_threshold: Minimum overlap ratio for merge/split detection
            io_manager: IO manager for logging (optional)
        """
        self.buffer = buffer or LineageBuffer()
        self.overlap_threshold = overlap_threshold
        self.io_manager = io_manager
    
    def detect(
        self,
        old_cells: List[Dict[str, Any]],
        new_cells: List[Dict[str, Any]],
    ) -> LineageResult:
        """
        Detect merge and split events between old and new cell sets.
        
        This is the main entry point for lineage detection. It performs:
        1. Build spatial indices for both cell sets
        2. Detect merges (multiple old cells -> single new cell)
        3. Detect splits (single old cell -> multiple new cells)
        4. Apply hysteresis buffer for confirmation
        5. Build lineage result with event classifications
        
        Args:
            old_cells: List of cell dicts from previous scan
            new_cells: List of cell dicts from current scan
            
        Returns:
            LineageResult containing all detected events and classifications
        """
        result = LineageResult()
        
        # Build spatial indices
        old_index = build_spatial_index(old_cells)
        new_index = build_spatial_index(new_cells)
        
        # Get ID sets for quick lookup
        old_ids = set(old_index.get('cells_data', {}).keys())
        new_ids = set(new_index.get('cells_data', {}).keys())
        
        # Track which cells have been matched
        matched_old: set = set()
        matched_new: set = set()
        
        # === MERGE DETECTION ===
        # For each new cell, find overlapping old cells
        for new_cell in new_cells:
            new_id = int(new_cell.get('id', 0))
            
            # Find overlapping old cells
            overlapping = find_overlapping_cells(
                new_cell, old_index, self.overlap_threshold
            )
            
            if len(overlapping) > 1:
                # Multiple old cells overlap with this new cell -> potential MERGE
                parent_ids = [pid for pid, _ in overlapping]
                overlap_ratios = {pid: ratio for pid, ratio in overlapping}
                
                # Select dominant parent
                dominant_parent = select_dominant_parent(parent_ids, old_index)
                
                # Record in hysteresis buffer
                confirmed = self.buffer.record_potential_merge(
                    new_id, parent_ids, dominant_parent
                )
                
                if confirmed:
                    # Create merge event
                    merge = MergeEvent(
                        child_id=new_id,
                        parent_ids=parent_ids,
                        dominant_parent=dominant_parent,
                        overlap_ratios=overlap_ratios,
                    )
                    result.merges.append(merge)
                    
                    # Mark all parents as matched
                    for pid in parent_ids:
                        matched_old.add(pid)
                    
                    # Mark child as matched
                    matched_new.add(new_id)
                    
                    # Set event type for child
                    result.cell_events[new_id] = LineageEvent.MERGE
                    result.cell_lineage[new_id] = {
                        'parent_ids': parent_ids,
                        'dominant_parent': dominant_parent,
                    }
                    
                    # Log the event
                    self._log_merge_event(merge)
        
        # === SPLIT DETECTION ===
        # For each old cell, find overlapping new cells
        for old_cell in old_cells:
            old_id = int(old_cell.get('id', 0))
            
            if old_id in matched_old:
                continue  # Already matched in merge detection
            
            # Find overlapping new cells
            # H3 Fix: Use old cell area as denominator (parent-relative overlap)
            overlapping = self._find_split_overlaps(
                old_cell, new_index, self.overlap_threshold
            )
            
            if len(overlapping) > 1:
                # Single old cell overlaps multiple new cells -> potential SPLIT
                child_ids = [cid for cid, _ in overlapping]
                overlap_ratios = {cid: ratio for cid, ratio in overlapping}
                
                # Select dominant child
                dominant_child = select_dominant_child(child_ids, new_index)
                
                # Record in hysteresis buffer
                confirmed = self.buffer.record_potential_split(
                    old_id, child_ids, dominant_child
                )
                
                if confirmed:
                    # Create split event
                    split = SplitEvent(
                        parent_id=old_id,
                        child_ids=child_ids,
                        dominant_child=dominant_child,
                        overlap_ratios=overlap_ratios,
                    )
                    result.splits.append(split)
                    
                    # Mark parent as matched
                    matched_old.add(old_id)
                    
                    # Mark all children as matched
                    for cid in child_ids:
                        matched_new.add(cid)
                    
                    # Set event type for children
                    for cid in child_ids:
                        if cid == dominant_child:
                            # Dominant child inherits parent ID
                            result.cell_events[cid] = LineageEvent.ACTIVE
                            result.cell_lineage[cid] = {
                                'split_from': old_id,
                                'is_dominant_child': True,
                            }
                        else:
                            # Secondary children are marked as SPLIT
                            result.cell_events[cid] = LineageEvent.SPLIT
                            result.cell_lineage[cid] = {
                                'split_from': old_id,
                                'is_dominant_child': False,
                            }
                    
                    # Log the event
                    self._log_split_event(split)
        
        # === 1-TO-1 MATCH DETECTION ===
        # For remaining unmatched new cells, check for 1-to-1 overlap with remaining unmatched old cells
        for new_cell in new_cells:
            new_id = int(new_cell.get('id', 0))
            if new_id in matched_new:
                continue
            
            # 1. Check same-ID overlap first (since find_overlapping_cells skips same-ID)
            old_cells_data = old_index.get('cells_data', {})
            if new_id in old_cells_data and new_id not in matched_old:
                old_data = old_cells_data[new_id]
                ratio = calculate_overlap_ratio(old_data['bbox'], new_cell.get('bbox', []))
                if ratio >= self.overlap_threshold:
                    matched_old.add(new_id)
                    matched_new.add(new_id)
                    result.cell_events[new_id] = LineageEvent.ACTIVE
                    continue

            # 2. Check other 1-to-1 overlaps
            overlapping = find_overlapping_cells(
                new_cell, old_index, self.overlap_threshold
            )
            
            if len(overlapping) == 1:
                old_id, _ = overlapping[0]
                if old_id not in matched_old:
                    matched_old.add(old_id)
                    matched_new.add(new_id)
                    result.cell_events[new_id] = LineageEvent.ACTIVE
                    result.cell_events[old_id] = LineageEvent.ACTIVE
        
        # === UNMATCHED CELLS ===
        # Old cells not matched -> DISSIPATED
        for old_id in old_ids:
            if old_id not in matched_old:
                result.unmatched_old.append(old_id)
                result.cell_events[old_id] = LineageEvent.DISSIPATED
        
        # New cells not matched -> truly new cells
        for new_id in new_ids:
            if new_id not in matched_new:
                result.unmatched_new.append(new_id)
                result.cell_events[new_id] = LineageEvent.ACTIVE
        
        return result
    
    def _log_merge_event(self, merge: MergeEvent) -> None:
        """Log a merge event per PRD TR4 format."""
        if self.io_manager:
            parent_str = ', '.join(str(pid) for pid in merge.parent_ids)
            msg = f"Event Detected: Merge (IDs: {parent_str} -> {merge.child_id})"
            self.io_manager.write_info(f"[CellDetection] {msg}")
    
    def _log_split_event(self, split: SplitEvent) -> None:
        """Log a split event per PRD TR4 format."""
        if self.io_manager:
            child_str = ', '.join(str(cid) for cid in split.child_ids)
            msg = f"Event Detected: Split (IDs: {split.parent_id} -> {child_str})"
            self.io_manager.write_info(f"[CellDetection] {msg}")

    def _find_split_overlaps(
        self,
        old_cell: Dict[str, Any],
        new_index: Dict[int, Dict[str, Any]],
        threshold: float,
    ) -> List[Tuple[int, float]]:
        """
        Find new cells overlapping old cell with ratio = intersection / old_cell_area.
        
        Unlike find_overlapping_cells (which uses new cell area as denominator),
        this function uses the old cell's area. For split detection, we need to
        know what fraction of the *parent* footprint each child captured.
        
        Args:
            old_cell: The old (parent) cell dict with 'id' and 'bbox'
            new_index: Spatial index of new cells
            threshold: Minimum overlap ratio to include
            
        Returns:
            List of (new_cell_id, overlap_ratio) tuples, sorted descending.
        """
        old_bbox = old_cell.get('bbox', [])
        old_id = int(old_cell.get('id', 0))
        if not old_bbox or len(old_bbox) < 3:
            return []
        
        old_lats = [pt[0] for pt in old_bbox]
        old_lons = [pt[1] for pt in old_bbox]
        old_bounds = {
            'min_lat': min(old_lats), 'max_lat': max(old_lats),
            'min_lon': min(old_lons), 'max_lon': max(old_lons),
        }
        
        results = []
        new_cells_data = new_index.get('cells_data', {})
        for new_id, new_data in new_cells_data.items():
            # Don't skip same-ID here - we need to detect splits where parent keeps same ID
            if not bounds_overlap(old_bounds, new_data):
                continue
            # Key: old_bbox as first arg = denominator is old cell area
            ratio = calculate_overlap_ratio(old_bbox, new_data['bbox'])
            if ratio >= threshold:
                results.append((new_id, ratio))
        
        results.sort(key=lambda x: x[1], reverse=True)
        return results


def detect_lineage_events(
    old_cells: List[Dict[str, Any]],
    new_cells: List[Dict[str, Any]],
    buffer: Optional[LineageBuffer] = None,
    overlap_threshold: float = DEFAULT_OVERLAP_THRESHOLD,
    io_manager: Optional[Any] = None,
) -> LineageResult:
    """
    Convenience function to detect lineage events without instantiating detector.
    
    Args:
        old_cells: List of cell dicts from previous scan
        new_cells: List of cell dicts from current scan
        buffer: LineageBuffer for hysteresis (created if None)
        overlap_threshold: Minimum overlap ratio for detection
        io_manager: IO manager for logging
        
    Returns:
        LineageResult containing all detected events
    """
    detector = LineageDetector(
        buffer=buffer,
        overlap_threshold=overlap_threshold,
        io_manager=io_manager,
    )
    return detector.detect(old_cells, new_cells)