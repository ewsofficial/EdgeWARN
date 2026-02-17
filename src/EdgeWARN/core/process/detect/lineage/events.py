"""
Lineage event types and result structures for storm cell tracking.

This module defines the event classification system for storm cell lineage
tracking, including merge, split, active, and dissipated events.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum


class LineageEvent(Enum):
    """
    Enumeration of storm cell lineage event types.
    
    Attributes:
        MERGE: Multiple parent cells combining into a single child cell
        SPLIT: Single parent cell dividing into multiple child cells
        ACTIVE: Normal continuation of a cell with no lineage change
        DISSIPATED: Cell removed without merging (ceased to exist)
    """
    MERGE = "MERGE"
    SPLIT = "SPLIT"
    ACTIVE = "ACTIVE"
    DISSIPATED = "DISSIPATED"


@dataclass
class MergeEvent:
    """
    Represents a detected merge event where multiple parents combine into one child.
    
    Attributes:
        child_id: ID of the resulting child cell
        parent_ids: List of IDs of the parent cells that merged
        dominant_parent: ID of the dominant parent (highest max_refl or largest num_gates)
        overlap_ratios: Dictionary mapping parent_id to overlap ratio with child
    """
    child_id: int
    parent_ids: List[int]
    dominant_parent: int
    overlap_ratios: Dict[int, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert merge event to dictionary representation."""
        return {
            'child_id': self.child_id,
            'parent_ids': self.parent_ids,
            'dominant_parent': self.dominant_parent,
            'overlap_ratios': self.overlap_ratios,
        }


@dataclass
class SplitEvent:
    """
    Represents a detected split event where one parent divides into multiple children.
    
    Attributes:
        parent_id: ID of the parent cell that split
        child_ids: List of IDs of the resulting child cells
        dominant_child: ID of the dominant child (inherits parent ID)
        overlap_ratios: Dictionary mapping child_id to overlap ratio with parent
    """
    parent_id: int
    child_ids: List[int]
    dominant_child: int
    overlap_ratios: Dict[int, float] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert split event to dictionary representation."""
        return {
            'parent_id': self.parent_id,
            'child_ids': self.child_ids,
            'dominant_child': self.dominant_child,
            'overlap_ratios': self.overlap_ratios,
        }


@dataclass
class LineageResult:
    """
    Complete result of lineage detection between two cell sets.
    
    This structure contains all detected merge and split events, as well as
    lists of unmatched cells from both old and new sets.
    
    Attributes:
        merges: List of detected merge events
        splits: List of detected split events
        unmatched_old: IDs of old cells that dissipated (no match in new set)
        unmatched_new: IDs of new cells that appeared (no match in old set)
        cell_events: Mapping of cell_id to its determined LineageEvent
        cell_lineage: Mapping of cell_id to lineage info (parent_ids or split_from)
    """
    merges: List[MergeEvent] = field(default_factory=list)
    splits: List[SplitEvent] = field(default_factory=list)
    unmatched_old: List[int] = field(default_factory=list)
    unmatched_new: List[int] = field(default_factory=list)
    cell_events: Dict[int, LineageEvent] = field(default_factory=dict)
    cell_lineage: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    
    def is_empty(self) -> bool:
        """
        Check if there are no lineage events to process.
        
        Returns:
            True if no merges, splits, or unmatched cells exist.
        """
        return (
            len(self.merges) == 0 and
            len(self.splits) == 0 and
            len(self.unmatched_old) == 0 and
            len(self.unmatched_new) == 0
        )
    
    def get_event_type(self, cell_id: int) -> LineageEvent:
        """
        Get the lineage event type for a specific cell.
        
        Args:
            cell_id: The cell ID to look up
            
        Returns:
            The LineageEvent for the cell, defaults to ACTIVE if not found.
        """
        return self.cell_events.get(cell_id, LineageEvent.ACTIVE)
    
    def get_parent_ids(self, cell_id: int) -> List[int]:
        """
        Get the parent IDs for a merged cell.
        
        Args:
            cell_id: The child cell ID to look up
            
        Returns:
            List of parent IDs, empty if not a merge child.
        """
        lineage = self.cell_lineage.get(cell_id, {})
        return lineage.get('parent_ids', [])
    
    def get_split_from(self, cell_id: int) -> Optional[int]:
        """
        Get the parent ID that a cell split from.
        
        Args:
            cell_id: The child cell ID to look up
            
        Returns:
            Parent ID if this cell split from a parent, None otherwise.
        """
        lineage = self.cell_lineage.get(cell_id, {})
        return lineage.get('split_from')
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert lineage result to dictionary representation."""
        return {
            'merges': [m.to_dict() for m in self.merges],
            'splits': [s.to_dict() for s in self.splits],
            'unmatched_old': self.unmatched_old,
            'unmatched_new': self.unmatched_new,
            'cell_events': {k: v.value for k, v in self.cell_events.items()},
            'cell_lineage': self.cell_lineage,
        }
