"""
Hysteresis buffer for lineage event confirmation.

This module provides the LineageBuffer class that tracks potential merge/split
events across multiple scans to prevent false positives from ProbSevere ID
instability.

The buffer persists state to disk since StormCellTracker is re-instantiated
each scan cycle.
"""

import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
import time


@dataclass
class PendingMerge:
    """
    Tracks a potential merge event awaiting confirmation.
    
    Attributes:
        child_id: ID of the resulting child cell
        parent_ids: Set of parent cell IDs that may be merging
        count: Number of consecutive scans this merge has been detected
        first_seen: Timestamp when first detected
        last_seen: Timestamp when most recently detected
        dominant_parent: ID of the dominant parent (updated each detection)
    """
    child_id: int
    parent_ids: Set[int]
    count: int = 1
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    dominant_parent: int = 0
    last_scan_number: int = -1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'child_id': self.child_id,
            'parent_ids': list(self.parent_ids),
            'count': self.count,
            'first_seen': self.first_seen,
            'last_seen': self.last_seen,
            'dominant_parent': self.dominant_parent,
            'last_scan_number': self.last_scan_number,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PendingMerge':
        """Create from dictionary (JSON deserialization)."""
        return cls(
            child_id=data['child_id'],
            parent_ids=set(data['parent_ids']),
            count=data.get('count', 1),
            first_seen=data.get('first_seen', time.time()),
            last_seen=data.get('last_seen', time.time()),
            dominant_parent=data.get('dominant_parent', 0),
            last_scan_number=data.get('last_scan_number', -1),
        )


@dataclass
class PendingSplit:
    """
    Tracks a potential split event awaiting confirmation.
    
    Attributes:
        parent_id: ID of the parent cell that may be splitting
        child_ids: Set of child cell IDs that may result from split
        count: Number of consecutive scans this split has been detected
        first_seen: Timestamp when first detected
        last_seen: Timestamp when most recently detected
        dominant_child: ID of the dominant child (updated each detection)
    """
    parent_id: int
    child_ids: Set[int]
    count: int = 1
    first_seen: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    dominant_child: int = 0
    last_scan_number: int = -1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'parent_id': self.parent_id,
            'child_ids': list(self.child_ids),
            'count': self.count,
            'first_seen': self.first_seen,
            'last_seen': self.last_seen,
            'dominant_child': self.dominant_child,
            'last_scan_number': self.last_scan_number,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'PendingSplit':
        """Create from dictionary (JSON deserialization)."""
        return cls(
            parent_id=data['parent_id'],
            child_ids=set(data['child_ids']),
            count=data.get('count', 1),
            first_seen=data.get('first_seen', time.time()),
            last_seen=data.get('last_seen', time.time()),
            dominant_child=data.get('dominant_child', 0),
            last_scan_number=data.get('last_scan_number', -1),
        )


class LineageBuffer:
    """
    Tracks potential merge/split events across scans for hysteresis filtering.
    
    This buffer persists state to disk since StormCellTracker is re-instantiated
    each scan cycle. It requires a minimum number of consecutive detections
    before confirming a lineage event.
    
    Attributes:
        min_confirmations: Minimum consecutive scans to confirm an event
        max_pending: Maximum number of pending events to track
        prune_after_scans: Prune entries inactive for this many scans
        pending_merges: Dict mapping child_id to PendingMerge
        pending_splits: Dict mapping parent_id to PendingSplit
        confirmed_merges: Set of confirmed merge child IDs (this scan)
        confirmed_splits: Set of confirmed split parent IDs (this scan)
    
    Example:
        >>> buffer = LineageBuffer(min_confirmations=2)
        >>> buffer.record_potential_merge(405, [405, 408], dominant=405)
        False  # Not yet confirmed
        >>> buffer.record_potential_merge(405, [405, 408], dominant=405)
        True   # Confirmed after 2 scans
    """
    
    BUFFER_FILE = "lineage_buffer.json"
    
    def __init__(
        self,
        min_confirmations: int = 2,
        max_pending: int = 100,
        prune_after_scans: int = 5,
        scan_interval_seconds: float = 300.0
    ):
        """
        Initialize the lineage buffer.
        
        Args:
            min_confirmations: Minimum consecutive detections to confirm event
            max_pending: Maximum pending events to track (memory limit)
            prune_after_scans: Prune entries inactive for this many scan intervals
            scan_interval_seconds: Expected time between scans (for pruning)
        """
        self.min_confirmations = min_confirmations
        self.max_pending = max_pending
        self.prune_after_scans = prune_after_scans
        self.scan_interval_seconds = scan_interval_seconds
        
        self.pending_merges: Dict[int, PendingMerge] = {}
        self.pending_splits: Dict[int, PendingSplit] = {}
        
        # Track confirmed events for current scan
        self.confirmed_merges: Set[int] = set()
        self.confirmed_splits: Set[int] = set()
        
        # Track which events were seen this scan (for pruning)
        self._active_this_scan: Set[Tuple[str, int]] = set()
        
        # H2 Fix: Monotonic scan counter for consecutive detection enforcement
        self._scan_number: int = 0
    
    @classmethod
    def load(cls, stormcell_dir: Path, **kwargs) -> 'LineageBuffer':
        """
        Load buffer state from disk.
        
        Args:
            stormcell_dir: Directory containing the buffer file
            **kwargs: Additional arguments passed to constructor
            
        Returns:
            LineageBuffer instance with loaded state
        """
        buffer = cls(**kwargs)
        
        buffer_file = stormcell_dir / cls.BUFFER_FILE
        if not buffer_file.exists():
            return buffer
        
        try:
            with open(buffer_file, 'r') as f:
                data = json.load(f)
            
            # Load pending merges
            for merge_data in data.get('pending_merges', []):
                merge = PendingMerge.from_dict(merge_data)
                buffer.pending_merges[merge.child_id] = merge
            
            # Load pending splits
            for split_data in data.get('pending_splits', []):
                split = PendingSplit.from_dict(split_data)
                buffer.pending_splits[split.parent_id] = split
            
            # Load configuration if present
            if 'config' in data:
                buffer.min_confirmations = data['config'].get(
                    'min_confirmations', buffer.min_confirmations
                )
                buffer.max_pending = data['config'].get(
                    'max_pending', buffer.max_pending
                )
                buffer._scan_number = data['config'].get(
                    'scan_number', 0
                )
                
        except (json.JSONDecodeError, KeyError, IOError):
            # Return empty buffer on error
            pass
        
        return buffer
    
    def save(self, stormcell_dir: Path) -> bool:
        """
        Persist buffer state to disk.
        
        Args:
            stormcell_dir: Directory to save the buffer file
            
        Returns:
            True if save successful, False otherwise
        """
        buffer_file = stormcell_dir / self.BUFFER_FILE
        
        try:
            data = {
                'config': {
                    'min_confirmations': self.min_confirmations,
                    'max_pending': self.max_pending,
                    'prune_after_scans': self.prune_after_scans,
                    'scan_number': self._scan_number,
                },
                'pending_merges': [
                    m.to_dict() for m in self.pending_merges.values()
                ],
                'pending_splits': [
                    s.to_dict() for s in self.pending_splits.values()
                ],
            }
            
            with open(buffer_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
            
        except IOError:
            return False
    
    def record_potential_merge(
        self,
        child_id: int,
        parent_ids: List[int],
        dominant_parent: int
    ) -> bool:
        """
        Record a potential merge event and check if it should be confirmed.
        
        Args:
            child_id: ID of the resulting child cell
            parent_ids: List of parent cell IDs
            dominant_parent: ID of the dominant parent
            
        Returns:
            True if the merge should be confirmed (count >= min_confirmations)
        """
        self._active_this_scan.add(('merge', child_id))
        
        if child_id in self.pending_merges:
            # Update existing pending merge
            pending = self.pending_merges[child_id]
            pending.parent_ids = set(parent_ids)
            # H2 Fix: Only increment if this is a consecutive scan
            if self._scan_number == pending.last_scan_number + 1:
                pending.count += 1
            else:
                pending.count = 1  # Reset — non-consecutive detection
            pending.last_scan_number = self._scan_number
            pending.last_seen = time.time()
            pending.dominant_parent = dominant_parent
        else:
            # Create new pending merge
            self.pending_merges[child_id] = PendingMerge(
                child_id=child_id,
                parent_ids=set(parent_ids),
                dominant_parent=dominant_parent,
                last_scan_number=self._scan_number,
            )
        
        # Check for confirmation
        if self.pending_merges[child_id].count >= self.min_confirmations:
            self.confirmed_merges.add(child_id)
            return True
        
        return False
    
    def record_potential_split(
        self,
        parent_id: int,
        child_ids: List[int],
        dominant_child: int
    ) -> bool:
        """
        Record a potential split event and check if it should be confirmed.
        
        Args:
            parent_id: ID of the parent cell
            child_ids: List of resulting child cell IDs
            dominant_child: ID of the dominant child
            
        Returns:
            True if the split should be confirmed (count >= min_confirmations)
        """
        self._active_this_scan.add(('split', parent_id))
        
        if parent_id in self.pending_splits:
            # Update existing pending split
            pending = self.pending_splits[parent_id]
            pending.child_ids = set(child_ids)
            # H2 Fix: Only increment if this is a consecutive scan
            if self._scan_number == pending.last_scan_number + 1:
                pending.count += 1
            else:
                pending.count = 1  # Reset — non-consecutive detection
            pending.last_scan_number = self._scan_number
            pending.last_seen = time.time()
            pending.dominant_child = dominant_child
        else:
            # Create new pending split
            self.pending_splits[parent_id] = PendingSplit(
                parent_id=parent_id,
                child_ids=set(child_ids),
                dominant_child=dominant_child,
                last_scan_number=self._scan_number,
            )
        
        # Check for confirmation
        if self.pending_splits[parent_id].count >= self.min_confirmations:
            self.confirmed_splits.add(parent_id)
            return True
        
        return False
    
    def is_merge_confirmed(self, child_id: int) -> bool:
        """Check if a merge event for this child is confirmed."""
        return child_id in self.confirmed_merges
    
    def is_split_confirmed(self, parent_id: int) -> bool:
        """Check if a split event for this parent is confirmed."""
        return parent_id in self.confirmed_splits
    
    def get_pending_merge(self, child_id: int) -> Optional[PendingMerge]:
        """Get pending merge data for a child ID."""
        return self.pending_merges.get(child_id)
    
    def get_pending_split(self, parent_id: int) -> Optional[PendingSplit]:
        """Get pending split data for a parent ID."""
        return self.pending_splits.get(parent_id)
    
    def clear_confirmed_events(self) -> None:
        """
        Clear confirmed events from the buffer after processing.
        
        Call this after lineage events have been applied to the cell data.
        """
        # Remove confirmed merges
        for child_id in self.confirmed_merges:
            self.pending_merges.pop(child_id, None)
        
        # Remove confirmed splits
        for parent_id in self.confirmed_splits:
            self.pending_splits.pop(parent_id, None)
        
        # Clear confirmation sets
        self.confirmed_merges.clear()
        self.confirmed_splits.clear()
    
    def prune_inactive(self) -> int:
        """
        Prune entries that have been inactive for too long.
        
        Returns:
            Number of pruned entries
        """
        prune_threshold = time.time() - (
            self.prune_after_scans * self.scan_interval_seconds
        )
        
        pruned = 0
        
        # Prune inactive merges
        inactive_merges = [
            child_id for child_id, pending in self.pending_merges.items()
            if pending.last_seen < prune_threshold
        ]
        for child_id in inactive_merges:
            del self.pending_merges[child_id]
            pruned += 1
        
        # Prune inactive splits
        inactive_splits = [
            parent_id for parent_id, pending in self.pending_splits.items()
            if pending.last_seen < prune_threshold
        ]
        for parent_id in inactive_splits:
            del self.pending_splits[parent_id]
            pruned += 1
        
        return pruned
    
    def end_scan(self, stormcell_dir: Path) -> None:
        """
        End of scan processing: prune inactive, save to disk.
        
        Args:
            stormcell_dir: Directory to save the buffer file
        """
        # Mark events not seen this scan as potentially inactive
        # (pruning happens based on time, not just absence)
        self._active_this_scan.clear()
        
        # H2 Fix: Increment scan counter for consecutive detection tracking
        self._scan_number += 1
        
        # Prune old entries
        self.prune_inactive()
        
        # Enforce max pending limit
        if len(self.pending_merges) > self.max_pending:
            # Remove oldest entries
            sorted_merges = sorted(
                self.pending_merges.items(),
                key=lambda x: x[1].last_seen
            )
            for child_id, _ in sorted_merges[:len(self.pending_merges) - self.max_pending]:
                del self.pending_merges[child_id]
        
        if len(self.pending_splits) > self.max_pending:
            sorted_splits = sorted(
                self.pending_splits.items(),
                key=lambda x: x[1].last_seen
            )
            for parent_id, _ in sorted_splits[:len(self.pending_splits) - self.max_pending]:
                del self.pending_splits[parent_id]
        
        # Save to disk
        self.save(stormcell_dir)
    
    def is_empty(self) -> bool:
        """
        Check if buffer has no pending or confirmed events.
        
        Returns:
            True if no pending events and no confirmed events this scan.
        """
        return (
            len(self.pending_merges) == 0 and
            len(self.pending_splits) == 0 and
            len(self.confirmed_merges) == 0 and
            len(self.confirmed_splits) == 0
        )
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get buffer statistics for logging/monitoring.
        
        Returns:
            Dictionary with buffer statistics
        """
        return {
            'pending_merges': len(self.pending_merges),
            'pending_splits': len(self.pending_splits),
            'confirmed_merges': len(self.confirmed_merges),
            'confirmed_splits': len(self.confirmed_splits),
            'min_confirmations': self.min_confirmations,
        }