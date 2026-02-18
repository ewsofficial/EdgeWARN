"""
Storm Cell Tracker with Lineage Detection and Kalman Filter Integration.

This module combines:
1. Lineage Detection: Merge/Split handling via spatial overlap and hysteresis.
2. Kalman Filtering: Motion prediction and continuity for temporary dropouts.
"""

from typing import List, Dict, Any, Optional, Tuple, Set
from pathlib import Path
from datetime import datetime
import numpy as np

from .lineage import (
    LineageEvent,
    LineageResult,
    LineageBuffer,
    LineageDetector,
    detect_lineage_events,
)

from .kalman import (
    KalmanFilter,
    KalmanObservation,
    ConfidenceCalculator,
    PredictionState,
    TrackingConfig,
    AssignmentConfig,
    haversine_distance,
)


class StormCellTracker:
    """
    Tracks storm cells across scans with lineage event detection and Kalman filtering.
    
    This class handles:
    - 1-to-1 cell ID matching and field updates
    - Merge detection (multiple parents -> single child)
    - Split detection (single parent -> multiple children)
    - Hysteresis buffering for false positive prevention
    - Kalman filter-based motion prediction
    - Continuity tracking (Prediction Mode) for dropped detection
    
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
        tracking_config: Optional[TrackingConfig] = None,
        assignment_config: Optional[AssignmentConfig] = None
    ):
        """
        Initialize the storm cell tracker.
        
        Args:
            ps_old: Previous scan ProbSevere data
            ps_new: Current scan ProbSevere data
            io_manager: IO manager for logging
            lineage_buffer: Optional pre-loaded LineageBuffer
            overlap_threshold: Minimum overlap ratio for merge/split detection
            tracking_config: Configuration for Kalman tracking
            assignment_config: Configuration for assignment (mostly for hybrid params)
        """
        self.ps_old = ps_old
        self.ps_new = ps_new
        self.io_manager = io_manager
        self.overlap_threshold = overlap_threshold
        
        # Lineage buffer
        self._lineage_buffer = lineage_buffer
        
        # Kalman Configuration
        self.tracking_config = tracking_config or TrackingConfig()
        self.assignment_config = assignment_config or AssignmentConfig()
        self.confidence_calc = ConfidenceCalculator(config=self.tracking_config)
        
        # Cache for Kalman filters (cell_id -> KalmanFilter)
        self._kalman_filters: Dict[int, KalmanFilter] = {}
        
        # Cache for prediction states (cell_id -> PredictionState)
        self._prediction_states: Dict[int, PredictionState] = {}
    
    def detect_lineage_events(
        self,
        old_cells: List[Dict[str, Any]],
        new_cells: List[Dict[str, Any]],
        stormcell_dir: Optional[Path] = None,
    ) -> LineageResult:
        """
        Detect merge and split events between old and new cell sets.
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
        dt_seconds: float = 120.0,
        lineage: Optional[LineageResult] = None,
    ) -> List[Dict[str, Any]]:
        """
        Updates cells using Lineage Detection + Kalman Continuity.
        
        Args:
            entries: List of cell dicts from previous scan
            updated_data: List of dicts with updated data
            timestamp: Current scan timestamp
            dt_seconds: Time since last scan in seconds
            lineage: Optional pre-calculated lineage result
            
        Returns:
            Updated list of cell entries
        """
        # 1. Initialize/Sync Kalman Filters for all existing tracks
        self._ensure_kalman_filters(entries)
        updated_map = {int(cell['id']): cell for cell in updated_data}
        
        # 2. Run Lineage Detection (Overlap based) if not provided
        if lineage is None:
            # We filter entries to only pass 'active' or 'predicted' to detector
            # logic. However, overlap check usually implies active polygons.
            # We pass all, LineageDetector handles geometry.
            lineage = self.detect_lineage_events(entries, updated_data)
        
        updated_entries = []
        processed_old_ids = set()
        processed_new_ids = set()
        
        # Track statistics
        stats = {
            'matches': 0, 'merges': 0, 'splits': 0, 
            'predicted': 0, 'reacquired': 0, 'new': 0, 'terminated': 0
        }
        
        # 3. Process Lineage Events (Merges, Splits, Overlap Matches)
        if lineage:
            # Process Merges
            for merge in lineage.merges:
                child_id = merge.child_id
                if child_id not in updated_map: continue
                
                child_data = updated_map[child_id]
                dominant_entry = self._find_entry(entries, merge.dominant_parent)
                if not dominant_entry: continue # Should not happen
                
                # Create merged entry
                merged_entry = dominant_entry.copy()
                self._update_cell_fields(merged_entry, child_data, timestamp)
                
                merged_entry['event_type'] = LineageEvent.MERGE.value
                merged_entry['parent_ids'] = merge.parent_ids
                merged_entry['split_from'] = None
                
                # Kalman Update
                self._update_kalman_with_observation(merged_entry, merge.dominant_parent)
                self._reset_prediction_state(child_id) # Child ID might be dominant parent ID
                
                updated_entries.append(merged_entry)
                
                processed_old_ids.update(merge.parent_ids)
                processed_new_ids.add(child_id)
                stats['merges'] += 1

            # Process Splits
            for split in lineage.splits:
                parent_entry = self._find_entry(entries, split.parent_id)
                if not parent_entry: continue
                
                for child_id in split.child_ids:
                    if child_id not in updated_map: continue
                    child_data = updated_map[child_id]
                    
                    if child_id == split.dominant_child:
                        # Dominant child inherits
                        new_entry = parent_entry.copy()
                        new_entry['id'] = child_id
                        new_entry['event_type'] = LineageEvent.ACTIVE.value
                        new_entry['split_from'] = split.parent_id
                        # Kalman Update on parent track
                        self._update_kalman_with_observation(new_entry, split.parent_id)
                    else:
                        # Secondary child is new
                        new_entry = child_data.copy()
                        new_entry['event_type'] = LineageEvent.SPLIT.value
                        new_entry['split_from'] = split.parent_id
                        new_entry['parent_ids'] = []
                        # Init new KF
                        self._update_kalman_with_observation(new_entry, child_id)

                    self._update_cell_fields(new_entry, child_data, timestamp)
                    updated_entries.append(new_entry)
                    processed_new_ids.add(child_id)
                    stats['splits'] += 1
                
                processed_old_ids.add(split.parent_id)

        # Process Normal Matches (Overlap)
        for cell in entries:
            cell_id = int(cell['id'])
            # Skip if already processed
            if cell_id in processed_old_ids: continue
            
            # Check if this cell ID exists in updated_map and WAS NOT used by merge/split
            # (Note: LineageDetector only outputs events. We need natural matches too)
            # Implication: LineageDetector.detect should return matches? 
            # Reviewing HEAD code: detect_lineage_events ONLY returned events.
            # HEAD's _apply_lineage_updates handled the loop: "if cell_id in updated_map"
            
            if cell_id in updated_map and cell_id not in processed_new_ids:
                # Direct Overlap Match
                updated = updated_map[cell_id]
                self._update_cell_fields(cell, updated, timestamp)
                
                cell['event_type'] = LineageEvent.ACTIVE.value
                cell['parent_ids'] = []
                cell['split_from'] = None
                
                self._update_kalman_with_observation(cell, cell_id)
                self._reset_prediction_state(cell_id)
                
                updated_entries.append(cell)
                processed_old_ids.add(cell_id)
                processed_new_ids.add(cell_id)
                stats['matches'] += 1

        # 4. Handle Unmatched Old Cells (Potential Prediction Mode)
        predicted_candidates = []
        for cell in entries:
            cell_id = int(cell['id'])
            if cell_id not in processed_old_ids:
                # Cell wasn't matched by overlap.
                # Try to enter/maintain prediction mode
                if self._handle_unmatched_cell(cell, cell_id, timestamp, dt_seconds):
                    predicted_candidates.append(cell)
                    # Don't add to updated_entries yet, might be re-acquired
                else:
                    stats['terminated'] += 1
        
        # 5. Handle Unmatched New Cells (Potential Re-acquisition or True New)
        for cell_id, cell_data in updated_map.items():
            if cell_id not in processed_new_ids:
                # This is a detection without an overlap match.
                # Try to match against predicted candidates
                reacquired_match = self._check_reacquisition(cell_data, predicted_candidates, timestamp)
                
                if reacquired_match:
                    # Found a match!
                    # reacquired_match is the OLD cell updated with NEW data
                    updated_entries.append(reacquired_match)
                    
                    # Remove from predicted_candidates so we don't add the ghost later
                    # (Note: reacquired_match is a reference to the obj in predicted_candidates)
                    if reacquired_match in predicted_candidates:
                        predicted_candidates.remove(reacquired_match)
                        
                    stats['reacquired'] += 1
                else:
                    # True New Cell
                    new_entry = cell_data.copy()
                    new_entry['event_type'] = LineageEvent.ACTIVE.value
                    new_entry['parent_ids'] = []
                    new_entry['split_from'] = None
                    if timestamp: new_entry['timestamp'] = timestamp
                    
                    self._update_kalman_with_observation(new_entry, cell_id)
                    updated_entries.append(new_entry)
                    stats['new'] += 1

        # 6. Add remaining Predicted cells
        updated_entries.extend(predicted_candidates)
        stats['predicted'] += len(predicted_candidates)
        
        # Log
        self.io_manager.write_info(
            f"Update Stats: {stats['matches']} matches, {stats['merges']} merges, {stats['splits']} splits, "
            f"{stats['reacquired']} re-acquired, {stats['predicted']} predicted, {stats['new']} new, {stats['terminated']} terminated"
        )
        
        return updated_entries

    def _ensure_kalman_filters(self, entries: List[Dict]):
        """Ensure KF exists for all tracks."""
        for track in entries:
            track_id = int(track['id'])
            if track_id not in self._kalman_filters:
                kf = KalmanFilter()
                kf.initialize_from_cell(track)
                self._kalman_filters[track_id] = kf

    def _find_entry(self, entries: List[Dict], entry_id: int) -> Optional[Dict]:
        """Helper to find entry by ID."""
        for e in entries:
            if int(e['id']) == int(entry_id): return e
        return None

    def _update_cell_fields(self, cell: Dict, updated: Dict, timestamp: Optional[str]) -> None:
        """Update cell fields from new observation."""
        cell['id'] = updated.get('id', cell['id'])
        cell['num_gates'] = updated.get('num_gates', cell['num_gates'])
        cell['centroid'] = updated.get('centroid', cell['centroid'])
        cell['max_refl'] = updated.get('max_refl', cell['max_refl'])
        if 'bbox' in updated:
            cell['bbox'] = updated['bbox']
        
        cell['tracking_mode'] = 'active'
        cell['prediction_count'] = 0
        cell['confidence'] = 1.0
        
        if timestamp:
            cell['timestamp'] = timestamp

    def _handle_unmatched_cell(self, cell: Dict, cell_id: int,
                               timestamp: Optional[str],
                               dt_seconds: float) -> bool:
        """
        Handle a cell that was not found in updated_data.
        Enters prediction mode if within time/confidence limits.
        Returns: True if cell should continue tracking, False if terminated.
        """
        # Ensure KF exists
        if cell_id not in self._kalman_filters:
             return False # If no KF, can't predict. Terminate.
        
        kf = self._kalman_filters[cell_id]
        
        # Initialize prediction state if not exists
        if cell_id not in self._prediction_states:
            self._prediction_states[cell_id] = PredictionState(
                start_timestamp=timestamp
            )
        
        pred_state = self._prediction_states[cell_id]
        
        # Get StormCast velocity
        control_u, control_v = self._get_stormcast_velocity(cell)
        
        # Perform Kalman prediction
        predicted_state = kf.predict(dt_seconds, control_u, control_v)
        
        # Update prediction state
        pred_state.increment(
            dt_seconds=dt_seconds,
            new_confidence=0.0,
            predicted_position=(predicted_state.lat, predicted_state.lon)
        )
        if timestamp: pred_state.last_update_timestamp = timestamp
        
        # Calculate confidence
        vel_var = kf.covariance.get_velocity_variance()
        pos_unc = kf.covariance.get_position_std_km(kf.ref_lat)
        
        confidence = self.confidence_calc.calculate(
            scans_predicted=pred_state.scan_count,
            time_predicted_seconds=pred_state.total_time_seconds,
            velocity_variance=vel_var,
            position_uncertainty_km=pos_unc
        )
        pred_state.confidence = confidence
        
        # Check termination
        should_terminate, reason = self.confidence_calc.should_terminate(
            confidence=confidence,
            confidence_threshold=self.tracking_config.min_confidence_threshold, # Use config
            max_time=self.tracking_config.max_prediction_time_seconds,
            time_predicted_seconds=pred_state.total_time_seconds,
            scans_predicted=pred_state.scan_count
        )
        
        if should_terminate:
            self.io_manager.write_info(f"Cell {cell_id} terminated: {reason}")
            # Clean up
            if cell_id in self._kalman_filters: del self._kalman_filters[cell_id]
            if cell_id in self._prediction_states: del self._prediction_states[cell_id]
            return False
        
        # Update cell with predicted position
        cell['centroid'] = [predicted_state.lat, predicted_state.lon]
        cell['tracking_mode'] = 'predicted'
        cell['prediction_count'] = pred_state.scan_count
        cell['confidence'] = confidence
        cell['kalman_predicted_centroid'] = [predicted_state.lat, predicted_state.lon]
        if timestamp: cell['timestamp'] = timestamp
        
        # Store Kalman state
        cell['kalman_state'] = kf.get_state_dict()
        
        return True

    def _check_reacquisition(self, new_cell: Dict, 
                             predicted_cells: List[Dict],
                             timestamp: Optional[str]) -> Optional[Dict]:
        """
        Check if a new cell matches a predicted cell for re-acquisition.
        """
        new_centroid = new_cell.get('centroid', [0, 0])
        new_lat, new_lon = new_centroid[0], new_centroid[1]
        
        best_match = None
        best_distance = float('inf')
        
        for cell in predicted_cells:
            # Note: We iterate passed list, which works for both 'predicted' and just-switched cells
            pred_centroid = cell.get('kalman_predicted_centroid') or cell.get('centroid')
            if not pred_centroid: continue
            
            pred_lat, pred_lon = pred_centroid[0], pred_centroid[1]
            distance = haversine_distance(new_lat, new_lon, pred_lat, pred_lon)
            
            if distance <= self.tracking_config.reacquisition_radius_km:
                if distance < best_distance:
                    best_distance = distance
                    best_match = cell
        
        if best_match:
            old_id = int(best_match['id'])
            
            # Update fields
            self._update_cell_fields(best_match, new_cell, timestamp)
            best_match['event_type'] = LineageEvent.ACTIVE.value # Re-acquired is active
            
            # Kalman Update
            self._update_kalman_with_observation(best_match, old_id)
            self._reset_prediction_state(old_id)
            
            self.io_manager.write_info(
                f"Re-acquired cell {old_id} (dist: {best_distance:.2f} km)"
            )
            return best_match
            
        return None

    def _update_kalman_with_observation(self, cell: Dict, cell_id: int) -> None:
        """Update Kalman filter with new observation."""
        if cell_id not in self._kalman_filters:
            kf = KalmanFilter()
            kf.initialize_from_cell(cell)
            self._kalman_filters[cell_id] = kf
            return
        
        kf = self._kalman_filters[cell_id]
        centroid = cell.get('centroid', [0, 0])
        ts = None
        if cell.get('timestamp'):
            try:
                ts = datetime.fromisoformat(cell.get('timestamp'))
            except: pass
            
        obs = KalmanObservation(
            lat=centroid[0],
            lon=centroid[1],
            timestamp=ts
        )
        kf.update(obs)

    def _reset_prediction_state(self, cell_id: int):
        if cell_id in self._prediction_states:
            self._prediction_states[cell_id].reset()

    def _get_stormcast_velocity(self, cell: Dict) -> Tuple[Optional[float], Optional[float]]:
        modules = cell.get('modules', {})
        stormcast = modules.get('StormCast', {})
        if stormcast.get('status') == 'success':
            return stormcast.get('u'), stormcast.get('v')
        return None, None

    def get_lineage_buffer(self) -> Optional[LineageBuffer]:
        return self._lineage_buffer
    
    def save_lineage_buffer(self, stormcell_dir: Path) -> bool:
        if self._lineage_buffer is None: return False
        self._lineage_buffer.clear_confirmed_events()
        self._lineage_buffer.end_scan(stormcell_dir)
        return True
