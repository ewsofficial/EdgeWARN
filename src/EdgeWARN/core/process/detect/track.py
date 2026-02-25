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
from .kalman.assignment import (
    run_hybrid_assignment,
    run_greedy_assignment,
    AssignmentResult
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
                
                # Kalman Update (observe under dominant parent's KF)
                self._update_kalman_with_observation(merged_entry, merge.dominant_parent)
                
                # C1 Fix: Migrate KF from dominant_parent key → child_id key
                # so future scans find the trained filter under the cell's new ID.
                if merge.dominant_parent != child_id:
                    if merge.dominant_parent in self._kalman_filters:
                        self._kalman_filters[child_id] = self._kalman_filters.pop(merge.dominant_parent)
                    if merge.dominant_parent in self._prediction_states:
                        self._prediction_states[child_id] = self._prediction_states.pop(merge.dominant_parent)
                
                self._reset_prediction_state(child_id)
                
                # Clean up KF/prediction entries for non-dominant parents
                for pid in merge.parent_ids:
                    if pid != merge.dominant_parent and pid != child_id:
                        self._kalman_filters.pop(pid, None)
                        self._prediction_states.pop(pid, None)
                
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

                        # H1 Fix: Migrate KF from parent_id → dominant child_id
                        if split.parent_id != child_id:
                            if split.parent_id in self._kalman_filters:
                                self._kalman_filters[child_id] = self._kalman_filters.pop(split.parent_id)
                            if split.parent_id in self._prediction_states:
                                self._prediction_states[child_id] = self._prediction_states.pop(split.parent_id)
                        self._reset_prediction_state(child_id)
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

                # H1 Fix: Clean up parent KF after split processing
                self._kalman_filters.pop(split.parent_id, None)
                self._prediction_states.pop(split.parent_id, None)

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

        # 4. Identify Unmatched Candidates for Secondary Assignment
        unmatched_tracks = []
        unmatched_tracks_map = {} # ID -> Entry
        for cell in entries:
            cell_id = int(cell['id'])
            if cell_id not in processed_old_ids:
                unmatched_tracks.append(cell)
                unmatched_tracks_map[cell_id] = cell
                
        unmatched_detections = []
        unmatched_detections_map = {} # ID -> Data
        for cell_id, cell_data in updated_map.items():
            if cell_id not in processed_new_ids:
                cell_data_with_id = cell_data.copy() # Ensure ID is present if not
                cell_data_with_id['id'] = cell_id
                unmatched_detections.append(cell_data_with_id)
                unmatched_detections_map[cell_id] = cell_data

        # 5. Run Secondary Assignment (Hybrid/Greedy) on Remainder
        # This handles fast-moving cells that lost overlap but are close enough/consistent enough.
        # NOTE: This step also handles re-acquisition of predicted cells (replaces
        # the former _check_reacquisition method that was removed as dead code).
        
        assignment_method = self.assignment_config.method
        assignment_result = None

        if unmatched_tracks and unmatched_detections:
            if assignment_method == 'hybrid':
                assignment_result = run_hybrid_assignment(
                    unmatched_tracks, unmatched_detections, self._kalman_filters,
                    self.assignment_config, dt_seconds
                )
            elif assignment_method == 'greedy':
                assignment_result = run_greedy_assignment(
                    unmatched_tracks, unmatched_detections, self._kalman_filters,
                    self.assignment_config, dt_seconds
                )
            # Add other methods if implemented
            else:
                # Default fallback to Greedy if unknown or legacy 'hungarian' mapped to hybrid usually
                assignment_result = run_hybrid_assignment(
                    unmatched_tracks, unmatched_detections, self._kalman_filters,
                    self.assignment_config, dt_seconds
                )
        
        # Process Assignment Results
        if assignment_result:
            # Matches (Re-acquisition / Continuation)
            for track_id, det_id in assignment_result.matched:
                track = unmatched_tracks_map.get(track_id)
                detection = unmatched_detections_map.get(det_id)
                
                if track and detection:
                    self._update_cell_fields(track, detection, timestamp)
                    # Reset lineage fields just in case
                    track['event_type'] = LineageEvent.ACTIVE.value
                    
                    self._update_kalman_with_observation(track, track_id)
                    self._reset_prediction_state(track_id)
                    
                    updated_entries.append(track)
                    processed_old_ids.add(track_id)
                    processed_new_ids.add(det_id)
                    stats['reacquired'] += 1
            
            # Remaining Unmatched Tracks -> Prediction Mode
            for track_id in assignment_result.unmatched_tracks:
                track = unmatched_tracks_map.get(track_id)
                if track and self._handle_unmatched_cell(track, track_id, timestamp, dt_seconds):
                    updated_entries.append(track)
                    stats['predicted'] += 1
                else:
                    stats['terminated'] += 1
            
            # Remaining Unmatched Detections -> New Cells
            for det_id in assignment_result.unmatched_detections:
                detection = unmatched_detections_map.get(det_id)
                if detection:
                    new_entry = detection.copy()
                    new_entry['event_type'] = LineageEvent.ACTIVE.value
                    new_entry['parent_ids'] = []
                    new_entry['split_from'] = None
                    new_entry['tracking_mode'] = 'active'
                    new_entry['prediction_count'] = 0
                    new_entry['confidence'] = 1.0
                    if timestamp: new_entry['timestamp'] = timestamp
                    
                    self._update_kalman_with_observation(new_entry, det_id)
                    updated_entries.append(new_entry)
                    stats['new'] += 1
        else:
            # Fallback if assignment didn't run (e.g. one list empty)
            # Unmatched Tracks -> Prediction
            for track in unmatched_tracks:
                track_id = int(track['id'])
                if self._handle_unmatched_cell(track, track_id, timestamp, dt_seconds):
                    updated_entries.append(track)
                    stats['predicted'] += 1
                else:
                    stats['terminated'] += 1
            
            # Unmatched Detections -> New Cells
            for detection in unmatched_detections:
                det_id = int(detection['id'])
                new_entry = detection.copy()
                new_entry['event_type'] = LineageEvent.ACTIVE.value
                new_entry['parent_ids'] = []
                new_entry['split_from'] = None
                new_entry['tracking_mode'] = 'active'
                new_entry['prediction_count'] = 0
                new_entry['confidence'] = 1.0
                if timestamp: new_entry['timestamp'] = timestamp
                
                self._update_kalman_with_observation(new_entry, det_id)
                updated_entries.append(new_entry)
                stats['new'] += 1
        
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
            except (ValueError, TypeError):
                pass
            
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
