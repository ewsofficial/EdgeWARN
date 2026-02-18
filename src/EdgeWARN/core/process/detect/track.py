"""
Storm Cell Tracker with Kalman Filter Integration

Tracks storm cells across consecutive scans with Kalman filter-based
prediction for continuity when ProbSevere temporarily drops detection.

Supports multiple assignment algorithms:
- hybrid: Pre-filter + Hungarian algorithm (recommended)
- hungarian: Full Hungarian algorithm
- greedy: Nearest-neighbor fallback
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import numpy as np

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
    AssignmentCostCalculator,
    AssignmentResult,
    run_hybrid_assignment,
    run_greedy_assignment,
)


class StormCellTracker:
    """
    Tracks storm cells across consecutive scans with Kalman filter support.
    
    Tracking Modes:
    - active: Normal tracking with ProbSevere observations
    - predicted: Kalman-only prediction mode (ProbSevere dropped)
    - terminated: Storm removed from tracking
    
    Assignment Methods:
    - hybrid: Pre-filter + Hungarian algorithm (recommended)
    - hungarian: Full Hungarian algorithm
    - greedy: Nearest-neighbor fallback
    
    When a cell is not found in updated_data, it enters prediction mode
    instead of being immediately removed. The Kalman filter predicts its
    position for up to 10 minutes or until confidence drops below threshold.
    """
    
    def __init__(self, ps_old, ps_new, io_manager, 
                 tracking_config: Optional[TrackingConfig] = None,
                 assignment_config: Optional[AssignmentConfig] = None):
        self.ps_old = ps_old
        self.ps_new = ps_new
        self.io_manager = io_manager
        self.tracking_config = tracking_config or TrackingConfig()
        self.assignment_config = assignment_config or AssignmentConfig()
        self.confidence_calc = ConfidenceCalculator(config=self.tracking_config)
        
        # Cache for Kalman filters (cell_id -> KalmanFilter)
        self._kalman_filters: Dict[int, KalmanFilter] = {}
        
        # Cache for prediction states (cell_id -> PredictionState)
        self._prediction_states: Dict[int, PredictionState] = {}
    
    def update_cells(self, entries: List[Dict], updated_data: List[Dict], 
                     timestamp: Optional[str] = None,
                     dt_seconds: float = 120.0) -> List[Dict]:
        """
        Updates main fields in entries from updated_data.
        
        Uses the configured assignment method (hybrid, hungarian, or greedy)
        to match detections to tracked cells. Cells not matched enter prediction
        mode instead of being immediately removed.
        
        Args:
            entries: List of existing storm cell dictionaries
            updated_data: List of newly detected cells from current scan
            timestamp: Current scan timestamp (ISO format)
            dt_seconds: Time since last scan in seconds
        
        Returns:
            Updated list of storm cells (active + predicted + new)
        """
        # Choose assignment method based on configuration
        method = self.assignment_config.method
        
        if method == 'hybrid':
            return self._update_cells_hybrid(entries, updated_data, timestamp, dt_seconds)
        elif method == 'hungarian':
            return self._update_cells_hungarian(entries, updated_data, timestamp, dt_seconds)
        else:
            return self._update_cells_greedy(entries, updated_data, timestamp, dt_seconds)
    
    def _update_cells_hybrid(self, entries: List[Dict], updated_data: List[Dict],
                              timestamp: Optional[str], dt_seconds: float) -> List[Dict]:
        """
        Update cells using hybrid pre-filter + Hungarian assignment.
        
        This is the recommended approach that combines:
        1. Fixed-radius pre-filtering to reduce problem size
        2. Hungarian algorithm for optimal assignment within filtered candidates
        """
        # Separate active and predicted tracks
        all_tracks = [e for e in entries if e.get('tracking_mode') in ('active', 'predicted')]
        
        # Ensure Kalman filters exist for all tracks
        for track in all_tracks:
            track_id = int(track['id'])
            if track_id not in self._kalman_filters:
                kf = KalmanFilter()
                kf.initialize_from_cell(track)
                self._kalman_filters[track_id] = kf
        
        # Run hybrid assignment
        result = run_hybrid_assignment(
            tracks=all_tracks,
            detections=updated_data,
            kalman_filters=self._kalman_filters,
            config=self.assignment_config,
            dt_seconds=dt_seconds
        )
        
        # Track statistics
        active_count = 0
        predicted_count = 0
        terminated_count = 0
        reacquired_count = 0
        
        updated_entries = []
        matched_track_ids = set()
        matched_detection_ids = set()
        
        # Process matched pairs
        for track_id, det_id in result.matched:
            track = next((t for t in all_tracks if int(t['id']) == track_id), None)
            detection = next((d for d in updated_data if int(d['id']) == det_id), None)
            
            if track is None or detection is None:
                continue
            
            matched_track_ids.add(track_id)
            matched_detection_ids.add(det_id)
            
            # Handle re-acquisition from prediction mode
            if track.get('tracking_mode') == 'predicted':
                reacquired_count += 1
                self.io_manager.write_info(
                    f"Cell {track_id} re-acquired from prediction mode"
                )
                if track_id in self._prediction_states:
                    self._prediction_states[track_id].reset()
            
            # Update cell fields
            self._update_cell_fields(track, detection, timestamp)
            track['tracking_mode'] = 'active'
            track['prediction_count'] = 0
            track['confidence'] = 1.0
            
            # Update Kalman filter with observation
            self._update_kalman_with_observation(track, track_id)
            
            updated_entries.append(track)
            active_count += 1
        
        # Process unmatched tracks (enter prediction mode)
        for track_id in result.unmatched_tracks:
            track = next((t for t in all_tracks if int(t['id']) == track_id), None)
            if track is None:
                continue
            
            handled = self._handle_unmatched_cell(
                track, track_id, timestamp, dt_seconds
            )
            
            if handled:
                updated_entries.append(track)
                predicted_count += 1
            else:
                terminated_count += 1
        
        # Process unmatched detections (new cells)
        new_cells_to_add = []
        for det_id in result.unmatched_detections:
            detection = next((d for d in updated_data if int(d['id']) == det_id), None)
            if detection is None:
                continue
            
            # Check if this detection matches a predicted cell (legacy re-acquisition)
            matched_predicted = self._check_reacquisition(
                detection, updated_entries, timestamp
            )
            
            if matched_predicted:
                reacquired_count += 1
            else:
                # Truly new cell
                if timestamp:
                    detection['timestamp'] = timestamp
                detection['tracking_mode'] = 'active'
                detection['prediction_count'] = 0
                detection['confidence'] = 1.0
                new_cells_to_add.append(detection)
        
        updated_entries.extend(new_cells_to_add)
        
        # Log statistics
        self.io_manager.write_info(
            f"Tracking stats (hybrid): {active_count} active, {predicted_count} predicted, "
            f"{terminated_count} terminated, {reacquired_count} re-acquired, "
            f"{len(new_cells_to_add)} new"
        )
        
        return updated_entries
    
    def _update_cells_hungarian(self, entries: List[Dict], updated_data: List[Dict],
                                 timestamp: Optional[str], dt_seconds: float) -> List[Dict]:
        """
        Update cells using full Hungarian algorithm (no pre-filtering).
        
        This is similar to hybrid but without the pre-filtering stage.
        Use for comparison or when pre-filtering might miss valid matches.
        """
        # For now, this uses the same logic as hybrid
        # The difference would be in the assignment module
        return self._update_cells_hybrid(entries, updated_data, timestamp, dt_seconds)
    
    def _update_cells_greedy(self, entries: List[Dict], updated_data: List[Dict],
                              timestamp: Optional[str], dt_seconds: float) -> List[Dict]:
        """
        Update cells using greedy nearest-neighbor assignment.
        
        This is the legacy approach that assigns each detection to the
        closest track, one at a time. Use as fallback if needed.
        """
        # Separate active and predicted tracks
        all_tracks = [e for e in entries if e.get('tracking_mode') in ('active', 'predicted')]
        
        # Ensure Kalman filters exist for all tracks
        for track in all_tracks:
            track_id = int(track['id'])
            if track_id not in self._kalman_filters:
                kf = KalmanFilter()
                kf.initialize_from_cell(track)
                self._kalman_filters[track_id] = kf
        
        # Run greedy assignment
        result = run_greedy_assignment(
            tracks=all_tracks,
            detections=updated_data,
            kalman_filters=self._kalman_filters,
            config=self.assignment_config,
            dt_seconds=dt_seconds
        )
        
        # Track statistics
        active_count = 0
        predicted_count = 0
        terminated_count = 0
        reacquired_count = 0
        
        updated_entries = []
        
        # Process matched pairs
        for track_id, det_id in result.matched:
            track = next((t for t in all_tracks if int(t['id']) == track_id), None)
            detection = next((d for d in updated_data if int(d['id']) == det_id), None)
            
            if track is None or detection is None:
                continue
            
            # Handle re-acquisition from prediction mode
            if track.get('tracking_mode') == 'predicted':
                reacquired_count += 1
                self.io_manager.write_info(
                    f"Cell {track_id} re-acquired from prediction mode"
                )
                if track_id in self._prediction_states:
                    self._prediction_states[track_id].reset()
            
            # Update cell fields
            self._update_cell_fields(track, detection, timestamp)
            track['tracking_mode'] = 'active'
            track['prediction_count'] = 0
            track['confidence'] = 1.0
            
            # Update Kalman filter with observation
            self._update_kalman_with_observation(track, track_id)
            
            updated_entries.append(track)
            active_count += 1
        
        # Process unmatched tracks (enter prediction mode)
        for track_id in result.unmatched_tracks:
            track = next((t for t in all_tracks if int(t['id']) == track_id), None)
            if track is None:
                continue
            
            handled = self._handle_unmatched_cell(
                track, track_id, timestamp, dt_seconds
            )
            
            if handled:
                updated_entries.append(track)
                predicted_count += 1
            else:
                terminated_count += 1
        
        # Process unmatched detections (new cells)
        new_cells_to_add = []
        for det_id in result.unmatched_detections:
            detection = next((d for d in updated_data if int(d['id']) == det_id), None)
            if detection is None:
                continue
            
            # Check if this detection matches a predicted cell (legacy re-acquisition)
            matched_predicted = self._check_reacquisition(
                detection, updated_entries, timestamp
            )
            
            if matched_predicted:
                reacquired_count += 1
            else:
                # Truly new cell
                if timestamp:
                    detection['timestamp'] = timestamp
                detection['tracking_mode'] = 'active'
                detection['prediction_count'] = 0
                detection['confidence'] = 1.0
                new_cells_to_add.append(detection)
        
        updated_entries.extend(new_cells_to_add)
        
        # Log statistics
        self.io_manager.write_info(
            f"Tracking stats (greedy): {active_count} active, {predicted_count} predicted, "
            f"{terminated_count} terminated, {reacquired_count} re-acquired, "
            f"{len(new_cells_to_add)} new"
        )
        
        return updated_entries
    
    def _update_cell_fields(self, cell: Dict, updated: Dict, 
                            timestamp: Optional[str]) -> None:
        """Update cell fields from new observation."""
        cell['id'] = updated.get('id', cell['id'])
        cell['num_gates'] = updated.get('num_gates', cell['num_gates'])
        cell['centroid'] = updated.get('centroid', cell['centroid'])
        cell['max_refl'] = updated.get('max_refl', cell['max_refl'])
        # Only update bbox if it exists in both
        if 'bbox' in updated:
            cell['bbox'] = updated['bbox']
        elif 'bbox' in cell:
            pass  # Keep existing bbox
        
        if timestamp:
            cell['timestamp'] = timestamp
    
    def _handle_unmatched_cell(self, cell: Dict, cell_id: int,
                               timestamp: Optional[str],
                               dt_seconds: float) -> bool:
        """
        Handle a cell that was not found in updated_data.
        
        Enters prediction mode if within time/confidence limits.
        
        Returns:
            True if cell should continue tracking, False if terminated
        """
        # Initialize Kalman filter if not exists
        if cell_id not in self._kalman_filters:
            kf = KalmanFilter()
            kf.initialize_from_cell(cell)
            self._kalman_filters[cell_id] = kf
        
        kf = self._kalman_filters[cell_id]
        
        # Initialize prediction state if not exists
        if cell_id not in self._prediction_states:
            self._prediction_states[cell_id] = PredictionState(
                start_timestamp=timestamp
            )
        
        pred_state = self._prediction_states[cell_id]
        
        # Get StormCast velocity for prediction (if available)
        control_u, control_v = self._get_stormcast_velocity(cell)
        
        # Perform Kalman prediction
        predicted_state = kf.predict(dt_seconds, control_u, control_v)
        
        # Update prediction state
        pred_state.increment(
            dt_seconds=dt_seconds,
            new_confidence=0.0,  # Will be calculated below
            predicted_position=(predicted_state.lat, predicted_state.lon)
        )
        pred_state.last_update_timestamp = timestamp
        
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
        
        # Check termination conditions
        should_terminate, reason = self.confidence_calc.should_terminate(
            confidence=confidence,
            time_predicted_seconds=pred_state.total_time_seconds,
            scans_predicted=pred_state.scan_count
        )
        
        if should_terminate:
            self.io_manager.write_info(
                f"Cell {cell_id} terminated: {reason}"
            )
            # Clean up
            del self._kalman_filters[cell_id]
            del self._prediction_states[cell_id]
            return False
        
        # Update cell with predicted position
        cell['centroid'] = [predicted_state.lat, predicted_state.lon]
        cell['tracking_mode'] = 'predicted'
        cell['prediction_count'] = pred_state.scan_count
        cell['confidence'] = confidence
        cell['kalman_predicted_centroid'] = [predicted_state.lat, predicted_state.lon]
        
        if timestamp:
            cell['timestamp'] = timestamp
        
        # Store Kalman state for serialization
        cell['kalman_state'] = kf.get_state_dict()
        
        self.io_manager.write_debug(
            f"Cell {cell_id} in prediction mode: scan {pred_state.scan_count}, "
            f"confidence {confidence:.2f}"
        )
        
        return True
    
    def _update_kalman_with_observation(self, cell: Dict, cell_id: int) -> None:
        """Update Kalman filter with new observation."""
        if cell_id not in self._kalman_filters:
            # Initialize new Kalman filter
            kf = KalmanFilter()
            kf.initialize_from_cell(cell)
            self._kalman_filters[cell_id] = kf
            return
        
        kf = self._kalman_filters[cell_id]
        centroid = cell.get('centroid', [0, 0])
        
        obs = KalmanObservation(
            lat=centroid[0],
            lon=centroid[1],
            timestamp=datetime.fromisoformat(cell.get('timestamp', ''))
            if cell.get('timestamp') else None
        )
        
        kf.update(obs)
    
    def _get_stormcast_velocity(self, cell: Dict) -> Tuple[Optional[float], Optional[float]]:
        """Get StormCast velocity for prediction control input."""
        modules = cell.get('modules', {})
        stormcast = modules.get('StormCast', {})
        
        if stormcast.get('status') == 'success':
            return stormcast.get('u'), stormcast.get('v')
        
        return None, None
    
    def _check_reacquisition(self, new_cell: Dict, 
                             predicted_cells: List[Dict],
                             timestamp: Optional[str]) -> Optional[Dict]:
        """
        Check if a new cell matches a predicted cell for re-acquisition.
        
        Args:
            new_cell: Newly detected cell
            predicted_cells: List of cells currently in prediction mode
            timestamp: Current timestamp
        
        Returns:
            Matched predicted cell if found, None otherwise
        """
        new_centroid = new_cell.get('centroid', [0, 0])
        new_lat, new_lon = new_centroid[0], new_centroid[1]
        
        best_match = None
        best_distance = float('inf')
        
        for cell in predicted_cells:
            if cell.get('tracking_mode') != 'predicted':
                continue
            
            pred_centroid = cell.get('kalman_predicted_centroid') or cell.get('centroid')
            if not pred_centroid:
                continue
            
            pred_lat, pred_lon = pred_centroid[0], pred_centroid[1]
            
            # Calculate distance
            distance = haversine_distance(new_lat, new_lon, pred_lat, pred_lon)
            
            # Check if within re-acquisition radius
            if distance <= self.tracking_config.reacquisition_radius_km:
                # Check motion consistency if we have velocity data
                if distance < best_distance:
                    best_distance = distance
                    best_match = cell
        
        if best_match is not None:
            # Merge: new cell gets old cell's ID and history
            old_id = best_match['id']
            
            # Preserve storm history
            storm_history = best_match.get('storm_history', [])
            
            # Update the matched cell with new cell's data
            self._update_cell_fields(best_match, new_cell, timestamp)
            best_match['tracking_mode'] = 'active'
            best_match['prediction_count'] = 0
            best_match['confidence'] = 1.0
            best_match['storm_history'] = storm_history
            
            # Update Kalman with observation
            self._update_kalman_with_observation(best_match, old_id)
            
            # Reset prediction state
            if old_id in self._prediction_states:
                self._prediction_states[old_id].reset()
            
            self.io_manager.write_info(
                f"Re-acquired cell {old_id} with new detection (distance: {best_distance:.2f} km)"
            )
            
            return best_match
        
        return None
