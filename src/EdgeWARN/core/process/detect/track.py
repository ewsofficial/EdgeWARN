"""
Storm Cell Tracker with Kalman Filter Integration

Tracks storm cells across consecutive scans with Kalman filter-based
prediction for continuity when ProbSevere temporarily drops detection.
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
    haversine_distance,
)


class StormCellTracker:
    """
    Tracks storm cells across consecutive scans with Kalman filter support.
    
    Tracking Modes:
    - active: Normal tracking with ProbSevere observations
    - predicted: Kalman-only prediction mode (ProbSevere dropped)
    - terminated: Storm removed from tracking
    
    When a cell is not found in updated_data, it enters prediction mode
    instead of being immediately removed. The Kalman filter predicts its
    position for up to 10 minutes or until confidence drops below threshold.
    """
    
    def __init__(self, ps_old, ps_new, io_manager, 
                 tracking_config: Optional[TrackingConfig] = None):
        self.ps_old = ps_old
        self.ps_new = ps_new
        self.io_manager = io_manager
        self.tracking_config = tracking_config or TrackingConfig()
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
        
        Cells not found in updated_data enter prediction mode instead of
        being immediately removed. Predicted cells are tracked for up to
        10 minutes or until confidence drops below threshold.
        
        Args:
            entries: List of existing storm cell dictionaries
            updated_data: List of newly detected cells from current scan
            timestamp: Current scan timestamp (ISO format)
            dt_seconds: Time since last scan in seconds
        
        Returns:
            Updated list of storm cells (active + predicted + new)
        """
        # Map updated_data by cell id for faster lookup
        updated_map = {int(cell['id']): cell for cell in updated_data}
        
        used_ids = set()
        updated_entries = []
        
        # Track statistics
        active_count = 0
        predicted_count = 0
        terminated_count = 0
        reacquired_count = 0
        
        # Process existing cells
        for cell in entries:
            cell_id = int(cell['id'])
            
            if cell_id in updated_map:
                # Cell found in updated_data - normal update
                updated = updated_map[cell_id]
                
                # Handle re-acquisition from prediction mode
                if cell.get('tracking_mode') == 'predicted':
                    reacquired_count += 1
                    self.io_manager.write_info(
                        f"Cell {cell_id} re-acquired from prediction mode"
                    )
                    # Reset prediction state
                    if cell_id in self._prediction_states:
                        self._prediction_states[cell_id].reset()
                
                # Update cell fields
                self._update_cell_fields(cell, updated, timestamp)
                cell['tracking_mode'] = 'active'
                cell['prediction_count'] = 0
                cell['confidence'] = 1.0
                
                # Update Kalman filter with observation
                self._update_kalman_with_observation(cell, cell_id)
                
                used_ids.add(cell_id)
                updated_entries.append(cell)
                active_count += 1
                
            else:
                # Cell not found in updated_data - enter prediction mode
                handled = self._handle_unmatched_cell(
                    cell, cell_id, timestamp, dt_seconds
                )
                
                if handled:
                    updated_entries.append(cell)
                    predicted_count += 1
                else:
                    terminated_count += 1
        
        # Check for re-acquisition of predicted cells among new cells
        new_cells_to_add = []
        for cell in updated_data:
            cell_id = int(cell['id'])
            if cell_id not in used_ids:
                # Check if this new cell matches a predicted cell
                matched_predicted = self._check_reacquisition(
                    cell, updated_entries, timestamp
                )
                
                if matched_predicted:
                    # Cell was re-acquired, don't add as new
                    used_ids.add(matched_predicted['id'])
                    reacquired_count += 1
                else:
                    # Truly new cell
                    if timestamp:
                        cell['timestamp'] = timestamp
                    cell['tracking_mode'] = 'active'
                    cell['prediction_count'] = 0
                    cell['confidence'] = 1.0
                    new_cells_to_add.append(cell)
        
        updated_entries.extend(new_cells_to_add)
        
        # Log statistics
        self.io_manager.write_info(
            f"Tracking stats: {active_count} active, {predicted_count} predicted, "
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
        cell['bbox'] = updated.get('bbox', cell['bbox'])
        
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
