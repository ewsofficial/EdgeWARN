"""
Kalman Filter State Estimation Module

Storm state tracking with forecast smoothing and adaptive covariance.
"""

from typing import List, Tuple, Optional
from datetime import datetime

from .config import KALMAN_PARAMS
from .types import KalmanState


class StormKalmanFilter:
    """
    Kalman filter for storm motion state estimation.
    
    State vector: [x, y, u, v]
    - x, y: Position (meters)
    - u, v: Velocity (m/s)
    
    Features:
    - Forecast smoothing to reduce velocity oscillations
    - Adaptive process noise based on storm behavior
    - Tracking-history-based observation uncertainty
    """
    
    def __init__(
        self,
        initial_state: Optional[List[float]] = None,
        initial_covariance: Optional[List[List[float]]] = None,
        alpha: float = None,
    ):
        """
        Initialize Kalman filter.
        
        Args:
            initial_state: Initial [x, y, u, v] state vector
            initial_covariance: Initial 4x4 covariance matrix
            alpha: Forecast smoothing coefficient (0-1). Default from config.
        """
        self.alpha = alpha if alpha is not None else KALMAN_PARAMS.alpha
        self._previous_velocity: Optional[Tuple[float, float]] = None
        
        if initial_state is not None:
            self.state = list(initial_state)
        else:
            self.state = [0.0, 0.0, 0.0, 0.0]
        
        if initial_covariance is not None:
            self.P = [list(row) for row in initial_covariance]
        else:
            # Default initial covariance
            self.P = [
                [KALMAN_PARAMS.q_pos, 0.0, 0.0, 0.0],
                [0.0, KALMAN_PARAMS.q_pos, 0.0, 0.0],
                [0.0, 0.0, KALMAN_PARAMS.q_vel * 3, 0.0],  # Higher initial velocity uncertainty
                [0.0, 0.0, 0.0, KALMAN_PARAMS.q_vel * 3],
            ]
    
    @property
    def x(self) -> float:
        return self.state[0]
    
    @property
    def y(self) -> float:
        return self.state[1]
    
    @property
    def u(self) -> float:
        return self.state[2]
    
    @property
    def v(self) -> float:
        return self.state[3]
    
    @property
    def position(self) -> Tuple[float, float]:
        return (self.state[0], self.state[1])
    
    @property
    def velocity(self) -> Tuple[float, float]:
        return (self.state[2], self.state[3])
    
    def predict(
        self, 
        dt: float,
        process_noise_scale: float = 1.0
    ) -> None:
        """
        Predict step: advance state forward in time.
        
        Applies forecast smoothing to velocity components.
        
        Args:
            dt: Time step in seconds
            process_noise_scale: Multiplier for process noise (increase for dynamic storms)
        """
        # State transition: x' = x + u*dt, y' = y + v*dt
        x_new = self.state[0] + self.state[2] * dt
        y_new = self.state[1] + self.state[3] * dt
        u_new = self.state[2]
        v_new = self.state[3]
        
        # Apply forecast smoothing if we have previous velocity
        if self._previous_velocity is not None:
            u_prev, v_prev = self._previous_velocity
            u_new = self.alpha * u_new + (1 - self.alpha) * u_prev
            v_new = self.alpha * v_new + (1 - self.alpha) * v_prev
        
        self.state = [x_new, y_new, u_new, v_new]
        
        # State transition matrix F
        F = [
            [1.0, 0.0, dt,  0.0],
            [0.0, 1.0, 0.0, dt ],
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0],
        ]
        
        # Process noise Q
        q_pos = KALMAN_PARAMS.q_pos * process_noise_scale
        q_vel = KALMAN_PARAMS.q_vel * process_noise_scale
        Q = [
            [q_pos, 0.0,   0.0,   0.0  ],
            [0.0,   q_pos, 0.0,   0.0  ],
            [0.0,   0.0,   q_vel, 0.0  ],
            [0.0,   0.0,   0.0,   q_vel],
        ]
        
        # P' = F @ P @ F^T + Q
        self.P = self._add_matrices(
            self._multiply_matrices(F, self._multiply_matrices(self.P, self._transpose(F))),
            Q
        )
        
        # Store velocity for next smoothing step
        self._previous_velocity = (u_new, v_new)
    
    def update(
        self,
        observation: Tuple[float, float],
        observation_uncertainty: Optional[Tuple[float, float]] = None,
        track_history: int = 5
    ) -> None:
        """
        Update step: incorporate position observation.
        
        Args:
            observation: Observed (x, y) position in meters
            observation_uncertainty: (sigma_x, sigma_y) in meters. If None, scaled by track history.
            track_history: Number of valid tracking samples (affects uncertainty scaling)
        """
        # Observation matrix H: observe only position
        H = [
            [1.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0],
        ]
        
        # Observation noise R
        if observation_uncertainty is not None:
            sigma_x, sigma_y = observation_uncertainty
        else:
            # Scale uncertainty by tracking history
            base_sigma = KALMAN_PARAMS.sigma_pos
            scale = 1.0 + 5.0 / max(track_history, 1)
            sigma_x = sigma_y = base_sigma * scale
        
        R = [
            [sigma_x * sigma_x, 0.0],
            [0.0, sigma_y * sigma_y],
        ]
        
        # Innovation: y = z - H @ x
        z = [observation[0], observation[1]]
        Hx = [
            H[0][0] * self.state[0] + H[0][2] * self.state[2],  # H has zeros at [0][1,3]
            H[1][1] * self.state[1] + H[1][3] * self.state[3],
        ]
        y = [z[0] - Hx[0], z[1] - Hx[1]]
        
        # S = H @ P @ H^T + R
        HP = self._multiply_matrices(H, self.P)
        HPHt = self._multiply_matrices(HP, self._transpose(H))
        S = self._add_matrices(HPHt, R)
        
        # K = P @ H^T @ S^(-1)
        PHt = self._multiply_matrices(self.P, self._transpose(H))
        S_inv = self._invert_2x2(S)
        K = self._multiply_matrices(PHt, S_inv)
        
        # x' = x + K @ y
        Ky = [sum(K[i][j] * y[j] for j in range(2)) for i in range(4)]
        self.state = [self.state[i] + Ky[i] for i in range(4)]
        
        # P' = (I - K @ H) @ P
        KH = self._multiply_matrices(K, H)
        I_KH = [[1.0 if i == j else 0.0 for j in range(4)] for i in range(4)]
        for i in range(4):
            for j in range(4):
                I_KH[i][j] -= KH[i][j]
        self.P = self._multiply_matrices(I_KH, self.P)
    
    def get_state(self) -> KalmanState:
        """Return current state as KalmanState object."""
        return KalmanState(
            state=list(self.state),
            covariance=[list(row) for row in self.P],
        )
    
    # -------------------------------------------------------------------------
    # Matrix utilities (avoiding numpy dependency for minimal footprint)
    # -------------------------------------------------------------------------
    
    @staticmethod
    def _multiply_matrices(A: List[List[float]], B: List[List[float]]) -> List[List[float]]:
        """Multiply two matrices."""
        rows_a, cols_a = len(A), len(A[0])
        rows_b, cols_b = len(B), len(B[0])
        result = [[0.0] * cols_b for _ in range(rows_a)]
        for i in range(rows_a):
            for j in range(cols_b):
                for k in range(cols_a):
                    result[i][j] += A[i][k] * B[k][j]
        return result
    
    @staticmethod
    def _add_matrices(A: List[List[float]], B: List[List[float]]) -> List[List[float]]:
        """Add two matrices."""
        return [[A[i][j] + B[i][j] for j in range(len(A[0]))] for i in range(len(A))]
    
    @staticmethod
    def _transpose(A: List[List[float]]) -> List[List[float]]:
        """Transpose a matrix."""
        return [[A[j][i] for j in range(len(A))] for i in range(len(A[0]))]
    
    @staticmethod
    def _invert_2x2(A: List[List[float]]) -> List[List[float]]:
        """Invert a 2x2 matrix."""
        det = A[0][0] * A[1][1] - A[0][1] * A[1][0]
        if abs(det) < 1e-10:
            # Singular matrix: return large identity
            return [[1e10, 0.0], [0.0, 1e10]]
        inv_det = 1.0 / det
        return [
            [A[1][1] * inv_det, -A[0][1] * inv_det],
            [-A[1][0] * inv_det, A[0][0] * inv_det],
        ]
