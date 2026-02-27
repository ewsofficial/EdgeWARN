from EdgeWARN.core.ctam.modules.StormCast.core.diagnostics import compute_bunkers_motion, compute_height_weights, compute_adaptive_steering, compute_effective_shear
from EdgeWARN.core.ctam.modules.StormCast.core.types import EnvironmentProfile

profile = EnvironmentProfile(
    winds={
        850: (10.0, 5.0),
        700: (15.0, 8.0),
        500: (20.0, 10.0),
        250: (25.0, 12.0)
    }
)
h_core = 6.0

print("Height weights:", compute_height_weights(h_core))
u_mean, v_mean = compute_adaptive_steering(profile, h_core)
print(f"Adaptive steering (u, v): {u_mean:.2f}, {v_mean:.2f}")

shear_u, shear_v = compute_effective_shear(profile, h_core)
print(f"Effective shear (u, v): {shear_u:.2f}, {shear_v:.2f}")

motion = compute_bunkers_motion(profile, h_core)
print(f"Bunkers motion (u, v): {motion[0]:.2f}, {motion[1]:.2f}")