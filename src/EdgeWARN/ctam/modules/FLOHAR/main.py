"""
FLOHAR — Standalone Entry Point

For testing/debugging the FLOHAR processor outside of CTAM.
For production use, the module is registered and run via the CTAM pipeline.
"""


def run_flohar():
    """
    Standalone entry point for FLOHAR processing.

    For CTAM integration, use FLOHARModule in flohar_module.py instead.
    This is provided for ad-hoc testing and debugging.
    """
    from .flohar_module import FLOHARModule

    module = FLOHARModule()
    result = module.run()

    metadata = result.get("metadata", {})
    print(f"FLOHAR processing complete: {metadata}")
    return result


if __name__ == "__main__":
    run_flohar()
