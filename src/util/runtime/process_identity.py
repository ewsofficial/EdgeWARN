"""Process identity helpers shared by supervised runtime children.

Kept free of any scientific-stack imports so every service's loop modules can
use them without widening their import graphs.
"""

import ctypes
import signal


def set_process_name(name: str) -> None:
    try:
        import multiprocessing

        multiprocessing.current_process().name = name
    except Exception:
        pass

    try:
        libc = ctypes.CDLL(None)
        pr_set_name = 15
        encoded = name.encode("utf-8")[:15]
        libc.prctl(pr_set_name, ctypes.c_char_p(encoded), 0, 0, 0)
    except Exception:
        pass


def set_parent_death_signal(sig: int = signal.SIGTERM) -> None:
    try:
        libc = ctypes.CDLL(None)
        pr_set_pdeathsig = 1
        libc.prctl(pr_set_pdeathsig, sig, 0, 0, 0)
    except Exception:
        pass
