import ctypes
import multiprocessing
import signal


def set_process_name(name: str) -> None:
    try:
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


def install_exit_signal_handlers() -> None:
    def _raise_system_exit(signum, _frame):
        raise SystemExit(signum)

    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(signum, _raise_system_exit)
        except Exception:
            pass


def configure_process_runtime(name: str) -> None:
    set_process_name(name)
    set_parent_death_signal()
    install_exit_signal_handlers()
