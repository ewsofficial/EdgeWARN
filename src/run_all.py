"""Optional all-services launcher (decomposition Phase 6).

A thin subprocess supervisor only. It starts the three direct services with
``subprocess.Popen`` using explicit argument lists and the current Python
executable, forwards SIGINT/SIGTERM, and exits nonzero if a started child
exits unexpectedly.

It performs no ingest, scientific work, or rendering; imports no pipeline
module; creates no ``multiprocessing.Manager``, queues, readiness events,
worker pools, or runtime artifacts. The launcher is not part of the readiness
protocol: cross-service coordination happens through the durable records
beneath ``<BASE_DIR>/state/realtime/`` exactly as when the services are
started directly.

Usage:

    python src/run_all.py [--services edgewarn,ewmrs,nexrad] [flags...]

Every flag the launcher accepts is routed only to the services that own it;
unset flags are simply not forwarded, so each child keeps resolving its own
YAML/env defaults and explicit CLI values always win.
"""

import argparse
import os
import signal
import subprocess
import sys
import threading
import time

from common.config import loader as config_loader, overlay


SERVICE_SCRIPTS = {
    "edgewarn": "run_edgewarn.py",
    "ewmrs": "run_ewmrs.py",
    "nexrad": "run_nexrad.py",
}

#: Bounded shutdown: SIGTERM first, then SIGKILL after this many seconds.
STOP_GRACE_SECONDS = 10.0

# Flag routing (plans/realtime-runner-decomposition-plan.md, CLI contract):
# each entry maps a launcher flag to the services that own it.
_ROUTING = {
    "--lat_limits": ("edgewarn",),
    "--lon_limits": ("edgewarn",),
    "--profile": ("edgewarn", "ewmrs", "nexrad"),
    "--disable-ctam": ("edgewarn",),
    "--disable-tracking": ("edgewarn",),
    "--disable-polygon-expansion": ("edgewarn",),
    "--disable-goes": ("edgewarn", "ewmrs"),
    "--disable-metar": ("ewmrs",),
    "--disable-nws": ("ewmrs",),
    "--disable-wpc": ("ewmrs",),
    "--refl-threshold": ("edgewarn",),
    "--min-seed-percentage": ("edgewarn",),
    "--drop-offset": ("edgewarn",),
}

#: Flags whose value is a space-separated list (nargs="+").
_LIST_FLAGS = {"--lat_limits", "--lon_limits"}


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Optional supervisor starting the three EdgeWARN services"
    )
    parser.add_argument(
        "--services",
        type=str,
        default=",".join(SERVICE_SCRIPTS),
        help="Comma-separated subset of services to start (default: all three)",
    )
    parser.add_argument("--base_dir", "--base-dir", dest="base_dir", type=str, default=None)
    parser.add_argument("--config-dir", type=str, default=None)
    parser.add_argument("--profile", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--lat_limits", nargs="+", type=float, default=None)
    parser.add_argument("--lon_limits", nargs="+", type=float, default=None)
    for flag in (
        "disable-ctam", "disable-tracking", "disable-polygon-expansion",
        "disable-goes", "disable-metar", "disable-nws", "disable-wpc",
        "disable-ewmrs", "disable-nexrad",
    ):
        parser.add_argument(f"--{flag}", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--refl-threshold", type=float, default=None)
    parser.add_argument("--min-seed-percentage", type=float, default=None)
    parser.add_argument("--drop-offset", type=float, default=None)
    parser.add_argument("--mrms-core-only", action=argparse.BooleanOptionalAction, default=None)
    args = parser.parse_args(argv)

    requested = [
        name.strip() for name in args.services.split(",") if name.strip()
    ]
    unknown = [name for name in requested if name not in SERVICE_SCRIPTS]
    if unknown:
        parser.error(
            f"unknown service(s): {', '.join(unknown)}; "
            f"expected any of {', '.join(SERVICE_SCRIPTS)}"
        )

    # Service omission flags resolve from YAML when not given on the CLI so a
    # deployment unit can pin its topology without repeating flags. The other
    # disable flags are pure pass-through: children keep their own layering.
    run_cfg = config_loader.load_config("runtime", config_dir=args.config_dir)["run"]
    omit_ewmrs = bool(overlay.resolve(
        args.disable_ewmrs,
        env_names=["EDGEWARN_DISABLE_EWMRS"],
        yaml_value=run_cfg["disable_ewmrs"],
        key="run.disable_ewmrs",
    ))
    omit_nexrad = bool(overlay.resolve(
        args.disable_nexrad,
        env_names=["EDGEWARN_DISABLE_NEXRAD"],
        yaml_value=run_cfg["disable_nexrad"],
        key="run.disable_nexrad",
    ))

    services = list(requested)
    if omit_ewmrs:
        services = [name for name in services if name != "ewmrs"]
    if omit_nexrad:
        services = [name for name in services if name != "nexrad"]
    if args.mrms_core_only:
        # MRMS-core-only implies disabling every non-primary component.
        services = [name for name in services if name == "edgewarn"]

    if not services:
        parser.error("service selection resolved to an empty set")
    return args, services


def build_service_commands(args, services, src_root):
    """Explicit argv per selected service; unset flags are never forwarded."""
    commands = {}
    for service in services:
        cmd = [sys.executable, str(os.path.join(src_root, SERVICE_SCRIPTS[service]))]
        if args.base_dir is not None:
            cmd += ["--base-dir", args.base_dir]
        if args.config_dir is not None:
            cmd += ["--config-dir", args.config_dir]
        for flag, owners in _ROUTING.items():
            if service not in owners:
                continue
            value = getattr(args, flag.lstrip("-").replace("-", "_"))
            if value is None:
                continue
            if value is True:
                cmd.append(flag)
            elif value is False:
                cmd.append(f"--no-{flag.lstrip('-')}")
            elif isinstance(value, list) or flag in _LIST_FLAGS:
                cmd += [flag] + [str(item) for item in value]
            else:
                cmd += [flag, str(value)]
        if service == "edgewarn" and args.mrms_core_only is not None:
            # Forwarded like every other routed boolean so an explicit
            # --no-mrms-core-only overrides a YAML-set true instead of
            # letting the child silently re-resolve it.
            cmd.append("--mrms-core-only" if args.mrms_core_only else "--no-mrms-core-only")
        commands[service] = cmd
    return commands


def supervise(commands, *, src_root, stop_event=None):
    """Start every command, forward signals, and wait; returns an exit code.

    A child exiting unexpectedly stops the launcher with a nonzero code after
    terminating the remaining children; a signal-driven shutdown exits zero
    when every child terminated within the grace window.
    """
    if stop_event is None:
        stop_event = threading.Event()

    previous_handlers = {}
    original_int = signal.getsignal(signal.SIGINT)

    def _forward(signum, _frame):
        print(f"[Launcher] Signal {signum} received; stopping children...")
        stop_event.set()

    # Install handlers BEFORE spawning so a signal landing during startup
    # still tears down whatever children already exist.
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous_handlers[signum] = signal.signal(signum, _forward)
        except ValueError:
            pass

    def _terminate_children(force=False):
        for proc in processes.values():
            if proc.poll() is None:
                try:
                    proc.send_signal(signal.SIGKILL if force else signal.SIGTERM)
                except OSError:
                    pass

    processes: dict[str, subprocess.Popen] = {}
    exit_code = 0
    try:
        for service, cmd in commands.items():
            try:
                processes[service] = subprocess.Popen(cmd, cwd=src_root)
            except Exception:
                print(f"[Launcher] Failed to start '{service}'; stopping started children")
                stop_event.set()
                _terminate_children()
                raise
        print(f"[Launcher] Started: {', '.join(f'{s} (pid {p.pid})' for s, p in processes.items())}")

        while not stop_event.is_set():
            for service, proc in processes.items():
                code = proc.poll()
                if code is None:
                    continue
                detail = (
                    "cleanly before shutdown"
                    if code == 0
                    else f"unexpectedly (rc={code})"
                )
                print(
                    f"[Launcher] Service '{service}' exited {detail}; "
                    "stopping the remaining children"
                )
                exit_code = 1
                stop_event.set()
                break
            if stop_event.is_set():
                break
            time.sleep(0.5)

        # Graceful stop: SIGTERM admits no new work in children; wait the
        # bounded interval, then escalate to SIGKILL for survivors.
        _terminate_children()
        deadline = time.monotonic() + STOP_GRACE_SECONDS
        while time.monotonic() < deadline:
            if all(proc.poll() is not None for proc in processes.values()):
                break
            time.sleep(0.2)
        survivors = [s for s, p in processes.items() if p.poll() is None]
        if survivors:
            print(f"[Launcher] Escalating to SIGKILL for: {', '.join(survivors)}")
            _terminate_children(force=True)
            exit_code = exit_code or 1

        for service, proc in processes.items():
            proc.wait()
            print(f"[Launcher] Service '{service}' terminated (rc={proc.returncode})")
    finally:
        try:
            signal.signal(signal.SIGINT, original_int)
        except ValueError:
            # Signal handlers can only be (re)installed from the main thread;
            # supervised-in-thread callers (tests) restore nothing.
            pass
        for signum, handler in previous_handlers.items():
            try:
                signal.signal(signum, handler)
            except ValueError:
                pass

    return exit_code


def main(argv=None):
    args, services = _parse_args(argv)
    src_root = os.path.dirname(os.path.abspath(__file__))
    commands = build_service_commands(args, services, src_root)
    print(
        "[Launcher] This optional supervisor performs no ingest, rendering, or "
        "coordination work; the direct commands remain the production path."
    )
    return supervise(commands, src_root=src_root)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("CTRL+C detected, exiting ...")
        sys.exit(0)
