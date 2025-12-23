"""Textual-based terminal monitor for EdgeWARN processes and system resources."""
from __future__ import annotations

from collections import deque
from pathlib import Path
from typing import Any, Iterable

from rich.panel import Panel
from rich.rule import Rule
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import Footer, Header, Log, Static

from .log_watcher import LogTailer, QueueLogAdapter
from .metrics import MetricSnapshot, MetricsSampler

# Characters for high resolution sparklines.
SPARKLINE_CHARS = "▁▂▃▄▅▆▇█"


def _compress_series(values: Iterable[float], width: int) -> list[float]:
    """Down-sample a series to fit the available width."""
    series = list(values)
    if len(series) <= width:
        return series
    stride = len(series) / width
    windowed: list[float] = []
    for idx in range(width):
        start = int(idx * stride)
        end = int((idx + 1) * stride)
        bucket = series[start:end] or [series[-1]]
        windowed.append(max(bucket))
    return windowed


def _sparkline(values: Iterable[float], max_value: float, width: int) -> Text:
    """Render a neon-style sparkline from numeric values."""
    line = Text()
    scaled = _compress_series(values, width)
    ceiling = max(max_value, 1e-6)
    for idx, value in enumerate(scaled):
        level = int((value / ceiling) * (len(SPARKLINE_CHARS) - 1))
        level = max(0, min(level, len(SPARKLINE_CHARS) - 1))
        # Gradient: magenta -> cyan -> blue for that "wow" vibe.
        hue = 200 + int(60 * (idx / max(1, len(scaled) - 1)))
        color = f"hsl({hue} 80% 60%)"
        line.append(SPARKLINE_CHARS[level], style=color)
    return line


class MemoryGraph(Static):
    """Top-left widget: live memory history rendered as a sparkline."""

    def __init__(self, max_points: int = 240, target_max_mb: float = 1600.0) -> None:
        super().__init__()
        self.history: deque[float] = deque(maxlen=max_points)
        self.target_max_mb = target_max_mb

    def push(self, value_mb: float) -> None:
        """Append a new memory reading and request a re-render."""
        self.history.append(value_mb)
        self.refresh()

    def render(self) -> Panel:
        width = max(self.size.width - 8, 24)
        if not self.history:
            return Panel(
                Text("Awaiting metrics ...", style="italic #7bd7ff"),
                title="Memory Flow",
                border_style="bold magenta",
                padding=(1, 2),
            )

        latest = self.history[-1]
        peak = max(self.history)
        spark = _sparkline(self.history, max(self.target_max_mb, peak), width)

        body = Text()
        body.append(f" live: {latest:6.1f} MB  ", style="bold cyan")
        body.append(f"peak: {peak:6.1f} MB\n", style="bold #ff79c6")
        body.append(Rule(style="#1f2335"))
        body.append("\n")
        body.append(spark)

        return Panel(
            body,
            title="Memory Flow (Python RSS)",
            subtitle="glow = recent, trail = history",
            border_style="bold magenta",
            padding=(1, 2),
        )


class ResourceMeters(Static):
    """Bottom-left widget: animated bars for CPU and network throughput."""

    snapshot: reactive[MetricSnapshot | None] = reactive(None)
    peak_net_mbps: reactive[float] = reactive(0.1)

    def update_snapshot(self, snapshot: MetricSnapshot) -> None:
        """Store a fresh snapshot and schedule a redraw."""
        self.snapshot = snapshot
        throughput_now = (snapshot.net_sent_per_s + snapshot.net_recv_per_s) * 8 / (1024 * 1024)
        self.peak_net_mbps = max(self.peak_net_mbps, throughput_now, 0.1)
        self.refresh()

    def _bar_line(
        self,
        label: str,
        value: float,
        limit: float,
        width: int,
        color: str,
        unit: str,
        hint: str | None = None,
    ) -> Text:
        """Construct a single gradient meter line."""
        effective_limit = max(limit, 1e-6)
        ratio = min(value / effective_limit, 1.0)
        filled = int(ratio * width)
        bar = Text()
        bar.append("█" * filled, style=color)
        bar.append("░" * (width - filled), style="#2e2e48")

        line = Text()
        line.append(f"{label:<12}", style="bold white")
        line.append(bar)
        line.append(f"  {value:6.2f}{unit}", style=f"bold {color}")
        if hint:
            line.append(f"   {hint}", style="#7e8aa0")
        return line

    def render(self) -> Panel:
        if self.snapshot is None:
            return Panel(
                Text("Spooling up metrics ...", style="italic #7bd7ff"),
                title="Resource Pulse",
                border_style="bold blue",
                padding=(1, 2),
            )

        snap = self.snapshot
        width = max(self.size.width - 16, 32)

        net_up = snap.net_sent_per_s * 8 / (1024 * 1024)
        net_down = snap.net_recv_per_s * 8 / (1024 * 1024)
        net_total = net_up + net_down

        lines = [
            self._bar_line("CPU total", snap.total_cpu, 100.0, width, "violet", "%"),
            self._bar_line("CPU python", snap.python_cpu, 100.0, width, "cyan", "%"),
            self._bar_line(
                "Net total",
                net_total,
                self.peak_net_mbps,
                width,
                "chartreuse3",
                " Mbps",
                hint=f"{net_up:.2f}↑ / {net_down:.2f}↓ Mbps (peak {self.peak_net_mbps:.2f})",
            ),
        ]

        body = Text("\n").join(lines)
        return Panel(
            body,
            title="Resource Pulse",
            subtitle="auto-scales with your peaks",
            border_style="bold blue",
            padding=(1, 2),
        )


class MonitorApp(App[None]):
    """Main terminal UI powered by Textual."""

    CSS = """
    Screen {
        background: #06080f;
        color: #e8f1ff;
    }
    #layout {
        layout: horizontal;
        height: 1fr;
    }
    #left-column {
        layout: vertical;
        width: 1fr;
        gutter: 1 0;
        padding: 1 1 0 1;
    }
    #memory {
        height: 2fr;
        background: transparent;
    }
    #meters {
        height: 1fr;
        background: transparent;
    }
    #log-container {
        width: 1.15fr;
        padding: 1 1 1 0;
    }
    Log {
        background: #0a0d16;
        color: #dbe8ff;
        border: round #8be9fd;
        padding: 1 1;
    }
    Header {
        background: #0b1020;
        color: #a0c4ff;
    }
    Footer {
        background: #0b1020;
        color: #a0c4ff;
    }
    """

    BINDINGS = [("q", "quit", "Quit")]

    def __init__(self, log_file: Path | None = None, log_queue: Any | None = None, refresh_ms: int = 1000) -> None:
        super().__init__()
        self.refresh_seconds = max(refresh_ms / 1000, 0.1)
        self.sampler = MetricsSampler()
        if log_queue is not None:
            self.tailer: LogTailer | QueueLogAdapter = QueueLogAdapter(log_queue)
        else:
            log_path = log_file or Path("server.log")
            self.tailer = LogTailer(log_path)
            self.tailer.seed(
                [
                    "EdgeWARN terminal monitor initialized.\n",
                    "Streaming resource usage for Python processes...\n",
                ]
            )

    def compose(self) -> ComposeResult:
        """Build the UI tree."""
        yield Header(show_clock=True)
        with Container(id="layout"):
            with Vertical(id="left-column"):
                yield MemoryGraph(id="memory")
                yield ResourceMeters(id="meters")
            with Container(id="log-container"):
                yield Log(id="log-view", auto_scroll=True, wrap=True, max_lines=1200)
        yield Footer()

    def on_mount(self) -> None:
        """Kick off periodic sampling and logging."""
        self.memory_graph = self.query_one("#memory", MemoryGraph)
        self.meters = self.query_one("#meters", ResourceMeters)
        self.log_view = self.query_one("#log-view", Log)

        self.set_interval(self.refresh_seconds, self._refresh_metrics)
        self.set_interval(self.refresh_seconds / 2, self._drain_logs)

        self.log_view.write("[magenta]EdgeWARN Terminal Monitor[/] · press [bold]Q[/] to exit")
        self.log_view.write("Listening for process output...\n")

    def _refresh_metrics(self) -> None:
        """Sample metrics and push them into the widgets."""
        snapshot = self.sampler.sample()
        mem_mb = snapshot.python_mem_bytes / (1024 * 1024)
        self.memory_graph.push(mem_mb)
        self.meters.update_snapshot(snapshot)

    def _drain_logs(self) -> None:
        """Read any newly available log lines and stream them to the right panel."""
        lines = list(self.tailer.read_new_lines())
        for line in lines:
            self.log_view.write(line.rstrip("\n"))


def run(log_file: str | None = None, log_queue: Any | None = None, refresh_ms: int = 1000) -> None:
    """Entry point used by multiprocessing from the scheduler."""
    app = MonitorApp(
        log_file=Path(log_file) if log_file else None,
        log_queue=log_queue,
        refresh_ms=refresh_ms,
    )
    app.run()


if __name__ == "__main__":
    run()
