"""Tkinter server monitor for Python processes and system resources."""
from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import scrolledtext, ttk
from typing import Any

from .log_watcher import LogTailer, QueueLogAdapter
from .metrics import MetricSnapshot, MetricsSampler


class MemoryGraph(tk.Canvas):
    """Simple line chart for visualizing memory usage history."""

    def __init__(self, master: tk.Widget, max_points: int = 120, **kwargs) -> None:
        super().__init__(master, **kwargs)
        self.max_points = max_points
        self.history: list[float] = []
        self.configure(background="#101014", highlightthickness=0)

    def add_point(self, value: float) -> None:
        self.history.append(value)
        if len(self.history) > self.max_points:
            self.history.pop(0)
        self._redraw()

    def _redraw(self) -> None:
        self.delete("all")
        width = self.winfo_width() or int(self.cget("width"))
        height = self.winfo_height() or int(self.cget("height"))
        if not self.history:
            return

        padding = 10
        usable_width = width - 2 * padding
        usable_height = height - 2 * padding
        max_val = 1000.0  # Fixed scale: 0 to 1000 MB

        # Grid lines every 100 MB
        for i in range(1, 11):
            val = i * 100
            y_fraction = 1.0 - (val / max_val)
            y = padding + usable_height * y_fraction
            self.create_line(
                padding,
                y,
                width - padding,
                y,
                fill="#2a2a38",
            )
            self.create_text(
                width - padding,
                y - 2,
                text=f"{val} MB",
                anchor="ne",
                fill="#6c6c7a",
                font=("Arial", 8),
            )

        if len(self.history) < 2:
            return

        step_x = usable_width / (self.max_points - 1)
        coords: list[float] = []
        for idx, val in enumerate(self.history):
            x = padding + idx * step_x
            # Clamp value to max_val for drawing
            draw_val = min(val, max_val)
            y = padding + usable_height * (1 - draw_val / max_val)
            coords.extend([x, y])
        self.create_line(*coords, fill="#7bd7ff", width=2, smooth=True)

        latest = self.history[-1]
        self.create_text(
            padding + 5,
            padding + 5,
            text=f"Python memory: {latest:.1f} MB",
            anchor="nw",
            fill="#d8e9ff",
            font=("Arial", 10, "bold"),
        )


class StatsPanel(ttk.Frame):
    """Displays CPU, GPU and network usage metrics."""

    def __init__(self, master: tk.Widget) -> None:
        super().__init__(master)
        self.configure(padding=10)
        self.columnconfigure(1, weight=1)

        self.cpu_var = tk.StringVar()
        self.python_cpu_var = tk.StringVar()
        self.mem_var = tk.StringVar()
        self.net_var = tk.StringVar()
        self.gpu_var = tk.StringVar()

        self._add_labeled_bar("CPU (total)", self.cpu_var, 0)
        self._add_labeled_bar("CPU (python)", self.python_cpu_var, 1)
        self._add_labeled_bar("Memory (total)", self.mem_var, 2)
        self._add_labeled_bar("Network", self.net_var, 3, maximum=100)
        self._add_labeled_bar("GPU", self.gpu_var, 4, maximum=100)

    def _add_labeled_bar(self, label: str, text_var: tk.StringVar, row: int, maximum: int = 100) -> None:
        ttk.Label(self, text=label).grid(row=row, column=0, sticky="w", pady=2)
        bar = ttk.Progressbar(self, orient="horizontal", mode="determinate", maximum=maximum)
        bar.grid(row=row, column=1, sticky="ew", pady=2, padx=(6, 0))
        value_label = ttk.Label(self, textvariable=text_var, width=16)
        value_label.grid(row=row, column=2, sticky="e", pady=2, padx=(6, 0))
        setattr(self, f"_bar_{row}", bar)

    def update_stats(self, snapshot: MetricSnapshot) -> None:
        getattr(self, "_bar_0")['value'] = snapshot.total_cpu
        self.cpu_var.set(f"{snapshot.total_cpu:5.1f}%")

        getattr(self, "_bar_1")['value'] = snapshot.python_cpu
        self.python_cpu_var.set(f"{snapshot.python_cpu:5.1f}%")

        getattr(self, "_bar_2")['value'] = snapshot.total_mem_percent
        mem_gb = snapshot.python_mem_bytes / (1024 ** 3)
        self.mem_var.set(f"{snapshot.total_mem_percent:5.1f}% | Py {mem_gb:.2f} GB")

        net_util = min(100.0, (snapshot.net_sent_per_s + snapshot.net_recv_per_s) / (1024 * 1024) * 10)
        getattr(self, "_bar_3")['value'] = net_util
        sent_mbps = snapshot.net_sent_per_s * 8 / (1024 * 1024)
        recv_mbps = snapshot.net_recv_per_s * 8 / (1024 * 1024)
        self.net_var.set(f"{sent_mbps:.2f}↑ / {recv_mbps:.2f}↓ Mbps")

        if snapshot.gpu_util_percent is None:
            getattr(self, "_bar_4")['value'] = 0
            self.gpu_var.set("GPU: N/A")
        else:
            getattr(self, "_bar_4")['value'] = snapshot.gpu_util_percent
            mem_text = (
                f"{snapshot.gpu_util_percent:4.1f}%"
                if snapshot.gpu_mem_percent is None
                else f"{snapshot.gpu_util_percent:4.1f}% | {snapshot.gpu_mem_percent:4.1f}% mem"
            )
            self.gpu_var.set(mem_text)


class LogPanel(ttk.Frame):
    """Scrollable text widget that streams server logs."""

    def __init__(self, master: tk.Widget, tailer: LogTailer | QueueLogAdapter) -> None:
        super().__init__(master)
        self.tailer = tailer
        self.configure(padding=10)
        self.text = scrolledtext.ScrolledText(
            self,
            wrap=tk.WORD,
            height=30,
            width=60,
            background="#0f0f12",
            foreground="#d8e9ff",
            insertbackground="#d8e9ff",
        )
        self.text.pack(fill="both", expand=True)
        self.text.configure(state="disabled")

    def append_lines(self, lines: list[str]) -> None:
        if not lines:
            return
        self.text.configure(state="normal")
        for line in lines:
            self.text.insert(tk.END, line)
        
        # Trim the log to avoid unbounded growth.
        current = self.text.get("1.0", tk.END).splitlines()[-self.tailer.max_lines :]
        self.text.delete("1.0", tk.END)
        self.text.insert(tk.END, "\n".join(current) + "\n")
            
        self.text.see(tk.END)
        self.text.configure(state="disabled")


class MonitorApp(tk.Tk):
    """Main application window."""

    def __init__(self, log_file: Path | None = None, log_queue: Any | None = None, refresh_ms: int = 1000) -> None:
        super().__init__()
        self.title("EdgeWARN Server Monitor")
        self.geometry("1100x650")
        self.configure(background="#06060a")

        self.sampler = MetricsSampler()
        self.refresh_ms = refresh_ms

        if log_queue:
            self.tailer = QueueLogAdapter(log_queue)
        else:
            log_path = log_file or Path("server.log")
            self.tailer = LogTailer(log_path)
            self.tailer.seed([
                "EdgeWARN server monitor initialized.\n",
                "Watching resource usage for Python processes...\n",
            ])

        self._build_layout()
        self.after(self.refresh_ms, self._update_loop)

    def _build_layout(self) -> None:
        self.columnconfigure(0, weight=1, uniform="col")
        self.columnconfigure(1, weight=1, uniform="col")
        self.rowconfigure(0, weight=1)
        self.rowconfigure(1, weight=0)

        # Memory graph (top-left)
        self.memory_graph = MemoryGraph(self, width=500, height=250)
        self.memory_graph.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        # Log panel (right)
        self.log_panel = LogPanel(self, tailer=self.tailer)
        self.log_panel.grid(row=0, column=1, rowspan=2, sticky="nsew", padx=10, pady=10)

        # Stats panel (bottom-left)
        self.stats_panel = StatsPanel(self)
        self.stats_panel.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))

    def _update_loop(self) -> None:
        snapshot = self.sampler.sample()
        # Convert bytes to MB
        mem_mb = snapshot.python_mem_bytes / (1024 * 1024)
        self.memory_graph.add_point(mem_mb)
        self.stats_panel.update_stats(snapshot)

        lines = list(self.tailer.read_new_lines())
        self.log_panel.append_lines(lines)

        self.after(self.refresh_ms, self._update_loop)


def run(log_file: str | None = None, log_queue: Any | None = None, refresh_ms: int = 1000) -> None:
    app = MonitorApp(
        log_file=Path(log_file) if log_file else None,
        log_queue=log_queue,
        refresh_ms=refresh_ms
    )
    app.mainloop()


if __name__ == "__main__":
    run()
