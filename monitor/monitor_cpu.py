#!/usr/bin/env python3
"""Compact macOS/Linux per-core meters; resize redraws reuse the latest sample."""
import argparse
import ctypes
import os
from pathlib import Path
import select
import signal
import sys
import time


class CpuTicks:
    def __init__(self):
        if sys.platform.startswith("linux"):
            self.lib = None
            return
        if sys.platform != "darwin":
            raise RuntimeError("CPU sampling requires macOS")
        self.lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib")
        self.lib.mach_host_self.restype = ctypes.c_uint
        self.host = self.lib.mach_host_self()
        self.task = ctypes.c_uint.in_dll(self.lib, "mach_task_self_").value
        self.lib.host_processor_info.argtypes = [
            ctypes.c_uint, ctypes.c_int, ctypes.POINTER(ctypes.c_uint),
            ctypes.POINTER(ctypes.POINTER(ctypes.c_uint)), ctypes.POINTER(ctypes.c_uint)]
        self.lib.host_processor_info.restype = ctypes.c_int
        self.lib.vm_deallocate.argtypes = [ctypes.c_uint, ctypes.c_size_t, ctypes.c_size_t]
        self.lib.vm_deallocate.restype = ctypes.c_int

    def read(self):
        if self.lib is None:
            return linux_ticks(Path("/proc/stat").read_text())
        cores, count = ctypes.c_uint(), ctypes.c_uint()
        ticks = ctypes.POINTER(ctypes.c_uint)()
        status = self.lib.host_processor_info(
            self.host, 2, ctypes.byref(cores), ctypes.byref(ticks), ctypes.byref(count))
        if status:
            raise RuntimeError(f"host_processor_info failed: {status}")
        try:
            if not cores.value or count.value != cores.value * 4:
                raise RuntimeError("Unexpected per-core counter layout")
            return [tuple(ticks[i:i + 4]) for i in range(0, count.value, 4)]
        finally:
            status = self.lib.vm_deallocate(
                self.task, ctypes.cast(ticks, ctypes.c_void_p).value,
                count.value * ctypes.sizeof(ctypes.c_uint))
            if status:
                raise RuntimeError(f"vm_deallocate failed: {status}")


def linux_ticks(text):
    result = []
    for line in text.splitlines():
        fields = line.split()
        if not fields or not fields[0].startswith("cpu") or not fields[0][3:].isdigit():
            continue
        values = list(map(int, fields[1:]))
        if len(values) < 8 or any(value < 0 for value in values):
            raise ValueError("Incomplete Linux CPU counters")
        user, nice, system, idle, iowait, irq, softirq, steal = values[:8]
        # Guest time is already included in user/nice; don't count it twice.
        result.append((user, system + irq + softirq + steal, idle + iowait, nice))
    if not result:
        raise ValueError("No per-core CPU counters")
    return result


def usage(previous, current):
    if len(previous) != len(current):
        raise RuntimeError("CPU count changed while monitoring")
    result = []
    for old, new in zip(previous, current):
        user, system, idle, nice = [(b - a) % (1 << 32) for a, b in zip(old, new)]
        total = user + system + idle + nice
        result.append(((user + nice) / total, system / total) if total else (0.0, 0.0))
    return result


def total_percent(values):
    return round(100 * sum(user + system for user, system in values) / len(values))


def title_sequence(prefix, suffix, values):
    """tmux takes the pane title from OSC 2, so the stream labels its own pane."""
    return f"\033]2;{prefix} {total_percent(values)}%{suffix}\033\\"


def watch_snapshot(args, resize_read):
    """Draw one host from the collector's snapshot; redraw only when it or the pane size changes."""
    import monitor_snapshot  # Only launchers render snapshots; sampling hosts never need it.
    reader, shown = monitor_snapshot.Reader(args.snapshot), None
    while True:
        values, problem = monitor_snapshot.cpu_state(reader, args.host)
        size = os.get_terminal_size(sys.stdout.fileno())
        if (values, problem, size) != shown:
            shown = (values, problem, size)
            # A host without a fresh sample must not keep its last total in the pane title.
            title = f"\033]2;{args.title_prefix}{args.title_suffix}\033\\" if problem else \
                title_sequence(args.title_prefix, args.title_suffix, values)
            lines = monitor_snapshot.wrap(problem, size.columns, size.lines) if problem else \
                render(values, size.columns, size.lines)
            print(title + "\033[H\033[J" + "".join(
                f"\033[{i + 1};1H{line}" for i, line in enumerate(lines)), end="", flush=True)
        if select.select([resize_read], [], [], 0.5)[0]:
            os.read(resize_read, 4096)


def render(values, width, height):
    """Column-major packing maximizes meter width without hiding cores."""
    count = len(values)
    if not count or width < 1 or height < 1:
        raise ValueError("CPU count and terminal dimensions must be positive")
    columns = (count + height - 1) // height
    rows = (count + columns - 1) // columns
    cell_width, extra = divmod(width - columns + 1, columns)
    digits = len(str(count))
    bars = cell_width >= digits + 9  # label, brackets, percentage, >=1 bar cell
    if cell_width < digits + 5:
        return ["Resize to show all CPU cores"[:width]]
    lines = []
    for row in range(rows):
        cells = []
        for column in range(columns):
            index = column * rows + row
            if index >= count:
                break
            size = cell_width + (column < extra)
            user, system = values[index]
            percent = (user + system) * 100
            if bars:
                length = size - digits - 8
                used = min(length, round((user + system) * length))
                sys_cells = min(used, round(system * length))
                bar = "#" * (used - sys_cells) + "*" * sys_cells + " " * (length - used)
                cells.append(f"{index + 1:>{digits}}[{bar}{percent:5.1f}%]")
            else:
                cells.append(f"{index + 1:>{digits}}:{percent:3.0f}%".ljust(size))
        lines.append(" ".join(cells))
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval", type=float, default=1.5)
    parser.add_argument("--width", type=int)
    parser.add_argument("--height", type=int)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--total", action="store_true", help="print one whole-node CPU percentage")
    parser.add_argument("--title-prefix", help="set the pane title to PREFIX <total>%%SUFFIX after each sample")
    parser.add_argument("--title-suffix", default="")
    parser.add_argument("--snapshot", help="render --host from this cluster-status.json instead of sampling")
    parser.add_argument("--host", help="host label in the snapshot")
    args = parser.parse_args()
    if not 0.2 <= args.interval <= 60 or any(v is not None and v < 1 for v in (args.width, args.height)):
        parser.error("interval must be 0.2–60 seconds; dimensions must be positive")
    if args.snapshot and (not args.host or args.title_prefix is None or args.once or args.total):
        parser.error("--snapshot needs --host and --title-prefix and draws continuously")
    if not args.snapshot:
        sampler = CpuTicks()
        previous = sampler.read()
    if args.total:
        time.sleep(args.interval)
        values = usage(previous, sampler.read())
        print(total_percent(values))
        return
    resize_read, resize_write = os.pipe()
    os.set_blocking(resize_write, False)
    signal.set_wakeup_fd(resize_write)
    signal.signal(signal.SIGWINCH, lambda *_: None)
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, lambda *_: sys.exit(0))
    deadline = time.monotonic() + args.interval
    values = None
    try:
        if not args.once:
            print("\033[?25l", end="", flush=True)
        if args.snapshot:
            watch_snapshot(args, resize_read)
        while True:
            ready, _, _ = select.select([resize_read], [], [], max(0, deadline - time.monotonic()))
            if ready:
                os.read(resize_read, 4096)
            title = ""
            if time.monotonic() >= deadline:
                current = sampler.read()
                values = usage(previous, current)
                previous = current
                deadline = time.monotonic() + args.interval
                if args.title_prefix is not None:
                    title = title_sequence(args.title_prefix, args.title_suffix, values)
            if values is None:
                continue
            if args.width is None or args.height is None:
                size = os.get_terminal_size(sys.stdout.fileno())
            width = args.width if args.width is not None else size.columns
            height = args.height if args.height is not None else size.lines
            lines = render(values, width, height)
            if args.once:
                print("\n".join(lines))
                return
            print(title + "\033[H\033[J" + "".join(
                f"\033[{i + 1};1H{line}" for i, line in enumerate(lines)), end="", flush=True)
    finally:
        signal.set_wakeup_fd(-1)
        os.close(resize_read)
        os.close(resize_write)
        if not args.once:
            print("\033[0m\033[?25h", end="", flush=True)


if __name__ == "__main__":
    main()
