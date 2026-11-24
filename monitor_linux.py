#!/usr/bin/env python3
"""Linux memory, mount, network and block-device telemetry without extra packages."""
import argparse
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import time


def memory(text):
    values = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) == 3 and fields[2] == "kB":
            values[fields[0].rstrip(":")] = int(fields[1]) * 1024
    total, available, free, swap_total, swap_free = (
        values[key] for key in ("MemTotal", "MemAvailable", "MemFree", "SwapTotal", "SwapFree"))
    if total <= 0 or not 0 <= free <= total or not 0 <= available <= total or not 0 <= swap_free <= swap_total:
        raise ValueError("Invalid Linux memory counters")
    cache = max(0, values["Cached"] + values["Buffers"] + values["SReclaimable"] - values["Shmem"])
    return dict(total=total, used=total-available, available=available, free=free,
                cache=min(total, cache), swap=swap_total-swap_free)


def psi_some_avg10(text):
    match = re.search(r"^some avg10=([0-9.]+)", text, re.M)
    if not match:
        raise ValueError("Malformed memory PSI counters")
    return float(match[1])


def memory_lines(data, width, pressure):
    gib = 1024**3
    percent = round(100 * data["used"] / data["total"])
    prefix, suffix = f"{data['total']/gib:.0f}G[", f"{percent:3d}%]"
    cells = max(0, width-len(prefix)-len(suffix))
    filled = round(cells * percent / 100)
    return [(prefix + "#"*filled + " "*(cells-filled) + suffix)[:width],
            f"Used:{data['used']/gib:.1f}G Cache:{data['cache']/gib:.1f}G"[:width],
            f"Avail:{data['available']/gib:.1f} Swap:{data['swap']/gib:.1f}G"[:width],
            f"PSI some: {pressure}"[:width]]


def mounted(root, text):
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 10 or "-" not in fields:
            raise ValueError("Malformed mountinfo")
        path = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4])
        if path == root.rstrip("/") or path == root == "/":
            return True
    return False


def default_interface(text):
    routes = []
    for line in text.splitlines()[1:]:
        fields = line.split()
        if len(fields) >= 8 and fields[1] == "00000000" and int(fields[3], 16) & 1:
            routes.append((int(fields[6]), fields[0]))
    if not routes:
        raise ValueError("No default IPv4 route; specify --interface")
    return min(routes)[1]


def network(text, interface):
    for line in text.splitlines():
        name, separator, counters = line.partition(":")
        if separator and name.strip() == interface:
            fields = counters.split()
            return int(fields[0]), int(fields[8])
    raise ValueError(f"No network counters for {interface}")


def disk_counters(text):
    fields = list(map(int, text.split()))
    if len(fields) < 11 or any(v < 0 for v in fields):
        raise ValueError("Invalid block-device counters")
    return fields[0]+fields[4], (fields[2]+fields[6])*512


def smb_connections(text):
    connections = received = sent = 0
    for line in text.splitlines()[1:]:
        fields = line.split()
        if len(fields) >= 5 and fields[1].endswith(":01BD") and fields[3] == "01":
            tx, rx = fields[4].split(":")
            connections += 1
            received += int(rx, 16)
            sent += int(tx, 16)
    return connections, received, sent


def io_sample(root, interface, disk, interval):
    interface = interface or default_interface(Path("/proc/net/route").read_text())
    if disk:
        stat = Path("/sys/class/block") / disk / "stat"
        if not stat.is_file():
            raise ValueError(f"No block device {disk}")
    else:
        device = os.stat(root).st_dev
        device_path = Path(f"/sys/dev/block/{os.major(device)}:{os.minor(device)}")
        stat = device_path / "stat"
        disk = device_path.resolve().name if stat.is_file() else "unavailable"
    before_net = network(Path("/proc/net/dev").read_text(), interface)
    before_disk = disk_counters(stat.read_text()) if stat.is_file() else None
    start = time.monotonic()
    time.sleep(interval)
    elapsed = time.monotonic()-start
    after_net = network(Path("/proc/net/dev").read_text(), interface)
    rx, tx = [max(0, b-a)*8/elapsed/1e6 for a, b in zip(before_net, after_net)]
    kb = ops = mb = "n/a"
    if before_disk is not None:
        after_disk = disk_counters(stat.read_text())
        count, size = [b-a for a, b in zip(before_disk, after_disk)]
        if count < 0 or size < 0:
            raise ValueError("Block-device counters reset")
        kb, ops, mb = f"{size/count/1024 if count else 0:.2f}", f"{count/elapsed:.1f}", f"{size/elapsed/1e6:.2f}"
    connections = [0, 0, 0]
    for name in ("tcp", "tcp6"):
        path = Path("/proc/net") / name
        if path.exists():
            connections = [a+b for a, b in zip(connections, smb_connections(path.read_text()))]
    return [interface, disk, f"{rx:.1f}", f"{tx:.1f}", kb, ops, mb, *map(str, connections)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["memory", "io", "mount"])
    parser.add_argument("--root", default=os.environ.get("PARALLEL_WORKERS_ROOT", ""))
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--interface", default="")
    parser.add_argument("--disk", default="")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--width", type=int)
    args = parser.parse_args()
    if args.mode in ("mount", "io") and not args.root:
        parser.error("source the inventory environment or pass --root")
    if not 0.1 <= args.interval <= 60 or (args.width is not None and args.width < 1):
        parser.error("interval must be 0.1–60 seconds and width positive")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    try:
        if args.mode == "mount":
            present = mounted(args.root, Path("/proc/self/mountinfo").read_text())
            print("MOUNTED" if present else "MISSING")
            return 0 if present else 1
        if args.mode == "io":
            print("\t".join(io_sample(args.root, args.interface, args.disk, args.interval)))
            return 0
        while True:
            data = memory(Path("/proc/meminfo").read_text())
            pressure = "n/a"
            path = Path("/proc/pressure/memory")
            if path.exists():
                pressure = f"{psi_some_avg10(path.read_text()):.2f}%"
            size = shutil.get_terminal_size()
            lines = memory_lines(data, args.width or size.columns, pressure)
            if not args.once:
                lines = lines[:max(1, size.lines)]
            # A final newline scrolls a full-height pane and hides the RAM bar.
            print(("" if args.once else "\033[H\033[J") + "\n".join(lines),
                  end="\n" if args.once else "", flush=True)
            if args.once:
                return 0
            time.sleep(args.interval)
    except (OSError, ValueError, KeyError, IndexError) as error:
        print(f"Linux telemetry unavailable: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
