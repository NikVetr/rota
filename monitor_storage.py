#!/usr/bin/env python3
"""Slow disk-space collection; fast rendering never queries a filesystem mount."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time

RESET = "\033[0m"
WHITE = "\033[97m"
COLORS = {"ok": "\033[38;5;120m", "warn": "\033[38;5;228m",
          "bad": "\033[38;5;203m"}


def node_label(name):
    return name[2:] if re.fullmatch(r"mm[0-9]+", name) else name


def node_prefix(names):
    return "Mini GB " if all(re.fullmatch(r"mm[0-9]+", name) for name in names) else "Node GB "


def required_width(names, readings=None):
    readings = readings or {}
    cells = []
    for name in names:
        free = readings.get(name, {}).get("free", 0)
        # Startup needs no remote queries; allow GB values up to a 10-TB disk.
        digits = max(4, len(str(int(free / 1e9))))
        cells.append(len(node_label(name)) + 1 + digits)
    # The I/O pane renders one column narrower than the pane to avoid wrapping.
    return max(32, len(node_prefix(names)) + sum(cells) + max(0, len(cells) - 1) + 1)


def sample(path, require_mount=False):
    try:
        if require_mount and not os.path.ismount(path):
            raise OSError(f"Not mounted: {path}")
        usage = shutil.disk_usage(path)
        return {"total": usage.total, "free": usage.free, "used": usage.used}
    except OSError as error:
        return {"error": str(error)}


def free_text(reading, unit="GB"):
    if "error" in reading or not reading:
        return WHITE + "?"
    free = reading["free"] / 1e9
    color = COLORS["bad" if free < 100 else "warn" if free < 500 else "ok"]
    return color + f"{int(free)}{unit}" + WHITE


def bar(name, reading, width, value_width=0):
    if "error" in reading or not reading:
        return WHITE + f"{name:<4} {'?':>{value_width}}" + RESET
    free = reading["free"] / 1e9
    percent = round(100 * reading["used"] / reading["total"])
    value = f"{int(free)}GB"
    padding = " " * max(0, value_width - len(value))
    prefix, suffix = f"{name:<4} {padding}{value}[", f"{percent:3d}%]"
    colored_prefix = WHITE + f"{name:<4} {padding}" + free_text(reading)
    if width < len(prefix) + len(suffix) + 2:
        return colored_prefix + RESET
    cells = max(0, width - len(prefix) - len(suffix))
    filled = min(cells, round(cells * percent / 100))
    return colored_prefix + "[" + "#" * filled + " " * (cells - filled) + suffix + RESET


def render(path, width, now=None):
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return WHITE + "Disk space unavailable" + RESET
    return render_data(data, width, now)


def render_data(data, width, now=None):
    now = time.time() if now is None else now
    try:
        age = now - data["epoch"]
        if age < 0 or age > 360:
            return WHITE + "Disk space stale" + RESET
        minis = data.get("minis", {})
        prefix = node_prefix(minis)
        mini_line = WHITE + prefix + " ".join(
            node_label(name) + ":" + free_text(reading, "")
            for name, reading in minis.items()) + RESET if minis else WHITE + "Mini GB unavailable" + RESET
        if len(re.sub(r"\x1b\[[0-9;]*m", "", mini_line)) > width:
            mini_line = mini_line.replace(prefix, "GB ", 1)
        if len(re.sub(r"\x1b\[[0-9;]*m", "", mini_line)) > width:
            mini_line = WHITE + prefix.rstrip() + ": widen pane" + RESET
        disks = [("Boot", data["boot"]), (data.get("shared_label", "Data"), data["shared"])]
        value_width = max(len(f"{int(reading['free'] / 1e9)}GB")
                          if reading and "error" not in reading else 1
                          for _, reading in disks)
        return "\n".join([bar(name, reading, width, value_width) for name, reading in disks] + [mini_line])
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError):
        return WHITE + "Disk space unavailable" + RESET


def remote_boot(host):
    code = "import json,shutil; u=shutil.disk_usage('/'); print(json.dumps(dict(zip(['total','used','free'],u))))"
    try:
        result = subprocess.run(["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                                 host, shlex.join(["python3", "-c", code])],
                                capture_output=True, text=True, timeout=10, check=True)
        return json.loads(result.stdout)
    except (subprocess.SubprocessError, ValueError) as error:
        return {"error": str(error)}


def stop_watch(signum, frame):
    # tmux run-shell otherwise overlays the live panel with an exit-143 error.
    raise SystemExit(0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["watch", "sample", "render", "width"])
    parser.add_argument("--file")
    parser.add_argument("--node-label", action="append", default=[])
    parser.add_argument("--root", default=os.environ.get("PARALLEL_WORKERS_ROOT", ""))
    parser.add_argument("--boot-json")
    parser.add_argument("--minis-json", default="{}")
    parser.add_argument("--mini-host", action="append", default=[], metavar="NAME=SSH_TARGET")
    parser.add_argument("--host")
    parser.add_argument("--remote-script")
    parser.add_argument("--session")
    parser.add_argument("--instance")
    parser.add_argument("--width", type=int, default=32)
    args = parser.parse_args()
    if args.mode == "width":
        print(required_width(args.node_label))
        return
    if not args.file:
        parser.error("--file is required for watch, sample and render")
    if args.mode in ("watch", "sample") and not args.root:
        parser.error("source the inventory environment or pass --root")
    if args.mode == "watch" and args.remote_script is None:
        args.remote_script = str(Path(args.root) / "monitor_storage.py")
    if args.mode == "render":
        print(render(args.file, args.width))
    elif args.mode == "sample":
        data = {"epoch": time.time(), "boot": json.loads(args.boot_json),
                "shared": sample(args.root, require_mount=True), "minis": json.loads(args.minis_json)}
        data["shared_label"] = "Data"
        path = Path(args.file)
        temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
        temporary.write_text(json.dumps(data))
        os.replace(temporary, path)
    else:
        if not all([args.host, args.session, args.instance]):
            parser.error("watch requires --host, --session and --instance")
        signal.signal(signal.SIGTERM, stop_watch)
        minis = {}
        for item in args.mini_host:
            name, separator, host = item.partition("=")
            if not separator or not name or not host:
                parser.error("--mini-host must be NAME=SSH_TARGET")
            minis[name] = host
        next_sample = 0
        while True:
            instance = subprocess.run(
                ["tmux", "show-options", "-qv", "-t", args.session, "@monitor_instance"],
                capture_output=True, text=True, timeout=5)
            if instance.returncode or instance.stdout.strip() != args.instance:
                return
            if time.monotonic() >= next_sample:
                hosts = dict(minis)
                with ThreadPoolExecutor(max_workers=4) as executor:
                    readings = dict(zip(hosts, executor.map(remote_boot, hosts.values())))
                subprocess.run(["tmux", "set-option", "-w", "-t", args.session + ":0",
                                "@storage_width", str(required_width(minis, readings))],
                               check=True, capture_output=True, text=True, timeout=5)
                command = ["python3", args.remote_script, "sample", "--file", args.file,
                           "--root", args.root, "--boot-json", json.dumps(sample("/")),
                           "--minis-json", json.dumps(readings)]
                command = shlex.join(command)
                try:
                    result = subprocess.run(
                        ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                         args.host, command], capture_output=True, text=True, timeout=15)
                    if result.returncode:
                        print("Disk-space refresh failed: " + result.stderr.strip(), file=sys.stderr)
                except subprocess.TimeoutExpired:
                    print("Disk-space refresh timed out", file=sys.stderr)
                next_sample = time.monotonic() + 180
            time.sleep(5)


if __name__ == "__main__":
    main()
