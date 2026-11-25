#!/usr/bin/env python3
"""Print one JSON line per tick of CPU, load, memory, share-mount and root-disk state.

Runs unchanged on macOS (system Python 3.9) and Linux. The share's mount state comes from
the mount table only; the share itself is never statted or read. While swap use is high, a
sample every SWAP_REPORT_SEC also lists the processes holding the most memory.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

import monitor_cpu
import monitor_linux
import monitor_mac

MOUNT_REFRESH_SEC = 60
DISK_REFRESH_SEC = 180
PRESSURE = {"LOW": "normal", "MED": "warn", "HIGH": "critical"}
# Percentage of the last 10 s in which some task stalled on memory.
PSI_WARN, PSI_CRITICAL = 10, 40
SWAP_REPORT_SEC = 600
TOP_PROCESSES = 8
UNITS = {"B": 1, "K": 1024, "M": 1024 ** 2, "G": 1024 ** 3, "T": 1024 ** 4}


def cpu_section(values):
    count = len(values)
    return {"cores": count,
            "busy_pct": round(100 * sum(u + s for u, s in values) / count, 1),
            "user_pct": round(100 * sum(u for u, _ in values) / count, 1),
            "sys_pct": round(100 * sum(s for _, s in values) / count, 1),
            "per_core": [round(100 * (u + s), 1) for u, s in values],
            "per_core_user_sys": [[round(u, 4), round(s, 4)] for u, s in values]}


def linux_pressure(psi, used_percent):
    if psi is None:
        return PRESSURE[monitor_mac.pressure_state(None, used_percent)]
    return "critical" if psi >= PSI_CRITICAL else "warn" if psi >= PSI_WARN else "normal"


def mac_memory(sample):
    data = monitor_mac.memory(sample)
    level = monitor_mac.pressure_state(sample["pressure_free"], data["percent"])
    return {"total_bytes": data["total"], "used_bytes": data["used"], "cache_bytes": data["cache"],
            "available_bytes": data["total"] - data["used"], "free_bytes": data["free"],
            "swap_used_bytes": data["swap"], "used_pct": data["percent"],
            "pressure": PRESSURE[level], "pressure_free_pct": sample["pressure_free"]}


def linux_memory(meminfo, psi_text):
    data = monitor_linux.memory(meminfo)
    psi = None if psi_text is None else monitor_linux.psi_some_avg10(psi_text)
    percent = round(100 * data["used"] / data["total"])
    return {"total_bytes": data["total"], "used_bytes": data["used"], "cache_bytes": data["cache"],
            "available_bytes": data["available"], "free_bytes": data["free"],
            "swap_used_bytes": data["swap"], "used_pct": percent,
            "pressure": linux_pressure(psi, percent), "psi_some_avg10": psi}


def mac_top_memory(text):
    """Parse `top -l 1 -o mem -stats pid,mem,command`. MEM is the physical footprint, which
    counts compressed and swapped pages, unlike RSS."""
    lines = text.splitlines()
    header = next((index for index, line in enumerate(lines) if line.split()[:2] == ["PID", "MEM"]), None)
    if header is None:
        raise ValueError("top printed no process table")
    processes = []
    for line in lines[header + 1:]:
        fields = line.split(None, 2)
        if not fields:
            continue
        size = re.fullmatch(r"([0-9.]+)([BKMGT])[+-]?", fields[1]) if len(fields) == 3 else None
        if not fields[0].isdigit() or not size:
            raise ValueError(f"unreadable top row: {line[:120]!r}")
        processes.append({"pid": int(fields[0]), "command": fields[2].strip(),
                          "bytes": round(float(size[1]) * UNITS[size[2]])})
    return processes


def linux_top_memory(proc=Path("/proc")):
    """Resident plus swapped memory per process; kernel threads report neither."""
    processes = []
    for status in proc.glob("[0-9]*/status"):
        try:
            fields = dict(line.split(":", 1) for line in status.read_text().splitlines() if ":" in line)
        except OSError:  # the process exited after the listing
            continue
        kib = [int(fields[key].split()[0]) for key in ("VmRSS", "VmSwap") if key in fields]
        if kib:
            processes.append({"pid": int(status.parent.name), "command": fields["Name"].strip(),
                              "bytes": 1024 * sum(kib)})
    return sorted(processes, key=lambda row: -row["bytes"])[:TOP_PROCESSES]


class Sampler:
    def __init__(self, root):
        if sys.platform == "darwin":
            self.kind, self.counters = "mac", monitor_mac.MacCounters()
        elif sys.platform.startswith("linux"):
            self.kind, self.counters = "linux", None
        else:
            raise RuntimeError(f"Unsupported platform: {sys.platform}")
        self.root, self.ticks = root, monitor_cpu.CpuTicks()

    def memory(self):
        # One failed read must not stop CPU sampling; the error is reported in its place.
        try:
            if self.kind == "mac":
                return mac_memory(self.counters.read())
            psi = Path("/proc/pressure/memory")
            return linux_memory(Path("/proc/meminfo").read_text(), psi.read_text() if psi.exists() else None)
        except (OSError, RuntimeError, ValueError, KeyError) as error:
            return {"error": f"memory unavailable: {error}"}

    def mount(self):
        try:
            if self.kind == "mac":
                result = subprocess.run(["/sbin/mount"], capture_output=True, text=True, timeout=10)
                if result.returncode:
                    raise OSError(f"/sbin/mount exited {result.returncode}")
                present = monitor_mac.mounted(self.root, result.stdout)
            else:
                present = monitor_linux.mounted(self.root, Path("/proc/self/mountinfo").read_text())
            state, error = ("mounted" if present else "missing"), None
        except (OSError, ValueError, subprocess.SubprocessError) as failure:
            state, error = "unknown", str(failure)
        return {"root": self.root, "state": state, "error": error, "checked": round(time.time(), 3)}

    def top_memory(self):
        try:
            if self.kind == "mac":
                result = subprocess.run(["/usr/bin/top", "-l", "1", "-o", "mem", "-n", str(TOP_PROCESSES),
                                         "-stats", "pid,mem,command"], capture_output=True, text=True, timeout=20)
                if result.returncode:
                    raise OSError(f"top exited {result.returncode}")
                processes = mac_top_memory(result.stdout)
            else:
                processes = linux_top_memory()
            return {"processes": processes, "error": None}
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            return {"processes": [], "error": str(error)}

    @staticmethod
    def disk():
        try:
            usage = shutil.disk_usage("/")
            reading = {"root_total_bytes": usage.total, "root_free_bytes": usage.free,
                       "root_used_bytes": usage.used, "error": None}
        except OSError as error:
            reading = {"error": str(error)}
        return dict(reading, checked=round(time.time(), 3))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", nargs="?", choices=["stream"])
    parser.add_argument("--interval", type=float, default=1.5)
    parser.add_argument("--root", default=os.environ.get("ROTA_ROOT", ""))
    parser.add_argument("--once", action="store_true", help="print one sample and exit (requires --json)")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--swap_report_gib", type=float, default=4.0,
                        help="list the largest processes while swap use is at least this (default: %(default)s)")
    args = parser.parse_args()
    if (args.mode == "stream") == args.once or (args.once and not args.json):
        parser.error("use either 'stream' or '--once --json'")
    if not 0.2 <= args.interval <= 60:
        parser.error("interval must be 0.2–60 seconds")
    if not args.swap_report_gib > 0:
        parser.error("--swap_report_gib must be positive")
    # A closed collector pipe ends the stream quietly instead of with a traceback.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, lambda *_: sys.exit(0))
    if not args.root:
        parser.error("source the inventory environment or pass --root")
    sampler = Sampler(args.root)
    previous = sampler.ticks.read()
    deadline = time.monotonic() + args.interval
    mount_due = disk_due = top_due = 0
    mount = disk = None
    while True:
        time.sleep(max(0, deadline - time.monotonic()))
        # Sample on a fixed cadence; after a stall (e.g. sleep), restart it rather than burst.
        deadline += args.interval
        if deadline < time.monotonic():
            deadline = time.monotonic() + args.interval
        current = sampler.ticks.read()
        values = monitor_cpu.usage(previous, current)
        previous = current
        now = time.monotonic()
        if now >= mount_due:
            mount, mount_due = sampler.mount(), now + MOUNT_REFRESH_SEC
        if now >= disk_due:
            disk, disk_due = sampler.disk(), now + DISK_REFRESH_SEC
        memory = sampler.memory()
        sample = {"kind": sampler.kind, "time": round(time.time(), 3), "cpu": cpu_section(values),
                  "load": [round(value, 2) for value in os.getloadavg()],
                  "memory": memory, "mount": mount, "disk": disk}
        if memory.get("swap_used_bytes", 0) < args.swap_report_gib * UNITS["G"]:
            top_due = 0  # report as soon as swap next rises past the threshold
        elif now >= top_due:
            sample["memory_top"], top_due = sampler.top_memory(), now + SWAP_REPORT_SEC
        print(json.dumps(sample, separators=(",", ":")), flush=True)
        if args.once:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
