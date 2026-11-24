#!/usr/bin/env python3
"""macOS memory meter from kernel counters, without spawning vm_stat or memory_pressure."""
import argparse
import ctypes
import json
import re
import shutil
import signal
import sys
import time

GIB = 1024 ** 3
HOST_VM_INFO64 = 4
DIM, META, BAR, LABEL, RESET = "\033[38;5;244m", "\033[38;5;179m", "\033[38;5;214m", "\033[97m", "\033[0m"
PRESSURE = {"HIGH": "\033[38;5;203m", "MED": "\033[38;5;228m", "LOW": "\033[38;5;120m"}


class VmStatistics64(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint32) for name in ("free", "active", "inactive", "wire")] + \
        [(name, ctypes.c_uint64) for name in ("zero_fill", "reactivations", "pageins", "pageouts",
                                              "faults", "cow_faults", "lookups", "hits", "purges")] + \
        [("purgeable", ctypes.c_uint32), ("speculative", ctypes.c_uint32)] + \
        [(name, ctypes.c_uint64) for name in ("decompressions", "compressions", "swapins", "swapouts")] + \
        [(name, ctypes.c_uint32) for name in ("compressor", "throttled", "external", "internal")] + \
        [("uncompressed_in_compressor", ctypes.c_uint64)]


class MacCounters:
    def __init__(self):
        if sys.platform != "darwin":
            raise RuntimeError("macOS memory sampling requires macOS")
        self.lib = ctypes.CDLL("/usr/lib/libSystem.B.dylib", use_errno=True)
        self.lib.mach_host_self.restype = ctypes.c_uint
        self.host = self.lib.mach_host_self()
        self.lib.host_statistics64.argtypes = [
            ctypes.c_uint, ctypes.c_int, ctypes.POINTER(VmStatistics64), ctypes.POINTER(ctypes.c_uint)]
        self.lib.host_statistics64.restype = ctypes.c_int
        self.lib.sysctlbyname.argtypes = [
            ctypes.c_char_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_size_t), ctypes.c_void_p, ctypes.c_size_t]
        self.lib.sysctlbyname.restype = ctypes.c_int

    def sysctl(self, name, size=8):
        buffer, length = ctypes.create_string_buffer(size), ctypes.c_size_t(size)
        if self.lib.sysctlbyname(name.encode(), buffer, ctypes.byref(length), None, 0):
            raise OSError(ctypes.get_errno(), f"sysctl {name} failed")
        return buffer.raw[:length.value]

    def number(self, name):
        return int.from_bytes(self.sysctl(name), sys.byteorder)

    def read(self):
        stats = VmStatistics64()
        count = ctypes.c_uint(ctypes.sizeof(stats) // 4)
        status = self.lib.host_statistics64(self.host, HOST_VM_INFO64, ctypes.byref(stats), ctypes.byref(count))
        if status:
            raise RuntimeError(f"host_statistics64 failed: {status}")
        # struct xsw_usage: total, avail, used (uint64), then pagesize and encrypted flags.
        swap = self.sysctl("vm.swapusage", 32)
        return dict(total=self.number("hw.memsize"), page=self.number("hw.pagesize"),
                    free=stats.free, file_backed=stats.external, purgeable=stats.purgeable,
                    swap=int.from_bytes(swap[16:24], sys.byteorder),
                    pressure_free=self.number("kern.memorystatus_level"))


def memory(sample):
    """Inactive includes anonymous app memory; file-backed already includes speculative pages."""
    total, page = sample["total"], sample["page"]
    if total <= 0 or page <= 0:
        raise ValueError("Invalid macOS memory counters")
    free = min(sample["free"] * page, total)
    cache = min((sample["file_backed"] + sample["purgeable"]) * page, total - free)
    used = total - cache - free
    return dict(total=total, used=used, cache=cache, free=free, swap=sample["swap"],
                percent=min(100, max(0, round(used * 100 / total))))


def mounted(root, text):
    """Read `/sbin/mount` output with the rules of monitor_mount_status.sh's remote check."""
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines or not all(re.fullmatch(r".+ on /.* \([^()]+\)", line) for line in lines):
        raise ValueError("Malformed mount table")
    return any(re.sub(r" \([^()]+\)$", "", re.sub(r"^.* on ", "", line)) == root for line in lines)


def pressure_state(free_percent, used_percent):
    if free_percent is not None:
        return "HIGH" if free_percent <= 5 else "MED" if free_percent <= 12 else "LOW"
    return "HIGH" if used_percent >= 90 else "MED" if used_percent >= 75 else "LOW"


def memory_lines(data, pressure, columns):
    total_tag, percent_text = f"{data['total'] / GIB:.0f}G", f"{data['percent']}%"
    width = max(8, max(18, columns) - len(total_tag) - len(percent_text) - 2)
    used_cells = data["percent"] * width // 100
    bar = "#" * used_cells + " " * (width - used_cells)
    swap = f"{data['swap'] / GIB:.2f}G"
    return [f"{META}{total_tag}{RESET}[{BAR}{bar}{DIM}{percent_text}{BAR}{RESET}]",
            f"{LABEL}Used:{RESET}{BAR}{data['used'] / GIB:.1f}G{RESET} {LABEL}Cache:{RESET}{BAR}{data['cache'] / GIB:.1f}G{RESET}",
            f"{LABEL}Free:{RESET}{BAR}{data['free'] / GIB:.1f}{RESET} {LABEL}Swap:{RESET}{BAR}{swap}{RESET} "
            f"{LABEL}P:{RESET}{PRESSURE[pressure]}{pressure}{RESET}"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["memory"])
    parser.add_argument("--interval", type=float, default=1)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--json", action="store_true", help="with --once, print one JSON sample")
    args = parser.parse_args()
    if not 0.1 <= args.interval <= 60:
        parser.error("interval must be 0.1–60 seconds")
    for sig in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, lambda *_: sys.exit(0))
    counters = MacCounters()
    if not args.once:
        print("\033[?25l\033[2J", end="", flush=True)
    try:
        while True:
            try:
                sample = counters.read()
                data = memory(sample)
            except (OSError, RuntimeError, ValueError) as error:
                if args.once:
                    print(f"macOS memory unavailable: {error}", file=sys.stderr)
                    return 2
                print("\033[HMemory unavailable\033[J", end="", flush=True)
                time.sleep(args.interval)
                continue
            pressure = pressure_state(sample["pressure_free"], data["percent"])
            if args.once and args.json:
                print(json.dumps(dict(data, pressure=pressure, pressure_free_percent=sample["pressure_free"])))
                return 0
            lines = memory_lines(data, pressure, shutil.get_terminal_size((24, 3)).columns)
            if args.once:
                print("\n".join(lines))
                return 0
            print("\033[H" + "\033[K\n".join(lines) + "\033[K\033[J", end="", flush=True)
            time.sleep(args.interval)
    finally:
        if not args.once:
            print("\033[0m\033[?25h", end="", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
