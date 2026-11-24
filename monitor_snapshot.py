#!/usr/bin/env python3
"""Render mmm panes from the cluster status collector's snapshot instead of sampling hosts.

Subcommands: `memory` draws one host's MEM pane; `mount`, `io` and `queue-status` print what
monitor_mount_status.sh, monitor_cluster_io.sh and monitor_queue_pane.sh need in snapshot mode.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import textwrap
import time

import monitor_linux
import monitor_mac
import monitor_sample
import monitor_storage

SERVICE = "parallel-workers-collector.service"
# The collector writes at least every 5 s; three missed heartbeats mean it stopped.
STALE_SEC = 15
START_HINT = (f"start it with: systemctl --user start {SERVICE} "
              "(install: admin/install_cluster_status_collector.sh), or run ./start_monitoring.sh --direct")
MAC_PRESSURE = {level: word for word, level in monitor_sample.PRESSURE.items()}


def default_path():
    state = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
    return Path(os.environ.get("PARALLEL_WORKERS_CLUSTER_SNAPSHOT") or state / "parallel-workers" / "cluster-status.json")


def age_text(seconds):
    seconds = max(0, int(seconds))
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds >= size:
            return f"{seconds // size}{unit}"
    return f"{seconds}s"


class Reader:
    """Re-parses the snapshot only when the collector replaced it; staleness is rechecked per call."""

    def __init__(self, path):
        self.path, self.key, self.snapshot, self.error = Path(path), None, None, None

    def read(self, now=None):
        """Return (snapshot, problem); problem names why there is no fresh snapshot."""
        now = time.time() if now is None else now
        try:
            info = self.path.stat()
        except FileNotFoundError:
            return None, f"No collector snapshot at {self.path}"
        except OSError as error:
            return None, f"Cannot read {self.path}: {error}"
        key = (info.st_mtime_ns, info.st_size, info.st_ino)
        if key != self.key:
            self.key = key
            try:
                self.snapshot, self.error = json.loads(self.path.read_text()), None
                if self.snapshot.get("schema") != 1:
                    self.snapshot, self.error = None, f"Unsupported snapshot schema in {self.path}"
            except (OSError, ValueError) as error:
                self.snapshot, self.error = None, f"Unreadable collector snapshot: {error}"
        if self.error:
            return None, self.error
        age = now - self.snapshot["updated"]
        if age > STALE_SEC:
            return None, f"Collector snapshot is {age_text(age)} old"
        return self.snapshot, None


def find_host(snapshot, host_id):
    return next((host for host in snapshot["hosts"] if host["id"] == host_id), None)


def host_problem(host, host_id, now):
    if host is None:
        return f"{host_id} is not in the collector's hosts file"
    if host["ok"]:
        return None
    last = f" (last sample {age_text(now - host['updated'])} ago)" if host["updated"] else ""
    return f"{host_id} unavailable: {host['error']}{last}"


def cpu_state(reader, host_id, now=None):
    """Per-core (user, system) fractions for monitor_cpu.py, or a message to show instead."""
    now = time.time() if now is None else now
    snapshot, problem = reader.read(now)
    if problem:
        return None, f"{problem}; {START_HINT}"
    host = find_host(snapshot, host_id)
    problem = host_problem(host, host_id, now)
    if problem:
        return None, problem
    return [tuple(pair) for pair in host["cpu"]["per_core_user_sys"]], None


def wrap(message, width, height):
    # Keep the service name and paths whole whenever they fit the pane.
    return textwrap.wrap(message, max(1, width), break_on_hyphens=False)[:max(1, height)] or [""]


def memory_lines(host, width):
    memory = host["memory"]
    if memory.get("error"):
        return [memory["error"]]
    if host["kind"] == "mac":
        data = dict(total=memory["total_bytes"], used=memory["used_bytes"], cache=memory["cache_bytes"],
                    free=memory["free_bytes"], swap=memory["swap_used_bytes"], percent=memory["used_pct"])
        return monitor_mac.memory_lines(data, MAC_PRESSURE[memory["pressure"]], width)
    data = dict(total=memory["total_bytes"], used=memory["used_bytes"], available=memory["available_bytes"],
                cache=memory["cache_bytes"], swap=memory["swap_used_bytes"])
    psi = memory["psi_some_avg10"]
    return monitor_linux.memory_lines(data, width, "n/a" if psi is None else f"{psi:.2f}%")


def watch_memory(path, host_id):
    reader, shown = Reader(path), None
    print("\033[?25l\033[2J", end="", flush=True)
    try:
        while True:
            now = time.time()
            size = shutil.get_terminal_size((26, 3))
            snapshot, problem = reader.read(now)
            if problem:
                problem = f"{problem}; {START_HINT}"
            else:
                host = find_host(snapshot, host_id)
                problem = host_problem(host, host_id, now)
            lines = wrap(problem, size.columns, size.lines) if problem else memory_lines(host, size.columns)
            frame = "\033[H" + "\033[K\n".join(lines[:size.lines]) + "\033[K\033[J"
            if frame != shown:
                print(frame, end="", flush=True)
                shown = frame
            time.sleep(0.5)
    finally:
        print("\033[0m\033[?25h", end="", flush=True)


def mount_states(reader, host_ids, now=None):
    """One '<glyph> <down-since epoch>' line per host, as monitor_mount_status.sh draws them."""
    now = time.time() if now is None else now
    snapshot, problem = reader.read(now)
    lines = []
    for host_id in host_ids:
        host = None if problem else find_host(snapshot, host_id)
        mount = host and host["ok"] and host["mount"] or {}
        glyph = {True: "✔", False: "✗"}.get(mount.get("ok"), "?")
        lines.append(f"{glyph} {int(mount['since']) if glyph == '✗' else 0}")
    return lines


def disk_reading(disk):
    if not disk or disk.get("error") or disk.get("root_total_bytes") is None:
        return {"error": (disk or {}).get("error") or "no sample"}
    return {"total": disk["root_total_bytes"], "free": disk["root_free_bytes"], "used": disk["root_used_bytes"]}


def storage_data(snapshot, root, now):
    """The storage-line inventory monitor_storage.py used to collect over SSH every 180 s."""
    hosts = snapshot["hosts"]
    readings = {host["id"]: disk_reading(host["disk"] if host["ok"] else None) for host in hosts}
    shared = snapshot["shared"]
    disk = shared.get("disk") if shared["ok"] else None
    share = ({"total": disk["total_bytes"], "free": disk["free_bytes"], "used": disk["used_bytes"]}
             if disk and disk.get("total_bytes") is not None else {"error": (disk or {}).get("error") or "no sample"})
    local = next((host["id"] for host in hosts if host["local"]), None)
    data = {"epoch": now, "boot": readings.get(local, {"error": "no local host"}), "shared": share,
            "shared_label": "Data", "minis": readings}
    return data


def rate_text(value, scale=1.0, digits=1):
    return "n/a" if value is None else f"{value * scale:.{digits}f}"


def io_fields(reader, root, width, now=None):
    """Values for monitor_cluster_io.sh's panel, as the variables its own sampler sets."""
    now = time.time() if now is None else now
    snapshot, problem = reader.read(now)
    if problem:
        return {"problem": wrap(f"{problem}; {START_HINT}", width, 20)}
    shared = snapshot["shared"]
    if not shared["ok"]:
        last = f" (last sample {age_text(now - shared['updated'])} ago)" if shared["updated"] else ""
        return {"problem": wrap(f"{shared['host']} I/O unavailable: {shared['error']}{last}", width, 20)}
    pool, network, smbd, disk, registry = (shared[key] for key in ("pool", "network", "smbd", "disk", "registry"))
    fields = {"pool_status": pool["state"]}
    if pool["state"] in ("active", "draining"):
        fields.update(pool_status="record", adaptive_pool_state=pool["state"].upper(),
                      adaptive_live_cpu=pool["capacity"], adaptive_live_memory=pool["capacity_memory_mib"],
                      adaptive_resource_state=pool["resource_state"], adaptive_io_state=pool["io_state"],
                      adaptive_smbd_tenths=round(pool["smbd_cpu_pct"] * 10), adaptive_quarantined=pool["quarantined"])
    age = registry["age_sec"]
    fields.update(
        rx_mbps=rate_text(network["rx_bytes_per_s"], 8e-6), tx_mbps=rate_text(network["tx_bytes_per_s"], 8e-6),
        smbd_cpu=rate_text(smbd["cpu_pct"]), smb_connections=int(smbd["sessions"]),
        smb_recv_q=int(smbd["recv_queue_bytes"]), smb_send_q=int(smbd["send_queue_bytes"]),
        sampled_disk=disk["name"], disk_mbps=rate_text(disk["bytes_per_s"], 1e-6, 2),
        disk_tps=rate_text(disk["ops_per_s"]), disk_kbt=rate_text(disk["kb_per_op"], digits=2),
        active_queues="n/a" if registry["active_queues"] is None else registry["active_queues"],
        registry_age_sec="n/a" if age is None else int(age + now - shared["updated"]),
        claimed_per_min=rate_text(registry["claimed_per_min"]),
        completed_per_min=rate_text(registry["completed_per_min"]),
        rewrite_retries_per_min=rate_text(registry["rewrite_retries_per_min"]),
        duplicate_retries_per_min=rate_text(registry["duplicate_retries_per_min"]),
        stale_locks_per_min=rate_text(registry["stale_locks_per_min"]),
        locks_held=int(registry["locks_held"]), oldest_lock_sec=int(registry["oldest_lock_sec"]))
    data = storage_data(snapshot, root, now)
    fields["storage_width"] = monitor_storage.required_width(data["minis"], data["minis"])
    fields["storage"] = monitor_storage.render_data(data, width, now).splitlines()
    return fields


def queue_status(reader, now=None):
    """A pane-title notice when queue data is not current; empty when it is."""
    now = time.time() if now is None else now
    snapshot, problem = reader.read(now)
    if problem:
        return f"{problem}; {START_HINT}"
    refresh = snapshot["queue_refresh"]
    if refresh["ok"] is None:
        return "Collector is building the first queue snapshot..."
    if not refresh["ok"]:
        failed = time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(refresh["attempted"]))
        return f"Refresh failed {failed}: {refresh['error']}; retaining snapshot"
    return ""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["memory", "mount", "io", "queue-status"])
    parser.add_argument("--snapshot", type=Path, default=default_path())
    parser.add_argument("--host", help="host label (memory)")
    parser.add_argument("--hosts", default="", help="space-separated host labels in pane order (mount)")
    parser.add_argument("--root", default=os.environ.get("PARALLEL_WORKERS_ROOT", ""))
    parser.add_argument("--width", type=int, default=32)
    args = parser.parse_args()
    reader = Reader(args.snapshot)
    if args.mode == "memory":
        if not args.host:
            parser.error("memory requires --host")
        for sig in (signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, lambda *_: sys.exit(0))
        watch_memory(args.snapshot, args.host)
    elif args.mode == "mount":
        print("\n".join(mount_states(reader, args.hosts.split())))
    elif args.mode == "io":
        for name, value in io_fields(reader, args.root, args.width).items():
            for item in value if isinstance(value, list) else [value]:
                print(f"{name}=" + str(item).replace("\n", " "))
    else:
        print(queue_status(reader).replace("\n", " "))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
