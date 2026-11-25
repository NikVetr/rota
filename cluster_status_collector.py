#!/usr/bin/env python3
"""Sample the cluster once for every dashboard and publish one JSON snapshot.

Runs on the launcher (the Ubuntu desktop) as the parallel-workers-collector user service.
Each remote host keeps one SSH session running monitor_sample.py, the launcher samples
itself as a local process, the coordinator's I/O arrives on one monitor_cluster_io.sh --json
stream, and queues are collected every 30 s. mmm and the desktop top bar render the result
without connecting to any host. Apart from the hosts file (read with a timeout and cached
locally), nothing on the shared volume is read from this host. The largest processes that
samplers report while a host's swap is high are appended to a bounded local event log, and
settled failure incidents (failed jobs and per-host holds, see failure_events.py) to another.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

from failure_events import FailureEvents
import queue_eta_history

SCHEMA = 1
SCRIPT_DIR = Path(__file__).resolve().parent
REMOTE_DIR = "$HOME/parallel-workers-monitor"
REMOTE_PATH = 'export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"'
# A dedicated connection per stream: a shared master would tie every stream to one socket.
SSH_OPTIONS = ("-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=5",
               "-o", "ServerAliveCountMax=2", "-o", "ControlMaster=no", "-o", "ControlPath=none",
               "-o", "StrictHostKeyChecking=accept-new", "-o", "LogLevel=ERROR")
BACKOFF_SEC = (5, 10, 20, 40, 60)
IO_REFRESH_SEC = 3
HOSTS_REFRESH_SEC = 300
HOSTS_READ_TIMEOUT_SEC = 5
QUEUE_TIMEOUT_SEC = 120
SCHEDULER_TIMEOUT_SEC = 30
HEARTBEAT_SEC = 5
QUEUE_GROUPS = {"0": "active", "1": "recent", "2": "earlier"}
QUEUE_FIELDS = 21
MEMORY_EVENTS_MAX_BYTES = 5 * 1024 ** 2  # then one rotated copy is kept
GIB = 1024 ** 3


def log(message):
    print(f"cluster-status: {message}", file=sys.stderr, flush=True)


def default_state_dir():
    return Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state") / "parallel-workers"


def runtime_snapshot(state):
    """Keep the 1.5 s rewrites in RAM; the documented state path becomes a symlink to them."""
    runtime = os.environ.get("XDG_RUNTIME_DIR")
    if not runtime or not Path(runtime).is_dir():
        return state / "cluster-status.json"
    target = Path(runtime) / "parallel-workers" / "cluster-status.json"
    target.parent.mkdir(mode=0o700, exist_ok=True)
    link = state / "cluster-status.json"
    if not (link.is_symlink() and os.readlink(link) == str(target)):
        state.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = state / f".cluster-status.json.link.{os.getpid()}"
        os.symlink(target, temporary)
        os.replace(temporary, link)
    return target


def atomic_write(path, text):
    """Readers see the old or the new file, never a partial one; mkstemp creates it 0600."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as handle:
            handle.write(text)
        os.replace(temporary, path)
    except BaseException:
        os.unlink(temporary)
        raise


def parse_hosts(text):
    hosts = []
    for line in text.splitlines():
        fields = line.split()
        if not fields or fields[0].startswith("#"):
            continue
        target = fields[0]
        if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@:-]*", target):
            raise ValueError(f"invalid SSH target in hosts file: {target!r}")
        if len(fields) >= 2 and not fields[1].isdigit():
            raise ValueError(f"invalid slot count in hosts file for {target}: {fields[1]!r}")
        hosts.append({"id": fields[2] if len(fields) >= 3 else target.rpartition("@")[2], "target": target,
                      "slots": int(fields[1]) if len(fields) >= 2 else None})
    if not hosts:
        raise ValueError("hosts file lists no hosts")
    ids = [host["id"] for host in hosts]
    if len(set(ids)) != len(ids):
        raise ValueError(f"hosts file repeats a host label: {ids}")
    return hosts


def is_local(target):
    """True when this machine owns the address, so it samples itself instead of using SSH."""
    address = target.rpartition("@")[2]
    try:
        family = socket.AF_INET6 if ipaddress.ip_address(address).version == 6 else socket.AF_INET
    except ValueError:
        return address in {"localhost", socket.gethostname(), socket.gethostname().split(".")[0]}
    with socket.socket(family, socket.SOCK_DGRAM) as probe:
        try:
            probe.bind((address, 0))
        except OSError:
            return False
    return True


class HostsSource:
    """Reads the shared hosts file in a helper thread so a hung SMB mount cannot block us."""

    def __init__(self, path, cache):
        self.path, self.cache, self.pending = Path(path), Path(cache), None

    def read(self):
        if self.pending is None or not self.pending.is_alive():
            result = {}

            def target():
                try:
                    result["text"] = self.path.read_text()
                except OSError as error:
                    result["error"] = error

            self.pending = threading.Thread(target=target, daemon=True, name="hosts-file")
            self.pending.start()
            self.pending.join(HOSTS_READ_TIMEOUT_SEC)
            if "text" in result:
                hosts = parse_hosts(result["text"])
                if not self.cache.exists() or self.cache.read_text() != result["text"]:
                    atomic_write(self.cache, result["text"])
                return hosts, "share"
            reason = str(result.get("error") or f"no reply within {HOSTS_READ_TIMEOUT_SEC} s")
        else:
            reason = "an earlier read is still blocked"
        if not self.cache.exists():
            raise RuntimeError(f"cannot read hosts file {self.path} ({reason}) and no cached copy exists at {self.cache}")
        return parse_hosts(self.cache.read_text()), f"cache ({self.path}: {reason})"


def stream_command(target, local, script, arguments, remote_dir=REMOTE_DIR):
    interpreter = "python3" if script.endswith(".py") else "bash"
    if local:
        return [sys.executable if interpreter == "python3" else "bash", str(SCRIPT_DIR / script), *arguments]
    remote = f'{REMOTE_PATH}; exec {interpreter} "{remote_dir}/{script}" {shlex.join(arguments)}'
    return ["ssh", *SSH_OPTIONS, target, "exec /bin/bash -lc " + shlex.quote(remote)]


class Stream(threading.Thread):
    """Keeps one JSON-lines command running and reconnects with exponential backoff."""

    def __init__(self, name, command, on_sample, on_down, on_crash, stall_sec):
        super().__init__(daemon=True, name=name)
        self.command, self.on_sample, self.on_down, self.on_crash = command, on_sample, on_down, on_crash
        self.stall_sec = stall_sec
        self.stopping = threading.Event()
        self.process = None
        self.last_output = time.monotonic()
        self.stalled = False
        self.failures = 0  # consecutive attempts that produced no sample

    def run(self):
        try:
            while not self.stopping.is_set():
                error = self.run_once()
                if self.stopping.is_set():
                    break
                delay = BACKOFF_SEC[min(max(self.failures - 1, 0), len(BACKOFF_SEC) - 1)]
                self.on_down(f"{error}; retrying in {delay} s")
                self.stopping.wait(delay)
        except Exception as error:  # an unexpected bug must stop the service, not one stream
            self.on_crash(self.name, error)

    def run_once(self):
        self.stalled = False
        try:
            self.process = subprocess.Popen(self.command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                            stderr=subprocess.STDOUT, text=True, start_new_session=True)
        except OSError as error:
            self.failures += 1
            return f"cannot start {self.command[0]}: {error}"
        self.last_output = time.monotonic()
        noise, sampled = None, False
        for line in self.process.stdout:
            self.last_output = time.monotonic()
            line = line.strip()
            if not line.startswith("{"):
                noise = line[-300:] or noise
                continue
            try:
                self.on_sample(json.loads(line))
                sampled = True
            except (KeyError, TypeError, ValueError) as error:
                noise = f"unreadable sample ({error}): {line[:200]}"
        status = self.process.wait()
        self.process.stdout.close()
        self.failures = 0 if sampled else self.failures + 1
        if self.stalled:
            return f"no data for {self.stall_sec:.0f} s"
        return noise or f"stream exited with status {status}"

    def check_stall(self):
        process = self.process
        if process is not None and process.poll() is None and time.monotonic() - self.last_output > self.stall_sec:
            self.stalled = True
            self.kill()

    def kill(self):
        process = self.process
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def stop(self):
        self.stopping.set()
        self.kill()


def parse_workers(field, state):
    """Split a worker column like '2-0-1 P50 RAM' the way render_queue_progress.sh does.

    `P50*` marks a queue with per-host priority overrides; 50 is its default for other hosts.
    """
    if field == "adaptive:n/a":
        return {"workers": None, "running": None, "priority": None, "priority_overrides": None, "admission": None}
    match = re.search(r" P(-?[0-9]+)(\*?)(?: |$)", field)
    vector, priority, overrides, admission = field, None, None, None
    if match:
        vector, priority, overrides = field[:match.start()], int(match[1]), match[2] == "*"
        admission = field[match.end():].strip() or None
    stale = state == "UNKNOWN"
    total = re.fullmatch(r"([0-9]+) run", vector)
    if total:
        running = None if stale else int(total[1])
    else:
        counts = [int(part) for part in vector.split("-") if part.isdigit()]
        running = None if stale or not counts else sum(counts)
    return {"workers": vector, "running": running, "priority": priority, "priority_overrides": overrides,
            "admission": admission}


def parse_queue_rows(text):
    """Parse monitor_queue_progress.sh --data_only rows (columns documented in render_queue_progress.sh)."""
    if text.startswith("Queue Progress:"):
        return []
    queues = []
    for line in text.splitlines():
        fields = line.split("\t")
        if len(fields) not in (QUEUE_FIELDS, QUEUE_FIELDS + 1):
            raise ValueError(f"queue row has {len(fields)} columns, expected {QUEUE_FIELDS} or {QUEUE_FIELDS + 1}: {line[:120]!r}")
        group, state = QUEUE_GROUPS[fields[0]], fields[4]
        label = re.fullmatch(r"(.*) \[b([0-9]+)\]", fields[5])
        eta = int(fields[10])
        queues.append(dict(
            {"id": fields[16], "project": fields[17], "label": label[1] if label else fields[5],
             "batch": int(label[2]) if label else None, "group": group, "state": state,
             "done": int(fields[7]), "total": int(fields[8]), "pct": float(fields[11]),
             "eta": eta if eta >= 0 else None, "started": int(fields[3]), "elapsed": int(fields[9]),
             "ended": int(fields[13]) if group != "active" else None, "failed": int(fields[18]),
             "failed_at": fields[19] or None, "rerun": int(fields[20]), "path": fields[15]},
            **parse_workers(fields[6], state)))
        if len(fields) > QUEUE_FIELDS and fields[21] != "-":
            queues[-1]["eta_warning"] = fields[21]
    return queues


class Collector:
    def __init__(self, args):
        self.args = args
        self.lock = threading.Lock()
        self.events_lock = threading.Lock()
        self.changed = threading.Event()
        self.stopping = threading.Event()
        self.failure = None
        self.started = round(time.time(), 3)
        self.hosts, self.streams, self.hosts_source = [], {}, None
        self.file_slots, self.pool_slots = {}, None
        self.coordinator, self.io_stream = None, None
        self.shared = {"host": None, "ok": False, "error": "starting", "updated": None}
        self.queues, self.queue_process = [], None
        self.scheduler = {"ok": None, "error": None, "attempted": None}
        self.queue_refresh = {"ok": None, "error": None, "updated": None, "attempted": None,
                              "path": str(args.queue_snapshot)}
        self.failure_events, self.failure_events_error = None, None

    # -- hosts -------------------------------------------------------------
    def apply_hosts(self, hosts, source):
        with self.lock:
            self.hosts_source = source
            self.file_slots = {host["id"]: host["slots"] for host in hosts}
            wanted = {host["id"]: host for host in hosts}
            for host_id in [key for key, entry in self.streams.items()
                            if wanted.get(key, {}).get("target") != entry["target"]]:
                log(f"{host_id}: removed or retargeted; stopping its stream")
                self.streams.pop(host_id)["stream"].stop()
            entries = {entry["id"]: entry for entry in self.hosts}
            self.hosts = []
            for host in hosts:
                entry = entries.get(host["id"])
                if entry is None or entry["target"] != host["target"]:
                    local = is_local(host["target"])
                    entry = {"id": host["id"], "target": host["target"], "local": local, "kind": None,
                             "ok": False, "error": "connecting", "updated": None, "cpu": None,
                             "load": None, "memory": None, "mount": None, "disk": None}
                self.hosts.append(entry)
                if host["id"] not in self.streams:
                    self.start_host_stream(entry)
            coordinator = self.find_coordinator(hosts)
            if coordinator != self.coordinator:
                self.start_io_stream(coordinator)
            self.changed.set()

    def find_coordinator(self, hosts):
        target = self.args.coordinator
        if not target:
            raise RuntimeError("Source the inventory environment or pass --coordinator.")
        host_id = next((host["id"] for host in hosts if host["target"] == target), target.rpartition("@")[2])
        return host_id, target

    def crash(self, name, error):
        log(f"{name} failed unexpectedly: {error!r}; stopping so the service restarts")
        self.failure = error
        self.stopping.set()

    def start_host_stream(self, entry):
        host_id = entry["id"]
        arguments = ["stream", "--interval", str(self.args.interval), "--root", self.args.root]
        command = stream_command(entry["target"], entry["local"], "monitor_sample.py", arguments, self.args.remote_dir)
        stream = Stream(f"host-{host_id}", command, lambda sample: self.host_sample(host_id, sample),
                        lambda error: self.host_down(host_id, error), self.crash,
                        stall_sec=max(20, 6 * self.args.interval))
        self.streams[host_id] = {"target": entry["target"], "stream": stream}
        stream.start()

    def start_io_stream(self, coordinator):
        if self.io_stream is not None:
            self.io_stream.stop()
        self.coordinator = coordinator
        host_id, target = coordinator
        arguments = ["--json", "--root", self.args.root, "--refresh_sec", str(IO_REFRESH_SEC),
                     "--rates_state_file", "/tmp/parallel-workers-collector-io-rates.json"]
        for option, variable in (("--interface", "PARALLEL_WORKERS_NETWORK_INTERFACE"), ("--disk", "PARALLEL_WORKERS_DISK")):
            if os.environ.get(variable):
                arguments += [option, os.environ[variable]]
        self.shared = {"host": host_id, "ok": False, "error": "connecting", "updated": None}
        command = stream_command(target, is_local(target), "monitor_cluster_io.sh", arguments, self.args.remote_dir)
        self.io_stream = Stream("io", command, self.io_sample, self.io_down, self.crash, stall_sec=6 * IO_REFRESH_SEC + 10)
        self.io_stream.start()

    def entry(self, host_id):
        return next((entry for entry in self.hosts if entry["id"] == host_id), None)

    def host_sample(self, host_id, sample):
        if sample["kind"] not in ("mac", "linux") or not sample["cpu"]["per_core_user_sys"]:
            raise ValueError("not a monitor_sample.py sample")
        now = round(time.time(), 3)
        with self.lock:
            entry = self.entry(host_id)
            if entry is None:
                return
            mount = sample.get("mount") or {}
            previous = entry["mount"] or {}
            state = mount.get("state")
            if not entry["ok"]:
                log(f"{host_id}: sampling")
            entry.update(ok=True, error=None, updated=now, kind=sample.get("kind"), cpu=sample.get("cpu"),
                         load=sample.get("load"), memory=sample.get("memory"), disk=sample.get("disk"),
                         mount={"root": mount.get("root"), "ok": {"mounted": True, "missing": False}.get(state),
                                "state": state, "error": mount.get("error"),
                                "since": previous.get("since") if previous.get("state") == state else now})
        if "memory_top" in sample:
            self.record_memory_top(host_id, sample)
        self.changed.set()

    def record_memory_top(self, host_id, sample):
        """Append a swap-triggered process list; the log outlives restarts, unlike the snapshot."""
        report, memory = sample["memory_top"], sample["memory"]
        if not isinstance(report["processes"], list):
            raise ValueError("memory_top has no process list")
        event = {"time": sample["time"], "host": host_id, "swap_used_bytes": memory["swap_used_bytes"],
                 "used_pct": memory["used_pct"], "processes": report["processes"], "error": report["error"]}
        path = self.args.memory_events
        with self.events_lock:
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if path.exists() and path.stat().st_size > MEMORY_EVENTS_MAX_BYTES:
                os.replace(path, path.with_name(path.name + ".1"))
            with os.fdopen(os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600), "a") as handle:
                handle.write(json.dumps(event, separators=(",", ":")) + "\n")
        largest = ", ".join(f"{row['command']} ({row['pid']}) {row['bytes'] / GIB:.1f}G"
                            for row in event["processes"][:3])
        log(f"{host_id}: swap {event['swap_used_bytes'] / GIB:.1f}G; largest: {largest or event['error']}")

    def host_down(self, host_id, error):
        with self.lock:
            entry = self.entry(host_id)
            if entry is None:
                return
            if entry["ok"]:
                log(f"{host_id}: {error}")
            entry.update(ok=False, error=error)
        self.changed.set()

    def io_sample(self, sample):
        if not all(isinstance(sample[key], dict) for key in ("pool", "network", "smbd", "disk", "registry")):
            raise ValueError("not a monitor_cluster_io.sh --json sample")
        # Sent while an adaptive pool runs; null when its budgets could not be read.
        slots = sample["pool"].get("slots")
        if slots is not None and not (isinstance(slots, dict) and all(
                type(count) is int and count >= 0 for count in slots.values())):
            raise ValueError(f"invalid pool job slots: {slots!r}")
        with self.lock:
            if not self.shared["ok"]:
                log(f"{self.shared['host']} I/O: sampling")
            pool = {key: value for key, value in sample["pool"].items() if key != "slots"}
            pool["in_use"] = self.pool_in_use()
            self.pool_slots = slots
            self.shared = dict(sample, pool=pool, host=self.shared["host"], ok=True, error=None, updated=round(time.time(), 3))
        self.changed.set()

    def io_down(self, error):
        with self.lock:
            if self.shared["ok"]:
                log(f"{self.shared['host']} I/O: {error}")
            self.shared.update(ok=False, error=error)
        self.changed.set()

    def slots(self, host_id):
        """Jobs the host takes: its budget while an adaptive pool runs, else its hosts-file slot count."""
        state = (self.shared.get("pool") or {}).get("state")
        if state in ("active", "draining"):
            return (self.pool_slots or {}).get(host_id)
        return self.file_slots.get(host_id) if state == "inactive" else None

    def pool_in_use(self):
        counts = [queue["running"] for queue in self.queues if queue["group"] == "active" and queue["running"] is not None]
        return sum(counts) if counts else None

    # -- queues ------------------------------------------------------------
    def queue_loop(self):
        try:
            while not self.stopping.is_set():
                started = time.monotonic()
                self.collect_queues()
                self.collect_scheduler()
                self.observe_failures()
                self.stopping.wait(max(0, started + self.args.queue_refresh_sec - time.monotonic()))
        except Exception as error:
            self.crash("queue collection", error)

    def collect_queues(self):
        with self.lock:
            targets = " ".join(entry["target"] for entry in self.hosts)
            _, coordinator = self.coordinator
        command = [str(SCRIPT_DIR / "monitor_queue_collect.sh"), "--root", self.args.root,
                   "--collector_script", str(SCRIPT_DIR / "monitor_queue_progress.sh"),
                   "--worker_script", str(SCRIPT_DIR / "monitor_queue_workers.sh"), "--hosts", targets,
                   "--fallback_host", coordinator, "--mode", "local" if is_local(coordinator) else "remote",
                   "--limit", "0", "--timeout_sec", str(QUEUE_TIMEOUT_SEC), "--eta_warnings"]
        if self.args.no_queue_registry:
            command.append("--no_registry")
        attempted, error, queues = round(time.time(), 3), None, None
        process = self.queue_process = subprocess.Popen(
            command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            start_new_session=True)
        try:
            output, errors = process.communicate(timeout=QUEUE_TIMEOUT_SEC)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            error = f"timed out after {QUEUE_TIMEOUT_SEC} s"
        else:
            if process.returncode or not output.strip():
                detail = errors.strip().splitlines()[-1:] or [f"exited with status {process.returncode}"]
                error = f"queue collection failed: {detail[0][-300:]}"
            else:
                try:
                    queues = parse_queue_rows(output)
                except (KeyError, ValueError) as failure:
                    error = f"unreadable queue rows: {failure}"
                else:
                    # Publish details first. Existing readers watch the canonical
                    # 21-column file; its replacement signals a completed refresh.
                    legacy = "\n".join("\t".join(line.split("\t")[:QUEUE_FIELDS])
                                       for line in output.splitlines()) + "\n"
                    atomic_write(Path(str(self.args.queue_snapshot) + ".details"), output)
                    atomic_write(self.args.queue_snapshot, legacy)
                    try:
                        queue_eta_history.record(self.args.queue_snapshot.parent / "queue-eta.sqlite3", queues)
                    except (OSError, sqlite3.Error) as failure:
                        log(f"could not record queue ETA history: {failure}")
        with self.lock:
            if error:
                if self.queue_refresh["ok"] is not False:
                    log(error)
                self.queue_refresh.update(ok=False, error=error, attempted=attempted)
            else:
                self.queues = queues
                self.queue_refresh.update(ok=True, error=None, attempted=attempted, updated=round(time.time(), 3))
                if self.shared.get("pool"):
                    self.shared["pool"]["in_use"] = self.pool_in_use()
        self.changed.set()

    # -- output ------------------------------------------------------------
    def collect_scheduler(self):
        """The coordinator's controllable state (scheduler_snapshot.py), for pw-control and the top bar."""
        with self.lock:
            _, target = self.coordinator
        command = stream_command(target, is_local(target), "scheduler_snapshot.py",
                                 ["--state-root", f"{self.args.root}/.parallel-workers/adaptive",
                                  "--scheduler", f"{self.args.root}/adaptive_scheduler.sh"],
                                 self.args.remote_dir)
        attempted, error, state = round(time.time(), 3), None, None
        try:
            result = subprocess.run(command, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                                    timeout=SCHEDULER_TIMEOUT_SEC, start_new_session=True)
        except subprocess.TimeoutExpired:
            error = f"timed out after {SCHEDULER_TIMEOUT_SEC} s"
        else:
            if result.returncode:
                detail = result.stderr.strip().splitlines()[-1:] or [f"exited with status {result.returncode}"]
                error = f"scheduler snapshot failed: {detail[0][-300:]}"
            else:
                try:
                    state = json.loads(result.stdout)
                    if state.get("schema") != 1 or not isinstance(state.get("queues"), list):
                        raise ValueError("unexpected scheduler snapshot schema")
                except ValueError as failure:
                    error = f"unreadable scheduler snapshot: {failure}"
        with self.lock:
            if error:
                if self.scheduler.get("ok") is not False:
                    log(error)
                # Keep the last good state for display, marked stale with the reason.
                self.scheduler.update(ok=False, error=error, attempted=attempted)
            else:
                self.scheduler = dict(state, ok=True, error=None, attempted=attempted)
        self.changed.set()

    def observe_failures(self):
        """Turn the latest scheduler snapshot into settled failure events (failure_events.py)."""
        with self.lock:
            hosts = [{"id": entry["id"], "ok": bool(entry["ok"])} for entry in self.hosts]
            scheduler = self.scheduler
            coordinator = self.coordinator
        if coordinator is None:
            return
        try:
            if self.failure_events is None:
                path = self.args.failure_events
                path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                self.failure_events = FailureEvents(path, path.with_name(path.name + ".state.json"),
                                                    root=self.args.root, coordinator=coordinator[1])
            self.failure_events.coordinator = coordinator[1]
            for event in self.failure_events.observe(time.time(), hosts, scheduler):
                log(f"failure event {event['id']}: {event['kind']} in {event['queue_id']} "
                    f"({len(event['jobs'])} jobs, {len(event['holds'])} holds)")
            self.failure_events_error = None
        except (OSError, ValueError, KeyError) as error:
            if self.failure_events_error is None:
                log(f"failure events unavailable: {error!r}")
            self.failure_events_error = str(error)

    def queue_rows(self):
        """Queue rows joined to their scheduler controls by the batch tag (TSV column 17)."""
        controls = {queue["tag"]: queue for queue in self.scheduler.get("queues", []) if "tag" in queue}
        return [dict(queue, control_id=controls.get(queue["id"], {}).get("queue_id"),
                     source_dir=controls.get(queue["id"], {}).get("source_dir"),
                     submitted_by=controls.get(queue["id"], {}).get("submitted_by"))
                for queue in self.queues]

    def snapshot(self):
        with self.lock:  # streams mutate these structures; serialize a consistent copy
            local = next((entry["id"] for entry in self.hosts if entry["local"]), None)
            away = {host["id"]: host["away"] for host in self.scheduler.get("hosts", [])}
            return json.dumps({"schema": SCHEMA, "updated": round(time.time(), 3),
                               "collector": {"pid": os.getpid(), "started": self.started,
                                             "hosts_source": self.hosts_source, "local_host": local},
                               "hosts": [dict(entry, slots=self.slots(entry["id"]), away=away.get(entry["id"]))
                                         for entry in self.hosts],
                               "shared": self.shared, "queues": self.queue_rows(),
                               "queue_refresh": self.queue_refresh,
                               "scheduler": self.scheduler}, separators=(",", ":")) + "\n"

    def write(self):
        atomic_write(self.args.snapshot, self.snapshot())

    def stop(self):
        self.stopping.set()
        with self.lock:
            streams = [entry["stream"] for entry in self.streams.values()] + [self.io_stream]
        for stream in streams:
            if stream is not None:
                stream.stop()
        process = self.queue_process
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def run(self, hosts_source):
        threading.Thread(target=self.queue_loop, daemon=True, name="queues").start()
        last_write = hosts_due = 0
        deadline = time.monotonic() + self.args.run_seconds if self.args.run_seconds else None
        while not self.stopping.is_set():
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                break
            if now >= hosts_due:
                if hosts_due:
                    self.apply_hosts(*hosts_source.read())
                hosts_due = now + HOSTS_REFRESH_SEC
            with self.lock:
                streams = [entry["stream"] for entry in self.streams.values()] + [self.io_stream]
            for stream in streams:
                stream.check_stall()
            if (self.changed.is_set() and now - last_write >= self.args.interval) or now - last_write >= HEARTBEAT_SEC:
                self.changed.clear()
                self.write()
                last_write = now
            self.stopping.wait(0.25)
        # No final write: an aging snapshot is how readers learn that sampling stopped.


def main():
    from cluster_config import load_monitor_environment
    load_monitor_environment()
    state = default_state_dir()
    root = os.environ.get("PARALLEL_WORKERS_ROOT", "")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", default=root, required=not root)
    parser.add_argument("--hosts_file", default=os.environ.get("PARALLEL_WORKERS_HOSTS_FILE"), required=not os.environ.get("PARALLEL_WORKERS_HOSTS_FILE"))
    parser.add_argument("--hosts_cache", type=Path, default=state / "hosts-ethernet.txt")
    parser.add_argument("--snapshot", type=Path, default=None,
                        help="default: $XDG_RUNTIME_DIR, linked from the state directory")
    parser.add_argument("--queue_snapshot", type=Path,
                        default=Path(os.environ.get("PARALLEL_WORKERS_QUEUE_SNAPSHOT") or state / "queue-progress.tsv"))
    parser.add_argument("--memory_events", type=Path, default=state / "memory-events.jsonl",
                        help="JSON lines of the largest processes on hosts with high swap (default: %(default)s)")
    parser.add_argument("--failure_events", type=Path, default=state / "events.jsonl",
                        help="JSON lines of settled failure incidents for Agent Bar (default: %(default)s)")
    parser.add_argument("--coordinator", default=os.environ.get("PARALLEL_WORKERS_COORDINATOR", ""),
                        help="SSH target of the host whose local disk is the shared volume")
    parser.add_argument("--remote_dir", default=REMOTE_DIR,
                        help="monitor helper directory on remote hosts (default: %(default)s)")
    parser.add_argument("--interval", type=float, default=1.5)
    parser.add_argument("--queue_refresh_sec", type=int, default=30)
    parser.add_argument("--no_queue_registry", action="store_true",
                        help="collect queues without rewriting QUEUE_PROGRESS.csv (read-only smoke tests)")
    parser.add_argument("--run_seconds", type=float, default=0, help="stop after this long (testing)")
    args = parser.parse_args()
    if not 0.5 <= args.interval <= 30 or not 10 <= args.queue_refresh_sec <= 600:
        parser.error("--interval must be 0.5–30 s and --queue_refresh_sec 10–600 s")
    if not re.fullmatch(r"(\$HOME/)?[A-Za-z0-9_./-]+", args.remote_dir):
        parser.error("--remote_dir must be a plain path, optionally starting with $HOME/")
    if args.snapshot is None:
        override = os.environ.get("PARALLEL_WORKERS_CLUSTER_SNAPSHOT")
        args.snapshot = Path(override) if override else runtime_snapshot(state)
    collector = Collector(args)
    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(sig, lambda *_: collector.stopping.set())
    source = HostsSource(args.hosts_file, args.hosts_cache)
    try:
        hosts, origin = source.read()
    except (RuntimeError, ValueError) as error:
        parser.exit(1, f"cluster-status: {error}\n")
    log(f"{len(hosts)} hosts from {origin}; snapshot {args.snapshot}")
    try:
        collector.apply_hosts(hosts, origin)
        collector.run(source)
    finally:
        # Streams run in their own sessions, so stop them even when an error escapes.
        collector.stop()
    return 1 if collector.failure else 0


if __name__ == "__main__":
    raise SystemExit(main())
