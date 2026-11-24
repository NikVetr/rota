#!/usr/bin/env python3
"""Adaptive shared-queue allocator and coordinator."""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import io
import json
import math
import os
import re
import signal
import socket
import subprocess
import sys
from pathlib import Path
import threading
import time
from typing import Any, Optional

from adaptive_calibration import calibrate
from adaptive_workflow import policy, dependency_hold, retry_verified_tail, unconfirmed_interruptions


class SnapshotError(ValueError):
    """Scheduler input or state is invalid."""


class RemoteExit(SnapshotError):
    """A reachable host's command exited nonzero."""

    def __init__(self, message: str, returncode: int) -> None:
        super().__init__(message)
        self.returncode = returncode


def _integer(record: dict[str, Any], key: str, minimum: Optional[int] = None) -> int:
    value = record.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SnapshotError(f"{key} must be an integer")
    if minimum is not None and value < minimum:
        raise SnapshotError(f"{key} must be >= {minimum}")
    return value


def _boolean(record: dict[str, Any], key: str, default: bool) -> bool:
    value = record.get(key, default)
    if not isinstance(value, bool):
        raise SnapshotError(f"{key} must be boolean")
    return value


def _identifier(record: dict[str, Any], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str) or not value or any(c in value for c in "\t\r\n"):
        raise SnapshotError(f"{key} must be a nonempty identifier without tabs or newlines")
    return value


def _records(snapshot: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = snapshot.get(key, [])
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise SnapshotError(f"{key} must be a list of objects")
    return value


IDENTIFIER = re.compile(r"^[A-Za-z0-9._-]+$")
PRIORITY = re.compile(r"-?[0-9]+")
DEFAULT_SCHEDULE_PRIORITY = 50
# A host whose SSH works but whose agent is gone is restarted once it has been
# stopped for a lease, and at most once per interval while restarts fail.
AGENT_STOPPED = 3
AGENT_RESTART_INTERVAL_SECONDS = 600


def parse_priority_spec(spec: str) -> tuple[Optional[int], dict[str, Any]]:
    """Parse INTEGER or comma items: one optional bare default, HOST=INTEGER, HOST=off."""
    if PRIORITY.fullmatch(spec):
        return int(spec), {}
    if not spec or any(character.isspace() for character in spec):
        raise SnapshotError(f"schedule priority SPEC must be nonempty without whitespace: {spec!r}")
    default: Optional[int] = None
    overrides: dict[str, Any] = {}
    for item in spec.split(","):
        host_id, separator, value = item.partition("=")
        if not item:
            raise SnapshotError(f"schedule priority SPEC has an empty item: {spec!r}")
        if not separator:
            if not PRIORITY.fullmatch(item):
                raise SnapshotError(f"malformed schedule priority integer: {item!r}")
            if default is not None:
                raise SnapshotError(f"schedule priority SPEC has more than one default: {spec!r}")
            default = int(item)
        elif not IDENTIFIER.fullmatch(host_id) or host_id in {".", ".."}:
            raise SnapshotError(f"invalid host ID in schedule priority SPEC: {host_id!r}")
        elif host_id in overrides:
            raise SnapshotError(f"duplicate host in schedule priority SPEC: {host_id}")
        elif value != "off" and not PRIORITY.fullmatch(value):
            raise SnapshotError(f"host priority must be an integer or off: {item!r}")
        else:
            overrides[host_id] = value if value == "off" else int(value)
    return default, overrides


def format_host_priorities(overrides: dict[str, Any]) -> str:
    return ",".join(f"{host_id}={overrides[host_id]}" for host_id in sorted(overrides))


def resolve_priority_spec(spec: str, host_ids: set[str]) -> tuple[int, dict[str, Any]]:
    default, overrides = parse_priority_spec(spec)
    unknown = sorted(set(overrides) - host_ids)
    if unknown:
        raise SnapshotError(
            f"unknown host ID(s) in schedule priority SPEC: {', '.join(unknown)}; "
            f"valid host IDs: {', '.join(sorted(host_ids))}"
        )
    return DEFAULT_SCHEDULE_PRIORITY if default is None else default, overrides


def _host_priority(priority: int, overrides: dict[str, Any], host_id: str) -> Optional[int]:
    """Effective priority of a queue on one host; None keeps the queue off it."""
    value = overrides.get(host_id, priority)
    return None if value == "off" else value


def _normalize(snapshot: dict[str, Any]) -> tuple[
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[tuple[str, str], int],
    dict[tuple[str, str], int],
    dict[tuple[str, str], int],
    dict[tuple[str, str], int],
    dict[str, str],
    int,
]:
    if not isinstance(snapshot, dict):
        raise SnapshotError("snapshot must be an object")
    if snapshot.get("schema_version") != 1:
        raise SnapshotError("schema_version must be 1")
    shared_io_capacity = _integer(snapshot, "shared_io_capacity", 0)

    hosts: dict[str, dict[str, Any]] = {}
    for raw in _records(snapshot, "hosts"):
        host_id = _identifier(raw, "host_id")
        if host_id in hosts:
            raise SnapshotError(f"duplicate host_id: {host_id}")
        cpu = _integer(raw, "cpu_units", 1)
        memory = _integer(raw, "memory_mib", 1)
        blocked = raw.get("blocked_queues", [])
        if not isinstance(blocked, list) or any(
            not isinstance(queue, str) or not queue for queue in blocked
        ):
            raise SnapshotError("blocked_queues must be a list of queue IDs")
        hosts[host_id] = {
            "cpu": cpu,
            "memory": memory,
            "live_cpu": min(cpu, _integer(raw, "live_cpu_units", 0))
            if "live_cpu_units" in raw else cpu,
            "live_memory": min(memory, _integer(raw, "live_memory_mib", 0))
            if "live_memory_mib" in raw else memory,
            "enabled": _boolean(raw, "enabled", True),
            "reachable": _boolean(raw, "reachable", True),
            "drain_only": _boolean(raw, "drain_only", False),
            "blocked_queues": set(blocked),
        }
    if not hosts:
        raise SnapshotError("hosts must not be empty")

    queues: dict[str, dict[str, Any]] = {}
    for raw in _records(snapshot, "queues"):
        queue_id = _identifier(raw, "queue_id")
        if queue_id in queues:
            raise SnapshotError(f"duplicate queue_id: {queue_id}")
        admission = raw.get("admission")
        if admission not in {"enabled", "paused"}:
            raise SnapshotError("admission must be enabled or paused")
        overrides = raw.get("host_priorities", {})
        if not isinstance(overrides, dict) or any(
            host_id not in hosts or (value != "off" and (
                isinstance(value, bool) or not isinstance(value, int)))
            for host_id, value in overrides.items()
        ):
            raise SnapshotError(f"host_priorities must map pool hosts to integers or off: {queue_id}")
        queues[queue_id] = {
            "cpu": _integer(raw, "cpu_units_per_job", 1),
            "memory": _integer(raw, "memory_mib_per_job", 1),
            "io": _integer(raw, "shared_io_units_per_job", 0),
            "priority": _integer(raw, "schedule_priority"),
            "host_priorities": overrides,
            "max": _integer(raw, "max_workers", 0),
            "pending": _integer(raw, "pending_lines", 0),
            "admission": admission,
        }
    observed: dict[tuple[str, str], int] = {}
    observed_memory: dict[tuple[str, str], int] = {}
    for raw in _records(snapshot, "reservations"):
        host_id = _identifier(raw, "host_id")
        queue_id = _identifier(raw, "queue_id")
        key = (host_id, queue_id)
        if host_id not in hosts or queue_id not in queues or key in observed:
            raise SnapshotError(f"invalid or duplicate reservation: {host_id}/{queue_id}")
        count = _integer(raw, "count", 0)
        minimum_memory = count * queues[queue_id]["memory"]
        memory = (
            _integer(raw, "memory_mib", 0)
            if "memory_mib" in raw else minimum_memory
        )
        if memory < minimum_memory:
            raise SnapshotError(
                f"reservation memory is below declared cost: {host_id}/{queue_id}"
            )
        observed[key] = count
        observed_memory[key] = memory

    envelopes = {(host, queue): observed.get((host, queue), 0)
                 for host in hosts for queue in queues}
    envelope_memory = {(host, queue): queues[queue]["memory"]
                       for host in hosts for queue in queues}
    seen_prior: set[tuple[str, str]] = set()
    for raw in _records(snapshot, "prior_targets"):
        host_id = _identifier(raw, "host_id")
        queue_id = _identifier(raw, "queue_id")
        key = (host_id, queue_id)
        if host_id not in hosts or queue_id not in queues or key in seen_prior:
            raise SnapshotError(f"invalid or duplicate prior target: {host_id}/{queue_id}")
        seen_prior.add(key)
        target = _integer(raw, "target_running", 0)
        acknowledged = _boolean(raw, "acknowledged", False)
        lease_valid = _boolean(raw, "lease_valid", False)
        if not acknowledged or lease_valid:
            envelopes[key] = max(envelopes[key], target)
            envelope_memory[key] = max(
                envelope_memory[key],
                _integer(raw, "memory_mib_per_job", 1)
                if "memory_mib_per_job" in raw else queues[queue_id]["memory"],
            )

    cursor_raw = snapshot.get("cursor", {})
    if not isinstance(cursor_raw, dict):
        raise SnapshotError("cursor must be an object")
    cursor: dict[str, str] = {}
    for priority, queue_id in cursor_raw.items():
        if (
            not isinstance(priority, str)
            or not priority.lstrip("-").isdigit()
            or not isinstance(queue_id, str)
        ):
            raise SnapshotError("cursor keys must be priorities and values must be queue IDs")
        cursor[str(int(priority))] = queue_id

    require_fit = not _boolean(snapshot, "allow_unschedulable_queues", False)
    for queue_id, queue in queues.items():
        if require_fit and not any(
            host["enabled"]
            and queue["cpu"] <= host["cpu"]
            and queue["memory"] <= host["memory"]
            and queue["io"] <= shared_io_capacity
            for host in hosts.values()
        ):
            raise SnapshotError(f"queue {queue_id} cannot fit on any enabled host")

    return (
        hosts, queues, envelopes, observed, observed_memory, envelope_memory, cursor,
        shared_io_capacity,
    )


def allocate(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Return a deterministic plan without reading external state."""
    (
        hosts, queues, envelopes, observed_counts, observed_memory, envelope_memory, cursor,
        io_capacity,
    ) = _normalize(snapshot)
    host_ids = sorted(hosts)
    queue_ids = sorted(queues)
    targets = {host: {queue: 0 for queue in queue_ids} for host in host_ids}

    current_cpu = {host: 0 for host in host_ids}
    current_memory = {host: 0 for host in host_ids}
    current_io = 0
    queue_envelope = {queue: 0 for queue in queue_ids}
    for (host, queue), count in envelopes.items():
        profile = queues[queue]
        observed_count = observed_counts.get((host, queue), 0)
        current_cpu[host] += count * profile["cpu"]
        current_memory[host] += (
            observed_memory.get((host, queue), observed_count * profile["memory"])
            + (count - observed_count) * envelope_memory[(host, queue)]
        )
        current_io += count * profile["io"]
        queue_envelope[queue] += count

    future_cpu = {host: 0 for host in host_ids}
    future_memory = {host: 0 for host in host_ids}
    future_io = 0
    drain_reservations: list[dict[str, Any]] = []
    # Every (queue, host) pair has its own level; a queue competes on a host
    # only at that host's level.
    level = {
        (queue_id, host_id): _host_priority(
            queues[queue_id]["priority"], queues[queue_id]["host_priorities"], host_id
        )
        for queue_id in queue_ids for host_id in host_ids
    }
    priorities = sorted({value for value in level.values() if value is not None}, reverse=True)

    def open_host(host_id: str) -> bool:
        host = hosts[host_id]
        return (
            host["enabled"]
            and host["reachable"]
            and not host["drain_only"]
        )

    def open_queue_host(host_id: str, queue_id: str) -> bool:
        return (
            open_host(host_id) and queue_id not in hosts[host_id]["blocked_queues"]
            and level[(queue_id, host_id)] is not None
        )

    def demand_limit(queue_id: str) -> int:
        queue = queues[queue_id]
        if queue["admission"] != "enabled" or queue["pending"] == 0:
            return 0
        replaceable = sum(
            envelopes[(host_id, queue_id)]
            for host_id in host_ids
            if open_queue_host(host_id, queue_id)
        )
        if queue["max"]:
            unavailable = queue_envelope[queue_id] - replaceable
            open_cap = max(0, queue["max"] - unavailable)
            return min(replaceable + queue["pending"], open_cap)
        return replaceable + queue["pending"]

    def total_target(queue_id: str) -> int:
        return sum(targets[host][queue_id] for host in host_ids)

    def candidates(queue_id: str, priority: Optional[int] = None) -> list[tuple[float, str, bool]]:
        queue = queues[queue_id]
        result: list[tuple[float, str, bool]] = []
        for host_id in host_ids:
            if not open_queue_host(host_id, queue_id) or (
                priority is not None and level[(queue_id, host_id)] != priority
            ):
                continue
            next_target = targets[host_id][queue_id] + 1
            additional = next_target > envelopes[(host_id, queue_id)]
            cpu = current_cpu[host_id] + (queue["cpu"] if additional else 0)
            memory = current_memory[host_id] + (queue["memory"] if additional else 0)
            io = current_io + (queue["io"] if additional else 0)
            if (
                cpu > hosts[host_id]["live_cpu"]
                or memory > hosts[host_id]["live_memory"]
                or io > io_capacity
            ):
                continue
            if future_cpu[host_id] + queue["cpu"] > hosts[host_id]["live_cpu"]:
                continue
            if future_memory[host_id] + queue["memory"] > hosts[host_id]["live_memory"]:
                continue
            if future_io + queue["io"] > io_capacity:
                continue
            dominant = max(
                cpu / hosts[host_id]["live_cpu"],
                memory / hosts[host_id]["live_memory"],
            )
            result.append((dominant, host_id, additional))
        return sorted(result)

    for priority in priorities:
        peers = sorted(
            queue_id for queue_id in queue_ids
            if any(level[(queue_id, host_id)] == priority for host_id in host_ids)
        )
        last = cursor.get(str(priority))
        if last in peers:
            split = peers.index(last) + 1
            peers = peers[split:] + peers[:split]

        while True:
            progress = False
            for queue_id in peers:
                if total_target(queue_id) >= demand_limit(queue_id):
                    continue
                choices = candidates(queue_id, priority)
                if not choices:
                    continue
                _, host_id, additional = choices[0]
                targets[host_id][queue_id] += 1
                profile = queues[queue_id]
                future_cpu[host_id] += profile["cpu"]
                future_memory[host_id] += profile["memory"]
                future_io += profile["io"]
                if additional:
                    current_cpu[host_id] += profile["cpu"]
                    current_memory[host_id] += profile["memory"]
                    current_io += profile["io"]
                    cursor[str(priority)] = queue_id
                progress = True
            if not progress:
                break

        # Reserve one drain destination when lower-priority reservations are
        # the only reason the next job cannot start. The reservation consumes
        # future-plan capacity, so lower priorities may refill other hosts.
        for queue_id in peers:
            if total_target(queue_id) >= demand_limit(queue_id) or candidates(queue_id, priority):
                continue
            profile = queues[queue_id]
            choices: list[tuple[float, str]] = []
            for host_id in host_ids:
                host = hosts[host_id]
                if not open_queue_host(host_id, queue_id) or level[(queue_id, host_id)] != priority:
                    continue
                if future_cpu[host_id] + profile["cpu"] > host["live_cpu"]:
                    continue
                if future_memory[host_id] + profile["memory"] > host["live_memory"]:
                    continue
                if future_io + profile["io"] > io_capacity:
                    continue
                cpu_deficit = max(
                    0, current_cpu[host_id] + profile["cpu"] - host["live_cpu"]
                )
                memory_deficit = max(
                    0, current_memory[host_id] + profile["memory"] - host["live_memory"]
                )
                io_deficit = max(0, current_io + profile["io"] - io_capacity)
                deficit = max(
                    cpu_deficit / host["live_cpu"],
                    memory_deficit / host["live_memory"],
                    io_deficit / max(1, io_capacity),
                )
                choices.append((deficit, host_id))
            if choices:
                _, host_id = min(choices)
                future_cpu[host_id] += profile["cpu"]
                future_memory[host_id] += profile["memory"]
                future_io += profile["io"]
                drain_reservations.append({"host_id": host_id, "queue_id": queue_id})
                break

    limits: dict[str, list[str]] = {}
    for queue_id, profile in queues.items():
        available = [host_id for host_id in host_ids if open_queue_host(host_id, queue_id)]
        reasons = []
        if not available:
            reasons.append("failure_hold" if any(
                open_host(host_id) and queue_id in hosts[host_id]["blocked_queues"]
                for host_id in host_ids
            ) else "host_unavailable")
        if profile["max"] and max(queue_envelope[queue_id], total_target(queue_id)) >= profile["max"]:
            reasons.append("worker_cap")
        if current_io + profile["io"] > io_capacity:
            reasons.append("shared_io_capacity")
        if available and not any(current_cpu[h] + profile["cpu"] <= hosts[h]["live_cpu"] for h in available):
            reasons.append("cpu")
        if available and not any(current_memory[h] + profile["memory"] <= hosts[h]["live_memory"] for h in available):
            reasons.append("memory")
        if not reasons and not candidates(queue_id):
            reasons.append("placement")
        limits[queue_id] = reasons

    return {
        "schema_version": 1,
        "targets": targets,
        "reservation_envelopes": {
            host: {queue: envelopes[(host, queue)] for queue in queue_ids}
            for host in host_ids
        },
        "drain_reservations": drain_reservations,
        "limits": limits,
        "cursor": {key: cursor[key] for key in sorted(cursor, key=int)},
    }


def _start_grants(
    plan: dict[str, Any], queues: dict[str, dict[str, Any]], open_hosts: set[str]
) -> dict[str, dict[str, int]]:
    targets = plan["targets"]
    envelopes = plan["reservation_envelopes"]
    grants = {
        host_id: {queue_id: 0 for queue_id in queues}
        for host_id in targets
    }
    for queue_id in sorted(queues):
        queue = queues[queue_id]
        remaining = min(
            queue["pending_lines"],
            # Allocation already charges all running and unacknowledged work.
            # A draining host must not cancel safe additions on another host.
            sum(
                max(0, targets[host_id][queue_id] - envelopes[host_id][queue_id])
                for host_id in open_hosts
            ),
        )
        if queue["max_workers"]:
            remaining = min(
                remaining,
                max(
                    0,
                    queue["max_workers"]
                    - sum(envelopes[host_id][queue_id] for host_id in envelopes),
                ),
            )
        for host_id in sorted(open_hosts):
            grant = min(
                remaining,
                max(
                    0,
                    targets[host_id][queue_id] - envelopes[host_id][queue_id],
                ),
            )
            grants[host_id][queue_id] = grant
            remaining -= grant
    return grants


def _pace_start_grants(
    plan: dict[str, Any], grants: dict[str, dict[str, int]],
    queues: dict[str, dict[str, Any]], states: dict[str, dict[str, Any]],
    pool_id: str, now: float, interval: float,
) -> None:
    """Pace added slots while allowing established slots to refill immediately."""
    if interval == 0:
        return
    for host_id, host_grants in grants.items():
        state = states[host_id]
        pacing = state.get("start_pacing", {"slots": 0, "next_epoch": 0, "cursor": ""})
        envelopes = plan["reservation_envelopes"][host_id]
        targets = plan["targets"][host_id]
        occupied = sum(envelopes.values())
        # Lower targets do not evict running jobs. A different queue's safe
        # start expands occupancy while those incumbent reservations finish.
        desired = sum(max(targets[q], envelopes[q]) for q in targets)
        slots = min(max(pacing["slots"], occupied), max(occupied, desired))
        next_epoch = pacing["next_epoch"]
        cursor = pacing["cursor"]
        seed = f"{pool_id}/{host_id}/{state['generation']}".encode()
        jitter = int(hashlib.sha256(seed).hexdigest()[:8], 16) / 0xffffffff
        if not occupied and not desired:
            slots, next_epoch = 0, 0
        if desired > slots and next_epoch == 0:
            # Independent host offsets spread even the first starts.
            next_epoch = now + interval * jitter / 2
        allowance = max(0, slots - occupied) + int(desired > slots and now >= next_epoch)
        # The allocator never targets a queue on a host where it is off.
        levels = {
            q: _host_priority(
                queues[q]["schedule_priority"], queues[q].get("host_priorities", {}), host_id
            )
            for q in host_grants
        }
        priorities = sorted({p for p in levels.values() if p is not None}, reverse=True)
        for priority in priorities:
            peers = sorted(q for q in host_grants if levels[q] == priority)
            if cursor in peers:
                split = peers.index(cursor) + 1
                peers = peers[split:] + peers[:split]
            # Round robin within each priority, including when several old slots refill.
            remaining = {q: host_grants[q] for q in peers}
            for q in peers:
                host_grants[q] = 0
            while allowance and any(remaining.values()):
                for q in peers:
                    if not allowance:
                        break
                    if remaining[q]:
                        host_grants[q] += 1
                        remaining[q] -= 1
                        allowance -= 1
                        cursor = q
        issued = occupied + sum(host_grants.values())
        if issued > slots:
            slots = issued
            next_epoch = now + interval * (0.75 + jitter / 2)
        for q in targets:
            targets[q] = min(targets[q], envelopes[q] + host_grants.get(q, 0))
        state["start_pacing"] = {"slots": slots, "next_epoch": next_epoch, "cursor": cursor}


ALLOCATION_HEADER = (
    "queue_id\ttarget_running\tstart_grants\tcpu_units_per_job\tmemory_mib_per_job\t"
    "shared_io_units_per_job\toriginal_queue_file\teditable_queue_file\t"
    "model_dir\tlog_dir\tlock_dir\ttag_clean\tqueue_sha256"
)
STATUS_HEADER = (
    "record_type\tpool_id\thost_id\tgeneration\tadmission\treservation_id\t"
    "queue_id\tstate\tpid\tcpu_units\tmemory_mib\tobserved_memory_mib\t"
    "charged_memory_mib\tmessage\t"
    "live_cpu_units\tlive_memory_mib\tsmbd_cpu_tenths\ttelemetry_state\tresource_sample_epoch\t"
    "smb_bytes\tcurrent_memory_mib\tsustained_memory_mib"
)
STATUS_COLUMNS = STATUS_HEADER.count("\t") + 1
SMB_STUCK_BYTES_PER_SECOND = 65536
SMB_STUCK_SECONDS = 60


def _parse_env(text: str, path: Path, required: set[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if not separator or not key or key in values:
            raise SnapshotError(f"malformed or duplicate entry in {path}")
        values[key] = value
    missing = sorted(required - values.keys())
    if missing:
        raise SnapshotError(f"{path} is missing: {', '.join(missing)}")
    return values


def _read_env(path: Path, required: set[str]) -> dict[str, str]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise SnapshotError(f"cannot read {path}: {error}") from error
    return _parse_env(text, path, required)


def _env_int(values: dict[str, str], key: str, minimum: Optional[int] = None) -> int:
    try:
        value = int(values[key])
    except (KeyError, ValueError) as error:
        raise SnapshotError(f"{key} must be an integer") from error
    if minimum is not None and value < minimum:
        raise SnapshotError(f"{key} must be >= {minimum}")
    return value


def _safe_value(value: str, label: str) -> str:
    if not value or any(character in value for character in "\t\r\n"):
        raise SnapshotError(f"{label} must be nonempty and contain no tabs or newlines")
    return value


def _atomic_text(path: Path, text: str, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    try:
        temporary.write_text(text, encoding="utf-8")
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    _atomic_text(path, json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SnapshotError(f"cannot read coordinator state {path}: {error}") from error
    if not isinstance(value, dict):
        raise SnapshotError(f"coordinator state is not an object: {path}")
    return value


def _load_hosts(path: Path) -> dict[str, dict[str, Any]]:
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle, dialect="excel-tab"))
    except (OSError, csv.Error) as error:
        raise SnapshotError(f"cannot read host resources {path}: {error}") from error
    expected = ["host_id", "ssh_target", "cpu_units", "memory_mib", "enabled"]
    if not rows or list(rows[0]) != expected:
        raise SnapshotError(f"{path} must have the exact adaptive resource header")
    hosts: dict[str, dict[str, Any]] = {}
    targets: set[str] = set()
    for row in rows:
        host_id = _safe_value(row["host_id"], "host_id")
        if not IDENTIFIER.fullmatch(host_id) or host_id in {".", ".."} or host_id in hosts:
            raise SnapshotError(f"invalid or duplicate host_id: {host_id}")
        ssh_target = _safe_value(row["ssh_target"], "ssh_target")
        if ssh_target in targets:
            raise SnapshotError(f"duplicate ssh_target: {ssh_target}")
        targets.add(ssh_target)
        if row["enabled"] not in {"0", "1"}:
            raise SnapshotError(f"enabled must be 0 or 1 for {host_id}")
        try:
            cpu = int(row["cpu_units"])
            memory = int(row["memory_mib"])
        except ValueError as error:
            raise SnapshotError(f"invalid resource budget for {host_id}") from error
        if cpu <= 0 or memory <= 0:
            raise SnapshotError(f"resource budgets must be positive for {host_id}")
        hosts[host_id] = {
            "ssh_target": ssh_target,
            "cpu_units": cpu,
            "memory_mib": memory,
            "enabled": row["enabled"] == "1",
        }
    return hosts


def _host_budgets(
    state_root: Path, pool_id: str, hosts: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    control = _read_json(state_root / "host-budgets.json", {
        "schema_version": 1, "pool_id": pool_id, "generation": 0, "hosts": {},
    })
    if control.get("schema_version") != 1 or control.get("pool_id") != pool_id:
        raise SnapshotError("host budget control identity mismatch")
    _integer(control, "generation", 0)
    overrides = control.get("hosts")
    if not isinstance(overrides, dict):
        raise SnapshotError("host budget overrides must be an object")
    for host_id, budget in overrides.items():
        if host_id not in hosts or not hosts[host_id]["enabled"]:
            raise SnapshotError(f"budget override requires an enabled host: {host_id}")
        if not isinstance(budget, dict) or set(budget) != {"cpu_units", "memory_mib"}:
            raise SnapshotError(f"invalid host budget: {host_id}")
        _integer(budget, "cpu_units", 1)
        _integer(budget, "memory_mib", 1)
    return control


def set_host_budget(state_root: Path, pool_id: str, host_id: str, cpu: int, memory: int) -> None:
    # The CLI holds the same transition lock used by queue controls and drain.
    pool = _read_env(state_root / "pool.env", {
        "pool_id", "state", "resources_file", "resources_sha256",
    })
    if pool["pool_id"] != pool_id or pool["state"] != "ACTIVE":
        raise SnapshotError("host budget change requires the expected active pool")
    resources = Path(pool["resources_file"])
    if hashlib.sha256(resources.read_bytes()).hexdigest() != pool["resources_sha256"]:
        raise SnapshotError("reviewed resource table hash mismatch")
    hosts = _load_hosts(resources)
    if host_id not in hosts or not hosts[host_id]["enabled"] or cpu < 1 or memory < 1:
        raise SnapshotError("positive budgets and an enabled host are required")
    control = _host_budgets(state_root, pool_id, hosts)
    control["hosts"][host_id] = {"cpu_units": cpu, "memory_mib": memory}
    control["generation"] += 1
    _atomic_json(state_root / "host-budgets.json", control)
    print(f"pool_id={pool_id}\nhost_id={host_id}\ncpu_units={cpu}\nmemory_mib={memory}")
    print(f"budget_generation={control['generation']}")


def _host_states(
    state_root: Path, pool_id: str, hosts: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Operator-declared away windows: no new work from away_from until away_until (or forever)."""
    control = _read_json(state_root / "host-states.json", {
        "schema_version": 1, "pool_id": pool_id, "generation": 0, "hosts": {},
    })
    if control.get("schema_version") != 1 or control.get("pool_id") != pool_id:
        raise SnapshotError("host state control identity mismatch")
    _integer(control, "generation", 0)
    entries = control.get("hosts")
    if not isinstance(entries, dict):
        raise SnapshotError("host states must be an object")
    for host_id, entry in entries.items():
        if host_id not in hosts or not hosts[host_id]["enabled"]:
            raise SnapshotError(f"host state requires an enabled host: {host_id}")
        if not isinstance(entry, dict) or set(entry) != {"away_from", "away_until", "reason", "set_epoch"}:
            raise SnapshotError(f"invalid host state: {host_id}")
        _integer(entry, "away_from", 0)
        _integer(entry, "set_epoch", 0)
        if entry["away_until"] is not None and (
                type(entry["away_until"]) is not int or entry["away_until"] <= entry["away_from"]):
            raise SnapshotError(f"invalid away window: {host_id}")
        if not isinstance(entry["reason"], str) or not entry["reason"] or any(c in entry["reason"] for c in "\r\n\t,:"):
            raise SnapshotError(f"invalid away reason: {host_id}")
    return control


def _away_hosts(control: dict[str, Any], now: float) -> set[str]:
    return {
        host_id for host_id, entry in control["hosts"].items()
        if entry["away_from"] <= now and (entry["away_until"] is None or now < entry["away_until"])
    }


def _parse_time(value: str) -> int:
    """Epoch seconds, or an ISO date/time in this host's local time zone."""
    if re.fullmatch(r"[0-9]+", value):
        return int(value)
    try:
        return int(datetime.fromisoformat(value).timestamp())
    except ValueError as error:
        raise SnapshotError(f"time must be epoch seconds or ISO local time: {value}") from error


def set_host_state(
    state_root: Path, pool_id: str, host_id: str, state: str,
    at: Optional[str], until: Optional[str], reason: str,
) -> None:
    # The CLI holds the same transition lock used by host budgets and queue controls.
    pool = _read_env(state_root / "pool.env", {"pool_id", "state", "resources_file"})
    if pool["pool_id"] != pool_id or pool["state"] not in {"ACTIVE", "DRAINING"}:
        raise SnapshotError("host state change requires the expected running pool")
    hosts = _load_hosts(Path(pool["resources_file"]))
    if host_id not in hosts or not hosts[host_id]["enabled"]:
        raise SnapshotError("host state requires an enabled pool host")
    control = _host_states(state_root, pool_id, hosts)
    now = int(time.time())
    start = _parse_time(at) if at else now
    entry = control["hosts"].get(host_id)
    if state == "away":
        end = _parse_time(until) if until else None
        if end is not None and end <= max(start, now):
            raise SnapshotError("--until must be later than the start and than now")
        if not reason or any(c in reason for c in "\r\n\t,:"):
            raise SnapshotError("--reason must be one line without tabs, commas or colons")
        control["hosts"][host_id] = {"away_from": start, "away_until": end, "reason": reason, "set_epoch": now}
    elif until:
        raise SnapshotError("--until applies only to --state away")
    elif start <= now:
        control["hosts"].pop(host_id, None)
    elif entry is None or entry["away_from"] >= start:
        raise SnapshotError("a scheduled return needs an away window that starts before it")
    else:
        entry.update(away_until=start, set_epoch=now)
    control["generation"] += 1
    _atomic_json(state_root / "host-states.json", control)
    current = control["hosts"].get(host_id)
    print(f"pool_id={pool_id}\nhost_id={host_id}")
    print(f"away_now={int(host_id in _away_hosts(control, now))}")
    print(f"away_from={current['away_from'] if current else ''}")
    print(f"away_until={(current['away_until'] or '') if current else ''}")
    print(f"host_state_generation={control['generation']}")


REGISTRATION_KEYS = {
    "schema_version", "pool_id", "queue_id", "batch_dir", "original_queue_file",
    "editable_queue_file", "model_dir", "log_dir", "lock_dir", "tag_clean",
    "resources_file", "resources_sha256", "queue_sha256", "control_file",
    "registered_epoch",
}
RESOURCE_KEYS = {
    "schema_version", "queue_id", "cpu_units_per_job", "memory_mib_per_job",
    "shared_io_units_per_job",
}
CONTROL_KEYS = {
    "schema_version", "queue_id", "generation", "admission", "schedule_priority",
    "max_workers", "updated_epoch",
}
PROFILE_KEYS = {"cpu_units_per_job", "memory_mib_per_job", "shared_io_units_per_job"}


def _accounting_profiles(state_root: Path, pool_id: str) -> dict[str, dict[str, int]]:
    record = _read_json(state_root / "queue-profiles.json", {
        "schema_version": 1, "pool_id": pool_id, "profiles": {},
    })
    if record.get("schema_version") != 1 or record.get("pool_id") != pool_id:
        raise SnapshotError("queue accounting profile identity mismatch")
    profiles = record.get("profiles")
    if not isinstance(profiles, dict):
        raise SnapshotError("invalid queue accounting profiles")
    for queue_id, profile in profiles.items():
        if (not isinstance(queue_id, str) or not IDENTIFIER.fullmatch(queue_id)
                or not isinstance(profile, dict) or set(profile) != PROFILE_KEYS):
            raise SnapshotError("invalid queue accounting profile")
        for key in PROFILE_KEYS:
            _integer(profile, key, 0 if key == "shared_io_units_per_job" else 1)
    return profiles


def _queue_job_ids(
    editable: Path, expected_hash: str, queue_id: str, cache: Optional[dict[str, Any]],
) -> list[str]:
    try:
        stat = editable.stat()
        signature = (
            stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
            expected_hash,
        )
        if cache is not None and cache.get(queue_id, (None,))[0] == signature:
            return cache[queue_id][1]
        queue_bytes = editable.read_bytes()
        queue_text = queue_bytes.decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise SnapshotError(f"cannot read editable queue for {queue_id}: {error}") from error
    if (
        not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
        or hashlib.sha256(queue_bytes).hexdigest() != expected_hash
    ):
        raise SnapshotError(f"queue hash mismatch for {queue_id}")
    # Records end only at LF, matching the claim helper's byte-offset cursor.
    job_ids = [line.split(None, 1)[0] for line in queue_text.split("\n") if line.strip()]
    if cache is not None:
        cache[queue_id] = (signature, job_ids)
    return job_ids


def _load_queue(
    path: Path, pool_id: str, pool_state: str, cache: Optional[dict[str, Any]],
    profiles: Optional[dict[str, dict[str, int]]] = None,
) -> tuple[str, dict[str, Any]]:
    registration = _read_env(path, REGISTRATION_KEYS)
    queue_id = registration["queue_id"]
    if registration["schema_version"] != "1" or registration["pool_id"] != pool_id:
        raise SnapshotError(f"registration identity mismatch: {path}")
    if not IDENTIFIER.fullmatch(queue_id) or path.name != f"{queue_id}.env":
        raise SnapshotError(f"invalid queue registration: {path}")
    resources_path = Path(registration["resources_file"])
    expected_hash = registration["resources_sha256"]
    try:
        resource_bytes = resources_path.read_bytes()
        resource_text = resource_bytes.decode("utf-8")
    except (OSError, UnicodeDecodeError) as error:
        raise SnapshotError(f"cannot hash resources for {queue_id}: {error}") from error
    actual_hash = hashlib.sha256(resource_bytes).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash) or actual_hash != expected_hash:
        raise SnapshotError(f"resource hash mismatch for {queue_id}")
    resources = _parse_env(resource_text, resources_path, RESOURCE_KEYS)
    if resources["schema_version"] not in {"1", "2"} or resources["queue_id"] != queue_id:
        raise SnapshotError(f"resource identity mismatch: {queue_id}")
    try:
        dependencies, retry_lost_jobs = policy(resources)
    except (ValueError, KeyError) as error:
        raise SnapshotError(f"invalid workflow policy for {queue_id}: {error}") from error
    hold = dependency_hold(dependencies)
    costs = {
        key: _env_int(resources, key, 0 if key == "shared_io_units_per_job" else 1)
        for key in PROFILE_KEYS
    }
    if profiles is not None:
        if queue_id in profiles and profiles[queue_id] != costs:
            raise SnapshotError(f"immutable resource profile changed: {queue_id}")
        profiles[queue_id] = costs
    status_path = Path(registration["batch_dir"]) / "adaptive" / "status.env"
    if status_path.is_symlink() or (status_path.exists() and not status_path.is_file()):
        raise SnapshotError(f"invalid queue status path: {status_path}")
    if not os.access(status_path.parent, os.W_OK | os.X_OK):
        raise SnapshotError(f"queue status directory is not writable: {status_path.parent}")
    control = _read_env(Path(registration["control_file"]), CONTROL_KEYS)
    # An empty host_priorities entry means no overrides, as when it is absent.
    overrides_text = control.get("host_priorities", "")
    if control["schema_version"] not in {"1", "2"} or (
        ("memory_mib_per_job" in control or overrides_text)
        and control["schema_version"] != "2"
    ):
        raise SnapshotError(f"unsupported queue schema: {queue_id}")
    if control["queue_id"] != queue_id:
        raise SnapshotError(f"queue metadata identity mismatch: {queue_id}")
    host_priorities: dict[str, Any] = {}
    if overrides_text:
        default, host_priorities = parse_priority_spec(overrides_text)
        if default is not None or format_host_priorities(host_priorities) != overrides_text:
            raise SnapshotError(f"host_priorities must be canonical HOST=VALUE overrides: {queue_id}")
    calibration_keys = {
        "memory_calibration_jobs", "memory_calibration_seconds",
        "memory_min_mib_per_job", "memory_calibration_generation",
    }
    calibration_policy = None
    if calibration_keys.intersection(control):
        if not calibration_keys.issubset(control) or control["schema_version"] != "2" or (
            "memory_mib_per_job" not in control
        ):
            raise SnapshotError(f"incomplete memory calibration policy: {queue_id}")
        calibration_policy = {
            "jobs": _env_int(control, "memory_calibration_jobs", 0),
            "seconds": _env_int(control, "memory_calibration_seconds", 0),
            "minimum_mib": _env_int(control, "memory_min_mib_per_job", 1),
            "generation": _env_int(control, "memory_calibration_generation", 1),
        }
        if (
            (calibration_policy["jobs"] != 0 and not 20 <= calibration_policy["jobs"] <= 50)
            or (calibration_policy["seconds"] != 0
                and not 600 <= calibration_policy["seconds"] <= 86400)
            or not (calibration_policy["jobs"] or calibration_policy["seconds"])
            or calibration_policy["minimum_mib"] > _env_int(control, "memory_mib_per_job", 1)
            or calibration_policy["generation"] > _env_int(control, "generation", 1)
        ):
            raise SnapshotError(f"invalid memory calibration policy: {queue_id}")
    admission = control["admission"]
    if admission not in {"enabled", "paused", "closed"}:
        raise SnapshotError(f"invalid admission for {queue_id}")
    effective_admission = (
        "paused" if pool_state == "DRAINING"
        else admission
    )
    editable = Path(_safe_value(registration["editable_queue_file"], "editable_queue_file"))
    claims = Path(_safe_value(registration["log_dir"], "log_dir")) / "claims"
    job_ids = _queue_job_ids(editable, registration["queue_sha256"], queue_id, cache)
    try:
        claimed = {entry.name for entry in os.scandir(claims)}
    except FileNotFoundError:
        claimed = set()
    except OSError as error:
        raise SnapshotError(f"cannot read claims for {queue_id}: {error}") from error
    pending = sum(f"job_{job_id}.claim" not in claimed for job_id in job_ids)
    run_meta = _read_env(Path(registration["batch_dir"]) / "run_meta.env", {"total_jobs"})
    profile = {
        "queue_id": queue_id,
        **costs,
        "memory_mib_per_job": (
            _env_int(control, "memory_mib_per_job", 1)
            if "memory_mib_per_job" in control else costs["memory_mib_per_job"]
        ),
        "schedule_priority": _env_int(control, "schedule_priority"),
        "host_priorities": host_priorities,
        "max_workers": _env_int(control, "max_workers", 0),
        "pending_lines": pending,
        "admission": "enabled" if effective_admission == "enabled" and not hold else "paused",
    }
    return queue_id, {
        "allocator": profile,
        "dependencies": dependencies,
        "dependency_hold": hold,
        "retry_lost_jobs": retry_lost_jobs,
        "job_ids": job_ids,
        "requested_memory_mib_per_job": profile["memory_mib_per_job"],
        "calibration_policy": calibration_policy,
        "initial_memory_mib_per_job": costs["memory_mib_per_job"],
        "control_generation": _env_int(control, "generation", 1),
        "total_jobs": _env_int(run_meta, "total_jobs", 1),
        "display_admission": effective_admission,
        "registration_path": path,
        "registration": registration,
        "control": control,
    }


def _load_queues(
    state_root: Path, pool_id: str, pool_state: str,
    cache: Optional[dict[str, Any]] = None, problems: Optional[dict[str, str]] = None,
    profiles: Optional[dict[str, dict[str, int]]] = None,
) -> dict[str, dict[str, Any]]:
    """Load registrations; with ``problems``, quarantine bad queues instead of raising."""
    queues: dict[str, dict[str, Any]] = {}
    for path in sorted((state_root / "queues").glob("*.env")):
        try:
            queue_id, queue = _load_queue(path, pool_id, pool_state, cache, profiles)
            queues[queue_id] = queue
        except SnapshotError as error:
            if problems is None:
                raise
            problems[path.stem] = str(error)
    return queues


def _parse_status(
    text: str, pool_id: str, host_id: str, queues: dict[str, dict[str, Any]],
    memory_history: Optional[dict[str, list[int]]] = None,
) -> dict[str, Any]:
    rows = text.splitlines()
    if not rows or rows[0] != STATUS_HEADER:
        raise SnapshotError(f"invalid status header from {host_id}")
    agent: Optional[list[str]] = None
    reservations: dict[str, int] = {}
    reservation_memory_mib: dict[str, int] = {}
    reservation_ids: dict[str, str] = {}
    reservation_charges: dict[str, int] = {}
    calibration_reports: dict[str, dict[str, Any]] = {}
    blocked_queues: dict[str, str] = {}
    uncertain = False
    for line in rows[1:]:
        columns = line.split("\t")
        if len(columns) != STATUS_COLUMNS or columns[1] != pool_id or columns[2] != host_id:
            raise SnapshotError(f"invalid status row from {host_id}")
        if columns[0] == "agent":
            if agent is not None:
                raise SnapshotError(f"duplicate agent status from {host_id}")
            agent = columns
            continue
        if columns[0] == "blocked_queue":
            queue_id = columns[6]
            if (
                not IDENTIFIER.fullmatch(queue_id) or queue_id in blocked_queues
                or columns[13] not in {"rapid_job_failures", "infrastructure_failed"}
                or any(value for index, value in enumerate(columns)
                       if index not in {0, 1, 2, 6, 13})
            ):
                raise SnapshotError(f"invalid failure feedback from {host_id}")
            blocked_queues[queue_id] = columns[13]
            continue
        if columns[0] == "calibration":
            queue_id = columns[6]
            if (
                not IDENTIFIER.fullmatch(queue_id) or queue_id in calibration_reports
                or any(value for index, value in enumerate(columns)
                       if index not in {0, 1, 2, 6, 13})
            ):
                raise SnapshotError(f"invalid calibration feedback from {host_id}")
            try:
                report = json.loads(columns[13])
            except ValueError as error:
                raise SnapshotError(f"invalid calibration JSON from {host_id}") from error
            if not isinstance(report, dict) or "host_id" in report:
                raise SnapshotError(f"invalid calibration report from {host_id}")
            calibration_reports[queue_id] = {**report, "host_id": host_id}
            continue
        if columns[0] != "reservation" or not columns[5] or columns[5] in reservation_ids:
            raise SnapshotError(f"invalid reservation status from {host_id}")
        if any(columns[13:20]):
            raise SnapshotError(f"reservation status contains agent telemetry from {host_id}")
        queue_id = columns[6]
        if queue_id not in queues or columns[7] not in {"starting", "running", "uncertain"}:
            raise SnapshotError(f"unknown reservation in status from {host_id}")
        profile = queues[queue_id]["allocator"]
        permitted_memory = {
            profile["memory_mib_per_job"],
            queues[queue_id].get("initial_memory_mib_per_job", profile["memory_mib_per_job"]),
            *(memory_history or {}).get(queue_id, []),
        }
        if columns[9] != str(profile["cpu_units_per_job"]) or columns[10] not in {
            str(memory) for memory in permitted_memory
        }:
            raise SnapshotError(f"reservation profile mismatch from {host_id}")
        try:
            observed_memory = int(columns[11])
            charged_memory = int(columns[12])
            current_memory = int(columns[20])
            sustained_memory = int(columns[21])
        except ValueError as error:
            raise SnapshotError(f"invalid reservation memory from {host_id}") from error
        declared_memory = int(columns[10])
        if (
            observed_memory < 0
            or current_memory < 0 or sustained_memory < 0
            or observed_memory < max(current_memory, (sustained_memory * 110 + 99) // 100)
            or charged_memory != max(declared_memory, observed_memory)
        ):
            raise SnapshotError(f"invalid reservation memory charge from {host_id}")
        reservation_ids[columns[5]] = queue_id
        reservation_charges[columns[5]] = charged_memory
        reservations[queue_id] = reservations.get(queue_id, 0) + 1
        reservation_memory_mib[queue_id] = (
            reservation_memory_mib.get(queue_id, 0)
            + max(charged_memory, profile["memory_mib_per_job"])
        )
        uncertain = uncertain or columns[7] == "uncertain"
    if agent is None:
        raise SnapshotError(f"missing agent status from {host_id}")
    if any(agent[20:]):
        raise SnapshotError(f"agent status contains reservation memory from {host_id}")
    try:
        generation = int(agent[3])
        live_cpu = int(agent[14])
        live_memory = int(agent[15])
        smbd_cpu_tenths = int(agent[16])
        sample_epoch = int(agent[18])
        smb_bytes = int(agent[19])
    except ValueError as error:
        raise SnapshotError(f"invalid agent telemetry from {host_id}") from error
    if generation < 0 or agent[4] not in {"enabled", "drain-only"}:
        raise SnapshotError(f"invalid agent state from {host_id}")
    if (
        live_cpu < 0 or live_memory < 0 or smbd_cpu_tenths < 0 or sample_epoch < 0
        or smb_bytes < -1 or agent[17] not in {"ok", "unavailable"}
    ):
        raise SnapshotError(f"invalid agent telemetry from {host_id}")
    uncertain = uncertain or agent[13] == "uncertain_reservation"
    return {
        "generation": generation, "reservations": reservations,
        "reservation_memory_mib": reservation_memory_mib,
        "reservation_ids": reservation_ids, "admission": agent[4],
        "reservation_charges_mib": reservation_charges,
        "calibration_reports": calibration_reports,
        "uncertain": uncertain, "blocked_queues": blocked_queues, "live_cpu_units": live_cpu,
        "live_memory_mib": live_memory, "smbd_cpu_tenths": smbd_cpu_tenths,
        "telemetry_state": agent[17], "resource_sample_epoch": sample_epoch,
        "smb_bytes": smb_bytes,
    }


def _ssh(
    target: str, command: str, timeout: float, input_text: Optional[str] = None
) -> str:
    connect_timeout = max(1, min(30, int(timeout)))
    # Reuse one connection per host; ticks otherwise pay two handshakes per host.
    arguments = [
        os.environ.get("ADAPTIVE_SSH_BIN", "ssh"), "-o", "BatchMode=yes",
        "-o", f"ConnectTimeout={connect_timeout}", "-o", "ControlMaster=auto",
        "-o", "ControlPersist=60", "-o", "ControlPath=/tmp/parallel-workers-adaptive-%C",
    ]
    if input_text is None:
        arguments.append("-n")
    arguments.extend([target, command])
    try:
        result = subprocess.run(
            arguments, input=input_text, text=True, capture_output=True,
            timeout=timeout, check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SnapshotError(f"SSH failed for {target}: {error}") from error
    if result.returncode:
        detail = result.stderr.strip() or f"exit {result.returncode}"
        # ssh itself exits 255; any other code came from the remote command.
        if result.returncode == 255:
            raise SnapshotError(f"SSH failed for {target}: {detail}")
        raise RemoteExit(f"SSH failed for {target}: {detail}", result.returncode)
    return result.stdout


def _remote_dir(pool_id: str) -> str:
    return f'"$HOME/.parallel-workers/adaptive/{pool_id}"'


class Coordinator:
    def __init__(
        self, state_root: Path, pool_id: str, resources_file: Path,
        shared_io_capacity: int, tick_seconds: float, lease_seconds: int,
        io_pressure_host: Optional[str] = None, smbd_cpu_limit_pct: int = 400,
        cpu_tolerance_pct: int = 10,
        start_interval_seconds: float = 4,
    ) -> None:
        if not IDENTIFIER.fullmatch(pool_id):
            raise SnapshotError("pool_id must contain only letters, numbers, dot, underscore, or dash")
        if shared_io_capacity < 0 or tick_seconds <= 0 or lease_seconds <= 0:
            raise SnapshotError("I/O capacity must be non-negative and timing values must be positive")
        if smbd_cpu_limit_pct <= 0:
            raise SnapshotError("SMB CPU limit must be a positive integer percentage")
        if not 0 <= cpu_tolerance_pct <= 100:
            raise SnapshotError("CPU tolerance must be between 0 and 100 percent of one core")
        if not math.isfinite(start_interval_seconds) or start_interval_seconds < 0:
            raise SnapshotError("start interval must be finite and non-negative")
        self.state_root = state_root
        self.pool_id = pool_id
        self.hosts = _load_hosts(resources_file)
        self.base_hosts = self.hosts
        self.budget_generation = 0
        self.away: set[str] = set()
        self.cpu_tolerance_pct = cpu_tolerance_pct
        self.start_interval_seconds = start_interval_seconds
        self.next_tick_seconds = tick_seconds
        if io_pressure_host is not None and io_pressure_host not in self.hosts:
            raise SnapshotError(f"I/O pressure host is not in the resource table: {io_pressure_host}")
        self.io_capacity = shared_io_capacity
        self.io_pressure_host = io_pressure_host
        self.smbd_cpu_limit_tenths = smbd_cpu_limit_pct * 10
        self.io_pressure_clear_samples = 3
        self.tick_seconds = tick_seconds
        self.lease_seconds = lease_seconds
        self.ssh_timeout = max(5.0, min(30.0, tick_seconds))
        self.queue_cache: dict[str, Any] = {}
        self.quarantined: dict[str, str] = {}
        self.quarantine_published = False
        self.memory_calibration: Optional[dict[str, Any]] = None
        self.saved_memory_calibration: Optional[dict[str, Any]] = None
        self.scheduler_script = Path(__file__).resolve().parent / "adaptive_scheduler.sh"
        self.agent_stopped_since: dict[str, float] = {}
        self.agent_restart_attempts: dict[str, float] = {}
        self.agent_restarts: dict[str, subprocess.Popen] = {}

    def _calibrate_memory(
        self, queues: dict[str, dict[str, Any]],
        polled: Optional[dict[str, dict[str, Any]]] = None,
    ) -> None:
        path = self.state_root / "memory-calibration.json"
        if self.memory_calibration is None:
            previous = _read_json(path, {
                "schema_version": 1, "pool_id": self.pool_id, "queues": {},
            })
            if (
                previous.get("schema_version") != 1 or previous.get("pool_id") != self.pool_id
                or not isinstance(previous.get("queues"), dict)
                or any(not IDENTIFIER.fullmatch(key) or not isinstance(value, dict)
                       for key, value in previous["queues"].items())
            ):
                raise SnapshotError("invalid memory calibration recovery state")
            self.memory_calibration = previous
            self.saved_memory_calibration = previous
        now = time.time()
        states = {}
        for queue_id, queue in queues.items():
            policy = queue["calibration_policy"]
            if policy is None:
                continue
            reports = []
            for host_id, status in (polled or {}).items():
                if not (
                    self.hosts[host_id]["enabled"] and status["reachable"]
                    and not status["uncertain"] and status["telemetry_state"] == "ok"
                    and 0 <= now - status.get("resource_sample_epoch", 0) <= self.lease_seconds
                ):
                    continue
                report = status.get("calibration_reports", {}).get(queue_id)
                if report is not None:
                    if queue_id in status.get("blocked_queues", {}):
                        # Failure feedback invalidates the cohort even while a hold
                        # prevents this host's measurements from lowering the floor.
                        report = {**report, "completed": [], "running": []}
                    reports.append(report)
            try:
                state = calibrate(
                    self.memory_calibration["queues"].get(queue_id),
                    baseline_mib=queue["requested_memory_mib_per_job"], policy=policy,
                    reports=reports, now=now,
                )
            except ValueError as error:
                raise SnapshotError(f"invalid memory calibration for {queue_id}: {error}") from error
            states[queue_id] = state
            queue["calibration_state"] = state
            queue["allocator"]["memory_mib_per_job"] = state["effective_memory_mib"]
        self.memory_calibration = {
            "schema_version": 1, "pool_id": self.pool_id, "queues": states,
        }
        # Keep polling timestamps in memory; persist only meaningful policy or
        # evidence changes, rather than adding a shared-volume write every tick.
        def durable(value: dict[str, Any]) -> dict[str, Any]:
            return {**value, "queues": {
                key: {field: item for field, item in state.items() if field != "updated_epoch"}
                for key, state in value["queues"].items()
            }}
        if polled is not None and durable(self.memory_calibration) != durable(self.saved_memory_calibration):
            _atomic_json(path, self.memory_calibration)
            self.saved_memory_calibration = self.memory_calibration

    def _record_quarantine(self, problems: dict[str, str]) -> None:
        if not self.quarantine_published or problems != self.quarantined:
            for queue_id, reason in sorted(problems.items()):
                if self.quarantined.get(queue_id) != reason:
                    print(f"adaptive coordinator: quarantined queue {queue_id}: {reason}", file=sys.stderr)
            _atomic_json(self.state_root / "quarantine.json", {
                "schema_version": 1, "pool_id": self.pool_id, "queues": problems,
            })
            self.quarantined = problems.copy()
            self.quarantine_published = True

    def _io_pressure(self, pressure: dict[str, Any]) -> tuple[str, int]:
        path = self.state_root / "io-pressure.json"
        previous = _read_json(path, {
            "schema_version": 1,
            "pool_id": self.pool_id,
            "state": "waiting",
            "clear_samples": 0,
        })
        if (
            previous.get("schema_version") != 1
            or previous.get("pool_id") != self.pool_id
            or previous.get("state") not in {"waiting", "ok", "high", "stuck", "unavailable"}
            or isinstance(previous.get("clear_samples"), bool)
            or not isinstance(previous.get("clear_samples"), int)
            or previous["clear_samples"] < 0
            or type(previous.get("smb_bytes", -1)) is not int
            or any(type(previous.get(key, 0)) is not int or previous.get(key, 0) < 0
                   for key in ("low_rate_since", "low_rate_samples"))
        ):
            raise SnapshotError("invalid I/O pressure recovery state")

        smbd_cpu_tenths = int(pressure["smbd_cpu_tenths"])
        sample_epoch = int(pressure.get("resource_sample_epoch", 0))
        smb_bytes = int(pressure.get("smb_bytes", -1))
        fresh = 0 <= time.time() - sample_epoch <= self.lease_seconds
        previous_epoch = previous.get("resource_sample_epoch", 0)
        new_sample = sample_epoch > previous_epoch
        smb_rate: Optional[float] = None
        if new_sample and previous_epoch > 0 and 0 <= previous.get("smb_bytes", -1) <= smb_bytes:
            smb_rate = (smb_bytes - previous["smb_bytes"]) / (sample_epoch - previous_epoch)
        low_rate_since = 0
        low_rate_samples = 0
        if (
            not pressure["reachable"] or pressure["uncertain"]
            or pressure["telemetry_state"] != "ok" or not fresh
        ):
            state = "unavailable"
            clear_samples = 0
        elif smbd_cpu_tenths >= self.smbd_cpu_limit_tenths:
            state = "high"
            if not new_sample and previous["state"] in {"high", "stuck"}:
                state = previous["state"]
                low_rate_since = previous.get("low_rate_since", 0)
                low_rate_samples = previous.get("low_rate_samples", 0)
            elif (
                smb_rate is not None and smb_rate < SMB_STUCK_BYTES_PER_SECOND
                and sample_epoch - previous_epoch <= self.lease_seconds
                and previous["state"] in {"high", "stuck"}
                and previous.get("smbd_cpu_tenths", 0) >= self.smbd_cpu_limit_tenths
            ):
                low_rate_since = previous.get("low_rate_since", 0) or previous_epoch
                low_rate_samples = previous.get("low_rate_samples", 0) + 1
                if low_rate_samples >= 3 and sample_epoch - low_rate_since >= SMB_STUCK_SECONDS:
                    state = "stuck"
            if state == "stuck" and previous["state"] != "stuck":
                print(
                    f"adaptive coordinator: smbd on {self.io_pressure_host} is at "
                    f"{smbd_cpu_tenths / 10:.1f}% CPU while serving {smb_rate:.0f} B/s "
                    f"for at least {SMB_STUCK_SECONDS}s; possible stall, inspect threads "
                    "and clients before considering a restart", file=sys.stderr,
                )
            clear_samples = 0
        elif previous["state"] == "ok":
            state = "ok"
            clear_samples = self.io_pressure_clear_samples
        else:
            clear_samples = min(
                self.io_pressure_clear_samples, previous["clear_samples"] + int(new_sample)
            )
            state = (
                "ok" if clear_samples >= self.io_pressure_clear_samples
                else previous["state"]
            )

        _atomic_json(path, {
            "schema_version": 1,
            "pool_id": self.pool_id,
            "state": state,
            "clear_samples": clear_samples,
            "smbd_cpu_tenths": smbd_cpu_tenths,
            "resource_sample_epoch": sample_epoch,
            "smb_bytes": smb_bytes,
            "low_rate_since": low_rate_since,
            "low_rate_samples": low_rate_samples,
            "updated_epoch": int(time.time()),
        })
        return state, smbd_cpu_tenths

    def _host_state(self, host_id: str) -> dict[str, Any]:
        path = self.state_root / "plans" / host_id / "state.json"
        default = {
            "generation": 0, "targets": {}, "envelope_targets": {},
            "controls": {}, "pending": False,
        }
        if not path.exists() and self.hosts[host_id]["enabled"]:
            raise SnapshotError(f"local plan state is missing for enabled host {host_id}")
        state = _read_json(path, default)
        for key in default:
            if key not in state:
                raise SnapshotError(f"host plan state is missing {key}: {host_id}")
        if (
            isinstance(state["generation"], bool)
            or not isinstance(state["generation"], int)
            or state["generation"] < 0
        ):
            raise SnapshotError(f"invalid generation in host plan state: {host_id}")
        if not isinstance(state["pending"], bool):
            raise SnapshotError(f"invalid pending flag in host plan state: {host_id}")
        for key in ("targets", "envelope_targets", "controls"):
            values = state[key]
            if not isinstance(values, dict) or any(
                not isinstance(queue_id, str) or not IDENTIFIER.fullmatch(queue_id)
                or isinstance(value, bool) or not isinstance(value, int) or value < 0
                for queue_id, value in values.items()
            ):
                raise SnapshotError(f"invalid {key} in host plan state: {host_id}")
        pacing = state.get("start_pacing", {"slots": 0, "next_epoch": 0, "cursor": ""})
        if (not isinstance(pacing, dict) or isinstance(pacing.get("slots"), bool)
                or not isinstance(pacing.get("slots"), int) or pacing["slots"] < 0
                or isinstance(pacing.get("next_epoch"), bool)
                or not isinstance(pacing.get("next_epoch"), (int, float))
                or not math.isfinite(pacing["next_epoch"]) or pacing["next_epoch"] < 0
                or not isinstance(pacing.get("cursor"), str)
                or (pacing["cursor"] and not IDENTIFIER.fullmatch(pacing["cursor"]))):
            raise SnapshotError(f"invalid start pacing in host plan state: {host_id}")
        if any(
            int(target) > int(state["envelope_targets"].get(queue_id, 0))
            for queue_id, target in state["targets"].items()
        ):
            raise SnapshotError(f"target exceeds its envelope in host plan state: {host_id}")
        for key in ("memory_mib_per_job", "envelope_memory_mib_per_job"):
            values = state.setdefault(key, {})
            if not isinstance(values, dict) or any(
                not isinstance(queue_id, str) or not IDENTIFIER.fullmatch(queue_id)
                or type(memory) is not int or memory < 1
                for queue_id, memory in values.items()
            ):
                raise SnapshotError(f"invalid {key} in host plan state: {host_id}")
        history = state.setdefault("memory_history", {})
        if not isinstance(history, dict) or any(
            not isinstance(queue_id, str) or not IDENTIFIER.fullmatch(queue_id)
            or not isinstance(values, list) or not values
            or any(type(memory) is not int or memory < 1 for memory in values)
            or len(set(values)) != len(values)
            for queue_id, values in history.items()
        ):
            raise SnapshotError(f"invalid memory_history in host plan state: {host_id}")
        if any(
            set(state[key]) != set(history)
            or any(memory not in history[queue_id] for queue_id, memory in state[key].items())
            for key in ("memory_mib_per_job", "envelope_memory_mib_per_job")
        ):
            raise SnapshotError(f"memory profile is not in host plan history: {host_id}")
        # First generation of each queue's current unchanged control, and of its
        # current run of zero targets, in this host's published plans.
        for key in ("control_since", "zero_since"):
            values = state.setdefault(key, {})
            if not isinstance(values, dict) or any(
                not isinstance(queue_id, str) or not IDENTIFIER.fullmatch(queue_id)
                or type(since) is not int or not 0 < since <= state["generation"]
                for queue_id, since in values.items()
            ):
                raise SnapshotError(f"invalid {key} in host plan state: {host_id}")
        return state

    def _poll(self, host_id: str, queues: dict[str, dict[str, Any]]) -> dict[str, Any]:
        host = self.hosts[host_id]
        memory_history = self._host_state(host_id)["memory_history"]
        remote = _remote_dir(self.pool_id)
        # Exit AGENT_STOPPED means the host answered but no agent owns its state.
        command = (
            f"set -e; state={remote}; lock=\"$state/agent.lock\"; snapshot=\"$state/.poll-lock.$$\"; "
            f"test -f \"$state/agent.env\"; test -f \"$lock\" || exit {AGENT_STOPPED}; "
            "ln \"$lock\" \"$snapshot\"; trap 'rm -f \"$snapshot\"' EXIT; "
            "pid=$(awk -F= '$1 == \"pid\" {print $2; exit}' \"$snapshot\"); "
            "script=$(awk -F= '$1 == \"script\" {sub(/^[^=]*=/, \"\"); print; exit}' \"$snapshot\"); "
            "owner_state=$(awk -F= '$1 == \"state_dir\" {sub(/^[^=]*=/, \"\"); print; exit}' \"$snapshot\"); "
            f"grep -Fqx 'pool_id={self.pool_id}' \"$snapshot\"; "
            f"grep -Fqx 'host_id={host_id}' \"$snapshot\"; "
            "test \"$owner_state\" = \"$state\"; test \"$script\" = \"$state/bin/mmadaptiveagent.sh\"; "
            "case $pid in ''|*[!0-9]*) exit 4;; esac; "
            "command=$(ps -ww -p \"$pid\" -o command= 2>/dev/null || true); "
            f"case \" $command \" in *\" $script \"*) ;; *) exit {AGENT_STOPPED};; esac; "
            f"case \" $command \" in *\" --state-dir $state \"*) ;; *) exit {AGENT_STOPPED};; esac; "
            "status_file=\"$state/status.tsv\"; test -f \"$status_file\"; "
            "case $(uname -s) in Darwin) mtime=$(stat -f %m \"$status_file\");; "
            "*) mtime=$(stat -c %Y \"$status_file\");; esac; "
            "now=$(date +%s); case $mtime:$now in *[!0-9:]*) exit 5;; esac; "
            f"age=$((now - mtime)); test \"$age\" -ge 0; test \"$age\" -le {self.lease_seconds}; "
            "cat \"$status_file\"; test \"$lock\" -ef \"$snapshot\""
        )
        cache = self.state_root / "last-known" / f"{host_id}.tsv"
        try:
            text = _ssh(host["ssh_target"], command, self.ssh_timeout)
            status = _parse_status(text, self.pool_id, host_id, queues, memory_history)
            if not 0 <= time.time() - status["resource_sample_epoch"] <= self.lease_seconds:
                status["telemetry_state"] = "unavailable"
            previous: Optional[dict[str, Any]] = None
            if cache.exists():
                try:
                    previous = _parse_status(
                        cache.read_text(encoding="utf-8"), self.pool_id, host_id, queues,
                        memory_history,
                    )
                except (OSError, SnapshotError):
                    previous = None
            if previous and any(
                reservation_id in status["reservation_ids"]
                and status["reservation_ids"][reservation_id] != queue_id
                for reservation_id, queue_id in previous["reservation_ids"].items()
            ):
                status["uncertain"] = True
            if status["uncertain"] and previous:
                for queue_id, count in previous["reservations"].items():
                    status["reservations"][queue_id] = max(
                        count, status["reservations"].get(queue_id, 0)
                    )
                for queue_id, memory in previous["reservation_memory_mib"].items():
                    status["reservation_memory_mib"][queue_id] = max(
                        memory, status["reservation_memory_mib"].get(queue_id, 0)
                    )
                for reservation_id, queue_id in previous["reservation_ids"].items():
                    status["reservation_ids"].setdefault(reservation_id, queue_id)
            elif not status["uncertain"]:
                _atomic_text(cache, text)
            status["reachable"] = True
            status["agent_stopped"] = False
            return status
        except SnapshotError as error:
            agent_stopped = isinstance(error, RemoteExit) and error.returncode == AGENT_STOPPED
            if cache.exists():
                try:
                    status = _parse_status(
                        cache.read_text(encoding="utf-8"), self.pool_id, host_id, queues,
                        memory_history,
                    )
                except (OSError, SnapshotError):
                    status = {
                        "generation": 0, "reservations": {},
                        "reservation_memory_mib": {}, "reservation_ids": {},
                        "admission": "drain-only", "uncertain": True,
                        "live_cpu_units": 0, "live_memory_mib": 0,
                        "smbd_cpu_tenths": 0, "telemetry_state": "unavailable",
                    }
            else:
                status = {
                    "generation": 0, "reservations": {},
                    "reservation_memory_mib": {}, "reservation_ids": {},
                    "admission": "drain-only", "uncertain": True,
                    "live_cpu_units": 0, "live_memory_mib": 0,
                    "smbd_cpu_tenths": 0, "telemetry_state": "unavailable",
                }
            status["reachable"] = False
            status["agent_stopped"] = agent_stopped
            return status

    def _restart_stopped_agents(self, polled: dict[str, dict[str, Any]], now: float) -> None:
        """Restart, through the scheduler, an agent that died on a host that still answers."""
        for host_id, process in list(self.agent_restarts.items()):
            if process.poll() is not None:
                del self.agent_restarts[host_id]
                print(f"adaptive coordinator: restart-agent for {host_id} exited {process.returncode}",
                      file=sys.stderr)
        for host_id, status in polled.items():
            if not status.get("agent_stopped"):
                self.agent_stopped_since.pop(host_id, None)
                continue
            since = self.agent_stopped_since.setdefault(host_id, now)
            if (
                host_id in self.agent_restarts or now - since < self.lease_seconds
                or now - self.agent_restart_attempts.get(host_id, -math.inf)
                < AGENT_RESTART_INTERVAL_SECONDS
            ):
                continue
            self.agent_restart_attempts[host_id] = now
            reason = f"coordinator: no agent on reachable host {host_id} for {int(now - since)} s"
            print(f"adaptive coordinator: {reason}; running restart-agent", file=sys.stderr)
            with (self.state_root / "agent-restarts.log").open("a", encoding="utf-8") as log:
                log.write(f"{time.strftime('%Y-%m-%dT%H:%M:%S%z', time.localtime(now))} {reason}\n")
                log.flush()
                self.agent_restarts[host_id] = subprocess.Popen(
                    [str(self.scheduler_script), "restart-agent", "--host_id", host_id,
                     "--expected-pool-id", self.pool_id, "--reason", reason],
                    stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )

    def _allocation_text(
        self, host_id: str, targets: dict[str, int], queues: dict[str, dict[str, Any]],
        start_grants: dict[str, int],
    ) -> str:
        output = io.StringIO()
        output.write(ALLOCATION_HEADER + "\n")
        for queue_id in sorted(queues):
            queue = queues[queue_id]
            profile = queue["allocator"]
            registration = queue["registration"]
            fields = [
                queue_id, str(targets[queue_id]), str(start_grants[queue_id]),
                str(profile["cpu_units_per_job"]),
                str(profile["memory_mib_per_job"]), str(profile["shared_io_units_per_job"]),
                registration["original_queue_file"], registration["editable_queue_file"],
                registration["model_dir"], registration["log_dir"], registration["lock_dir"],
                registration["tag_clean"], registration["queue_sha256"],
            ]
            for field in fields:
                _safe_value(field, f"allocation value for {host_id}/{queue_id}")
            output.write("\t".join(fields) + "\n")
        return output.getvalue()

    def _publish(
        self, host_id: str, targets: dict[str, int], start_grants: dict[str, int],
        queues: dict[str, dict[str, Any]], polled: dict[str, Any], state: dict[str, Any],
    ) -> dict[str, Any]:
        generation = max(int(state["generation"]), int(polled["generation"])) + 1
        allocations = self._allocation_text(host_id, targets, queues, start_grants)
        digest = hashlib.sha256(allocations.encode()).hexdigest()
        filename = f"allocations.{generation}.tsv"
        lease = "\n".join([
            "schema_version=1", f"pool_id={self.pool_id}", f"host_id={host_id}",
            f"generation={generation}", f"ttl_seconds={self.lease_seconds}",
            f"allocations_file={filename}", f"allocations_sha256={digest}", "",
        ])
        lease += (
            f"cpu_units={self.hosts[host_id]['cpu_units']}\n"
            f"memory_mib={self.hosts[host_id]['memory_mib']}\n"
            f"cpu_tolerance_pct={self.cpu_tolerance_pct}\n"
        )
        plan_dir = self.state_root / "plans" / host_id
        controls = {
            queue_id: queue["control_generation"] for queue_id, queue in queues.items()
        }
        envelope_targets = {
            queue_id: max(
                int(state["envelope_targets"].get(queue_id, 0)),
                int(targets.get(queue_id, 0)),
            )
            for queue_id in state["envelope_targets"].keys() | targets.keys()
        }
        memory_profiles = {
            **state.get("memory_mib_per_job", {}),
            **{queue_id: queue["allocator"]["memory_mib_per_job"]
               for queue_id, queue in queues.items()},
        }
        envelope_memory = {
            queue_id: max(
                memory,
                state.get("envelope_memory_mib_per_job", {}).get(queue_id, memory)
                if state["envelope_targets"].get(queue_id, 0) else memory,
            )
            for queue_id, memory in memory_profiles.items()
        }
        memory_history = {
            queue_id: sorted(set(state.get("memory_history", {}).get(queue_id, [])) | {memory})
            for queue_id, memory in memory_profiles.items()
        }
        # Lease renewals republish unchanged queues. Keeping each queue's first
        # generation lets an acknowledgement of any later plan count for it, so
        # hosts need not all acknowledge the latest plan within one tick.
        previous_control_since = state.get("control_since", {})
        control_since = {
            queue_id: previous_control_since[queue_id]
            if queue_id in previous_control_since
            and state["controls"].get(queue_id) == control
            and state.get("memory_mib_per_job", {}).get(queue_id) == memory_profiles[queue_id]
            else generation
            for queue_id, control in controls.items()
        }
        zero_since = {
            queue_id: state.get("zero_since", {}).get(queue_id, generation)
            for queue_id, target in targets.items() if not target
        }
        precommit_state = {
            "generation": generation,
            "targets": targets,
            "envelope_targets": envelope_targets,
            "controls": controls,
            "control_since": control_since,
            "zero_since": zero_since,
            "pending": True,
            "budget_generation": self.budget_generation,
            "start_pacing": state.get("start_pacing", {"slots": 0, "next_epoch": 0, "cursor": ""}),
            "memory_mib_per_job": memory_profiles,
            "envelope_memory_mib_per_job": envelope_memory,
            "memory_history": memory_history,
        }
        _atomic_json(plan_dir / "state.json", precommit_state)

        remote = _remote_dir(self.pool_id)
        marker = f"__ADAPTIVE_LEASE_{digest}__"
        command = (
            f"mkdir -p {remote} && allocation={remote}/{filename}.tmp.$$ && "
            f"lease={remote}/lease.env.tmp.$$ && "
            "trap 'rm -f \"$allocation\" \"$lease\"' EXIT && "
            f"while IFS= read -r line; do [ \"$line\" = {marker} ] && break; "
            f"printf '%s\\n' \"$line\"; done > \"$allocation\" && "
            f"cat > \"$lease\" && chmod 600 \"$allocation\" \"$lease\" && "
            f"mv -f \"$allocation\" {remote}/{filename} && "
            f"mv -f \"$lease\" {remote}/lease.env && trap - EXIT"
        )
        prune_before = int(state["generation"])
        if prune_before and polled["generation"] >= prune_before:
            command += (
                f" && for old in {remote}/allocations.*.tsv; do "
                "old_generation=${old##*/allocations.}; old_generation=${old_generation%.tsv}; "
                f"case $old_generation in *[!0-9]*|'') continue;; esac; "
                f"[ \"$old_generation\" -ge {prune_before} ] || rm -f \"$old\"; done"
            )
        _ssh(
            self.hosts[host_id]["ssh_target"], command, self.ssh_timeout,
            f"{allocations}{marker}\n{lease}",
        )
        new_state = {
            "generation": generation,
            "targets": targets, "envelope_targets": envelope_targets,
            "controls": controls, "control_since": control_since,
            "zero_since": zero_since, "pending": False,
            "budget_generation": self.budget_generation,
            "start_pacing": precommit_state["start_pacing"],
            "memory_mib_per_job": memory_profiles,
            "envelope_memory_mib_per_job": envelope_memory,
            "memory_history": memory_history,
        }
        _atomic_json(plan_dir / "state.json", new_state)
        return new_state

    def _write_ready(
        self, state: str, resource_state: str = "waiting", live_cpu: int = 0,
        live_memory: int = 0, io_pressure_state: str = "waiting",
        smbd_cpu_tenths: int = 0, unavailable: Optional[dict[str, str]] = None,
    ) -> None:
        unavailable = unavailable or {}
        ready = "\n".join([
            "schema_version=1", f"pool_id={self.pool_id}", f"pid={os.getpid()}",
            f"state={state}", f"resource_state={resource_state}",
            f"live_cpu_units={live_cpu}", f"live_memory_mib={live_memory}",
            f"io_pressure_state={io_pressure_state}",
            f"smbd_cpu_tenths={smbd_cpu_tenths}",
            f"quarantined_queues={len(self.quarantined)}",
            # Declared away hosts, then enabled hosts taking no new work for another reason.
            "away_hosts=" + ",".join(sorted(self.away)),
            "unavailable_hosts=" + ",".join(f"{host}:{reason}" for host, reason in sorted(unavailable.items())),
            "memory_control_version=1",
            "memory_calibration_version=1",
            "workflow_version=1",
            "host_priority_version=1",
            f"updated_epoch={int(time.time())}", "",
        ])
        _atomic_text(self.state_root / "coordinator.ready", ready, 0o644)

    def _queue_transition(self, queue: dict[str, Any], recover: bool = False) -> int:
        lock = self.state_root.parent / "mode-transition.lock"
        try:
            lock.mkdir()
        except FileExistsError:
            return 0
        except OSError as error:
            raise SnapshotError(f"cannot acquire scheduler transition lock: {error}") from error
        try:
            owner = "\n".join([
                f"pid={os.getpid()}", f"host={socket.gethostname()}",
                "command=adaptive_coordinator.py queue transition", "",
            ])
            _atomic_text(lock / "owner.env", owner, 0o644)
            current = _read_env(Path(queue["registration"]["control_file"]), CONTROL_KEYS)
            if current == queue["control"]:
                if recover:
                    if self._pool_state() != "ACTIVE":
                        return 0
                    return retry_verified_tail(queue["registration"], queue["job_ids"])
                queue["registration_path"].unlink()
            return 0
        finally:
            try:
                (lock / "owner.env").unlink(missing_ok=True)
                lock.rmdir()
            except OSError as error:
                raise SnapshotError(f"cannot release scheduler transition lock: {error}") from error

    def _pool_state(self) -> str:
        pool = _read_env(self.state_root / "pool.env", {"pool_id", "state"})
        if pool["pool_id"] != self.pool_id:
            raise SnapshotError("pool marker does not match the running coordinator")
        if pool["state"] not in {"STARTING", "ACTIVE", "DRAINING"}:
            raise SnapshotError(f"unsupported pool state: {pool['state']}")
        return pool["state"]

    def tick(self) -> bool:
        pool_file = self.state_root / "pool.env"
        pool = _read_env(pool_file, {"pool_id", "state"})
        if pool["pool_id"] != self.pool_id or pool["state"] not in {"ACTIVE", "DRAINING"}:
            raise SnapshotError("pool marker does not match the running coordinator")
        budgets = _host_budgets(self.state_root, self.pool_id, self.base_hosts)
        self.budget_generation = budgets["generation"]
        self.hosts = {
            host_id: {**host, **budgets["hosts"].get(host_id, {})}
            for host_id, host in self.base_hosts.items()
        }
        self.away = _away_hosts(_host_states(self.state_root, self.pool_id, self.base_hosts), time.time())
        problems: dict[str, str] = {}
        saved_profiles = _accounting_profiles(self.state_root, self.pool_id)
        profiles = saved_profiles.copy()
        queues = _load_queues(
            self.state_root, self.pool_id, pool["state"], self.queue_cache, problems, profiles,
        )
        for queue_id in list(queues):
            if (queues[queue_id]["dependencies"] or queues[queue_id]["retry_lost_jobs"]) and (
                pool.get("workflow_version") != "1"
            ):
                problems[queue_id] = "pool does not support workflow options"
                del queues[queue_id]
                continue
            if queues[queue_id]["calibration_policy"] is not None and (
                pool.get("memory_calibration_version") != "1"
            ):
                problems[queue_id] = "pool does not support memory calibration"
                del queues[queue_id]
                continue
            unknown = sorted(queues[queue_id]["allocator"]["host_priorities"].keys() - self.hosts.keys())
            if unknown:
                problems[queue_id] = f"host_priorities name hosts outside the pool: {', '.join(unknown)}"
                del queues[queue_id]
        self._calibrate_memory(queues)
        # Persist costs before issuing leases so recovery can still charge a queue
        # whose registration, control, or application paths subsequently break.
        if profiles != saved_profiles:
            _atomic_json(self.state_root / "queue-profiles.json", {
                "schema_version": 1, "pool_id": self.pool_id, "profiles": profiles,
            })
        accounting = {
            queue_id: {"allocator": {
                "queue_id": queue_id, **profile, "schedule_priority": 0,
                "max_workers": 0, "pending_lines": 0, "admission": "paused",
            }} for queue_id, profile in profiles.items()
        }
        accounting.update(queues)
        for stale in set(self.queue_cache) - set(queues):
            del self.queue_cache[stale]
        states = {host_id: self._host_state(host_id) for host_id in self.hosts}
        for state in states.values():
            for queue_id, queue in accounting.items():
                baseline = queue.get(
                    "initial_memory_mib_per_job", queue["allocator"]["memory_mib_per_job"]
                )
                state["memory_mib_per_job"].setdefault(queue_id, baseline)
                state["envelope_memory_mib_per_job"].setdefault(queue_id, baseline)
                state["memory_history"].setdefault(queue_id, [baseline])
        enabled_ids = [host_id for host_id, host in self.hosts.items() if host["enabled"]]
        poll_ids = set(enabled_ids)
        if self.io_pressure_host is not None:
            poll_ids.add(self.io_pressure_host)
        worker_count = max(1, min(8, len(poll_ids)))
        polled: dict[str, dict[str, Any]] = {
            host_id: {
                "generation": 0, "reservations": {},
                "reservation_memory_mib": {}, "uncertain": False,
                "reservation_ids": {}, "admission": "drain-only", "reachable": False,
                "live_cpu_units": 0, "live_memory_mib": 0,
                "smbd_cpu_tenths": 0, "telemetry_state": "unavailable",
            }
            for host_id in self.hosts if host_id not in poll_ids
        }
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = {
                host_id: executor.submit(self._poll, host_id, accounting)
                for host_id in sorted(poll_ids)
            }
            polled.update({host_id: future.result() for host_id, future in futures.items()})
        self._restart_stopped_agents(polled, time.time())
        self._calibrate_memory(queues, polled)
        # Recompute fresh reservation charges when calibration changes a floor.
        # Uncertain/offline hosts may only increase their aggregate charge;
        # their cached identities need not describe the complete reservation set.
        for status in polled.values():
            if status["reachable"] and not status["uncertain"]:
                memory = {}
                for reservation_id, queue_id in status["reservation_ids"].items():
                    memory[queue_id] = memory.get(queue_id, 0) + max(
                        status["reservation_charges_mib"][reservation_id],
                        accounting[queue_id]["allocator"]["memory_mib_per_job"],
                    )
                status["reservation_memory_mib"] = memory
            else:
                for queue_id, count in status["reservations"].items():
                    status["reservation_memory_mib"][queue_id] = max(
                        status["reservation_memory_mib"][queue_id],
                        count * accounting[queue_id]["allocator"]["memory_mib_per_job"],
                    )
        plan_acknowledged = {
            host_id: bool(
                status["reachable"]
                and not status["uncertain"]
                and not states[host_id]["pending"]
                and status["generation"] == int(states[host_id]["generation"])
            )
            for host_id, status in polled.items()
        }
        for host_id in enabled_ids:
            if plan_acknowledged[host_id]:
                states[host_id]["envelope_targets"] = {
                    queue_id: int(polled[host_id]["reservations"].get(queue_id, 0))
                    for queue_id in accounting
                }
                states[host_id]["envelope_memory_mib_per_job"] = {
                    queue_id: states[host_id]["memory_mib_per_job"].get(
                        queue_id, queue["allocator"]["memory_mib_per_job"]
                    )
                    for queue_id, queue in accounting.items()
                }

        def acknowledged_since(host_id: str, key: str, queue_id: str) -> bool:
            status = polled[host_id]
            # A generation beyond the local state means a plan this state never
            # recorded; like the exact match, it acknowledges nothing.
            return plan_acknowledged[host_id] or bool(
                status["reachable"] and not status["uncertain"]
                and queue_id in states[host_id][key]
                and states[host_id][key][queue_id]
                <= int(status["generation"]) <= int(states[host_id]["generation"])
            )

        control_applied = {
            host_id: {
                queue_id: bool(
                    acknowledged_since(host_id, "control_since", queue_id)
                    and int(states[host_id]["controls"].get(queue_id, 0))
                    == queue["control_generation"]
                    and states[host_id]["memory_mib_per_job"].get(queue_id)
                    == queue["allocator"]["memory_mib_per_job"]
                )
                for queue_id, queue in queues.items()
            }
            for host_id, status in polled.items()
        }

        reservations: list[dict[str, Any]] = []
        for host_id, status in polled.items():
            reservations.extend(
                {
                    "host_id": host_id,
                    "queue_id": queue_id,
                    "count": count,
                    "memory_mib": status["reservation_memory_mib"][queue_id],
                }
                for queue_id, count in status["reservations"].items()
            )
        now = int(time.time())
        prior_targets: list[dict[str, Any]] = []
        for host_id, state in states.items():
            for queue_id, target in state["envelope_targets"].items():
                if queue_id not in accounting:
                    continue
                prior_targets.append({
                    "host_id": host_id, "queue_id": queue_id,
                    "target_running": int(target),
                    "acknowledged": plan_acknowledged[host_id],
                    "lease_valid": polled[host_id]["admission"] == "enabled",
                    "memory_mib_per_job": state["envelope_memory_mib_per_job"].get(
                        queue_id, accounting[queue_id]["allocator"]["memory_mib_per_job"]
                    ),
                })
        cursor_path = self.state_root / "cursor.json"
        cursor = _read_json(cursor_path, {})
        live_capacities = {
            host_id: (
                min(host["cpu_units"], int(polled[host_id]["live_cpu_units"])),
                min(host["memory_mib"], int(polled[host_id]["live_memory_mib"])),
            ) if (
                polled[host_id]["reachable"]
                and not polled[host_id]["uncertain"]
                and polled[host_id]["telemetry_state"] == "ok"
            ) else (0, 0)
            for host_id, host in self.hosts.items()
        }
        allocator_hosts = [{
            "host_id": host_id,
            "cpu_units": host["cpu_units"],
            "memory_mib": host["memory_mib"],
            "live_cpu_units": live_capacities[host_id][0],
            "live_memory_mib": live_capacities[host_id][1],
            "enabled": host["enabled"],
            "reachable": bool(polled[host_id]["reachable"]),
            # An away host finishes its running jobs but starts none.
            "drain_only": bool(
                polled[host_id]["uncertain"]
                or polled[host_id]["telemetry_state"] != "ok"
                or host_id in self.away
            ),
            "blocked_queues": list(polled[host_id].get("blocked_queues", {})),
        } for host_id, host in sorted(self.hosts.items())]
        smbd_cpu_tenths = 0
        io_pressure_state = "disabled"
        if self.io_pressure_host is not None:
            io_pressure_state, smbd_cpu_tenths = self._io_pressure(
                polled[self.io_pressure_host]
            )
        allocator_queues = {
            queue_id: {
                **queue["allocator"],
                "admission": (
                    "paused" if io_pressure_state in {"waiting", "high", "stuck", "unavailable"}
                    and queue["allocator"]["shared_io_units_per_job"] > 0
                    else queue["allocator"]["admission"]
                ),
            }
            for queue_id, queue in accounting.items()
        }
        plan = allocate({
            "schema_version": 1,
            # Without a verified cost, the missing queue may still consume any
            # amount of shared I/O. Zero-I/O work can continue on healthy hosts.
            "shared_io_capacity": 0 if problems.keys() - profiles.keys() else self.io_capacity,
            "hosts": allocator_hosts,
            "queues": [allocator_queues[queue_id] for queue_id in sorted(accounting)],
            "reservations": reservations,
            "prior_targets": prior_targets,
            "cursor": cursor,
            "allow_unschedulable_queues": True,
        })
        open_hosts = {
            host["host_id"] for host in allocator_hosts
            if host["enabled"] and host["reachable"] and not host["drain_only"]
        }
        start_grants = _start_grants(
            plan, allocator_queues,
            open_hosts,
        )
        _atomic_json(cursor_path, plan["cursor"])

        prior_zero_acknowledged = all(
            plan_acknowledged[host_id]
            and int(states[host_id]["generation"]) > 0
            and not any(int(target) for target in states[host_id]["targets"].values())
            for host_id in enabled_ids
        )
        no_reservations = not any(status["reservations"] for status in polled.values())
        busy = (
            not no_reservations
            or any(any(grants.values()) for grants in start_grants.values())
            or any(any(counts.values()) for counts in plan["reservation_envelopes"].values())
        )
        self.next_tick_seconds = min(1.0, self.tick_seconds) if busy else self.tick_seconds
        # The durable envelope includes every grant that could have reached a
        # node. An offline node with no such grant has nothing to acknowledge
        # for a finished, paused or closed queue: it cannot start one of its
        # jobs before it reconnects and receives the current control. Requiring
        # its lease renewal would retain finished registrations (and their
        # shared-filesystem probes) and block retry and rebinding of closed
        # queues for as long as a laptop is away.
        offline_queue_empty = {
            host_id: {
                queue_id: (
                    not polled[host_id]["reachable"]
                    and int(states[host_id]["targets"].get(queue_id, 0)) == 0
                    and int(states[host_id]["envelope_targets"].get(queue_id, 0)) == 0
                    and not polled[host_id]["reservations"].get(queue_id, 0)
                ) for queue_id in queues
            } for host_id in enabled_ids
        }
        queue_zero_acknowledged = {
            queue_id: all(
                not host["enabled"] or (
                    acknowledged_since(host_id, "zero_since", queue_id)
                    and int(states[host_id]["targets"].get(queue_id, 0)) == 0
                ) or offline_queue_empty[host_id][queue_id]
                for host_id, host in self.hosts.items()
            )
            for queue_id in queues
        }

        unpaced_starts = {
            queue_id for queue_id in queues
            if any(grants[queue_id] for grants in start_grants.values())
        }
        _pace_start_grants(
            plan, start_grants, allocator_queues, states,
            self.pool_id, time.time(), self.start_interval_seconds,
        )

        for queue_id, queue in queues.items():
            running = sum(status["reservations"].get(queue_id, 0) for status in polled.values())
            target = sum(plan["targets"][host_id][queue_id] for host_id in self.hosts)
            potential_workers = sum(
                plan["reservation_envelopes"][host_id][queue_id] for host_id in self.hosts
            )
            generation = queue["control_generation"]
            pending = queue["allocator"]["pending_lines"]
            # Closures and pauses are what retry, rebinding and hard stops wait on; an
            # enabled queue with pending jobs still waits for every host's plan.
            excusable = not pending or queue["display_admission"] != "enabled"
            complete = all(
                not host["enabled"] or control_applied[host_id][queue_id]
                or (excusable and offline_queue_empty[host_id][queue_id])
                for host_id, host in self.hosts.items()
            )
            admission = queue["allocator"]["admission"]
            display_admission = queue["display_admission"]
            effective_admission = display_admission
            if display_admission != "enabled":
                blocking_reason = "pool_draining" if pool["state"] == "DRAINING" else display_admission
            elif queue["dependency_hold"]:
                blocking_reason = queue["dependency_hold"]
                effective_admission = "blocked"
            elif not pending:
                blocking_reason = "queue_empty"
                effective_admission = "blocked"
            elif allocator_queues[queue_id]["admission"] == "paused":
                blocking_reason = f"smb_{io_pressure_state}"
                effective_admission = "blocked"
            elif any(grants[queue_id] for grants in start_grants.values()):
                blocking_reason = "none"
            elif queue_id in unpaced_starts:
                blocking_reason = "start_pacing"
                effective_admission = "blocked"
            else:
                blocking_reason = "+".join(plan["limits"][queue_id]) or "awaiting_plan"
                effective_admission = "blocked"
            if admission == "paused" and not queue_zero_acknowledged[queue_id]:
                complete = False
            if running and (admission == "paused" or pool["state"] == "DRAINING"):
                lifecycle = "DRAINING"
            elif running:
                lifecycle = "RUNNING"
            elif pending and pool["state"] != "DRAINING" and (
                display_admission == "enabled"
                or (display_admission == "paused" and not potential_workers)
            ):
                # Lease renewals can lag even when an idle paused queue has nothing to drain.
                lifecycle = "PENDING"
            elif not queue_zero_acknowledged[queue_id]:
                lifecycle = "DRAINING"
            elif pending and pool["state"] != "DRAINING" and display_admission != "closed":
                lifecycle = "PENDING"
            else:
                lifecycle = ""
                if (queue["retry_lost_jobs"] and pool["state"] == "ACTIVE"
                        and display_admission != "closed" and not pending and not potential_workers):
                    try:
                        eligible = retry_verified_tail(queue["registration"], queue["job_ids"], apply=False)
                        if eligible:
                            # Preserve an eligible paused tail and retry a busy
                            # transition later instead of retiring its queue.
                            lifecycle = "PENDING"
                            if display_admission == "enabled" and not queue["dependency_hold"] and complete:
                                recovered = self._queue_transition(queue, recover=True)
                                queue["allocator"]["pending_lines"] = recovered
                                blocking_reason = "lost_jobs_requeued" if recovered else "lost_jobs_wait"
                                effective_admission = "blocked"
                    except (OSError, ValueError, KeyError) as error:
                        problems[queue_id] = f"cannot recover verified tail: {error}"
                        continue
                done_dir = Path(queue["registration"]["batch_dir"]) / "done"
                try:
                    completed = sum(
                        entry.name.startswith("job_") and entry.name.endswith(".done")
                        and entry.is_file(follow_symlinks=False)
                        for entry in os.scandir(done_dir)
                    )
                except OSError as error:
                    problems[queue_id] = f"cannot read completion markers in {done_dir}: {error}"
                    continue
                lifecycle = lifecycle or (
                    "COMPLETED" if completed == queue["total_jobs"]
                    else "FINISHED_INCOMPLETE"
                )
            calibration = queue.get("calibration_state", {})
            status_text = "\n".join([
                "schema_version=1", f"queue_id={queue_id}", f"lifecycle={lifecycle}",
                f"admission={display_admission}",
                f"effective_admission={effective_admission}",
                f"blocking_reason={blocking_reason}",
                "dependency_queues=" + ",".join(item["queue_id"] for item in queue["dependencies"]),
                f"retry_lost_jobs={int(queue['retry_lost_jobs'])}",
                "failure_holds=" + ",".join(
                    f"{host_id}:{status['blocked_queues'][queue_id]}"
                    for host_id, status in sorted(polled.items())
                    if queue_id in status.get("blocked_queues", {})
                ),
                f"schedule_priority={queue['allocator']['schedule_priority']}",
                *([f"host_priorities={format_host_priorities(queue['allocator']['host_priorities'])}"]
                  if queue["allocator"]["host_priorities"] else []),
                f"max_workers={queue['allocator']['max_workers']}",
                f"initial_memory_mib_per_job={queue['initial_memory_mib_per_job']}",
                f"requested_memory_mib_per_job={queue['requested_memory_mib_per_job']}",
                f"effective_memory_mib_per_job={queue['allocator']['memory_mib_per_job']}",
                f"memory_calibration_state={calibration.get('status', 'off')}",
                f"memory_calibration_reason={calibration.get('reason', 'disabled')}",
                f"memory_calibration_sample_jobs={calibration.get('sample_jobs', 0)}",
                f"memory_calibration_candidate_mib={calibration.get('candidate_mib', '')}",
                "share_interrupted_jobs=" + str(len(unconfirmed_interruptions(
                    Path(queue["registration"]["batch_dir"])))),
                f"control_generation={generation}",
                f"propagation={'complete' if complete else 'pending'}",
                f"running_workers={running}", f"target_workers={target}",
                "running_by_host=" + ",".join(
                    f"{host_id}:{polled[host_id]['reservations'].get(queue_id, 0)}"
                    for host_id in sorted(enabled_ids)
                ),
                f"updated_epoch={now}", "",
            ])
            batch_dir = Path(queue["registration"]["batch_dir"])
            try:
                _atomic_text(batch_dir / "adaptive" / "status.env", status_text, 0o666)
                if (
                    lifecycle in {"COMPLETED", "FINISHED_INCOMPLETE"}
                    and queue_zero_acknowledged[queue_id]
                    and complete
                ):
                    self._queue_transition(queue)
            except (OSError, SnapshotError) as error:
                problems[queue_id] = f"cannot publish queue status or cleanup {queue_id}: {error}"

        # Reporting is part of queue validation. Do it before leases so a batch
        # with broken status/completion paths never receives another grant.
        for queue_id in problems.keys() & queues.keys():
            del queues[queue_id]
            for host_id in self.hosts:
                plan["targets"][host_id][queue_id] = 0
                start_grants[host_id][queue_id] = 0
        self._record_quarantine(problems)
        publish_ids = [
            host_id for host_id in enabled_ids
            if polled[host_id]["reachable"] and not polled[host_id]["uncertain"]
            and (
                states[host_id]["pending"]
                or plan_acknowledged[host_id]
                or (
                    int(polled[host_id]["generation"]) < int(states[host_id]["generation"])
                    and polled[host_id]["admission"] == "drain-only"
                )
            )
        ]
        with ThreadPoolExecutor(max_workers=max(1, min(8, len(publish_ids)))) as executor:
            futures = {
                host_id: executor.submit(
                    self._publish, host_id, plan["targets"][host_id],
                    start_grants[host_id], queues,
                    polled[host_id], states[host_id],
                )
                for host_id in publish_ids
            }
            for host_id, future in futures.items():
                try:
                    states[host_id] = future.result()
                except SnapshotError as error:
                    print(f"adaptive coordinator: publication to {host_id} failed: {error}", file=sys.stderr)

        controls_acknowledged = all(
            control_applied[host_id][queue_id]
            for host_id in enabled_ids for queue_id in queues
        )
        drain_complete = (
            pool["state"] == "DRAINING" and prior_zero_acknowledged
            and controls_acknowledged and no_reservations
        )

        available_hosts = sum(
            host["enabled"] and live_capacities[host_id][0] > 0
            and live_capacities[host_id][1] > 0
            for host_id, host in self.hosts.items()
        )
        resource_state = (
            "ok" if available_hosts == len(enabled_ids)
            else "unavailable" if available_hosts == 0 else "degraded"
        )
        unavailable = {
            host_id: "unreachable" if not polled[host_id]["reachable"]
            else "uncertain" if polled[host_id]["uncertain"] else "telemetry_or_share"
            for host_id in enabled_ids
            if host_id not in self.away and host_id not in open_hosts
        }
        self._write_ready(
            "ready", resource_state,
            sum(live_capacities[host_id][0] for host_id in enabled_ids),
            sum(live_capacities[host_id][1] for host_id in enabled_ids),
            io_pressure_state, smbd_cpu_tenths, unavailable,
        )
        if drain_complete:
            drained = "\n".join([
                "schema_version=1", f"pool_id={self.pool_id}",
                f"drained_epoch={now}", "",
            ])
            _atomic_text(self.state_root / "drained.env", drained, 0o644)
        return drain_complete

    def serve(self, once: bool) -> None:
        lock = self.state_root / "coordinator.lock"
        try:
            lock.mkdir(parents=True)
        except FileExistsError as error:
            raise SnapshotError(f"coordinator lock already exists: {lock}") from error
        _atomic_text(lock / "owner.env", f"pool_id={self.pool_id}\npid={os.getpid()}\n", 0o644)
        stop = threading.Event()
        previous_handlers: dict[int, Any] = {}
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, lambda _signal, _frame: stop.set())
        try:
            while not stop.is_set() and self._pool_state() == "STARTING":
                self._write_ready("waiting")
                stop.wait(min(1.0, self.tick_seconds))
            while not stop.is_set():
                started = time.monotonic()
                drained = self.tick()
                if once or drained:
                    break
                stop.wait(max(0.0, self.next_tick_seconds - (time.monotonic() - started)))
        finally:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
            try:
                (lock / "owner.env").unlink()
                lock.rmdir()
            except OSError:
                pass


def project_summary(state_root: Path, pool_id: str, project: str, now: Optional[float] = None) -> str:
    """Read registered queue metadata only; never scan jobs or change controls."""
    if not project or project == "?":
        raise SnapshotError("no project label; submit with --project <source-repo>")
    now = time.time() if now is None else now
    pool = _read_env(state_root / "pool.env", {"pool_id"})
    if pool["pool_id"] != pool_id:
        raise SnapshotError("pool identity changed")
    counts = dict.fromkeys(("running", "paused", "waiting", "closing", "unknown"), 0)
    workers = unavailable = 0
    for path in sorted((state_root / "queues").glob("*.env")):
        # Retirement can remove a registration between listing and reading it.
        if not path.exists():
            continue
        registration = _read_env(path, {"pool_id", "queue_id", "batch_dir"})
        if registration["pool_id"] != pool_id:
            continue
        batch = Path(registration["batch_dir"])
        meta = _read_env(batch / "run_meta.env", set())
        if meta.get("project_name") != project:
            continue
        control = _read_env(batch / "adaptive" / "control.env", {"queue_id", "admission"})
        if control["queue_id"] != registration["queue_id"]:
            raise SnapshotError(f"queue identity mismatch: {batch}")
        admission = control["admission"]
        if admission not in {"enabled", "paused", "closed"}:
            raise SnapshotError(f"invalid admission: {batch}")
        running = None
        try:
            status = _read_env(batch / "adaptive" / "status.env",
                               {"queue_id", "running_workers", "updated_epoch"})
            if status["queue_id"] != registration["queue_id"]:
                raise SnapshotError("status identity mismatch")
            age = now - _env_int(status, "updated_epoch", 0)
            if not 0 <= age <= 120:
                raise SnapshotError("status stale")
            running = _env_int(status, "running_workers", 0)
        except SnapshotError:
            unavailable += 1
        if running is not None:
            workers += running
        state = ("paused" if admission == "paused" else "closing" if admission == "closed"
                 else "unknown" if running is None else "running" if running else "waiting")
        counts[state] += 1
    details = ", ".join(f"{count} {state}" for state, count in counts.items() if count)
    worker_text = f"{workers} workers" if not unavailable else (
        f"{workers} known workers; worker status unavailable/stale for {unavailable} queues")
    return f"Project {project}: {sum(counts.values())} registered queues ({details or 'none'}); {worker_text}."


def main(argv: Optional[list[str]] = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if arguments[:1] == ["project-summary"]:
        parser = argparse.ArgumentParser(description="Summarize registered queues for one project")
        parser.add_argument("command", choices=["project-summary"])
        parser.add_argument("--state-root", type=Path, required=True)
        parser.add_argument("--pool-id", required=True)
        parser.add_argument("--project", required=True)
        args = parser.parse_args(arguments)
        try:
            print(project_summary(args.state_root, args.pool_id, args.project))
        except (OSError, SnapshotError) as error:
            print(f"Project summary unavailable: {error}", file=sys.stderr)
            return 2
        return 0
    if arguments[:1] == ["priority-spec"]:
        parser = argparse.ArgumentParser(description="Validate a per-host schedule priority SPEC")
        parser.add_argument("command", choices=["priority-spec"])
        parser.add_argument("--resources-file", type=Path, required=True)
        # Pass as --spec=VALUE: a SPEC may begin with a negative default.
        parser.add_argument("--spec", required=True)
        args = parser.parse_args(arguments)
        try:
            default, overrides = resolve_priority_spec(args.spec, set(_load_hosts(args.resources_file)))
        except SnapshotError as error:
            print(f"adaptive coordinator: {error}", file=sys.stderr)
            return 2
        print(f"{default}\t{format_host_priorities(overrides)}")
        return 0
    if arguments[:1] == ["set-host-state"]:
        parser = argparse.ArgumentParser(description="Declare a pool host away (no new work) or present")
        parser.add_argument("command", choices=["set-host-state"])
        parser.add_argument("--state-root", type=Path, required=True)
        parser.add_argument("--pool-id", required=True)
        parser.add_argument("--host-id", required=True)
        parser.add_argument("--state", choices=["away", "present"], required=True)
        parser.add_argument("--at")
        parser.add_argument("--until")
        parser.add_argument("--reason", default="")
        args = parser.parse_args(arguments)
        try:
            set_host_state(args.state_root, args.pool_id, args.host_id, args.state,
                           args.at, args.until, args.reason)
        except (OSError, SnapshotError) as error:
            print(f"adaptive coordinator: {error}", file=sys.stderr)
            return 2
        return 0
    if arguments[:1] in (["set-host-budget"], ["profile-fits"]):
        parser = argparse.ArgumentParser(description="Update an active pool host budget")
        parser.add_argument("command", choices=["set-host-budget", "profile-fits"])
        parser.add_argument("--state-root", type=Path, required=True)
        parser.add_argument("--pool-id", required=True)
        parser.add_argument("--host-id")
        parser.add_argument("--cpu-units", type=int, required=True)
        parser.add_argument("--memory-mib", type=int, required=True)
        args = parser.parse_args(arguments)
        try:
            if args.cpu_units <= 0 or args.memory_mib <= 0:
                raise SnapshotError("CPU and memory must be positive")
            if args.command == "set-host-budget":
                set_host_budget(args.state_root, args.pool_id, args.host_id, args.cpu_units, args.memory_mib)
            else:
                pool = _read_env(args.state_root / "pool.env", {"pool_id", "resources_file"})
                if pool["pool_id"] != args.pool_id:
                    raise SnapshotError("pool identity changed")
                hosts = _load_hosts(Path(pool["resources_file"]))
                budgets = _host_budgets(args.state_root, args.pool_id, hosts)
                effective = [{**host, **budgets["hosts"].get(host_id, {})} for host_id, host in hosts.items()]
                if not any(h["enabled"] and h["cpu_units"] >= args.cpu_units
                           and h["memory_mib"] >= args.memory_mib for h in effective):
                    raise SnapshotError("this job profile does not fit any enabled adaptive host")
        except (OSError, SnapshotError) as error:
            print(f"adaptive coordinator: {error}", file=sys.stderr)
            return 2
        return 0
    if arguments[:1] == ["serve"]:
        parser = argparse.ArgumentParser(description="Run the adaptive coordinator service")
        parser.add_argument("serve")
        parser.add_argument("--state-root", type=Path, required=True)
        parser.add_argument("--pool-id", required=True)
        parser.add_argument("--resources-file", type=Path, required=True)
        parser.add_argument("--shared-io-capacity", type=int, required=True)
        parser.add_argument("--tick-seconds", type=float, required=True)
        parser.add_argument("--lease-seconds", type=int, required=True)
        parser.add_argument("--io-pressure-host")
        parser.add_argument("--smbd-cpu-limit-pct", type=int, default=400)
        parser.add_argument("--cpu-tolerance-pct", type=int, default=10)
        parser.add_argument("--start-interval-seconds", type=float, default=4)
        parser.add_argument("--once", action="store_true")
        args = parser.parse_args(arguments)
        try:
            Coordinator(
                args.state_root, args.pool_id, args.resources_file,
                args.shared_io_capacity, args.tick_seconds, args.lease_seconds,
                args.io_pressure_host or None, args.smbd_cpu_limit_pct, args.cpu_tolerance_pct,
                args.start_interval_seconds,
            ).serve(args.once)
        except SnapshotError as error:
            print(f"adaptive coordinator: {error}", file=sys.stderr)
            return 2
        return 0

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--simulate", type=Path, required=True, metavar="SNAPSHOT.json")
    args = parser.parse_args(arguments)
    try:
        with args.simulate.open(encoding="utf-8") as handle:
            snapshot = json.load(handle)
        result = allocate(snapshot)
    except (OSError, json.JSONDecodeError, SnapshotError) as error:
        print(f"adaptive coordinator: {error}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
