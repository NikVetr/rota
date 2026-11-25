#!/usr/bin/env python3
"""Change a live pool's I/O allowance via recovery with unchanged scheduler code.

Run on the coordinator host. Defaults to a read-only preflight. Running workers
and queue controls are preserved; only the coordinator receives SIGTERM.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import time


def run(args: list[str], timeout: int = 30) -> str:
    return subprocess.check_output(args, text=True, timeout=timeout)


def record(text: str) -> dict[str, str]:
    result = {}
    for line in text.splitlines():
        key, value = line.split("=", 1)
        if key in result:
            raise RuntimeError(f"duplicate record key: {key}")
        result[key] = value
    return result


def command(pid: int) -> str:
    result = subprocess.run(["ps", "-ww", "-p", str(pid), "-o", "command="],
                            text=True, capture_output=True, check=False)
    if result.returncode not in (0, 1):
        raise RuntimeError(f"cannot inspect PID {pid}: {result.stderr}")
    return result.stdout.strip()


def validate(root: Path, pool_id: str, previous: int, capacity: int) -> dict[str, str]:
    pool = record((root / ".rota/adaptive/pool.env").read_text())
    if pool["pool_id"] != pool_id or pool["state"] != "ACTIVE":
        raise RuntimeError("expected ACTIVE pool is required; preserve pause/drain controls")
    if min(previous, capacity) < 1 or int(pool["shared_io_capacity"]) != previous:
        raise RuntimeError("positive capacities and matching previous allowance are required")
    if not pool["io_pressure_host"] or int(pool["smbd_cpu_limit_pct"]) < 1:
        raise RuntimeError("a configured SMB pressure guard is required")
    paths = {"resources": Path(pool["resources_file"]), **{
        key: root / name for key, name in {
            "daemon": "mmhostdaemon.sh", "worker": "mmworker.sh",
            "claim": "adaptive_claim.py", "memory": "adaptive_memory.py",
            "calibration": "adaptive_calibration.py", "recovery": "adaptive_recovery.py",
            "coordinator": "adaptive_coordinator.py",
        }.items()
    }}
    for key, path in paths.items():
        if hashlib.sha256(path.read_bytes()).hexdigest() != pool[key + "_sha256"]:
            raise RuntimeError(f"recorded {key} differs; this tool cannot upgrade code")
    pid = int(pool["coordinator_pid"])
    words = shlex.split(command(pid))
    if (str(paths["coordinator"]) not in words or "--pool-id" not in words
            or words[words.index("--pool-id") + 1] != pool_id):
        raise RuntimeError("local coordinator PID identity mismatch")
    owner = record((root / ".rota/adaptive/coordinator.lock/owner.env").read_text())
    if owner != {"pool_id": pool_id, "pid": str(pid)}:
        raise RuntimeError("coordinator lock identity mismatch")
    return pool


def adjust(root: Path, pool_id: str, previous: int, capacity: int, apply: bool) -> None:
    root = root.absolute()
    state = root / ".rota/adaptive"
    # Child scheduler calls must see --root, not whatever root the environment names.
    cli = ["/usr/bin/env", f"ROTA_ROOT={root}", str(root / "adaptive_scheduler.sh")]
    pool = validate(root, pool_id, previous, capacity)
    health = record(run([*cli, "pool-status"]))
    if any(health.get(k) != v for k, v in {
        "pool_id": pool_id, "coordinator_ready": "1", "resource_state": "ok",
        "io_pressure_state": "ok",
    }.items()):
        raise RuntimeError("pool/telemetry is unhealthy; investigate before changing capacity")
    with Path(pool["resources_file"]).open() as stream:
        hosts = list(csv.DictReader(stream, delimiter="\t"))
    for host in hosts:
        if host["enabled"] == "1":
            run(["ssh", "-n", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
                 host["ssh_target"], "true"])
    print(f"Preflight passed: {pool_id}, I/O {previous} -> {capacity}", flush=True)
    if not apply or previous == capacity:
        return

    lock = state.parent / "mode-transition.lock"
    lock.mkdir()  # Never reclaim another operator's lock.
    try:
        (lock / "owner.env").write_text(
            f"pid={os.getpid()}\nhost={socket.gethostname()}\n"
            f"command=adjust-io-capacity\ncreated_epoch={int(time.time())}\n")
        pool = validate(root, pool_id, previous, capacity)
        marker = state / "pool.env"
        original = marker.read_text()
        audit = state / "audit" / f"io-capacity-{time.time_ns()}"
        audit.mkdir(parents=True)
        (audit / "pool-before.env").write_text(original)
        (audit / "change.json").write_text(json.dumps({
            "pool_id": pool_id, "previous": previous, "capacity": capacity,
            "epoch": time.time(), "phase": "prepared",
        }) + "\n")
        pid = int(pool["coordinator_pid"])
        print(f"Stopping coordinator PID {pid}; running jobs remain intact", flush=True)
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 60
        while command(pid):
            if time.monotonic() >= deadline:
                raise RuntimeError("coordinator did not exit; allowance unchanged; inspect before recovery")
            time.sleep(0.5)
        if marker.read_text() != original:
            raise RuntimeError("pool marker changed; refusing to overwrite operator changes")
        updated = original.replace(f"shared_io_capacity={previous}\n",
                                   f"shared_io_capacity={capacity}\n")
        if record(updated)["shared_io_capacity"] != str(capacity):
            raise RuntimeError("could not update capacity record")
        temporary = audit / "pool-updated.env"
        temporary.write_text(updated)
        temporary.chmod(marker.stat().st_mode & 0o777)
        temporary.replace(marker)
    finally:
        (lock / "owner.env").unlink(missing_ok=True)
        lock.rmdir()

    # Recovery rechecks identity and all binary hashes under the transition lock.
    # Its host daemons retain existing reservations and do not terminate workers.
    recovery = [*cli, "pool-start", "--recover-inactive", "--expected-pool-id", pool_id]
    try:
        output = run(recovery, timeout=240)
        (audit / "recovery.log").write_text(output)
        health = record(run([*cli, "pool-status"]))
        if any(health.get(k) != v for k, v in {
            "pool_id": pool_id, "state": "ACTIVE", "coordinator_ready": "1",
            "shared_io_capacity": str(capacity),
            "io_pressure_host": pool["io_pressure_host"],
            "smbd_cpu_limit_pct": pool["smbd_cpu_limit_pct"],
        }.items()):
            raise RuntimeError("recovered pool status differs; inspect the audit")
        (audit / "pool-after.json").write_text(json.dumps(health, indent=2) + "\n")
    except Exception:
        print(f"Recovery needs inspection. Audit: {audit}\nCommand: {shlex.join(recovery)}", flush=True)
        raise
    print(f"Updated I/O capacity to {capacity}; pool identity and SMB guard preserved. Audit: {audit}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=os.environ.get("ROTA_ROOT"), required=not os.environ.get("ROTA_ROOT"))
    parser.add_argument("--expected-pool-id", required=True)
    parser.add_argument("--expected-capacity", type=int, required=True)
    parser.add_argument("--capacity", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    adjust(args.root, args.expected_pool_id, args.expected_capacity, args.capacity, args.apply)
