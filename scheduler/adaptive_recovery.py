#!/usr/bin/env python3
"""Audited node-local failure resets and uncertain-reservation resolution."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import time
import uuid

# Clock corrections after boot must not make a post-boot reservation look older.
BOOT_MARGIN_SECONDS = 60


class RecoveryError(ValueError):
    pass


def read_env(path: Path) -> dict[str, str]:
    if path.is_symlink() or not path.is_file():
        raise RecoveryError(f"missing or unsafe state file: {path}")
    result: dict[str, str] = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        if not separator or not key or key in result:
            raise RecoveryError(f"malformed state file: {path}")
        result[key] = value
    return result


def identifier(value: str) -> str:
    if value in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise RecoveryError("invalid recovery identifier")
    return value


def publish(path: Path, values: dict) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}")
    try:
        with temporary.open("x") as output:
            os.chmod(temporary, 0o600)
            output.write(json.dumps(values, sort_keys=True) + "\n" if path.suffix == ".json"
                         else "".join(f"{key}={value}\n" for key, value in values.items()))
            output.flush()
            os.fsync(output.fileno())
        # Never replace an existing result or pending operator request.
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def process_snapshot() -> str:
    return subprocess.check_output(
        ["ps", "-ww", "-axo", "pid=,ppid=,pgid=,stat=,command="], text=True,
    )


def boot_epoch() -> int:
    if platform.system() == "Darwin":
        text = subprocess.check_output(["sysctl", "-n", "kern.boottime"], text=True)
        match = re.search(r"\bsec = ([0-9]+)", text)
    else:
        match = re.search(r"^btime ([0-9]+)$", Path("/proc/stat").read_text(), re.MULTILINE)
    if match is None or int(match.group(1)) <= 0:
        raise RecoveryError("cannot read the host boot time")
    return int(match.group(1))


def other_processes(text: str):
    """Yield live (pid, fields) rows outside this helper's own process ancestry."""
    rows = {}
    for line in text.splitlines():
        fields = line.split(None, 4)
        if len(fields) != 5 or any(not item.isdigit() for item in fields[:3]):
            raise RecoveryError("malformed process audit")
        rows[int(fields[0])] = fields
    excluded = set()
    ancestor = os.getpid()
    while ancestor in rows and ancestor not in excluded:
        excluded.add(ancestor)
        ancestor = int(rows[ancestor][1])
    if os.getpid() not in excluded:
        raise RecoveryError("recovery process is missing from the process audit")
    for process_id, fields in rows.items():
        if process_id not in excluded and "Z" not in fields[3]:
            yield process_id, fields


def audit_processes(text: str, reservation_id: str, pid: str, tag: str) -> None:
    for process_id, fields in other_processes(text):
        tokens = fields[4].split()
        if (
            pid and (str(process_id) == pid or fields[2] == pid)
            or reservation_id in tokens or tag in tokens
        ):
            raise RecoveryError(f"worker, process group, or tagged batch process remains: {process_id}")


def audit_rebooted(text: str, state: Path, reservation_ids: list[str]) -> None:
    # PIDs recorded before the boot may now belong to unrelated processes, so
    # only an agent or a worker carrying a reservation identity is refused.
    agent = str(state / "bin" / "mmadaptiveagent.sh")
    for process_id, fields in other_processes(text):
        tokens = fields[4].split()
        if agent in tokens or any(reservation_id in tokens for reservation_id in reservation_ids):
            raise RecoveryError(f"adaptive agent or reservation worker is running: {process_id}")


def resolve_rebooted(state: Path, pool: str, host: str, evidence: dict[str, str]) -> dict[str, str]:
    booted = boot_epoch()
    lost = {}
    for path in sorted((state / "reservations").glob("*.env")):
        reservation = read_env(path)
        reservation_id = identifier(reservation.get("reservation_id", ""))
        if (
            path.name != f"{reservation_id}.env"
            or not re.fullmatch(re.escape(f"{pool}.{host}.") + r"[1-9][0-9]*", reservation_id)
            or any(reservation.get(key) != value for key, value in
                   {"schema_version": "1", "pool_id": pool, "host_id": host}.items())
            or reservation.get("state") not in {"starting", "running", "uncertain"}
            or not reservation.get("started_epoch", "").isdigit()
        ):
            raise RecoveryError(f"invalid reservation record: {path}")
        # A result is the agent's to record; a reservation from this boot may still run.
        if (not (state / "results" / f"{reservation_id}.env").exists()
                and int(reservation["started_epoch"]) < booted - BOOT_MARGIN_SECONDS):
            lost[reservation_id] = reservation
    first = process_snapshot()
    audit_rebooted(first, state, list(lost))
    if not lost:
        return {"rebooted_resolved": "0", "boot_epoch": str(booted)}
    archive = state / "audit" / "resolutions" / evidence["token"]
    archive.mkdir(parents=True, mode=0o700)
    publish(archive / "operator.env", {**evidence, "boot_epoch": str(booted),
                                       "reservation_ids": ",".join(lost)})
    (archive / "processes-before.txt").write_text(first)
    second = process_snapshot()
    audit_rebooted(second, state, list(lost))
    (archive / "processes-final.txt").write_text(second)
    for reservation_id, reservation in lost.items():
        if read_env(state / "reservations" / f"{reservation_id}.env") != reservation:
            raise RecoveryError(f"reservation changed during the audit: {reservation_id}")
        result = {
            "reservation_id": reservation_id, "claimed_job_id": "",
            "outcome": "host_rebooted", "job_exit_code": "",
        }
        publish(archive / f"reservation-{reservation_id}.env", reservation)
        publish(state / "results" / f"{reservation_id}.env", result)
    return {"rebooted_resolved": str(len(lost)), "boot_epoch": str(booted),
            "audit_dir": str(archive)}


def recover(state: Path, request: dict[str, str]) -> dict[str, str]:
    action = request["action"]
    if action not in {"reset-failures", "resolve-uncertain", "resolve-rebooted"}:
        raise RecoveryError("unknown recovery action")
    verify_lost = request.get("verify_lost_job", "0")
    if verify_lost not in {"0", "1"} or (verify_lost == "1" and action != "resolve-uncertain"):
        raise RecoveryError("invalid verified-loss request")
    pool = identifier(request["pool_id"])
    host = identifier(request["host_id"])
    reason = request["reason"]
    if not reason.strip() or any(character in reason for character in "\r\n\t"):
        raise RecoveryError("a single-line audit reason is required")
    agent = read_env(state / "agent.env")
    if agent["pool_id"] != pool or agent["host_id"] != host:
        raise RecoveryError("agent identity changed")
    # A reboot proof covers every reservation on the host, not one queue.
    queue = None if action == "resolve-rebooted" else identifier(request["queue_id"])
    if queue is not None:
        batch = Path(request["batch_dir"])
        meta = read_env(batch / "run_meta.env")
        if (
            meta["pool_id"] != pool or meta["run_token"] != queue
            or meta["execution_mode"] != "adaptive-shared-queue"
        ):
            raise RecoveryError("batch identity changed")
    token = uuid.uuid4().hex
    evidence = {
        "schema_version": "1", "pool_id": pool, "host_id": host,
        **({} if queue is None else {"queue_id": queue}), "token": token, "reason": reason,
        "operator_host": socket.gethostname(), "created_epoch": str(int(time.time())),
    }
    lock = state / "recovery.lock"
    lock.mkdir()
    try:
        publish(lock / "owner.env", {**evidence, "pid": str(os.getpid()), "action": action})
        if action == "resolve-rebooted":
            return resolve_rebooted(state, pool, host, evidence)
        if action == "reset-failures":
            destination = state / "failure-reset-requests" / f"{queue}.env"
            destination.parent.mkdir(exist_ok=True)
            publish(destination, evidence)
            return {"reset_requested": "1", "token": token, "host_id": host, "queue_id": queue}

        reservation_id = identifier(request["reservation_id"])
        if not re.fullmatch(re.escape(f"{pool}.{host}.") + r"[1-9][0-9]*", reservation_id):
            raise RecoveryError("reservation identity does not match the requested host and pool")
        reservation_file = state / "reservations" / f"{reservation_id}.env"
        reservation = read_env(reservation_file)
        if any(reservation[key] != value for key, value in {
            "schema_version": "1", "pool_id": pool, "host_id": host,
            "queue_id": queue, "reservation_id": reservation_id, "state": "uncertain",
        }.items()):
            raise RecoveryError("only the expected uncertain reservation can be resolved")
        pid = reservation["pid"]
        if pid and (not pid.isdigit() or int(pid) < 1):
            raise RecoveryError("invalid reservation PID")
        queue_file = Path(meta["editable_queue_file"])
        if hashlib.sha256(queue_file.read_bytes()).hexdigest() != meta["editable_queue_sha256"]:
            raise RecoveryError("immutable queue hash changed")
        tag = meta["tag_clean"]
        if not re.fullmatch(r"[A-Za-z0-9_.:-]+", tag):
            raise RecoveryError("invalid batch tag")
        owner_path = None
        owner = None
        if verify_lost == "1":
            resources_file = batch / "adaptive/resources.env"
            if hashlib.sha256(resources_file.read_bytes()).hexdigest() != meta["resources_sha256"]:
                raise RecoveryError("immutable resource hash changed")
            resources = read_env(resources_file)
            if resources.get("schema_version") != "2" or resources.get("queue_id") != queue or resources.get("retry_lost_jobs") != "1":
                raise RecoveryError("queue did not opt in to lost-job retries")
            for candidate in (batch / "claims").glob("job_*.claim/owner.json"):
                if candidate.is_symlink() or candidate.parent.is_symlink():
                    raise RecoveryError("unsafe claim ownership record")
                record = json.loads(candidate.read_text())
                if record.get("reservation_id") == reservation_id:
                    if owner_path is not None:
                        raise RecoveryError("reservation owns multiple claims")
                    owner_path, owner = candidate, record
            job_ids = {line.split(None, 1)[0] for line in queue_file.read_text().split("\n") if line.strip()}
            if (owner is None or owner.get("job_id") not in job_ids
                    or owner.get("queue_sha256") != meta["editable_queue_sha256"]
                    or owner_path.parent.name != f"job_{owner['job_id']}.claim"):
                raise RecoveryError("no matching durable claim ownership for lost reservation")
            if (batch / "done" / f"job_{owner['job_id']}.done").exists():
                raise RecoveryError("job already completed; resolve without --verify_lost_job")
            if (batch / "adaptive/lost-retries" / f"job_{owner['job_id']}.claim").exists():
                raise RecoveryError("lost-job retry already used; resolve without --verify_lost_job")
        first = process_snapshot()
        audit_processes(first, reservation_id, pid, tag)
        archive = state / "audit" / "resolutions" / token
        archive.mkdir(parents=True, mode=0o700)
        publish(archive / "operator.env", {**evidence, "reservation_id": reservation_id})
        publish(archive / "reservation.env", reservation)
        (archive / "processes-before.txt").write_text(first)
        second = process_snapshot()
        audit_processes(second, reservation_id, pid, tag)
        (archive / "processes-final.txt").write_text(second)
        if read_env(reservation_file) != reservation:
            raise RecoveryError("reservation changed during the audit")
        if owner_path is not None:
            if json.loads(owner_path.read_text()) != owner:
                raise RecoveryError("claim ownership changed during the audit")
            if (state / "results" / f"{reservation_id}.env").exists():
                raise RecoveryError("reservation already has a result; audit it instead of declaring a loss")
            if (batch / "done" / f"job_{owner['job_id']}.done").exists():
                raise RecoveryError("job completed during the audit")
            # Publish shared evidence BEFORE releasing the local reservation.
            # An interrupted publication leaves the uncertain reservation charged.
            receipt = {**evidence, "reservation_id": reservation_id, "owner": owner,
                       "audit_dir": str(archive)}
            destination = batch / "adaptive/verified-losses" / f"job_{owner['job_id']}.json"
            destination.parent.mkdir(exist_ok=True)
            if destination.exists():
                previous = json.loads(destination.read_text())
                if any(previous.get(key) != receipt[key] for key in ("pool_id", "host_id", "queue_id", "reservation_id", "owner")):
                    raise RecoveryError("conflicting verified-loss receipt")
            else:
                publish(destination, receipt)
        result = {
            "reservation_id": reservation_id, "claimed_job_id": "",
            "outcome": "infrastructure_failed", "job_exit_code": "",
        }
        publish(archive / "result.env", result)
        publish(state / "results" / f"{reservation_id}.env", result)
        return {"resolution_published": "1", "reservation_id": reservation_id, "audit_dir": str(archive)}
    finally:
        (lock / "owner.env").unlink(missing_ok=True)
        lock.rmdir()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", required=True, type=Path)
    args = parser.parse_args()
    try:
        request = json.load(sys.stdin)
        print(json.dumps(recover(args.state_dir, request)))
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(f"adaptive recovery: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
