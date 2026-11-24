#!/usr/bin/env python3
"""Derive low-frequency scheduler rates from QUEUE_PROGRESS.csv."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import tempfile
import time


def count_claims(batch_dir: Path) -> int:
    shard_dir = batch_dir / "shards"
    shard_started = 0
    shard_records = 0
    try:
        for progress_path in shard_dir.glob("host_*.env"):
            values: dict[str, str] = {}
            try:
                for line in progress_path.read_text(encoding="utf-8").splitlines():
                    key, separator, value = line.partition("=")
                    if separator:
                        values[key] = value
                shard_started += int(values.get("started_jobs", "0"))
                shard_records += 1
            except (OSError, ValueError):
                continue
    except OSError:
        pass
    if shard_records:
        return shard_started

    claims_dir = batch_dir / "claims"
    try:
        return sum(
            entry.is_dir(follow_symlinks=False) and entry.name.endswith(".claim")
            for entry in os.scandir(claims_dir)
        )
    except OSError:
        return 0


def read_new_log_events(path: Path, previous: dict[str, int] | None) -> tuple[dict[str, int], dict[str, int]]:
    try:
        stat_result = path.stat()
    except OSError:
        return {"inode": 0, "offset": 0}, {"rewrite": 0, "duplicate": 0, "stale": 0}

    inode = stat_result.st_ino
    size = stat_result.st_size
    offset = size
    if previous and previous.get("inode") == inode and 0 <= previous.get("offset", -1) <= size:
        offset = previous["offset"]

    events = {"rewrite": 0, "duplicate": 0, "stale": 0}
    if offset < size:
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                handle.seek(offset)
                for line in handle:
                    if "(attempt " in line and "Failed to " in line:
                        events["rewrite"] += 1
                    if "Skipping already-claimed job " in line:
                        events["duplicate"] += 1
                    if "Removed stale queue lock " in line:
                        events["stale"] += 1
        except OSError:
            pass

    return {"inode": inode, "offset": size}, events


def atomic_json_write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as handle:
        json.dump(value, handle, separators=(",", ":"))
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def atomic_lines_write(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as handle:
        for line in lines:
            handle.write(f"{line}\n")
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--lock-paths", required=True, type=Path)
    args = parser.parse_args()

    try:
        with args.registry.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    except (OSError, csv.Error):
        return 1

    if not rows:
        return 1

    try:
        previous = json.loads(args.state.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        previous = {}

    sample_epoch = max(int(row.get("update_epoch") or 0) for row in rows)
    previous_epoch = int(previous.get("sample_epoch", 0))
    elapsed = sample_epoch - previous_epoch if previous_epoch > 0 else 0
    previous_batches = previous.get("batches", {})
    previous_logs = previous.get("logs", {})

    batches: dict[str, dict[str, int]] = {}
    logs: dict[str, dict[str, int]] = {}
    lock_paths: set[str] = set()
    claimed_delta = 0
    done_delta = 0
    event_delta = {"rewrite": 0, "duplicate": 0, "stale": 0}
    active_queues = 0

    for row in rows:
        batch_text = row.get("batch_dir", "")
        if not batch_text:
            continue
        batch_dir = Path(batch_text)
        state = row.get("state", "")
        claimed = count_claims(batch_dir)
        try:
            done = int(row.get("done_jobs") or 0)
            start_epoch = int(row.get("start_epoch") or 0)
        except ValueError:
            done = 0
            start_epoch = 0
        batches[batch_text] = {"claimed": claimed, "done": done}

        old = previous_batches.get(batch_text)
        if elapsed > 0:
            if old:
                claimed_delta += max(0, claimed - int(old.get("claimed", 0)))
                done_delta += max(0, done - int(old.get("done", 0)))
            elif start_epoch >= previous_epoch:
                claimed_delta += claimed
                done_delta += done

        active = state in {"PENDING", "RUNNING", "DRAINING"} or (
            state == "UNKNOWN" and row.get("priority") == "0"
        )
        if not active and state != "UNKNOWN":
            continue
        if active:
            active_queues += 1

        queue_file = Path(row.get("queue_file", "queue.txt")).name
        lock_name = queue_file[:-4] if queue_file.endswith(".txt") else queue_file
        project_dir = batch_dir.parent.parent
        lock_paths.add(str(project_dir / "lock" / f"{lock_name}.lock"))

        try:
            worker_logs = list(batch_dir.glob("worker_*.log"))
        except OSError:
            worker_logs = []
        for log_path in worker_logs:
            log_key = str(log_path)
            log_state, events = read_new_log_events(log_path, previous_logs.get(log_key))
            logs[log_key] = log_state
            if elapsed > 0:
                for event_name, count in events.items():
                    event_delta[event_name] += count

    rate_factor = 60.0 / elapsed if elapsed > 0 else None

    def rate_text(count: int) -> str:
        return "n/a" if rate_factor is None else f"{count * rate_factor:.1f}"

    atomic_lines_write(args.lock_paths, sorted(lock_paths))
    atomic_json_write(
        args.state,
        {
            "sample_epoch": sample_epoch,
            "batches": batches,
            "logs": logs,
        },
    )

    values = {
        "sample_epoch": str(sample_epoch),
        "registry_age_sec": str(max(0, int(time.time()) - sample_epoch)),
        "active_queues": str(active_queues),
        "claimed_per_min": rate_text(claimed_delta),
        "completed_per_min": rate_text(done_delta),
        "rewrite_retries_per_min": rate_text(event_delta["rewrite"]),
        "duplicate_retries_per_min": rate_text(event_delta["duplicate"]),
        "stale_locks_per_min": rate_text(event_delta["stale"]),
    }
    for key, value in values.items():
        print(f"{key}\t{value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
