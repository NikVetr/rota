#!/usr/bin/env python3
"""Estimate remaining queue time at its current concurrency on the storage host."""

import argparse
from collections import defaultdict
from dataclasses import dataclass, field
import json
import os
import random
from pathlib import Path
from statistics import mean, median
from typing import Optional

from scheduler_snapshot import optional_env, read_env, reservation_host


SAMPLE_SIZE = 128
RECENT_CLAIMS = 32
MIN_GAP_PAIRS = 5
MAX_GAP_SECONDS = 30


def finish_time(residuals: list[float], pending: int, typical: float) -> float:
    """Give each waiting job to the next free slot, assuming a typical runtime."""
    floor_time = max(max(residuals), (sum(residuals) + pending * typical) / len(residuals))
    # At this lower bound each slot has finished whole jobs. Any jobs still
    # missing must finish in the next wave, even for a million-job backlog.
    whole_jobs = [int((floor_time - r) / typical) for r in residuals]
    missing = pending - sum(whole_jobs)
    if missing <= 0:
        return floor_time
    next_finishes = sorted(r + (n + 1) * typical for r, n in zip(residuals, whole_jobs))
    return next_finishes[missing - 1]


@dataclass
class JobTimes:
    durations: list[float]
    by_host: dict[str, list[float]]
    active: list[str]
    claimed: int
    done: int
    dispatch: dict[str, tuple[float, int]] = field(default_factory=dict)
    recent: list[tuple[float, Optional[float]]] = field(default_factory=list)

    def warning(self, now, active_count, oldest_age=0):
        if not self.durations:
            return ""
        complete = [end - start for start, end in self.recent if end is not None]
        ages = [now - start for start, end in self.recent if end is None]
        # Unclosed claims can include failures. Only use their ages if the
        # scheduler accounts for all of them as running.
        if len(self.active) != active_count:
            ages = []
        average = mean(self.durations)
        if len(complete) + len(ages) >= 8:
            if mean(complete + ages) > 2 * average:
                return "recent_slower"
            if len(complete) == len(self.recent) and mean(complete) < average / 2:
                return "recent_faster"
        if max([oldest_age, *ages]) > max(self.durations):
            return "long_running"
        return ""


def recent_names(batch, claimed):
    """Read a bounded tail of the adaptive queue at the last published claim."""
    if len(claimed) <= RECENT_CLAIMS:
        return list(claimed)
    try:
        cursor = json.loads((batch / "claim-cursor.json").read_bytes())
        end = cursor["offset"]
        with (batch / "queue-edit.txt").open("rb") as stream:
            if type(end) is not int or not 0 < end <= os.fstat(stream.fileno()).st_size:
                return []
            start = max(0, end - 131072)
            stream.seek(start)
            lines = stream.read(end - start).splitlines()
        if start:
            lines = lines[1:]  # May begin in the middle of a command.
        ids = [line.split(None, 1)[0].decode("ascii") for line in lines if line.strip()]
        if not ids or ids[-1] != cursor["previous_job_id"]:
            return []
        return [f"job_{job}.claim" for job in ids[-RECENT_CLAIMS:] if f"job_{job}.claim" in claimed]
    except (OSError, ValueError, KeyError, TypeError):
        return []  # Older/recovering batches still have the ordinary timing sample.


def recent_timings(batch, now, pool_id, claimed, done):
    recent = []
    hosts = defaultdict(list)
    for name in recent_names(batch, claimed):
        claim = batch / "claims" / name
        try:
            start = claim.stat().st_mtime
            end = (batch / "done" / (name[:-6] + ".done")).stat().st_mtime if name[:-6] in done else None
            if start > now or (end is not None and not start < end <= now):
                continue
            recent.append((start, end))
            owner = json.loads((claim / "owner.json").read_bytes())["reservation_id"]
            if isinstance(owner, str) and owner.startswith(pool_id + "."):
                host, _, serial = owner[len(pool_id) + 1:].rpartition(".")
                if host and serial.isdigit():
                    hosts[host].append((int(serial), start, end))
        except (OSError, ValueError, KeyError, TypeError):
            continue
    dispatch = {}
    for host, records in hosts.items():
        records.sort(key=lambda r: r[0])
        gaps = []
        streak = []
        for previous, current in zip(records, records[1:]):
            serial, _, end = previous
            next_serial, start, _ = current
            if next_serial == serial + 1 and end is not None and 0 <= start - end <= MAX_GAP_SECONDS:
                streak.append(start - end)
            else:
                if len(streak) >= MIN_GAP_PAIRS:
                    gaps.extend(streak)
                streak = []
        if len(streak) >= MIN_GAP_PAIRS:
            gaps.extend(streak)
        if gaps:
            dispatch[host] = (median(gaps), len(gaps))
    return recent, dispatch


def claim_host(claim: Path, pool_id: str):
    try:
        reservation = json.loads((claim / "owner.json").read_text())["reservation_id"]
        return reservation_host(reservation, pool_id) if isinstance(reservation, str) else None
    except (OSError, KeyError, ValueError, TypeError):
        return None


def sample_jobs(batch: Path, now: float, pool_id: str = "") -> JobTimes:
    done_dir, claims = batch / "done", batch / "claims"
    done = {name[:-5] for name in os.listdir(done_dir)
            if name.startswith("job_") and name.endswith(".done")}
    sample = random.Random(0).sample(sorted(done), min(SAMPLE_SIZE, len(done)))
    durations = []
    by_host = defaultdict(list)
    for job in sample:
        claim = claims / (job + ".claim")
        try:
            finish = (done_dir / (job + ".done")).stat().st_mtime
            start = claim.stat().st_mtime
        except FileNotFoundError:
            continue
        if start < finish <= now:
            durations.append(finish - start)
            host = claim_host(claim, pool_id) if pool_id else None
            if host:
                by_host[host].append(finish - start)
    claimed = {name for name in os.listdir(claims)
               if name.startswith("job_") and name.endswith(".claim")}
    recent, dispatch = recent_timings(batch, now, pool_id, claimed, done) if pool_id and durations else ([], {})
    return JobTimes(durations, dict(by_host), [name for name in claimed if name[:-6] not in done],
                    len(claimed), len(done), dispatch, recent)


def residual(durations, age, tail):
    longer = [duration - age for duration in durations if duration > age]
    return mean(longer) if longer else (None if tail else mean(durations))


def current_eta(batch, remaining, workers, now, times, occupied=None):
    if remaining <= 0 or workers <= 0 or not times.durations:
        return None
    typical = mean(times.durations)
    slots = min(remaining, workers)
    gap = (sum(n * times.dispatch.get(h, (0, 0))[0] for h, n in occupied.items()) / workers
           if occupied else 0)
    if len(times.active) != slots:
        waves = (remaining + slots - 1) // slots
        return typical * waves + gap * (waves - 1)
    residuals = []
    for name in times.active:
        age = max(0, now - (batch / "claims" / name).stat().st_mtime)
        left = residual(times.durations, age, remaining == slots)
        if left is None:
            return None
        residuals.append(left)
    return finish_time(residuals, remaining - slots, typical + gap)


def shared_eta(batch: Path, remaining: int, workers: int, now: float, adaptive=False) -> Optional[float]:
    if remaining <= 0 or workers <= 0:
        return None
    pool_id, occupied = "", {}
    if adaptive:
        meta = optional_env(batch / "run_meta.env")
        status = optional_env(batch / "adaptive/status.env")
        if meta.get("run_token") and status.get("queue_id") == meta["run_token"]:
            pool_id = meta.get("pool_id", "")
            occupied = {h: int(n) for h, n in (pair.split(":") for pair in
                        status.get("running_by_host", "").split(",") if pair)}
            if sum(occupied.values()) != workers:
                occupied = {}
    return current_eta(batch, remaining, workers, now, sample_jobs(batch, now, pool_id), occupied)


def shard_eta(batch: Path, total: int, now: float) -> Optional[float]:
    # Immutable shards cannot hand their backlog to another host. Their slots
    # are fixed at launch, so each host's own throughput measures its capacity.
    meta = read_env(batch / "run_meta.env")
    launch = read_env(batch / "launch_status.env")
    if launch["state"] != "active" or launch["run_token"] != meta["run_token"]:
        return None
    released = int(launch["updated_epoch"])
    max_age = max(60, 3 * int(meta["shard_progress_sync_sec"]))
    assigned = 0
    estimates = []
    for path in (batch / "shards").glob("host_*.env"):
        host = read_env(path)
        jobs, finished = int(host["total_jobs"]), int(host["finished_jobs"])
        assigned += jobs
        if finished == jobs:
            continue
        running, slots = int(host["running_jobs"]), int(host["worker_slots"])
        updated = int(host["updated_epoch"])
        if (host["state"] != "RUNNING" or not 0 < finished < jobs or
                not 0 < running <= slots or not released < updated <= now or now - updated > max_age):
            return None
        worker_sec = (updated - released) * min(slots, jobs) / finished
        estimates.append(max(1, worker_sec * (jobs - finished) / running - (now - updated)))
    return max(estimates) if estimates and assigned == total else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("batch", type=Path)
    parser.add_argument("mode")
    parser.add_argument("total", type=int)
    parser.add_argument("done", type=int)
    parser.add_argument("workers", help="running adaptive workers or a fixed-slot vector")
    parser.add_argument("now", type=int)
    args = parser.parse_args()
    eta = None
    try:
        if args.mode == "host-local-shards":
            eta = shard_eta(args.batch, args.total, args.now)
        else:
            workers = sum(int(n) for n in args.workers.split("-"))
            eta = shared_eta(args.batch, args.total - args.done, workers, args.now,
                             adaptive=args.mode == "adaptive-shared-queue")
    except (FileNotFoundError, ValueError, KeyError):
        pass  # Missing timing, partial worker scan, or incomplete shard report.
    print(-1 if eta is None else max(1, round(eta)))


if __name__ == "__main__":
    main()
