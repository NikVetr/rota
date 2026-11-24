#!/usr/bin/env python3
"""Estimate remaining queue time at its current concurrency on the storage host."""

import argparse
import os
import random
from pathlib import Path
from statistics import mean
from typing import Optional

from scheduler_snapshot import read_env


SAMPLE_SIZE = 128


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


def shared_eta(batch: Path, remaining: int, workers: int, now: float) -> Optional[float]:
    if remaining <= 0 or workers <= 0:
        return None
    done_dir, claims = batch / "done", batch / "claims"
    done = {name[:-5] for name in os.listdir(done_dir)
            if name.startswith("job_") and name.endswith(".done")}
    # Bound metadata reads on large queues. Sorting makes the sample reproducible
    # across directory iteration orders without favoring short jobs or early IDs.
    sample = random.Random(0).sample(sorted(done), min(SAMPLE_SIZE, len(done)))
    durations = []
    for job in sample:
        try:
            finish = (done_dir / (job + ".done")).stat().st_mtime
            start = (claims / (job + ".claim")).stat().st_mtime
        except FileNotFoundError:
            continue
        if start < finish <= now:
            durations.append(finish - start)
    if not durations:
        return None
    typical = mean(durations)
    slots = min(remaining, workers)
    active = [name for name in os.listdir(claims) if name.startswith("job_")
              and name.endswith(".claim") and name[:-6] not in done]
    # Failed claims can remain on disk, and status can lag a new assignment.
    # Only credit time already spent when the two sources agree on occupancy.
    if len(active) != slots:
        return typical * ((remaining + slots - 1) // slots)
    residuals = []
    for name in active:
        age = max(0, now - (claims / name).stat().st_mtime)
        longer = [duration - age for duration in durations if duration > age]
        if not longer and remaining == slots:
            return None  # A final job has already outlasted the entire sample.
        residuals.append(mean(longer) if longer else typical)
    return finish_time(residuals, remaining - slots, typical)


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
            eta = shared_eta(args.batch, args.total - args.done, workers, args.now)
    except (FileNotFoundError, ValueError, KeyError):
        pass  # Missing timing, partial worker scan, or incomplete shard report.
    print(-1 if eta is None else max(1, round(eta)))


if __name__ == "__main__":
    main()
