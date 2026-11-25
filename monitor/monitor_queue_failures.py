#!/usr/bin/env python3
"""Count claimed adaptive jobs that ended without a completion marker.

A claimed job has failed when no host still reports its reservation in the
coordinator's last-known tables and no done marker exists. Outage casualties and
ordinary job errors both qualify; either way the owning project must review or
rerun it. A failure counts as rerun once a later batch of the same project has
queued, claimed or finished the same job ID, however it was resubmitted.

Reads batch directories on stdin. For each adaptive batch prints the batch, the
outstanding failures, the newest outstanding failure's last activity epoch, and
the failures already rerun.
"""
import argparse
import glob
import json
import os
from pathlib import Path
import sys
import time

# A fresh claim can precede the first lease report of its reservation.
GRACE_SEC = 120


def live_reservations(state_root):
    if not (state_root / "pool.env").exists():
        return set()  # No pool, so nothing can still be running adaptively.
    tables = sorted((state_root / "last-known").glob("*.tsv"))
    if not tables:
        raise ValueError(f"Pool has no last-known host tables under {state_root}")
    live = set()
    for table in tables:
        lines = table.read_text().splitlines()
        header = lines[0].split("\t") if lines else []
        if "record_type" not in header or "reservation_id" not in header:
            raise ValueError(f"Unexpected last-known header in {table}")
        kind, reservation = header.index("record_type"), header.index("reservation_id")
        for line in lines[1:]:
            fields = line.split("\t")
            if len(fields) > reservation and fields[kind] == "reservation":
                live.add(fields[reservation])
    return live


def run_meta(batch):
    try:
        lines = (batch / "run_meta.env").read_text().splitlines()
    except FileNotFoundError:
        return {}
    return dict(line.split("=", 1) for line in lines if "=" in line)


def batch_number(path):
    parts = path.name.split("_")
    return int(parts[1]) if len(parts) > 2 and parts[0] == "batch" and parts[1].isdigit() else None


def failed_jobs(batch, live, now):
    """Map each failed job name (job_<ID>) to its last activity epoch, reservation and newest log."""
    claims, done_dir = batch / "claims", batch / "done"
    if not claims.is_dir():
        return {}
    done = set(os.listdir(done_dir)) if done_dir.is_dir() else set()
    failures = {}
    for name in os.listdir(claims):
        if not (name.startswith("job_") and name.endswith(".claim")):
            continue
        job = name[:-len(".claim")]
        if job + ".done" in done:
            continue
        owner = claims / name / "owner.json"
        try:
            started = owner.stat().st_mtime
            reservation = json.loads(owner.read_text())["reservation_id"]
        except FileNotFoundError:
            continue  # Still being written.
        except (ValueError, KeyError):
            print(f"Unreadable claim owner {owner}; counting it as failed", file=sys.stderr)
            reservation = None
        if reservation in live or now - started < GRACE_SEC:
            continue
        logs = [(os.stat(path).st_mtime, path) for path in
                glob.glob(os.path.join(glob.escape(str(batch / "jobs")), glob.escape(job) + "_*.log"))]
        failures[job] = {"last_activity": max([started, *(mtime for mtime, _ in logs)]),
                         "reservation_id": reservation, "log": max(logs)[1] if logs else None}
    return failures


def rerun_jobs(batch):
    """Job names queued, claimed or finished by a later batch in the same logs directory."""
    number = batch_number(batch)
    jobs = set()
    if number is None:
        return jobs
    for sibling in batch.parent.iterdir():
        later = batch_number(sibling)
        if later is None or later <= number:
            continue
        for subdir, suffix in (("done", ".done"), ("claims", ".claim")):
            if (sibling / subdir).is_dir():
                jobs.update(name[:-len(suffix)] for name in os.listdir(sibling / subdir) if name.endswith(suffix))
        queue = Path(run_meta(sibling).get("editable_queue_file") or sibling / "queue-edit.txt")
        if queue.is_file():
            jobs.update("job_" + line.split(None, 1)[0] for line in queue.read_text().splitlines() if line.strip())
    return jobs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state_root", required=True, type=Path)
    args = parser.parse_args()
    try:
        live = live_reservations(args.state_root)
    except (OSError, ValueError) as error:
        print(f"Failed-job scan unavailable: {error}", file=sys.stderr)
        return 2
    now = time.time()
    for line in sys.stdin:
        batch = Path(line.rstrip("\n"))
        if not line.strip() or not batch.is_dir() or run_meta(batch).get("execution_mode") != "adaptive-shared-queue":
            continue
        failures = failed_jobs(batch, live, now)
        rerun = rerun_jobs(batch) if failures else set()
        outstanding = [failure["last_activity"] for job, failure in failures.items() if job not in rerun]
        print(f"{batch}\t{len(outstanding)}\t{int(max(outstanding, default=0))}\t{len(failures) - len(outstanding)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
