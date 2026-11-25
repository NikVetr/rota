#!/usr/bin/env python3
"""Print the adaptive pool's controllable state as one JSON document.

Runs on the coordinator host, reading only its local scheduler state: the pool marker,
the reviewed resource table with budget overrides and away windows, each registered queue's
control, status and metadata files, its failed jobs (claimed, ended without a done marker, not
rerun) and its per-host failure holds. The cluster status collector publishes the result as
cluster-status.json's `scheduler` section, from which `rota control describe` and failure events
are built.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
import monitor_queue_failures  # noqa: E402  (deployed beside this script)

SCHEMA = 1
FAILED_JOBS_MAX = 200  # per queue; failed_jobs_total keeps the full count
RETIRED_SECONDS = 7 * 86400  # the coordinator's retired-queue index keeps entries this long
COMMANDS = ("set-host-state", "confirm-interrupted-jobs", "set-host-budget", "pool-drain")


def read_env(path):
    values = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key] = value
    return values


def optional_env(path):
    return read_env(path) if path.is_file() else {}


def read_json(path, default):
    return json.loads(path.read_text()) if path.is_file() else default


def integer(value, default=None):
    return int(value) if isinstance(value, str) and value.lstrip("-").isdigit() else default


def host_priorities(text):
    """`worker=10,laptop=off` -> {"worker": 10, "laptop": "off"}."""
    result = {}
    for item in filter(None, (text or "").split(",")):
        host, _, value = item.partition("=")
        result[host] = "off" if value == "off" else int(value)
    return result


def running_by_host(text):
    """Reported reservations per host, or None before the first queue status."""
    if not text:
        return None
    counts = {}
    for item in text.split(","):
        host, _, count = item.partition(":")
        if not host or host in counts or not count.isdecimal():
            raise ValueError(f"invalid running_by_host entry: {item!r}")
        counts[host] = int(count)
    return counts


def hosts_section(root, pool, now):
    with open(pool["resources_file"], newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))
    budgets = read_json(root / "host-budgets.json", {"hosts": {}})["hosts"]
    states = read_json(root / "host-states.json", {"hosts": {}})["hosts"]
    hosts = []
    for row in rows:
        budget = budgets.get(row["host_id"], {})
        window = states.get(row["host_id"])
        hosts.append({
            "id": row["host_id"], "enabled": row["enabled"] == "1",
            "base_cpu_units": int(row["cpu_units"]), "base_memory_mib": int(row["memory_mib"]),
            "cpu_units": int(budget.get("cpu_units", row["cpu_units"])),
            "memory_mib": int(budget.get("memory_mib", row["memory_mib"])),
            "away": bool(window) and window["away_from"] <= now
            and (window["away_until"] is None or now < window["away_until"]),
            "away_window": {key: window[key] for key in ("away_from", "away_until", "reason")}
            if window else None,
            # Opt-in host settings; the resource table has these columns only when one is used.
            "gpu_units": int(row.get("gpu_units") or 0),
            "owner_idle_seconds": int(row.get("owner_idle_seconds") or 0),
            "owner_active": row.get("owner_active") or "hold",
        })
    return hosts


def usage_summary(status):
    """CPU time of finished jobs, summed by the coordinator from per-job usage records."""
    return {
        "jobs": integer(status.get("jobs_with_usage"), 0),
        "cpu_hours_succeeded": round(float(status.get("cpu_seconds_succeeded") or 0) / 3600, 4),
        "cpu_hours_failed": round(float(status.get("cpu_seconds_failed") or 0) / 3600, 4),
        "state": status.get("usage_state") or "unknown",
    }


def retired_queues(root, now):
    """Queues retired within RETIRED_SECONDS, with their final lifecycle and usage."""
    result = []
    # Beside the pool state, so queues retired before a drain stay listed in the next pool.
    for path in sorted((root.parent / "retired-queues").glob("*.env")):
        try:
            entry = read_env(path)
            retired = int(entry["retired_epoch"])
            if now - retired > RETIRED_SECONDS:
                continue
            status = optional_env(Path(entry["batch_dir"]) / "adaptive/status.env")
            result.append({"queue_id": entry["queue_id"], "tag": entry["tag_clean"],
                           "batch_dir": entry["batch_dir"], "retired": retired,
                           "lifecycle": status.get("lifecycle"), "usage": usage_summary(status)})
        except (OSError, KeyError, ValueError) as error:
            result.append({"queue_id": path.stem, "error": str(error)})
    return result


def queue_entry(registration):
    reg = read_env(registration)
    batch = Path(reg["batch_dir"])
    control = read_env(batch / "adaptive/control.env")
    resources = read_env(batch / "adaptive/resources.env")
    status = optional_env(batch / "adaptive/status.env")
    meta = optional_env(batch / "run_meta.env")
    return {
        "queue_id": reg["queue_id"], "tag": reg["tag_clean"], "batch_dir": str(batch),
        "run_name": reg.get("run_name") or meta.get("run_name") or None,
        "project": meta.get("project_name") or None,
        "source_dir": meta.get("source_dir") or None,
        "submitted_by": meta.get("submitted_by") or None,
        "lifecycle": status.get("lifecycle"), "blocking_reason": status.get("blocking_reason"),
        "running_workers": integer(status.get("running_workers"), 0),
        "running_by_host": running_by_host(status.get("running_by_host")),
        "gpu_units_per_job": int(resources.get("gpu_units_per_job", 0)),
        "memory_limit_mib": int(resources["memory_limit_mib"]) if "memory_limit_mib" in resources else None,
        "target_workers": integer(status.get("target_workers"), 0),
        "share_interrupted_jobs": integer(status.get("share_interrupted_jobs"), 0),
        "usage": usage_summary(status),
        "control": {
            "generation": int(control["generation"]),
            "admission": control["admission"],
            "schedule_priority": int(control["schedule_priority"]),
            "host_priorities": host_priorities(control.get("host_priorities")),
            "max_workers": int(control["max_workers"]),
            "memory_mib_per_job": int(control.get("memory_mib_per_job", resources["memory_mib_per_job"])),
            "effective_memory_mib_per_job": integer(status.get("effective_memory_mib_per_job")),
            "calibration": {
                "jobs": int(control.get("memory_calibration_jobs", 0)),
                "seconds": int(control.get("memory_calibration_seconds", 0)),
                "minimum_mib": integer(control.get("memory_min_mib_per_job")),
                "state": status.get("memory_calibration_state", "off"),
            },
        },
    }


def reservation_host(reservation, pool_id):
    """`<pool>.<host>.<n>` -> host; reservations from an earlier pool fall back to the middle field."""
    if not reservation:
        return None
    if reservation.startswith(pool_id + "."):
        return reservation[len(pool_id) + 1:].rpartition(".")[0] or None
    parts = reservation.rsplit(".", 2)
    return parts[1] if len(parts) == 3 else None


def host_holds(root):
    """{queue_id: [{"host_id", "reason"}]} from the daemons' blocked_queue rows in last-known tables."""
    holds = {}
    for table in sorted((root / "last-known").glob("*.tsv")):
        lines = table.read_text().splitlines()
        header = lines[0].split("\t") if lines else []
        if not {"record_type", "host_id", "queue_id", "message"} <= set(header):
            raise ValueError(f"unexpected last-known header in {table}")
        index = {name: header.index(name) for name in ("record_type", "host_id", "queue_id", "message")}
        for line in lines[1:]:
            fields = line.split("\t")
            if len(fields) > max(index.values()) and fields[index["record_type"]] == "blocked_queue":
                holds.setdefault(fields[index["queue_id"]], []).append(
                    {"host_id": fields[index["host_id"]], "reason": fields[index["message"]]})
    return holds


def add_failures(root, pool_id, queues, now):
    """Attach failed_jobs, failed_jobs_total and holds to each readable queue entry."""
    live = monitor_queue_failures.live_reservations(root)
    holds = host_holds(root)
    for queue in queues:
        if "error" in queue:
            continue
        batch = Path(queue["batch_dir"])
        failures = monitor_queue_failures.failed_jobs(batch, live, now)
        rerun = monitor_queue_failures.rerun_jobs(batch) if failures else set()
        outstanding = sorted(((job, failure) for job, failure in failures.items() if job not in rerun),
                             key=lambda item: item[1]["last_activity"])
        queue["failed_jobs_total"] = len(outstanding)
        queue["failed_jobs"] = [{
            "job_id": job[len("job_"):], "reservation_id": failure["reservation_id"],
            "host_id": reservation_host(failure["reservation_id"], pool_id),
            "last_activity": round(failure["last_activity"], 3), "log": failure["log"],
        } for job, failure in outstanding[-FAILED_JOBS_MAX:]]
        queue["holds"] = holds.get(queue["queue_id"], [])


def snapshot(root, scheduler):
    now = time.time()
    pool_file = root / "pool.env"
    if not pool_file.is_file():
        return {"schema": SCHEMA, "updated": round(now, 3), "pool": {"state": "INACTIVE"},
                "hosts": [], "queues": [], "retired_queues": retired_queues(root, now), "commands": []}
    pool = read_env(pool_file)
    ready = optional_env(root / "coordinator.ready")
    lease = integer(pool.get("lease_seconds"), 20)
    updated = integer(ready.get("updated_epoch"), 0)
    live = (ready.get("pool_id") == pool["pool_id"] and ready.get("state") == "ready"
            and 0 <= now - updated <= lease)
    queues = []
    for registration in sorted((root / "queues").glob("*.env")):
        try:
            queues.append(queue_entry(registration))
        except (OSError, KeyError, ValueError) as error:
            # One unreadable queue must not hide the others; report it without controls.
            queues.append({"queue_id": registration.stem, "error": str(error)})
    failures_error = None
    try:
        add_failures(root, pool["pool_id"], queues, now)
    except (OSError, ValueError) as error:
        failures_error = f"failed-job scan unavailable: {error}"
    usage = scheduler.read_text() if scheduler.is_file() else ""
    return {
        "schema": SCHEMA, "updated": round(now, 3),
        "pool": {
            "id": pool["pool_id"], "name": pool.get("pool_name"), "state": pool["state"],
            "backfill": pool.get("backfill", "0") == "1",
            "coordinator_live": live, "coordinator_updated": updated or None,
            "away_hosts": list(filter(None, ready.get("away_hosts", "").split(","))),
            "unavailable_hosts": dict(item.split(":", 1) for item in
                                      filter(None, ready.get("unavailable_hosts", "").split(","))),
            "running_workers": sum(queue.get("running_workers", 0) for queue in queues),
        },
        "hosts": hosts_section(root, pool, now),
        "queues": queues,
        "retired_queues": retired_queues(root, now),
        "failures_error": failures_error,
        # Commands the deployed scheduler offers, so controls arriving with a later pool stay disabled.
        "commands": [command for command in COMMANDS if f"\n  {command} " in usage],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    root = os.environ.get("ROTA_ROOT")
    parser.add_argument("--state-root", type=Path, default=Path(root) / ".rota/adaptive" if root else None, required=not root)
    parser.add_argument("--scheduler", type=Path, default=Path(root) / "adaptive_scheduler.sh" if root else None, required=not root)
    args = parser.parse_args()
    try:
        print(json.dumps(snapshot(args.state_root, args.scheduler), separators=(",", ":")))
    except (OSError, KeyError, ValueError) as error:
        print(f"scheduler snapshot: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
