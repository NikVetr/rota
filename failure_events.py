"""Group failed adaptive jobs and per-host holds into incidents and append them as JSON lines.

The cluster status collector calls `FailureEvents.observe` after each scheduler snapshot. A failed
job is a claimed job that ended without a done marker and was not rerun; a hold is a queue that
one host refuses after an infrastructure failure or rapid job failures. New failures and holds of
one queue collect into a pending incident, which is written as one event once it settles: no new
failure for `settle_sec` and no involved host still unreachable. Each failure and hold is reported
once; a hold that is cleared and returns is reported again. On the first run the failures already
present are recorded as known without events, so a restart never replays history.

Readers (Agent Bar) tail the events file and deliver each event `id` once. The file is renamed to
`<name>.1` once it exceeds `max_bytes`, so a reader reopens it when its inode changes.
"""
import hashlib
import json
import os
from pathlib import Path
import shlex

STATE_VERSION = 1
SETTLE_SEC = 120
OUTAGE_MATCH_SEC = 600  # an outage starting this long after a job's last activity still explains it
OUTAGE_KEEP_SEC = 86400
MAX_BYTES = 5 * 1024 ** 2


def atomic_json(path, value):
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as handle:
        handle.write(json.dumps(value, separators=(",", ":")))
    os.replace(temporary, path)


class FailureEvents:
    def __init__(self, events_path, state_path, root, coordinator,
                 settle_sec=SETTLE_SEC, max_bytes=MAX_BYTES):
        self.events_path, self.state_path = Path(events_path), Path(state_path)
        self.root, self.coordinator = root, coordinator
        self.settle_sec, self.max_bytes = settle_sec, max_bytes
        try:
            self.state = json.loads(self.state_path.read_text())
            if self.state.get("version") != STATE_VERSION:
                raise ValueError("unknown failure-event state version")
        except FileNotFoundError:
            self.state = {"version": STATE_VERSION, "baseline": False, "jobs": {}, "holds": {},
                          "pending": {}, "outages": {}}

    # -- outages --------------------------------------------------------------
    def track_outages(self, hosts, now):
        outages = self.state["outages"]
        for host in hosts:
            intervals = outages.setdefault(host["id"], [])
            open_interval = intervals[-1] if intervals and intervals[-1]["ended"] is None else None
            if not host["ok"] and open_interval is None:
                intervals.append({"started": round(now, 3), "ended": None})
            elif host["ok"] and open_interval is not None:
                open_interval["ended"] = round(now, 3)
        for host_id, intervals in list(outages.items()):
            outages[host_id] = [interval for interval in intervals
                                if interval["ended"] is None or now - interval["ended"] < OUTAGE_KEEP_SEC]

    def host_down(self, host_id):
        intervals = self.state["outages"].get(host_id) or []
        return bool(intervals) and intervals[-1]["ended"] is None

    def outage_for(self, host_id, epoch):
        """The outage that plausibly ended a job last active at `epoch`, if any."""
        for interval in reversed(self.state["outages"].get(host_id) or []):
            if (interval["started"] <= epoch + OUTAGE_MATCH_SEC
                    and (interval["ended"] is None or interval["ended"] >= epoch - SETTLE_SEC)):
                return interval
        return None

    # -- incidents ------------------------------------------------------------
    def observe(self, now, hosts, scheduler):
        """Update from one collector pass; returns the events written."""
        self.track_outages(hosts, now)
        if not scheduler.get("ok") or scheduler.get("failures_error") or "queues" not in scheduler:
            self.save()
            return []
        queues = {queue["queue_id"]: queue for queue in scheduler["queues"]
                  if "error" not in queue and "failed_jobs" in queue}
        jobs = {f"{queue_id}|{job['reservation_id'] or job['job_id']}": job
                for queue_id, queue in queues.items() for job in queue["failed_jobs"]}
        holds = {f"{queue_id}|{hold['host_id']}": hold
                 for queue_id, queue in queues.items() for hold in queue["holds"]}
        known_jobs, known_holds, pending = self.state["jobs"], self.state["holds"], self.state["pending"]
        if not self.state["baseline"]:
            known_jobs.update(dict.fromkeys(jobs, round(now, 3)))
            known_holds.update(dict.fromkeys(holds, round(now, 3)))
            self.state["baseline"] = True
            self.save()
            return []
        for kind, current, known in (("jobs", jobs, known_jobs), ("holds", holds, known_holds)):
            for key, item in current.items():
                if key in known:
                    continue
                known[key] = round(now, 3)
                queue_id = key.partition("|")[0]
                incident = pending.setdefault(queue_id, {"jobs": {}, "holds": {}, "first": round(now, 3)})
                incident[kind][key] = item
                incident["last_new"] = round(now, 3)
            # A rerun job or a cleared hold leaves the snapshot; forget it so a recurrence reports again.
            for key in [key for key in known if key not in current]:
                del known[key]
        events = []
        for queue_id, incident in list(pending.items()):
            for kind, current in (("jobs", jobs), ("holds", holds)):
                incident[kind] = {key: item for key, item in incident[kind].items() if key in current}
            if not incident["jobs"] and not incident["holds"]:
                del pending[queue_id]
                continue
            involved = ({job["host_id"] for job in incident["jobs"].values()}
                        | {hold["host_id"] for hold in incident["holds"].values()})
            if now - incident["last_new"] < self.settle_sec or any(map(self.host_down, involved)):
                continue
            events.append(self.event(queues[queue_id], incident, involved, now))
            del pending[queue_id]
        for event in events:
            self.append(event)
        self.save()
        return events

    def event(self, queue, incident, involved, now):
        jobs = sorted(incident["jobs"].values(), key=lambda job: job["last_activity"])
        outages, entries = {}, []
        for job in jobs:
            outage = self.outage_for(job["host_id"], job["last_activity"]) if job["host_id"] else None
            if outage:
                outages[(job["host_id"], outage["started"])] = {"host_id": job["host_id"], **outage}
            entries.append({"job_id": job["job_id"], "reservation_id": job["reservation_id"],
                            "host_id": job["host_id"], "kind": "outage" if outage else "unknown",
                            "last_activity": job["last_activity"], "log": job["log"]})
        holds = [{"host_id": hold["host_id"], "reason": hold["reason"],
                  "reset_command": self.reset_command(queue["batch_dir"], hold["host_id"])}
                 for hold in sorted(incident["holds"].values(), key=lambda hold: hold["host_id"])]
        keys = sorted(incident["jobs"]) + sorted(incident["holds"])
        ends = [outage["ended"] for outage in outages.values() if outage["ended"] is not None]
        return {
            "id": "pw-" + hashlib.sha256("\n".join(keys).encode()).hexdigest()[:20],
            "at": round(now, 3), "kind": "job_failures" if entries else "hold",
            "queue_id": queue["queue_id"], "queue_tag": queue["tag"], "batch_dir": queue["batch_dir"],
            "project": queue["project"], "run_name": queue["run_name"], "conversation": queue["submitted_by"],
            "host_id": next(iter(involved)) if len(involved) == 1 else None,
            "started": jobs[0]["last_activity"] if jobs else incident["first"],
            "ended": max(ends + [job["last_activity"] for job in jobs], default=incident["first"]),
            "jobs": entries, "holds": holds, "outages": list(outages.values()),
            "queue_failed_jobs_total": queue["failed_jobs_total"],
        }

    def reset_command(self, batch_dir, host_id):
        command = (f"{self.root}/adaptive_scheduler.sh reset-failures --batch_dir {shlex.quote(batch_dir)} "
                   f"--host_id {shlex.quote(host_id)} --reason '<fault fixed; outputs audited>'")
        return f"ssh {self.coordinator} {shlex.quote(command)}"

    # -- files ----------------------------------------------------------------
    def append(self, event):
        line = json.dumps(event, separators=(",", ":")) + "\n"
        try:
            if self.events_path.stat().st_size + len(line) > self.max_bytes:
                os.replace(self.events_path, self.events_path.with_name(self.events_path.name + ".1"))
        except FileNotFoundError:
            pass
        with os.fdopen(os.open(self.events_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600), "a") as handle:
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())

    def save(self):
        atomic_json(self.state_path, self.state)
