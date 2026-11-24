#!/usr/bin/env python3
"""Project adaptive queue finishes with the deployed allocator and today's eligible hosts."""
import argparse
from collections import Counter, defaultdict
import csv
from dataclasses import dataclass, field
import json
import math
from pathlib import Path
from statistics import mean
import sys
import time
from typing import Any

from monitor_queue_eta import claim_host, current_eta, residual, sample_jobs
from scheduler_snapshot import hosts_section, host_priorities, read_env


@dataclass
class Queue:
    profile: dict
    pending: int
    typical: float
    host_seconds: dict = field(default_factory=dict)
    depends: set = field(default_factory=set)
    succeeds: bool = True

    def duration(self, host):
        return self.host_seconds.get(host, self.typical)


@dataclass
class Slot:
    host: str
    queue: str
    finish: float
    memory: int


def contested_slots(hosts, queues, live, pending, completed, slots, targets, io_capacity):
    """Find occupied slots that a waiting peer could take on its next turn."""
    totals = Counter(s.queue for s in slots)
    waiting = {}
    for q in live:
        item = queues[q]
        cap = item.profile["max_workers"]
        if pending[q] and item.profile["admission"] == "enabled" and not item.depends - completed:
            if not cap or totals[q] < cap:
                waiting[q] = item.profile
    if not waiting:
        return set()
    used_io = sum(queues[q].profile["shared_io_units_per_job"] * n for q, n in totals.items())
    free_io = max(0, io_capacity - used_io)
    io_waiters = set()
    contested = set()
    for host in hosts:
        h = host["host_id"]
        if not host.get("enabled", True) or not host.get("reachable", True) or host.get("drain_only"):
            continue
        blocked = host.get("blocked_queues", [])
        cpu = host.get("live_cpu_units", host["cpu_units"])
        memory = host.get("live_memory_mib", host["memory_mib"])
        levels = {}
        for q, profile in waiting.items():
            level = profile["host_priorities"].get(h, profile["schedule_priority"])
            if (level != "off" and q not in blocked
                    and profile["cpu_units_per_job"] <= cpu and profile["memory_mib_per_job"] <= memory
                    and profile["shared_io_units_per_job"] <= io_capacity):
                levels[q] = level
                if profile["shared_io_units_per_job"] > free_io:
                    io_waiters.add((h, q))
        for q, target in targets[h].items():
            if not target:
                continue
            priority = queues[q].profile["host_priorities"].get(h, queues[q].profile["schedule_priority"])
            if priority != "off" and any(other != q and (value > priority or
                    (value == priority and totals[other] < totals[q])) for other, value in levels.items()):
                contested.add((h, q))
    # A refill can consume the I/O that a peer on another host needs.
    # Let the allocator choose a destination when that reservation ends.
    if io_waiters:
        for slot in slots:
            if queues[slot.queue].profile["shared_io_units_per_job"] and any(
                    h != slot.host and q != slot.queue for h, q in io_waiters):
                contested.add((slot.host, slot.queue))
    return contested


def skip_repeated_cycle(cycles, cursor, live, pending, slots, now):
    # A repeating round-robin allocation can skip complete cycles even
    # when one host/slot is contested. Leave a full pool-width tail.
    key = (tuple(sorted(cursor.items())), tuple(sorted((q, pending[q] > 0) for q in live)),
           tuple(sorted((s.host, s.queue, round(s.finish - now, 6), s.memory) for s in slots)))
    if key in cycles:
        previous, before = cycles[key]
        used = {q: before[q] - pending[q] for q in live if before[q] > pending[q]}
        jumps = min((max(0, pending[q] - len(slots)) // n for q, n in used.items()), default=0)
        if jumps and now > previous:
            shift = jumps * (now - previous)
            for q, n in used.items():
                pending[q] -= jumps * n
            now += shift
            for s in slots:
                s.finish += shift
    if len(cycles) >= 128:
        cycles.clear()
    cycles[key] = now, dict(pending)
    return now


def skip_stable_refills(queues, pending, slots, counts, targets, contested, now):
    # Jump over guaranteed in-place refills. In dt seconds, n slots
    # complete at most n + dt * sum(1 / runtime) jobs. Stop before a
    # queue empties or an excess worker returns its slot to the allocator.
    by_queue = defaultdict(list)
    horizon = math.inf
    for slot in slots:
        by_queue[slot.queue].append(slot)
        if ((slot.host, slot.queue) in contested or counts[slot.host, slot.queue] > targets[slot.host][slot.queue]
                or slot.memory != queues[slot.queue].profile["memory_mib_per_job"]):
            horizon = min(horizon, slot.finish)
    for q, active in by_queue.items():
        rate = sum(1 / queues[q].duration(s.host) for s in active if math.isfinite(s.finish))
        if rate:
            horizon = min(horizon, max(min(s.finish for s in active),
                                       now + (pending[q] - len(active)) / rate))
    if not math.isfinite(horizon):
        return
    for s in slots:
        if s.finish < horizon and counts[s.host, s.queue] <= targets[s.host][s.queue]:
            duration = queues[s.queue].duration(s.host)
            n = min(pending[s.queue], math.ceil((horizon - s.finish) / duration))
            s.finish += n * duration
            pending[s.queue] -= n


def forecast(hosts, queues, slots, io_capacity, allocate, cursor=None, cpu_deadline=math.inf):
    """Discrete finishes, bulk-refilling unchanged slots across long backlogs."""
    # Unknown runtimes occupy their placement indefinitely. Other hosts/queues
    # can still yield a forecast without guessing when those workers will free up.
    pending = {q: item.pending for q, item in queues.items()}
    live = set(queues)
    completed = set()
    finish = {}
    slots = [Slot(s.host, s.queue, s.finish, s.memory) for s in slots]
    now = 0.0
    replan = True
    targets = {}
    contested = set()
    cycles = {}
    plans = 0
    while live and time.process_time() < cpu_deadline:
        counts = Counter((s.host, s.queue) for s in slots)
        if replan:
            plans += 1
            if plans > 1000:
                # Bound collector work on pathologically interleaved workloads.
                # Publish only the queues whose projected finish is known.
                break
            memory = Counter()
            for s in slots:
                memory[s.host, s.queue] += s.memory
            profiles = [dict(queues[q].profile, queue_id=q, pending_lines=pending[q],
                             admission="paused" if queues[q].depends - completed else queues[q].profile["admission"])
                        for q in sorted(live)]
            plan = allocate({"schema_version": 1, "hosts": hosts, "queues": profiles,
                             "shared_io_capacity": io_capacity, "allow_unschedulable_queues": True,
                             "cursor": cursor or {}, "reservations": [
                                 {"host_id": h, "queue_id": q, "count": n, "memory_mib": memory[h, q]}
                                 for (h, q), n in counts.items()]})
            targets, cursor = plan["targets"], plan["cursor"]
            for h, rows in targets.items():
                for q, target in rows.items():
                    add = min(pending[q], max(0, target - counts[h, q]))
                    for _ in range(add):
                        slots.append(Slot(h, q, now + queues[q].duration(h), queues[q].profile["memory_mib_per_job"]))
                    pending[q] -= add
                    counts[h, q] += add
            contested = contested_slots(hosts, queues, live, pending, completed, slots, targets, io_capacity)
            if contested:
                now = skip_repeated_cycle(cycles, cursor, live, pending, slots, now)
        if not slots:
            break
        skip_stable_refills(queues, pending, slots, counts, targets, contested, now)
        now = min(s.finish for s in slots)
        if not math.isfinite(now):
            break
        kept = []
        replan = False
        for s in slots:
            if s.finish <= now:
                if (pending[s.queue] and (s.host, s.queue) not in contested
                        and s.memory == queues[s.queue].profile["memory_mib_per_job"]
                        and counts[s.host, s.queue] <= targets[s.host][s.queue]):
                    s.finish = now + queues[s.queue].duration(s.host)
                    s.memory = queues[s.queue].profile["memory_mib_per_job"]
                    pending[s.queue] -= 1
                else:
                    counts[s.host, s.queue] -= 1
                    replan = True
                    continue
            kept.append(s)
        slots = kept
        busy = {s.queue for s in slots}
        for q in list(live - busy):
            if not pending[q]:
                finish[q] = now
                if queues[q].succeeds:
                    completed.add(q)
                live.remove(q)
                replan = True
    return finish


def read_hosts(root, pool, ready, now, lease):
    hosts, charges = [], defaultdict(list)
    unavailable = {s.partition(":")[0] for s in ready.get("unavailable_hosts", "").split(",") if s}
    for h in hosts_section(root, pool, now):
        name = h["id"]
        host: dict[str, Any] = dict(host_id=name, enabled=h["enabled"], cpu_units=h["cpu_units"], memory_mib=h["memory_mib"],
                    live_cpu_units=0, live_memory_mib=0, reachable=False, drain_only=h["away"], blocked_queues=[])
        hosts.append(host)
        if not h["enabled"]:
            continue
        try:
            with (root / "last-known" / (name + ".tsv")).open() as stream:
                rows = list(csv.DictReader(stream, delimiter="\t"))
        except FileNotFoundError:
            continue
        if not rows or any(r["pool_id"] != pool["pool_id"] or r["host_id"] != name for r in rows):
            continue
        agent = next((r for r in rows if r["record_type"] == "agent"), None)
        if agent is None:
            continue
        for r in rows:
            if r["record_type"] == "blocked_queue":
                host["blocked_queues"].append(r["queue_id"])
            elif r["record_type"] == "reservation":
                charges[name, r["queue_id"]].append(int(r["charged_memory_mib"]))
        host["reachable"] = (name not in unavailable and agent["telemetry_state"] == "ok"
                             and -2 <= now - int(agent["resource_sample_epoch"]) <= lease
                             and not any(r["state"] == "uncertain" for r in rows))
        if host["reachable"]:
            host.update(live_cpu_units=int(agent["live_cpu_units"]), live_memory_mib=int(agent["live_memory_mib"]))
    return hosts, charges


def queue_snapshot(state_root, pool, now, lease, hosts, charges, dependency_hold):
    queues, slots, rows = {}, [], {}
    reachable = {h["host_id"] for h in hosts if h["reachable"]}
    for reg_path in sorted((state_root / "queues").glob("*.env")):
        reg = read_env(reg_path)
        q = reg["queue_id"]
        batch = Path(reg["batch_dir"])
        status = read_env(batch / "adaptive/status.env")
        if reg["pool_id"] != pool["pool_id"] or status["queue_id"] != q:
            raise ValueError("queue belongs to a different pool or batch")
        if status["lifecycle"] in ("COMPLETED", "FINISHED_INCOMPLETE"):
            continue
        if not -2 <= now - int(status["updated_epoch"]) <= max(60, lease):
            raise ValueError("stale queue status")
        occupied = {h: int(n) for h, n in (pair.split(":") for pair in status["running_by_host"].split(",") if pair)}
        count = sum(occupied.values())
        reason = status["blocking_reason"]
        enabled = status["admission"] == "enabled" and not (
            reason.startswith(("dependency_failed", "dependency_unverified", "dependency_unavailable", "smb_")))
        if not enabled and not count:
            continue
        res = read_env(batch / "adaptive/resources.env")
        # A retired predecessor can already be successful. Check each pinned
        # identity with the same workflow reader used by the coordinator.
        depends = {d["queue_id"] for d in json.loads(res.get("dependencies", "[]"))
                   if dependency_hold([d])} if reason == "dependency_wait" else set()
        memory = int(status["effective_memory_mib_per_job"])
        profile = dict(cpu_units_per_job=int(res["cpu_units_per_job"]), memory_mib_per_job=memory,
                       shared_io_units_per_job=int(res["shared_io_units_per_job"]),
                       max_workers=int(status["max_workers"]), schedule_priority=int(status["schedule_priority"]),
                       host_priorities=host_priorities(status.get("host_priorities")), admission="enabled" if enabled else "paused")
        meta = read_env(batch / "run_meta.env")
        if meta["run_token"] != q or meta["pool_id"] != pool["pool_id"]:
            raise ValueError("queue run metadata changed")
        total = int(meta["total_jobs"])
        times = sample_jobs(batch, now, pool["pool_id"])
        pending = max(0, total - times.claimed)
        if not pending and not count:
            continue
        typical = mean(times.durations) if times.durations else math.inf
        rates = {h: mean(v) for h, v in times.by_host.items() if len(v) >= 3}
        queue = Queue(profile, pending, typical, rates, depends,
                      succeeds=times.done + pending + count == total and not int(status.get("share_interrupted_jobs", 0)))
        queues[q] = queue
        residuals = defaultdict(list)
        if len(times.active) == count and times.durations:
            for name in times.active:
                claim = batch / "claims" / name
                h = claim_host(claim, pool["pool_id"])
                if h is not None:
                    age = max(0, now - claim.stat().st_mtime)
                    samples = times.by_host[h] if h in rates else times.durations
                    left = residual(samples, age, not pending)
                    residuals[h].append(left if left is not None else math.inf)
        current = current_eta(batch, pending + count, count, now, times)
        for h, n in occupied.items():
            ends = residuals[h] if len(residuals[h]) == n else [math.inf if not pending and current is None else queue.duration(h)] * n
            charge = charges[h, q]
            for i, end in enumerate(ends):
                slots.append(Slot(h, q, end if h in reachable else math.inf,
                                  max(memory, charge[i]) if i < len(charge) else memory))
        rows[q] = {"batch": str(batch), "allocation_eta_sec": seconds(current), "pool_eta_sec": None}
    return queues, slots, rows


def seconds(value):
    return max(1, round(value)) if value is not None and math.isfinite(value) else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="deployed scheduler directory")
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--json", action="store_true", help="report both ETAs in seconds")
    args = parser.parse_args()
    root = args.state_root or args.root / ".parallel-workers/adaptive"
    try:
        sys.path.insert(0, str(args.root.resolve()))
        from adaptive_coordinator import allocate  # type: ignore[reportMissingImports]
        from adaptive_workflow import dependency_hold  # type: ignore[reportMissingImports]
        pool, ready = read_env(root / "pool.env"), read_env(root / "coordinator.ready")
        now, lease = time.time(), int(pool["lease_seconds"])
        if (pool["state"] != "ACTIVE" or ready["pool_id"] != pool["pool_id"] or ready["state"] != "ready"
                or not -2 <= now - int(ready["updated_epoch"]) <= lease or int(ready.get("quarantined_queues", 0))):
            raise ValueError("pool is inactive, stale or has quarantined queues")
        hosts, charges = read_hosts(root, pool, ready, now, lease)
        queues, slots, rows = queue_snapshot(root, pool, now, lease, hosts, charges, dependency_hold)
        cursor_path = root / "cursor.json"
        cursor = json.loads(cursor_path.read_text()) if cursor_path.is_file() else {}
        predicted = forecast(hosts, queues, slots, int(pool["shared_io_capacity"]), allocate, cursor,
                             cpu_deadline=time.process_time() + 1.5)
        for q, eta in predicted.items():
            rows[q]["pool_eta_sec"] = seconds(eta)
    except (OSError, ValueError, KeyError, ImportError, TypeError) as error:
        print(f"pool ETA unavailable: {error}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        for row in rows.values():
            print(row["batch"], row["allocation_eta_sec"] or -1, row["pool_eta_sec"] or -1, sep="\t")
    return 0


if __name__ == "__main__":
    sys.exit(main())
