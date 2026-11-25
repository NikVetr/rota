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
    dispatch_seconds: dict = field(default_factory=dict)

    def duration(self, host):
        return self.host_seconds.get(host, self.typical) + self.dispatch_seconds.get(host, 0)


@dataclass
class Slot:
    host: str
    queue: str
    finish: float
    memory: int


def contested_slots(hosts, queues, live, pending, completed, slots, targets, io_capacity, early_waiters=()):
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
                    and profile.get("gpu_units_per_job", 0) <= host.get("gpu_units", 0)
                    and profile["shared_io_units_per_job"] <= io_capacity):
                levels[q] = level
                if profile["shared_io_units_per_job"] > free_io:
                    io_waiters.add((h, q))
        for q, target in targets[h].items():
            if not target:
                continue
            priority = queues[q].profile["host_priorities"].get(h, queues[q].profile["schedule_priority"])
            if priority != "off" and any(other != q and (other in early_waiters or value > priority or
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


def forecast(hosts, queues, slots, io_capacity, allocate, cursor=None, cpu_deadline=math.inf, backfill=False):
    """Discrete finishes, bulk-refilling unchanged slots across long backlogs. With backfill, each
    plan sees the simulated slots' finish times as the hosts' release estimates."""
    # Unknown runtimes occupy their placement indefinitely. Other hosts/queues
    # can still yield a forecast without guessing when those workers will free up.
    pending = {q: item.pending for q, item in queues.items()}
    sample_queues = {q: item.profile["early_sample_remaining"] for q, item in queues.items()
                     if "early_sample_remaining" in item.profile}
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
            if sample_queues:
                for profile in profiles:
                    q = profile["queue_id"]
                    if q in sample_queues:
                        remaining = sample_queues[q] - (queues[q].pending - pending[q])
                        if remaining >= 0:
                            profile["early_sample_remaining"] = remaining
                        else:
                            del profile["early_sample_remaining"]
            plan_hosts, policy = hosts, {}
            if backfill:
                releases = defaultdict(list)
                for s in slots:
                    if math.isfinite(s.finish):
                        releases[s.host].append([max(0, math.ceil(s.finish - now)),
                                                 queues[s.queue].profile["cpu_units_per_job"], s.memory])
                plan_hosts = [dict(h, release_estimates=sorted(releases[h["host_id"]])) for h in hosts]
                policy = {"backfill": True}
            plan = allocate({"schema_version": 1, "hosts": plan_hosts, "queues": profiles,
                             "shared_io_capacity": io_capacity, "allow_unschedulable_queues": True,
                             "cursor": cursor or {}, **policy, "reservations": [
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
            # Reallocate on the next sample finish, or on the next release that
            # could let a waiting sample start. Never bulk-refill across it.
            sampling = {q: budget - (queues[q].pending - pending[q]) for q, budget in sample_queues.items()
                        if q in live and queues[q].pending - pending[q] <= budget}
            holders = {(s.host, s.queue) for s in slots if s.queue in sampling} if sampling else set()
            waiters = {q for q, remaining in sampling.items() if remaining and
                       queues[q].profile["admission"] == "enabled" and not queues[q].depends - completed}
            contested = contested_slots(hosts, queues, live, pending, completed, slots, targets, io_capacity,
                                        () if holders else waiters)
            if plan.get("early_sample") or waiters:
                contested.update(holders)
            if contested and not (holders and (waiters or plan.get("early_sample"))):
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
        if h["gpu_units"]:
            host["gpu_units"] = h["gpu_units"]
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


def queue_snapshot(state_root, pool, now, lease, hosts, charges, dependency_hold, backfill_runtime=None):
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
        if int(res.get("gpu_units_per_job", 0)):
            profile["gpu_units_per_job"] = int(res["gpu_units_per_job"])
        runtime = backfill_runtime(batch) if backfill_runtime else None
        if runtime is not None:
            profile["backfill_runtime_seconds"] = runtime
        meta = read_env(batch / "run_meta.env")
        if meta["run_token"] != q or meta["pool_id"] != pool["pool_id"]:
            raise ValueError("queue run metadata changed")
        total = int(meta["total_jobs"])
        times = sample_jobs(batch, now, pool["pool_id"])
        pending = max(0, total - times.claimed)
        if res.get("early_eta_sample") == "1" and total >= 20 and times.claimed <= 2:
            profile["early_sample_remaining"] = 2 - times.claimed
        if not pending and not count:
            continue
        typical = mean(times.durations) if times.durations else math.inf
        rates = {h: mean(v) for h, v in times.by_host.items() if len(v) >= 3}
        queue = Queue(profile, pending, typical, rates, depends,
                      succeeds=times.done + pending + count == total and not int(status.get("share_interrupted_jobs", 0)),
                      dispatch_seconds={h: gap for h, (gap, _) in times.dispatch.items()})
        queues[q] = queue
        residuals = defaultdict(list)
        oldest_age = 0
        if len(times.active) == count and times.durations:
            for name in times.active:
                claim = batch / "claims" / name
                h = claim_host(claim, pool["pool_id"])
                if h is not None:
                    age = max(0, now - claim.stat().st_mtime)
                    oldest_age = max(oldest_age, age)
                    samples = times.by_host[h] if h in rates else times.durations
                    left = residual(samples, age, not pending)
                    residuals[h].append(left if left is not None else math.inf)
        current = current_eta(batch, pending + count, count, now, times, occupied)
        for h, n in occupied.items():
            ends = residuals[h] if len(residuals[h]) == n else [math.inf if not pending and current is None else rates.get(h, typical)] * n
            charge = charges[h, q]
            for i, end in enumerate(ends):
                slots.append(Slot(h, q, end if h in reachable else math.inf,
                                  max(memory, charge[i]) if i < len(charge) else memory))
        rows[q] = {"batch": str(batch), "allocation_eta_sec": seconds(current), "pool_eta_sec": None,
                   "sample_jobs": len(times.durations), "mean_runtime_sec": typical if times.durations else None,
                   "dispatch_by_host": {h: {"seconds": gap, "pairs": pairs} for h, (gap, pairs) in times.dispatch.items()},
                   "eta_warning": times.warning(now, count, oldest_age)}
    return queues, slots, rows


def seconds(value):
    return max(1, round(value)) if value is not None and math.isfinite(value) else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="deployed scheduler directory")
    parser.add_argument("--state-root", type=Path)
    parser.add_argument("--json", action="store_true", help="report both ETAs in seconds")
    parser.add_argument("--allocation-only", action="store_true", help="skip the pool simulation")
    parser.add_argument("--with-warnings", action="store_true", help="append the timing warning to tab-separated rows")
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
        backfill = pool.get("backfill") == "1"
        runtime = None
        if backfill:
            # The coordinator's own estimate: p90 of the queue's recent successful runtimes.
            from adaptive_coordinator import BACKFILL_MIN_SAMPLES, job_timing, nearest_rank  # type: ignore[reportMissingImports]

            def backfill_runtime(batch):
                durations = job_timing(batch / "claims", batch / "done", {})["durations"]
                return math.ceil(nearest_rank(durations, 0.9)) if len(durations) >= BACKFILL_MIN_SAMPLES else None
            runtime = backfill_runtime
        queues, slots, rows = queue_snapshot(root, pool, now, lease, hosts, charges, dependency_hold, runtime)
        cursor_path = root / "cursor.json"
        cursor = json.loads(cursor_path.read_text()) if cursor_path.is_file() else {}
        predicted = {} if args.allocation_only else forecast(
            hosts, queues, slots, int(pool["shared_io_capacity"]), allocate, cursor,
            cpu_deadline=time.process_time() + 1.5, backfill=backfill)
        for q, eta in predicted.items():
            rows[q]["pool_eta_sec"] = seconds(eta)
    except (OSError, ValueError, KeyError, ImportError, TypeError) as error:
        print(f"pool ETA unavailable: {error}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        for row in rows.values():
            fields = [row["batch"], row["allocation_eta_sec"] or -1, row["pool_eta_sec"] or -1]
            if args.with_warnings:
                fields.append(row["eta_warning"] or "-")
            print(*fields, sep="\t")
    return 0


if __name__ == "__main__":
    sys.exit(main())
