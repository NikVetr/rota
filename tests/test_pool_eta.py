import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
# The scheduler modules are siblings when deployed on the coordinator.
SCHEDULER = ROOT / "scheduler"
sys.path.insert(0, str(SCHEDULER))
from adaptive_coordinator import allocate  # type: ignore[reportMissingImports]
from monitor_pool_eta import Queue, Slot, forecast


def queue(jobs, seconds: float = 60, priority=50, memory=100, cap=0, **extra):
    return Queue(dict(cpu_units_per_job=1, memory_mib_per_job=memory, shared_io_units_per_job=1,
                      max_workers=cap, schedule_priority=priority, host_priorities={}, admission="enabled"),
                 jobs, seconds, **extra)


def host(name="node", cpu=2, mem=1000, **extra):
    return dict(host_id=name, cpu_units=cpu, memory_mib=mem, **extra)


class PoolEtaTests(unittest.TestCase):
    def test_two_early_jobs_then_normal_priority_after_slot_release(self):
        sample = queue(20, 5)
        jobs = {"sample": sample, "high": queue(9, 10, priority=90)}
        running = [Slot("node", "high", 30, 100)]
        for remaining in (0, 1, 2):
            sample.profile["early_sample_remaining"] = remaining
            self.assertEqual(forecast([host(cpu=1)], jobs, running, 1, allocate),
                             {"high": 120 + 5 * remaining, "sample": 220})

    def test_equal_peers_finish_in_ten_and_thirty_five_minutes(self):
        jobs = {"A": queue(9), "B": queue(59)}
        running = [Slot("node", q, 60, 100) for q in jobs]
        self.assertEqual(forecast([host()], jobs, running, 2, allocate), {"A": 600, "B": 2100})

    def test_only_future_jobs_pay_measured_dispatch_on_their_host(self):
        q = queue(2, 7, dispatch_seconds={"node": 8})
        running = [Slot("node", "A", 3, 100)]
        self.assertEqual(forecast([host(cpu=1)], {"A": q}, running, 1, allocate), {"A": 33})
        self.assertEqual(forecast([host("other", cpu=1)], {"A": q}, [], 1, allocate), {"A": 14})
        self.assertEqual(forecast([host()], {"A": queue(3, math.inf)}, [], 2, allocate), {})

    def test_coordinator_snapshot_reaches_both_reported_estimates(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = Path(tmp)
            for name in ("last-known", "queues"):
                (state / name).mkdir()
            now = int(time.time())
            resource = state / "hosts.tsv"
            resource.write_text("host_id\tenabled\tcpu_units\tmemory_mib\nnode\t1\t2\t1000\n")
            (state / "pool.env").write_text(
                f"pool_id=pool\nstate=ACTIVE\nlease_seconds=60\nshared_io_capacity=2\nresources_file={resource}\n")
            (state / "coordinator.ready").write_text(f"pool_id=pool\nstate=ready\nupdated_epoch={now}\n")
            (state / "last-known/node.tsv").write_text(
                "record_type\tpool_id\thost_id\tstate\ttelemetry_state\tresource_sample_epoch\tlive_cpu_units\tlive_memory_mib\n"
                f"agent\tpool\tnode\tactive\tok\t{now}\t2\t1000\n")
            for q, left in [("A", 10), ("B", 60)]:
                batch = state / q
                for name in ("adaptive", "claims", "done"):
                    (batch / name).mkdir(parents=True)
                (state / "queues" / (q + ".env")).write_text(f"queue_id={q}\npool_id=pool\nbatch_dir={batch}\n")
                (batch / "run_meta.env").write_text(f"run_token={q}\npool_id=pool\ntotal_jobs={left+1}\n")
                (batch / "adaptive/resources.env").write_text("cpu_units_per_job=1\nshared_io_units_per_job=1\n")
                (batch / "adaptive/status.env").write_text(
                    f"queue_id={q}\nupdated_epoch={now}\nlifecycle=RUNNING\nrunning_by_host=node:1\n"
                    "admission=enabled\nblocking_reason=none\neffective_memory_mib_per_job=100\n"
                    "schedule_priority=50\nmax_workers=0\n")
                for job, start in [("sample", now-100), ("running", now)]:
                    claim = batch / "claims" / ("job_" + job + ".claim")
                    claim.mkdir()
                    (claim / "owner.json").write_text(json.dumps({"reservation_id": "pool.node.1"}))
                    os.utime(claim, (start, start))
                done = batch / "done/job_sample.done"
                done.touch()
                os.utime(done, (now - 40, now - 40))
            report = json.loads(subprocess.check_output(
                [sys.executable, str(ROOT / "monitor_pool_eta.py"), "--root", str(SCHEDULER),
                 "--state-root", str(state), "--json"], text=True))
            for q, fixed, pool in [("A", 600, 600), ("B", 3600, 2100)]:
                self.assertAlmostEqual(report[q]["allocation_eta_sec"], fixed, delta=3)
                self.assertAlmostEqual(report[q]["pool_eta_sec"], pool, delta=3)

    def test_tail_runs_to_completion_and_waiting_queue_uses_first_released_slot(self):
        jobs = {"A": queue(0, 60), "B": queue(1, 30)}
        self.assertEqual(forecast([host()], jobs, [Slot("node","A",10,100), Slot("node","A",100,100)],
                                  2, allocate), {"A": 100, "B": 40})

    def test_caps_memory_io_and_unavailable_hosts(self):
        for hosts, capacity, jobs in [
            ([host()], 1, queue(4)), ([host(mem=150)], 2, queue(4)),
            ([host()], 2, queue(4, cap=1)),
            ([host(cpu=1), host("away", drain_only=True)], 3, queue(4)),
        ]:
            self.assertEqual(forecast(hosts, {"A": jobs}, [], capacity, allocate)["A"], 240)

    def test_dependencies_priority_and_measured_host_speed(self):
        jobs = {"A": queue(1, 10, priority=90), "B": queue(2, depends={"A"}, host_seconds={"slow": 120})}
        jobs["A"].profile["host_priorities"] = {"slow":"off"}
        self.assertEqual(forecast([host("quick",cpu=1),host("slow",cpu=1)], jobs, [], 2, allocate),
                         {"A":10, "B":130})
        jobs["A"].succeeds = False
        self.assertEqual(forecast([host("quick",cpu=1),host("slow",cpu=1)], jobs, [], 2, allocate), {"A":10})

    def test_untimed_competitor_keeps_its_capacity(self):
        out = forecast([host()], {"A":queue(100,math.inf),"B":queue(10)}, [], 2, allocate)
        self.assertEqual(out, {"B":600})

    def test_one_shared_slot_rotates_without_starving_equal_priorities(self):
        result = forecast([host(cpu=1)], {"A":queue(10001), "B":queue(10001)}, [], 1, allocate)
        self.assertEqual(sorted(result.values()), [20001*60, 20002*60])

    def test_shared_io_released_on_one_host_can_start_a_peer_on_another(self):
        hosts = [host("left", cpu=1), host("right", cpu=1)]
        queues = {"A": queue(9), "B": queue(1)}
        queues["A"].profile["host_priorities"] = {"right": "off"}
        queues["B"].profile["host_priorities"] = {"left": "off"}
        running = [Slot("left", "A", 60, 100)]
        cursor = {"50": "A"}
        self.assertEqual(forecast(hosts, queues, running, 1, allocate, cursor), {"B": 120, "A": 660})
        self.assertEqual(forecast(hosts, queues, running, 2, allocate, cursor), {"B": 60, "A": 600})

    def test_device_capacity_serializes_gpu_jobs(self):
        jobs = queue(12, 60)
        jobs.profile["gpu_units_per_job"] = 1
        self.assertEqual(forecast([host(cpu=4, gpu_units=1)], {"A": jobs}, [], 4, allocate), {"A": 720})

    def test_backfill_replay_lets_short_jobs_use_capacity_held_for_a_large_job(self):
        def queues():
            large = queue(1, 60, priority=90)
            large.profile["cpu_units_per_job"] = 2
            small = queue(10, 60)
            small.profile["backfill_runtime_seconds"] = 60
            return {"L": large, "S": small}
        running = [Slot("node", "S", 600, 100)]
        held = forecast([host()], queues(), running, 4, allocate)
        backfilled = forecast([host()], queues(), running, 4, allocate, backfill=True)
        # Held: S waits for L (600-660), then runs two at a time. Backfilled: S uses the held CPU
        # while 60 s + 30 s margin still ends before L could start, leaving one job for after L.
        self.assertEqual(held, {"L": 660, "S": 960})
        self.assertEqual(backfilled, {"L": 660, "S": 720})

    def test_large_backlog_skips_equivalent_waves(self):
        self.assertEqual(forecast([host()], {"A":queue(1_000_001)}, [], 2, allocate), {"A":500001*60})
