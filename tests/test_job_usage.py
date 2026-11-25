#!/usr/bin/env python3
"""Per-queue CPU totals from per-job usage records, and backfill timing from claims and done markers."""

import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scheduler"))
SPEC = importlib.util.spec_from_file_location("coordinator", ROOT / "scheduler/adaptive_coordinator.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def record(directory, job, exit_code, user, system, complete=True):
    lines = [f"job_id={job}", "host=h1", f"exit_code={exit_code}", "wall_seconds=9"]
    if complete:
        lines += [f"cpu_user_seconds={user}", f"cpu_system_seconds={system}"]
    (directory / f"job_{job}.env").write_text("\n".join(lines) + "\n")


class UsageTotalsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.batch = Path(temporary.name)
        self.usage = self.batch / "usage"

    def test_missing_directory_is_zero(self):
        self.assertEqual(MODULE.usage_totals(self.batch, {}),
                         {"state": "ok", "jobs": 0, "succeeded": 0.0, "failed": 0.0})

    def test_sums_succeeded_and_failed_jobs(self):
        self.usage.mkdir()
        record(self.usage, "a", 0, 10.5, 1.5)
        record(self.usage, "b", 0, 2, 0)
        record(self.usage, "c", 7, 3, 1)
        totals = MODULE.usage_totals(self.batch, {})
        self.assertEqual((totals["jobs"], totals["succeeded"], totals["failed"]), (3, 14.0, 4.0))

    def test_incomplete_record_is_read_on_a_later_scan(self):
        self.usage.mkdir()
        cache = {}
        record(self.usage, "a", 0, 1, 1, complete=False)
        self.assertEqual(MODULE.usage_totals(self.batch, cache)["jobs"], 0)
        # Completing the file in place does not change the directory's mtime.
        stat = self.usage.stat()
        record(self.usage, "a", 0, 1, 1)
        os.utime(self.usage, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.assertEqual(MODULE.usage_totals(self.batch, cache)["succeeded"], 2.0)

    def test_unchanged_directory_is_not_reread(self):
        self.usage.mkdir()
        cache = {}
        record(self.usage, "a", 0, 1, 0)
        MODULE.usage_totals(self.batch, cache)
        stat = self.usage.stat()
        (self.usage / "job_a.env").write_text("garbage\n")  # records are immutable once complete
        os.utime(self.usage, ns=(stat.st_atime_ns, stat.st_mtime_ns))
        self.assertEqual(MODULE.usage_totals(self.batch, cache)["succeeded"], 1.0)


class JobTimingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.claims, self.done = root / "claims", root / "done"
        self.claims.mkdir()
        self.done.mkdir()

    def claim(self, job, at, reservation=None):
        path = self.claims / f"job_{job}.claim"
        path.mkdir()
        if reservation:
            (path / "owner.json").write_text(f'{{"reservation_id": "{reservation}", "job_id": "{job}"}}')
        os.utime(path, (at, at))

    def finish(self, job, at):
        path = self.done / f"job_{job}.done"
        path.write_text("")
        os.utime(path, (at, at))

    def test_durations_and_running_claims(self):
        for index in range(6):
            self.claim(f"d{index}", 1000 + index, f"p.h.{index}")
            self.finish(f"d{index}", 1100 + index * 11)
        self.claim("run", 2000, "p.h.9")
        timing = MODULE.job_timing(self.claims, self.done, {})
        self.assertEqual(timing["durations"], [100.0, 110.0, 120.0, 130.0, 140.0, 150.0])
        self.assertEqual(timing["starts"], {"p.h.9": 2000.0})

    def test_done_before_claim_is_not_a_sample(self):
        self.claim("skewed", 1000)
        self.finish("skewed", 900)  # clock skew between hosts
        self.claim("fine", 1000)
        self.finish("fine", 1100)
        self.assertEqual(MODULE.job_timing(self.claims, self.done, {})["durations"], [100.0])

    def test_only_recent_completions_count(self):
        for index in range(MODULE.BACKFILL_SAMPLE_JOBS + 5):
            self.claim(f"j{index}", 0)
            self.finish(f"j{index}", 10 + index)
        timing = MODULE.job_timing(self.claims, self.done, {})
        self.assertEqual(len(timing["durations"]), MODULE.BACKFILL_SAMPLE_JOBS)
        self.assertEqual(timing["durations"][0], 15.0)

    def test_claim_without_owner_is_retried(self):
        cache = {}
        self.claim("run", 2000)
        self.assertEqual(MODULE.job_timing(self.claims, self.done, cache)["starts"], {})
        (self.claims / "job_run.claim" / "owner.json").write_text('{"reservation_id": "p.h.1"}')
        os.utime(self.claims / "job_run.claim", (2000, 2000))  # owner.json follows mkdir at once
        self.assertEqual(MODULE.job_timing(self.claims, self.done, cache)["starts"], {"p.h.1": 2000.0})

    def test_finished_claim_leaves_the_running_set(self):
        cache = {}
        self.claim("a", 100, "p.h.1")
        self.assertIn("p.h.1", MODULE.job_timing(self.claims, self.done, cache)["starts"])
        self.finish("a", 200)
        timing = MODULE.job_timing(self.claims, self.done, cache)
        self.assertEqual((timing["starts"], timing["durations"]), ({}, [100.0]))

    def test_nearest_rank(self):
        values = [float(v) for v in range(1, 11)]
        self.assertEqual((MODULE.nearest_rank(values, 0.1), MODULE.nearest_rank(values, 0.9)), (1.0, 9.0))


class BackfillEstimateTests(unittest.TestCase):
    def test_release_estimates_and_runtimes(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as name:
            batch = Path(name)
            claims, done = batch / "claims", batch / "done"
            claims.mkdir()
            done.mkdir()
            for index in range(5):  # durations 100..140 s
                (claims / f"job_d{index}.claim").mkdir()
                os.utime(claims / f"job_d{index}.claim", (1000, 1000))
                (done / f"job_d{index}.done").write_text("")
                os.utime(done / f"job_d{index}.done", (1100 + 10 * index,) * 2)
            for job, reservation, started in (("r1", "p.h1.1", 5000), ("r2", "p.h1.2", 4900)):
                (claims / f"job_{job}.claim").mkdir()
                (claims / f"job_{job}.claim" / "owner.json").write_text(
                    f'{{"reservation_id": "{reservation}"}}')
                os.utime(claims / f"job_{job}.claim", (started, started))
            queues = {"q": {
                "registration": {"log_dir": str(batch), "batch_dir": str(batch)},
                "allocator": {"cpu_units_per_job": 1, "memory_mib_per_job": 1024},
            }}
            polled = {"h1": {
                "reservation_ids": {"p.h1.1": "q", "p.h1.2": "q", "p.h1.3": "q"},
                "reservation_charges_mib": {"p.h1.1": 2048},
            }}
            fake = SimpleNamespace(timing_cache={})
            releases, runtimes = MODULE.Coordinator._backfill_estimates(fake, queues, polled, 5030)
        # p10 = 100 s: the job claimed 30 s ago has 70 s left; the older one 0. p.h1.3 has
        # not claimed yet, so it has no estimate (it may end at once).
        self.assertEqual(releases, {"h1": [[70, 1, 2048], [0, 1, 1024]]})
        self.assertEqual(runtimes, {"q": 140})

    def test_too_few_samples_give_no_estimates(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as name:
            batch = Path(name)
            (batch / "claims").mkdir()
            queues = {"q": {"registration": {"log_dir": str(batch), "batch_dir": str(batch)},
                            "allocator": {"cpu_units_per_job": 1, "memory_mib_per_job": 1024}}}
            releases, runtimes = MODULE.Coordinator._backfill_estimates(
                SimpleNamespace(timing_cache={}), queues, {"h1": {"reservation_ids": {"x": "q"}}}, 0)
        self.assertEqual((releases, runtimes), ({}, {}))


if __name__ == "__main__":
    unittest.main()
