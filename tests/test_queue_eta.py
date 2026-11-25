import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "monitor"))
from monitor_queue_eta import current_eta, sample_jobs, shared_eta, shard_eta


class QueueEtaTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.batch = Path(directory.name)
        for name in ("claims", "done", "shards"):
            (self.batch / name).mkdir()

    def job(self, name, start, finish=None):
        claim = self.batch / "claims" / f"job_{name}.claim"
        claim.mkdir()
        os.utime(claim, (start, start))
        if finish is not None:
            marker = self.batch / "done" / f"job_{name}.done"
            marker.touch()
            os.utime(marker, (finish, finish))

    def test_pauses_and_current_workers(self):
        self.job("early", 10, 30)
        self.job("after_long_pause", 8000, 8040)
        self.assertEqual(shared_eta(self.batch, 12, 2, 9000), 180)
        self.assertEqual(shared_eta(self.batch, 12, 4, 9500), 90)
        self.assertIsNone(shared_eta(self.batch, 12, 0, 9500))

    def test_work_already_in_progress(self):
        self.job("one", 100, 140)
        self.job("two", 110, 150)
        self.job("active1", 180)
        self.job("active2", 190)
        self.assertEqual(shared_eta(self.batch, 2, 2, 200), 30)
        self.assertEqual(shared_eta(self.batch, 3, 2, 200), 60)
        self.assertIsNone(shared_eta(self.batch, 2, 2, 250))

    def test_legacy_and_inconsistent_times(self):
        (self.batch / "done" / "job_legacy.done").touch()
        self.job("invalid", 200, 100)
        self.assertIsNone(shared_eta(self.batch, 10, 2, 900))

    def test_dispatch_requires_a_series_of_adjacent_serial_reservations(self):
        def observed(serial, start, end):
            self.job(str(serial), start, end)
            claim = self.batch / "claims" / f"job_{serial}.claim"
            (claim / "owner.json").write_text(json.dumps({"reservation_id": f"pool.node.{serial}"}))
            os.utime(claim, (start, start))

        for serial in range(6):
            observed(serial, 100 + serial * 15, 107 + serial * 15)
        # A hold, an intervening reservation, and overlapping jobs are not refills.
        observed(6, 800, 810)
        observed(8, 815, 825)
        observed(9, 817, 827)
        observed(10, 830, None)
        times = sample_jobs(self.batch, 832, "pool")
        self.assertEqual(times.dispatch, {"node": (8, 5)})
        unchanged_tail = current_eta(self.batch, 1, 1, 832, times)
        self.assertEqual(unchanged_tail, current_eta(self.batch, 1, 1, 832, times, {"node": 1}))
        baseline = current_eta(self.batch, 3, 1, 832, times)
        corrected = current_eta(self.batch, 3, 1, 832, times, {"node": 1})
        assert baseline is not None and corrected is not None
        self.assertAlmostEqual(corrected - baseline, 16)
        self.assertFalse(sample_jobs(self.batch, 832, "different-pool").dispatch)

    def test_recent_claim_window_includes_unfinished_work_without_changing_the_mean(self):
        queue = []
        for i in range(130):
            queue.append(f"{i} run\n")
            self.job(str(i), 100 + i, 110 + i if i < 100 else None)
        source = "".join(queue)
        (self.batch / "queue-edit.txt").write_text(source)
        (self.batch / "claim-cursor.json").write_text(json.dumps({"offset": len(source), "previous_job_id": "129"}))
        times = sample_jobs(self.batch, 500, "pool")
        self.assertEqual(times.durations, [10] * 100)
        self.assertEqual(times.warning(500, 30), "recent_slower")
        self.assertEqual(times.warning(500, 0), "")

    def test_fixed_shards_finish_on_the_slowest_host(self):
        (self.batch / "run_meta.env").write_text(
            "run_token=q\nstart_epoch=100\nshard_progress_sync_sec=30\n")
        (self.batch / "launch_status.env").write_text("state=active\nrun_token=q\nupdated_epoch=1000\n")
        for name, finished, running in [("host_01", 8, 2), ("host_02", 4, 1)]:
            (self.batch / "shards" / (name + ".env")).write_text(
                f"state=RUNNING\ntotal_jobs=10\nfinished_jobs={finished}\n"
                f"worker_slots=2\nrunning_jobs={running}\nupdated_epoch=1100\n")
        self.assertEqual(shard_eta(self.batch, 20, 1100), 300)
        self.assertIsNone(shard_eta(self.batch, 20, 1250))
        self.assertIsNone(shard_eta(self.batch, 30, 1100))
