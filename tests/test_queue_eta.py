import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from monitor_queue_eta import shared_eta, shard_eta


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
