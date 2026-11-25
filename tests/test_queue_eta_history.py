import tempfile
import unittest
import sqlite3
from contextlib import closing
from pathlib import Path

from queue_eta_history import record, report


class HistoryTests(unittest.TestCase):
    def test_frozen_forecasts_follow_only_their_own_run_to_completion(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "history.sqlite3"
            base = dict(id="one", started=100, label="short", project="p", running=1,
                        done=1, total=20, elapsed=10, eta=300, state="RUNNING")
            record(path, [dict(base, done=0, eta=None)])
            self.assertEqual(report(path, ""), [])
            record(path, [base])
            record(path, [dict(base, elapsed=20, eta=200)])
            record(path, [dict(base, done=10, elapsed=100, eta=150, eta_warning="recent_slower")])
            record(path, [dict(base, started=200, state="COMPLETED", done=20, ended=250)])
            rows = report(path, "short")
            self.assertEqual([(r["predicted"], r["actual"]) for r in rows], [(350, None), (410, None)])
            record(path, [dict(base, elapsed=20, eta=5)])
            self.assertEqual([r["predicted"] for r in report(path, "short", True)], [350, 320, 410])
            record(path, [dict(base, state="FINISHED_INCOMPLETE", ended=400)])
            self.assertTrue(all(r["outcome"] == "FINISHED_INCOMPLETE" and r["error_sec"] is None
                                for r in report(path, "")))
            record(path, [base])
            self.assertTrue(all(r["outcome"] is None for r in report(path, "")))
            record(path, [dict(base, done=20, state="COMPLETED", ended=360)])
            self.assertEqual([r["error_sec"] for r in report(path, "one")], [10, -50])
            record(path, [base])
            self.assertEqual([r["error_sec"] for r in report(path, "one")], [10, -50])

    def test_preserves_existing_quarterly_checkpoints(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "history"
            with closing(sqlite3.connect(path)) as db, db:
                db.execute("""CREATE TABLE forecasts (id, started, quarter, label, project, issued, done,
                              total, workers, eta, warning, outcome, actual)""")
                db.execute("INSERT INTO forecasts VALUES ('x',1,0,'jobs','p',10,1,8,1,20,NULL,'COMPLETED',33)")
            record(path, [])
            self.assertEqual(report(path, "jobs")[0]["error_sec"], 3)
