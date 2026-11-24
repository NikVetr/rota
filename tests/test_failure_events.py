import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

_spec = importlib.util.spec_from_file_location(
    "failure_events", Path(__file__).resolve().parents[1] / "failure_events.py")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)
FailureEvents = _module.FailureEvents

T0 = 1_800_000_000.0


def job(job_id, host="node2", reservation=None, last=T0):
    return {"job_id": job_id, "reservation_id": reservation or f"p.1.{host}.{job_id}", "host_id": host,
            "last_activity": last, "log": f"/logs/jobs/job_{job_id}_{host}.log"}


def scheduler(jobs=(), holds=(), ok=True):
    return {"ok": ok, "failures_error": None, "queues": [{
        "queue_id": "q1", "tag": "tag1", "batch_dir": "/srv/cluster/proj/logs/batch_00001_x",
        "project": "proj", "run_name": "fits", "submitted_by": "claude:abc",
        "failed_jobs": list(jobs), "failed_jobs_total": len(jobs), "holds": list(holds)}]}


UP = [{"id": "node2", "ok": True}, {"id": "node1", "ok": True}]
DOWN = [{"id": "node2", "ok": False}, {"id": "node1", "ok": True}]


class FailureEventTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "events.jsonl"
        self.events = self.tracker()
        self.assertEqual(self.events.observe(T0 - 600, UP, scheduler([job("old")])), [])

    def tearDown(self):
        self.directory.cleanup()

    def tracker(self, **options):
        return FailureEvents(self.path, self.path.with_name("state.json"), **dict({"root": "/srv/cluster", "coordinator": "storage"}, **options))

    def written(self):
        return [json.loads(line) for line in self.path.read_text().splitlines()] if self.path.exists() else []

    def test_existing_failures_are_a_baseline_not_events(self):
        self.assertEqual(self.events.observe(T0, UP, scheduler([job("old")])), [])
        self.assertEqual(self.written(), [])

    def test_an_outage_becomes_one_event_after_the_host_returns_and_failures_settle(self):
        self.events.observe(T0 + 30, DOWN, scheduler([job("old")]))
        lost = [job("old"), job("a", last=T0 + 20), job("b", last=T0 + 25)]
        # Failures appear only once the host reports again; they wait for the settle period.
        self.assertEqual(self.events.observe(T0 + 3600, UP, scheduler(lost)), [])
        self.assertEqual(self.events.observe(T0 + 3660, UP, scheduler(lost)), [])
        [event] = self.events.observe(T0 + 3725, UP, scheduler(lost))
        self.assertEqual((event["kind"], event["queue_id"], event["conversation"], event["host_id"]),
                         ("job_failures", "q1", "claude:abc", "node2"))
        self.assertEqual([(entry["job_id"], entry["kind"]) for entry in event["jobs"]],
                         [("a", "outage"), ("b", "outage")])
        self.assertEqual(event["outages"], [{"host_id": "node2", "started": T0 + 30, "ended": T0 + 3600}])
        self.assertEqual(self.written(), [event])
        # Reported once, also across a restart.
        self.assertEqual(self.tracker().observe(T0 + 9000, UP, scheduler(lost)), [])

    def test_a_host_still_down_holds_the_incident(self):
        lost = [job("old"), job("c", last=T0)]
        self.events.observe(T0 + 10, DOWN, scheduler(lost))
        self.assertEqual(self.events.observe(T0 + 1000, DOWN, scheduler(lost)), [])
        self.assertEqual(len(self.events.observe(T0 + 1030, UP, scheduler(lost))), 1)

    def test_holds_report_with_a_reset_command_and_again_after_clearing(self):
        hold = {"host_id": "node2", "reason": "infrastructure_failed"}
        self.events.observe(T0, UP, scheduler([job("old")], [hold]))
        [event] = self.events.observe(T0 + 200, UP, scheduler([job("old")], [hold]))
        self.assertEqual((event["kind"], event["jobs"]), ("hold", []))
        self.assertIn("reset-failures --batch_dir /srv/cluster/proj/logs/batch_00001_x --host_id node2",
                      event["holds"][0]["reset_command"])
        self.events.observe(T0 + 300, UP, scheduler([job("old")]))  # cleared
        self.events.observe(T0 + 400, UP, scheduler([job("old")], [hold]))
        self.assertEqual(len(self.events.observe(T0 + 600, UP, scheduler([job("old")], [hold]))), 1)

    def test_jobs_rerun_before_settling_are_not_reported(self):
        self.events.observe(T0, UP, scheduler([job("old"), job("d")]))
        self.assertEqual(self.events.observe(T0 + 300, UP, scheduler([job("old")])), [])
        self.assertEqual(self.written(), [])

    def test_failures_without_an_outage_are_unknown_and_a_failed_scan_changes_nothing(self):
        self.events.observe(T0, UP, scheduler([job("old"), job("e", host="node1", last=T0)]))
        self.assertEqual(self.events.observe(T0 + 300, UP, scheduler(ok=False)), [])
        [event] = self.events.observe(T0 + 330, UP, scheduler([job("old"), job("e", host="node1", last=T0)]))
        self.assertEqual((event["jobs"][0]["kind"], event["outages"]), ("unknown", []))

    def test_the_event_log_rotates(self):
        events = self.tracker(max_bytes=10)
        events.observe(T0, UP, scheduler([job("old"), job("f")]))
        events.observe(T0 + 200, UP, scheduler([job("old"), job("f")]))
        events.observe(T0 + 300, UP, scheduler([job("old"), job("f"), job("g", last=T0 + 300)]))
        events.observe(T0 + 500, UP, scheduler([job("old"), job("f"), job("g", last=T0 + 300)]))
        self.assertTrue(self.path.with_name("events.jsonl.1").exists())
        self.assertEqual([entry["job_id"] for entry in self.written()[0]["jobs"]], ["g"])


if __name__ == "__main__":
    unittest.main()
