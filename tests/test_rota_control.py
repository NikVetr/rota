"""rota control describe/apply and the scheduler snapshot it is built from."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


control = load("rota_control")
snapshot_module = load("scheduler_snapshot")
TAG = "_srv_cluster_proj__srv_cluster_proj_jobs.txt__h1234__b00003"
QUEUE = {
    "queue_id": "batch_00003_q", "tag": TAG, "batch_dir": "/srv/cluster/proj/logs/batch_00003",
    "run_name": "fits", "project": "proj", "source_dir": "/home/u/proj", "submitted_by": "claude:abc",
    "lifecycle": "RUNNING", "blocking_reason": "none", "running_workers": 2, "target_workers": 3,
    "share_interrupted_jobs": 0,
    "control": {"generation": 4, "admission": "enabled", "schedule_priority": 50,
                "host_priorities": {"node5": "off"}, "max_workers": 0, "memory_mib_per_job": 1024,
                "effective_memory_mib_per_job": 1024,
                "calibration": {"jobs": 0, "seconds": 0, "minimum_mib": None, "state": "off"}},
}
HOSTS = [{"id": "node1", "enabled": True, "base_cpu_units": 10, "base_memory_mib": 14336, "cpu_units": 10,
          "memory_mib": 14336, "away": False, "away_window": None},
         {"id": "node5", "enabled": True, "base_cpu_units": 10, "base_memory_mib": 14336, "cpu_units": 10,
          "memory_mib": 14336, "away": True, "away_window": {"away_from": 1, "away_until": None, "reason": "trip"}}]
STATE = {"schema": 1, "updated": 0, "pool": {"id": "pool-1", "name": "mixed", "state": "ACTIVE",
                                            "coordinator_live": True, "away_hosts": ["node5"],
                                            "unavailable_hosts": {}, "running_workers": 2},
         "hosts": HOSTS, "queues": [QUEUE], "commands": ["confirm-interrupted-jobs", "set-host-budget", "pool-drain"]}


def collector_snapshot(state=STATE, age=5):
    now = time.time()
    return {"schema": 1, "shared": {"host": "storage", "pool": {"capacity": 50, "io_state": "ok"}},
            "queues": [{"id": TAG, "label": "fits", "batch": 3}],
            "scheduler": dict(copy.deepcopy(state), updated=now - age, ok=True, error=None)}


def controls(entry):
    return {item["name"]: item for item in entry["controls"]}


class DescribeTests(unittest.TestCase):
    def test_queue_host_and_pool_controls_follow_state(self):
        described = control.describe(collector_snapshot())
        self.assertEqual((described["protocol"], described["coordinator"]["reachable"]), (1, True))
        queue = described["queues"][0]
        self.assertEqual((queue["id"], queue["snapshot_id"], queue["label"]), ("batch_00003_q", TAG, "fits [b00003]"))
        self.assertEqual((queue["source_dir"], queue["conversation"]), ("/home/u/proj", "claude:abc"))
        by_name = controls(queue)
        self.assertTrue(by_name["pause"]["enabled"])
        self.assertFalse(by_name["resume"]["enabled"])
        self.assertEqual(by_name["priority"]["value"], {"default": 50, "hosts": {"node5": "off"}})
        self.assertFalse(by_name["confirm_interrupted"]["enabled"], "nothing to confirm")
        node5 = controls(described["hosts"][1])
        self.assertEqual(node5["availability"]["value"]["state"], "away")
        self.assertFalse(node5["availability"]["enabled"])
        self.assertIn("next fresh pool", node5["availability"]["disabled_reason"])
        drain = controls(described["pool"])["drain"]
        self.assertEqual((drain["enabled"], drain["danger"]), (True, "irreversible"))

    def test_stale_or_failed_state_disables_every_control(self):
        for snapshot in (collector_snapshot(age=600),
                         {"scheduler": {"ok": False, "error": "scheduler snapshot failed: boom"}}):
            described = control.describe(snapshot)
            self.assertFalse(described["coordinator"]["reachable"])
            self.assertTrue(described["coordinator"]["error"])
            for entry in described["queues"] + described["hosts"] + [described["pool"]]:
                self.assertFalse(any(item["enabled"] for item in entry["controls"]), entry["controls"])

    def test_interrupted_jobs_and_closed_queue(self):
        state = copy.deepcopy(STATE)
        state["queues"][0]["share_interrupted_jobs"] = 2
        state["queues"][0]["control"]["admission"] = "closed"
        by_name = controls(control.describe(collector_snapshot(state))["queues"][0])
        self.assertTrue(by_name["confirm_interrupted"]["enabled"])
        for name in ("pause", "resume", "priority", "max_workers", "memory"):
            self.assertFalse(by_name[name]["enabled"], name)


FAKE_SSH = r'''#!/usr/bin/env python3
import json, os, sys
command = sys.argv[-1]
if "scheduler_snapshot.py" in command:
    print(open(os.environ["FAKE_STATE"]).read())
    sys.exit(0)
with open(os.environ["FAKE_LOG"], "a") as log:
    log.write(command + "\n")
mode = os.environ.get("FAKE_MODE", "ok")
if mode == "unreachable":
    sys.exit(255)
if mode.startswith("locked"):
    counter = os.environ["FAKE_LOG"] + ".locks"
    seen = int(open(counter).read()) if os.path.exists(counter) else 0
    if seen < int(mode[6:]):
        open(counter, "w").write(str(seen + 1))
        print("Error: scheduler state is locked: /x", file=sys.stderr)
        sys.exit(1)
if mode == "refuse":
    print("Error: cannot resume while the adaptive pool is not ACTIVE", file=sys.stderr)
    sys.exit(1)
print("generation=5")
'''


class ApplyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "ssh").write_text(FAKE_SSH)
        (self.root / "ssh").chmod(0o755)
        self.log = self.root / "commands.log"
        self.state(STATE)

    def state(self, state):
        (self.root / "state.json").write_text(json.dumps(state))

    def apply(self, request, mode="ok"):
        request = {"protocol": 1, "request_id": "r1", "source": "test", **request}
        env = dict(os.environ, ROTA_ROOT="/srv/cluster", ROTA_COORDINATOR="fixture", ROTA_CONTROL_SSH=str(self.root / "ssh"), FAKE_STATE=str(self.root / "state.json"),
                   FAKE_LOG=str(self.log), FAKE_MODE=mode)
        result = subprocess.run([sys.executable, str(ROOT / "rota_control.py"), "apply"], input=json.dumps(request),
                                capture_output=True, text=True, env=env, timeout=60)
        commands = self.log.read_text().splitlines() if self.log.exists() else []
        self.log.unlink(missing_ok=True)
        return result.returncode, json.loads(result.stdout), commands

    def test_priority_change_runs_one_audited_command(self):
        code, result, commands = self.apply({
            "target": {"kind": "queue", "id": "batch_00003_q"}, "control": "priority",
            "value": {"default": 60, "hosts": {"node5": "off", "node1": 70}},
            "expect": {"pool_id": "pool-1", "value": {"default": 50, "hosts": {"node5": "off"}}}})
        self.assertEqual((code, result["ok"]), (0, True), result)
        self.assertEqual(len(commands), 1)
        self.assertIn("set-priority --batch_dir /srv/cluster/proj/logs/batch_00003 "
                      "--schedule_priority 60,node1=70,node5=off --expected-pool-id pool-1", commands[0])
        self.assertIn("control-audit.tsv", commands[0])
        self.assertIn("r1", commands[0])

    def test_expectations_and_targets_are_checked_before_any_command(self):
        cases = [({"target": {"kind": "queue", "id": "batch_00003_q"}, "control": "max_workers", "value": 4,
                   "expect": {"pool_id": "pool-0"}}, "conflict"),
                 ({"target": {"kind": "queue", "id": "batch_00003_q"}, "control": "max_workers", "value": 4,
                   "expect": {"value": 2}}, "conflict"),
                 ({"target": {"kind": "queue", "id": "missing"}, "control": "pause"}, "unknown_target"),
                 ({"target": {"kind": "host", "id": "node1"}, "control": "availability",
                   "value": {"state": "away", "reason": "meeting"}}, "not_supported"),
                 ({"target": {"kind": "queue", "id": "batch_00003_q"}, "control": "max_workers", "value": -1},
                  "invalid_value"),
                 ({"target": {"kind": "queue", "id": "batch_00003_q"}, "control": "priority",
                   "value": {"default": 1, "hosts": {"nope": 2}}}, "invalid_value")]
        for request, code in cases:
            with self.subTest(code=code, request=request):
                status, result, commands = self.apply(request)
                self.assertEqual((status, result["ok"], result["error"]["code"]), (1, False, code), result)
                self.assertEqual(commands, [])

    def test_memory_floor_alone_is_refused_while_calibrating(self):
        state = copy.deepcopy(STATE)
        state["queues"][0]["control"]["calibration"] = {"jobs": 30, "seconds": 0, "minimum_mib": 256,
                                                         "state": "learning"}
        self.state(state)
        request = {"target": {"kind": "queue", "id": "batch_00003_q"}, "control": "memory"}
        status, result, commands = self.apply(dict(request, value={"memory_mib_per_job": 900}))
        self.assertEqual((status, result["error"]["code"], commands), (1, "not_supported", []))
        status, result, commands = self.apply(dict(request, value={
            "memory_mib_per_job": 900, "calibration_jobs": 20, "calibration_min_mib": 256}))
        self.assertEqual(status, 0, result)
        self.assertIn("set-memory --batch_dir /srv/cluster/proj/logs/batch_00003 --memory_mib_per_job 900 "
                      "--memory_calibration_jobs 20 --memory_calibration_seconds 0 --memory_min_mib_per_job 256",
                      commands[0])
        status, result, commands = self.apply(dict(request, value={
            "memory_mib_per_job": 900, "calibration_jobs": 0}))
        self.assertEqual(status, 0, result)
        self.assertNotIn("calibration", commands[0].split("&&")[0], "turning calibration off passes no policy")

    def test_held_lock_is_retried_then_reported(self):
        request = {"target": {"kind": "queue", "id": "batch_00003_q"}, "control": "pause"}
        status, result, commands = self.apply(request, mode="locked2")
        self.assertEqual((status, result["ok"], len(commands)), (0, True, 3))
        status, result, _ = self.apply(request, mode="locked99")
        self.assertEqual((status, result["error"]["code"], result["error"]["retryable"]), (1, "lock_held", True))

    def test_transport_refusal_and_malformed_requests(self):
        request = {"target": {"kind": "queue", "id": "batch_00003_q"}, "control": "resume"}
        status, result, _ = self.apply(request, mode="unreachable")
        self.assertEqual((status, result["error"]["code"]), (1, "unreachable"))
        status, result, _ = self.apply(request, mode="refuse")
        self.assertEqual((status, result["error"]["code"]), (1, "not_supported"))
        result = subprocess.run([sys.executable, str(ROOT / "rota_control.py"), "apply"], input="{not json",
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(json.loads(result.stdout)["ok"])


class SnapshotTests(unittest.TestCase):
    def test_state_root_becomes_one_document(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "adaptive"
            batch = Path(directory) / "batch"
            (root / "queues").mkdir(parents=True)
            (batch / "adaptive").mkdir(parents=True)
            resources = Path(directory) / "resources.tsv"
            resources.write_text("host_id\tssh_target\tcpu_units\tmemory_mib\tenabled\n"
                                 "node1\tu@a\t10\t14336\t1\nnode2\tu@b\t8\t14336\t1\n")
            (root / "pool.env").write_text(f"pool_id=p1\npool_name=mixed\nstate=ACTIVE\nlease_seconds=20\n"
                                           f"resources_file={resources}\n")
            (root / "coordinator.ready").write_text(
                f"pool_id=p1\nstate=ready\nupdated_epoch={int(time.time())}\naway_hosts=node2\n"
                "unavailable_hosts=node1:telemetry_or_share\n")
            (root / "host-budgets.json").write_text(json.dumps({"hosts": {"node1": {"cpu_units": 4, "memory_mib": 8192}}}))
            (root / "host-states.json").write_text(json.dumps({"hosts": {"node2": {
                "away_from": 1, "away_until": None, "reason": "trip", "set_epoch": 1}}}))
            (root / "queues/q1.env").write_text(f"queue_id=q1\nbatch_dir={batch}\ntag_clean=tag1\nrun_name=fits\n")
            (batch / "adaptive/control.env").write_text(
                "generation=3\nadmission=paused\nschedule_priority=90\nhost_priorities=node5=off,node1=10\n"
                "max_workers=4\nmemory_calibration_jobs=30\nmemory_calibration_seconds=0\nmemory_min_mib_per_job=256\n")
            (batch / "adaptive/resources.env").write_text("memory_mib_per_job=1024\n")
            (batch / "adaptive/status.env").write_text("lifecycle=PENDING\nrunning_workers=1\nshare_interrupted_jobs=2\n"
                                                       "memory_calibration_state=learning\n")
            (batch / "run_meta.env").write_text("project_name=proj\nsubmitted_by=codex:x\n")
            (root / "queues/broken.env").write_text("queue_id=broken\n")
            scheduler = Path(directory) / "adaptive_scheduler.sh"
            scheduler.write_text("Commands:\n  pool-drain [--x]\n  set-host-budget --host_id\n")
            state = snapshot_module.snapshot(root, scheduler)
            with (root / "pool.env").open("a") as stream:
                stream.write("backfill=1\n")
            with (batch / "adaptive/resources.env").open("a") as stream:
                stream.write("gpu_units_per_job=2\nmemory_limit_mib=2048\n")
            with (batch / "adaptive/status.env").open("a") as stream:
                stream.write("running_by_host=node1:1,node2:0\n")
            configured = snapshot_module.snapshot(root, scheduler)
        self.assertEqual(state["pool"]["id"], "p1")
        self.assertFalse(state["pool"]["backfill"])
        self.assertTrue(configured["pool"]["backfill"])
        gpu_queue = configured["queues"][1]
        self.assertEqual(gpu_queue["gpu_units_per_job"], 2)
        self.assertEqual(gpu_queue["memory_limit_mib"], 2048)
        self.assertEqual(gpu_queue["running_by_host"], {"node1": 1, "node2": 0})
        self.assertTrue(state["pool"]["coordinator_live"])
        self.assertEqual((state["pool"]["away_hosts"], state["pool"]["unavailable_hosts"]),
                         (["node2"], {"node1": "telemetry_or_share"}))
        self.assertEqual(state["commands"], ["set-host-budget", "pool-drain"])
        node1, node2 = state["hosts"]
        self.assertEqual((node1["cpu_units"], node1["base_cpu_units"], node1["away"]), (4, 10, False))
        self.assertEqual((node2["away"], node2["away_window"]["reason"]), (True, "trip"))
        broken, queue = state["queues"]
        self.assertIn("error", broken)
        self.assertEqual(queue["gpu_units_per_job"], 0)
        self.assertIsNone(queue["memory_limit_mib"])
        self.assertIsNone(queue["running_by_host"])
        self.assertEqual((queue["tag"], queue["project"], queue["submitted_by"], queue["share_interrupted_jobs"]),
                         ("tag1", "proj", "codex:x", 2))
        self.assertEqual(queue["control"]["host_priorities"], {"node5": "off", "node1": 10})
        self.assertEqual(queue["control"]["memory_mib_per_job"], 1024)
        self.assertEqual(queue["control"]["calibration"], {"jobs": 30, "seconds": 0, "minimum_mib": 256,
                                                           "state": "learning"})

        self.assertIn("failed-job scan unavailable", state["failures_error"])

    def test_retired_queues_keep_their_final_usage_for_a_week(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "adaptive"
            (root.parent / "retired-queues").mkdir(parents=True)
            now = int(time.time())
            for name, retired in (("q0", now - 60), ("old", now - 8 * 86400)):
                batch = Path(directory) / name
                (batch / "adaptive").mkdir(parents=True)
                (batch / "adaptive/status.env").write_text(
                    "lifecycle=COMPLETED\njobs_with_usage=3\ncpu_seconds_succeeded=7200.0\n"
                    "cpu_seconds_failed=36.0\nusage_state=ok\n")
                (root.parent / "retired-queues" / f"{name}.env").write_text(
                    f"queue_id={name}\ntag_clean=tag-{name}\nbatch_dir={batch}\nretired_epoch={retired}\n")
            state = snapshot_module.snapshot(root, Path(directory) / "missing.sh")
        retired, = state["retired_queues"]
        self.assertEqual((retired["queue_id"], retired["tag"], retired["lifecycle"]), ("q0", "tag-q0", "COMPLETED"))
        self.assertEqual(retired["usage"], {"jobs": 3, "cpu_hours_succeeded": 2.0, "cpu_hours_failed": 0.01,
                                            "state": "ok"})

    def test_failed_jobs_and_holds_reach_each_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "adaptive"
            batch = Path(directory) / "logs" / "batch_00001_x"
            (root / "queues").mkdir(parents=True)
            (root / "last-known").mkdir()
            (batch / "adaptive").mkdir(parents=True)
            (batch / "done").mkdir()
            (batch / "jobs").mkdir()
            resources = Path(directory) / "resources.tsv"
            resources.write_text("host_id\tssh_target\tcpu_units\tmemory_mib\tenabled\nnode2\tu@b\t8\t14336\t1\n")
            (root / "pool.env").write_text(f"pool_id=p.1\nstate=ACTIVE\nresources_file={resources}\n")
            (root / "last-known/node2.tsv").write_text(
                "record_type\tpool_id\thost_id\tgeneration\tadmission\treservation_id\tqueue_id\tstate\t"
                "pid\tcpu_units\tmemory_mib\tobserved_memory_mib\tcharged_memory_mib\tmessage\n"
                "reservation\tp.1\tnode2\t1\tenabled\tp.1.node2.7\tq1\trunning\t1\t1\t1\t0\t0\t\n"
                "blocked_queue\tp.1\tnode2\t\t\t\tq1\t\t\t\t\t\t\tinfrastructure_failed\n")
            (root / "queues/q1.env").write_text(f"queue_id=q1\nbatch_dir={batch}\ntag_clean=tag1\n")
            (batch / "adaptive/control.env").write_text(
                "generation=1\nadmission=enabled\nschedule_priority=50\nmax_workers=0\n")
            (batch / "adaptive/resources.env").write_text("memory_mib_per_job=512\n")
            for job, reservation in (("lost", "p.1.node2.5"), ("live", "p.1.node2.7"), ("ok", "p.1.node1.2")):
                owner = batch / "claims" / f"job_{job}.claim" / "owner.json"
                owner.parent.mkdir(parents=True)
                owner.write_text(json.dumps({"job_id": job, "reservation_id": reservation}))
                os.utime(owner, (1e9, 1e9))
            (batch / "done/job_ok.done").touch()
            (batch / "jobs/job_lost_compute-node2.log").write_text("started\n")
            os.utime(batch / "jobs/job_lost_compute-node2.log", (1e9 + 60, 1e9 + 60))
            state = snapshot_module.snapshot(root, Path(directory) / "missing.sh")
        queue = state["queues"][0]
        self.assertIsNone(state["failures_error"])
        self.assertEqual(queue["failed_jobs_total"], 1)
        self.assertEqual(queue["failed_jobs"], [{
            "job_id": "lost", "reservation_id": "p.1.node2.5", "host_id": "node2", "last_activity": 1e9 + 60,
            "log": str(batch / "jobs/job_lost_compute-node2.log")}])
        self.assertEqual(queue["holds"], [{"host_id": "node2", "reason": "infrastructure_failed"}])


if __name__ == "__main__":
    unittest.main()
