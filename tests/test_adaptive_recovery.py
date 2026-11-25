#!/usr/bin/env python3
"""Audited recovery must preserve durable claims and reject live work."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("adaptive_recovery", ROOT / "scheduler/adaptive_recovery.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def env(path, **values):
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()))


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state = self.root / "state"
        self.batch = self.root / "batch"
        for directory in (self.state / "results", self.state / "reservations", self.batch / "claims", self.batch / "done"):
            directory.mkdir(parents=True)
        env(self.state / "daemon.env", pool_id="pool", host_id="host")
        (self.batch / "queue-edit.txt").write_text("one true\n")
        (self.batch / "claims/job_one.claim").mkdir()
        (self.batch / "done/job_one.done").write_text("completed output already exists")
        env(self.batch / "run_meta.env", pool_id="pool", run_token="queue", tag_clean="tag",
            execution_mode="adaptive-shared-queue", editable_queue_file=self.batch / "queue-edit.txt",
            editable_queue_sha256=hashlib.sha256((self.batch / "queue-edit.txt").read_bytes()).hexdigest())
        self.reservation = self.state / "reservations/pool.host.1.env"
        env(self.reservation, schema_version=1, pool_id="pool", host_id="host", queue_id="queue",
            reservation_id="pool.host.1", state="uncertain", pid="99999999",
            generation=1, cpu_units=1, memory_mib=256, shared_io_units=0)
        self.request = dict(action="resolve-uncertain", pool_id="pool", host_id="host", queue_id="queue",
                            batch_dir=str(self.batch), reservation_id="pool.host.1", reason="audited processes and outputs")
        self.snapshot = f"{os.getpid()} {os.getppid()} 1 S python recovery\n1 0 1 S init\n"

    def recover(self, snapshots=None):
        with mock.patch.object(MODULE, "process_snapshot", side_effect=snapshots or [self.snapshot, self.snapshot]):
            return MODULE.recover(self.state, self.request)

    def assert_untouched(self):
        self.assertTrue((self.batch / "claims/job_one.claim").is_dir())
        self.assertEqual((self.batch / "done/job_one.done").read_text(), "completed output already exists")
        self.assertTrue(self.reservation.is_file())

    def test_resolution_is_audited_and_does_not_infer_success(self):
        result = self.recover()
        self.assertEqual(result["resolution_published"], "1")
        published = MODULE.read_env(self.state / "results/pool.host.1.env")
        self.assertEqual(published["outcome"], "infrastructure_failed")
        self.assertEqual(published["claimed_job_id"], "")
        audit = Path(result["audit_dir"])
        self.assertEqual(MODULE.read_env(audit / "operator.env")["reason"], self.request["reason"])
        self.assertEqual((audit / "processes-final.txt").read_text(), self.snapshot)
        self.assert_untouched()

    def prepare_lost_job(self):
        (self.batch / "done/job_one.done").unlink()
        adaptive = self.batch / "adaptive"
        adaptive.mkdir()
        resources = adaptive / "resources.env"
        env(resources, schema_version=2, queue_id="queue", retry_lost_jobs=1, dependencies="[]")
        meta = MODULE.read_env(self.batch / "run_meta.env")
        meta["resources_sha256"] = hashlib.sha256(resources.read_bytes()).hexdigest()
        env(self.batch / "run_meta.env", **meta)
        owner = {"job_id": "one", "reservation_id": "pool.host.1",
                 "queue_sha256": meta["editable_queue_sha256"]}
        (self.batch / "claims/job_one.claim/owner.json").write_text(json.dumps(owner))
        self.request["verify_lost_job"] = "1"

    def test_verified_loss_retains_claim_until_coordinator_tail_recovery(self):
        self.prepare_lost_job()
        self.recover()
        receipt = json.loads((self.batch / "adaptive/verified-losses/job_one.json").read_text())
        self.assertEqual(receipt["owner"]["reservation_id"], "pool.host.1")
        self.assertEqual(receipt["reason"], self.request["reason"])
        self.assertTrue((self.batch / "claims/job_one.claim").is_dir())
        self.assertFalse((self.batch / "done/job_one.done").exists())

    def test_no_loss_receipt_when_process_is_still_present(self):
        self.prepare_lost_job()
        with self.assertRaises(MODULE.RecoveryError):
            self.recover([self.snapshot, self.snapshot + "123 1 99999999 S orphan\n"])
        self.assertFalse((self.batch / "adaptive/verified-losses").exists())

    def test_verified_loss_rejects_completed_job_or_existing_result(self):
        self.prepare_lost_job()
        done = self.batch / "done/job_one.done"
        done.touch()
        with self.assertRaisesRegex(MODULE.RecoveryError, "completed"):
            self.recover()
        done.unlink()
        env(self.state / "results/pool.host.1.env", outcome="job_failed")
        with self.assertRaisesRegex(MODULE.RecoveryError, "already has a result"):
            self.recover()
        self.assertFalse((self.batch / "adaptive/verified-losses").exists())

    def test_unowned_legacy_claim_cannot_be_verified_for_retry(self):
        self.prepare_lost_job()
        (self.batch / "claims/job_one.claim/owner.json").unlink()
        with self.assertRaisesRegex(MODULE.RecoveryError, "ownership"):
            self.recover()

    def test_pid_process_group_reservation_and_batch_work_are_refused(self):
        for row in (
            "99999999 1 1 S unrelated reused PID\n",
            "123 1 99999999 S orphan\n",
            "123 1 123 S bash worker pool.host.1\n",
            "123 1 123 S Rscript work.R --job_tag tag\n",
        ):
            with self.subTest(row=row), self.assertRaises(MODULE.RecoveryError):
                self.recover([self.snapshot + row])
            self.assertFalse((self.state / "results/pool.host.1.env").exists())
            self.assert_untouched()

    def test_work_appearing_between_audits_is_refused(self):
        with self.assertRaises(MODULE.RecoveryError):
            self.recover([self.snapshot, self.snapshot + "123 1 123 S job --job_tag tag\n"])
        self.assertFalse((self.state / "results/pool.host.1.env").exists())

    def test_reused_pid_zombie_is_not_running_work(self):
        self.recover([self.snapshot + "99999999 1 99999999 Z dead\n"] * 2)

    def test_only_uncertain_and_expected_identities_can_be_resolved(self):
        for key, value in (("state", "running"), ("host_id", "other"), ("queue_id", "other")):
            original = self.reservation.read_text()
            record = MODULE.read_env(self.reservation)
            record[key] = value
            env(self.reservation, **record)
            with self.subTest(key=key), self.assertRaises(MODULE.RecoveryError):
                self.recover()
            self.reservation.write_text(original)

    def test_changed_queue_is_refused(self):
        (self.batch / "queue-edit.txt").write_text("changed true\n")
        with self.assertRaisesRegex(MODULE.RecoveryError, "hash"):
            self.recover()

    def test_existing_result_and_pending_reset_are_never_overwritten(self):
        result_file = self.state / "results/pool.host.1.env"
        result_file.write_text("original result")
        with self.assertRaises(FileExistsError):
            self.recover()
        self.assertEqual(result_file.read_text(), "original result")
        self.request["action"] = "reset-failures"
        self.recover()
        pending = self.state / "failure-reset-requests/queue.env"
        original = pending.read_text()
        with self.assertRaises(FileExistsError):
            self.recover()
        self.assertEqual(pending.read_text(), original)

    def test_reason_and_identifier_validation(self):
        for reason in ("", " ", "audit\ninjected=value", "audit\ttab"):
            self.request["reason"] = reason
            with self.subTest(reason=reason), self.assertRaises(MODULE.RecoveryError):
                self.recover()
        self.request["reason"] = "audited"
        self.request["reservation_id"] = "../other"
        with self.assertRaises(MODULE.RecoveryError):
            self.recover()

    def test_pidless_uncertainty_can_be_resolved_after_audit(self):
        values = MODULE.read_env(self.reservation)
        values["pid"] = ""
        env(self.reservation, **values)
        self.recover()
        self.assert_untouched()

    def test_missing_self_in_process_snapshot_is_refused(self):
        with self.assertRaises(MODULE.RecoveryError):
            self.recover(["1 0 1 S init\n"])


class RebootTests(unittest.TestCase):
    BOOT = 5000

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.state = Path(self.temporary.name) / "state"
        for directory in ("results", "reservations"):
            (self.state / directory).mkdir(parents=True)
        env(self.state / "daemon.env", pool_id="pool", host_id="host")
        self.request = dict(action="resolve-rebooted", pool_id="pool", host_id="host",
                            reason="restart-daemon after the host stopped answering")
        self.snapshot = f"{os.getpid()} {os.getppid()} 1 S python recovery\n1 0 1 S init\n"
        self.reserve(1, started=1000)

    def reserve(self, number, started, state="running"):
        path = self.state / f"reservations/pool.host.{number}.env"
        env(path, schema_version=1, pool_id="pool", host_id="host", reservation_id=f"pool.host.{number}",
            queue_id="queue", generation=1, cpu_units=1, memory_mib=256, shared_io_units=0,
            pid=4242, state=state, started_epoch=started)
        return path

    def recover(self, extra=""):
        snapshot = self.snapshot + extra
        with mock.patch.object(MODULE, "boot_epoch", return_value=self.BOOT), \
                mock.patch.object(MODULE, "process_snapshot", return_value=snapshot):
            return MODULE.recover(self.state, self.request)

    def result(self, number):
        path = self.state / f"results/pool.host.{number}.env"
        return MODULE.read_env(path) if path.exists() else None

    def test_reservations_from_before_the_boot_are_lost_without_a_failure(self):
        self.reserve(2, started=self.BOOT - 300, state="uncertain")
        outcome = self.recover()
        self.assertEqual(outcome["rebooted_resolved"], "2")
        for number in (1, 2):
            self.assertEqual(self.result(number)["outcome"], "host_rebooted")
            self.assertEqual(self.result(number)["claimed_job_id"], "")
            # The daemon records the result and removes the reservation itself.
            self.assertTrue((self.state / f"reservations/pool.host.{number}.env").exists())
        operator = MODULE.read_env(Path(outcome["audit_dir"]) / "operator.env")
        self.assertEqual(operator["boot_epoch"], str(self.BOOT))
        self.assertEqual(operator["reservation_ids"], "pool.host.1,pool.host.2")
        self.assertEqual(operator["reason"], self.request["reason"])
        self.assertNotIn("queue_id", operator)

    def test_work_started_near_or_after_the_boot_is_left_for_the_daemon(self):
        (self.state / "reservations/pool.host.1.env").unlink()
        self.reserve(2, started=self.BOOT - MODULE.BOOT_MARGIN_SECONDS)
        self.reserve(3, started=self.BOOT + 10)
        self.assertEqual(self.recover()["rebooted_resolved"], "0")
        self.assertIsNone(self.result(2))
        self.assertIsNone(self.result(3))

    def test_an_existing_result_is_never_replaced(self):
        env(self.state / "results/pool.host.1.env", reservation_id="pool.host.1", outcome="succeeded")
        self.assertEqual(self.recover()["rebooted_resolved"], "0")
        self.assertEqual(self.result(1)["outcome"], "succeeded")

    def test_a_running_daemon_or_reservation_worker_is_refused(self):
        for row in (
            f"300 1 300 S bash {self.state}/bin/mmhostdaemon.sh --state-dir {self.state}\n",
            "301 1 301 S bash mmworker.sh 1 queue pool.host.1 result\n",
        ):
            with self.subTest(row=row), self.assertRaises(MODULE.RecoveryError):
                self.recover(row)
            self.assertIsNone(self.result(1))
            self.assertFalse((self.state / "recovery.lock").exists())

    def test_a_reused_pre_boot_pid_is_not_running_work(self):
        self.assertEqual(self.recover("4242 1 4242 S /usr/libexec/unrelated\n")["rebooted_resolved"], "1")

    def test_a_foreign_reservation_record_is_refused(self):
        values = MODULE.read_env(self.state / "reservations/pool.host.1.env")
        values["pool_id"] = "other"
        env(self.state / "reservations/pool.host.1.env", **values)
        with self.assertRaisesRegex(MODULE.RecoveryError, "invalid reservation record"):
            self.recover()
        self.assertIsNone(self.result(1))

    def test_boot_time_sources(self):
        darwin = "{ sec = 1793838383, usec = 120000 } Wed Nov  4 09:46:23 2026\n"
        with mock.patch.object(MODULE.platform, "system", return_value="Darwin"), \
                mock.patch.object(MODULE.subprocess, "check_output", return_value=darwin):
            self.assertEqual(MODULE.boot_epoch(), 1793838383)
        if Path("/proc/stat").exists():
            with mock.patch.object(MODULE.platform, "system", return_value="Linux"):
                self.assertTrue(0 < MODULE.boot_epoch() <= time.time())


if __name__ == "__main__":
    unittest.main()
