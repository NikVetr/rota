#!/usr/bin/env python3
"""Dependency binding and safe, bounded lost-job tail retries."""

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("workflow", ROOT / "scheduler/adaptive_workflow.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def env(path, **values):
    path.write_text("".join(f"{key}={value}\n" for key, value in values.items()))


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / "state"
        (self.state / "queues").mkdir(parents=True)
        (self.state / "queue-index").mkdir()
        self.batch = self.make_batch("first", "stage-one")
        self.dependency = MODULE.identity(self.batch)

    def make_batch(self, queue, name):
        batch = self.root / queue
        (batch / "adaptive").mkdir(parents=True)
        (batch / "claims").mkdir()
        (batch / "done").mkdir()
        env(batch / "run_meta.env", execution_mode="adaptive-shared-queue",
            pool_id="pool", run_token=queue, run_name=name)
        self.status(batch, "PENDING")
        env(self.state / "queue-index" / f"{queue}.env", batch_dir=batch,
            pool_id="pool", queue_id=queue, run_name=name)
        return batch

    def status(self, batch, lifecycle, **extra):
        env(batch / "adaptive/status.env", queue_id=batch.name, lifecycle=lifecycle,
            **{"propagation": "complete", "running_workers": 0, "target_workers": 0, **extra})

    def test_path_name_and_id_bind_to_same_queue_after_retirement(self):
        for reference in (str(self.batch), "stage-one", "first"):
            self.assertEqual(MODULE.resolve_dependencies(self.state, [reference]), [self.dependency])
        self.assertEqual(MODULE.resolve_dependencies(self.state, [str(self.batch), "first"]), [self.dependency])

    def test_ambiguous_and_missing_names_are_rejected(self):
        self.make_batch("second", "stage-one")
        for reference in ("stage-one", "missing"):
            with self.assertRaisesRegex(ValueError, "matches"):
                MODULE.resolve_dependencies(self.state, [reference])

    def test_only_successful_zero_worker_completion_unlocks(self):
        for lifecycle in ("PENDING", "RUNNING", "DRAINING"):
            self.status(self.batch, lifecycle)
            self.assertEqual(MODULE.dependency_hold([self.dependency]), "dependency_wait")
        self.status(self.batch, "FINISHED_INCOMPLETE")
        self.assertEqual(MODULE.dependency_hold([self.dependency]), "dependency_failed")
        for values in ({"propagation": "pending"}, {"running_workers": 1}, {"target_workers": 1}):
            self.status(self.batch, "COMPLETED", **values)
            self.assertEqual(MODULE.dependency_hold([self.dependency]), "dependency_wait")
        self.status(self.batch, "COMPLETED")
        self.assertEqual(MODULE.dependency_hold([self.dependency]), "")

    def test_unconfirmed_share_interruptions_hold_dependents(self):
        self.status(self.batch, "COMPLETED")
        records = self.batch / "share-interrupted"
        records.mkdir()
        (records / "job_7.node1").write_text("job_id=7\n")
        self.assertEqual(MODULE.unconfirmed_interruptions(self.batch), ["job_7.node1"])
        self.assertEqual(MODULE.dependency_hold([self.dependency]), "dependency_unverified")
        # Waiting on an unfinished predecessor is reported first.
        self.status(self.batch, "RUNNING")
        self.assertEqual(MODULE.dependency_hold([self.dependency]), "dependency_wait")
        self.status(self.batch, "COMPLETED")
        (records / "confirmed").mkdir()
        (records / "job_7.node1").rename(records / "confirmed/job_7.node1")
        self.assertEqual(MODULE.unconfirmed_interruptions(self.batch), [])
        self.assertEqual(MODULE.dependency_hold([self.dependency]), "")

    def test_dependency_replacement_and_disappearance_hold_closed(self):
        self.status(self.batch, "COMPLETED")
        meta = self.batch / "run_meta.env"
        meta.write_text(meta.read_text().replace("pool_id=pool", "pool_id=another"))
        self.assertEqual(MODULE.dependency_hold([self.dependency]), "dependency_unavailable")
        meta.unlink()
        self.assertEqual(MODULE.dependency_hold([self.dependency]), "dependency_unavailable")

    def test_multiple_dependencies_require_all_successes(self):
        other = self.make_batch("other", "stage-two")
        self.status(self.batch, "COMPLETED")
        dependencies = [self.dependency, MODULE.identity(other)]
        self.assertEqual(MODULE.dependency_hold(dependencies), "dependency_wait")
        self.status(other, "COMPLETED")
        self.assertEqual(MODULE.dependency_hold(dependencies), "")

    def test_maintenance_retry_rebinds_only_explicit_retired_dependencies(self):
        replacement = self.make_batch("replacement", "stage-one-retry")
        self.status(self.batch, "FINISHED_INCOMPLETE")
        with self.assertRaisesRegex(ValueError, "reconcile the predecessor first"):
            MODULE.retry_dependencies([self.dependency], [])
        selected = MODULE.retry_dependencies([self.dependency], [[str(self.batch), str(replacement)]])
        self.assertEqual(selected, [MODULE.identity(replacement)])
        self.assertEqual(MODULE.dependency_hold(selected), "dependency_wait")
        self.status(replacement, "COMPLETED")
        self.assertEqual(MODULE.dependency_hold(selected), "")
        for extra in ({"running_workers": 1}, {"target_workers": 1}, {"propagation": "pending"}):
            self.status(self.batch, "FINISHED_INCOMPLETE", **extra)
            with self.assertRaisesRegex(ValueError, "fully retired"):
                MODULE.retry_dependencies([self.dependency], [[str(self.batch), str(replacement)]])
        self.status(self.batch, "COMPLETED")
        self.assertEqual(MODULE.retry_dependencies([self.dependency], []), [self.dependency])
        with self.assertRaisesRegex(ValueError, "fully retired"):
            MODULE.retry_dependencies([self.dependency], [[str(self.batch), str(replacement)]])

    def test_retry_rejects_unrelated_duplicate_and_failed_replacements(self):
        replacement = self.make_batch("replacement", "stage-one-retry")
        self.status(self.batch, "FINISHED_INCOMPLETE")
        pair = [str(self.batch), str(replacement)]
        for pairs in ([pair, pair], [[str(replacement), str(self.batch)]]):
            with self.assertRaisesRegex(ValueError, "exactly once"):
                MODULE.retry_dependencies([self.dependency], pairs)
        self.status(replacement, "FINISHED_INCOMPLETE")
        with self.assertRaisesRegex(ValueError, "dependency_failed"):
            MODULE.retry_dependencies([self.dependency], [pair])

    def test_policy_requires_new_schema_and_refuses_self_dependency(self):
        self.assertEqual(MODULE.policy({"schema_version": "1"}), ([], False))
        with self.assertRaises(ValueError):
            MODULE.policy({"schema_version": "1", "dependencies": "[]"})
        with self.assertRaisesRegex(ValueError, "itself"):
            MODULE.policy({"schema_version": "2", "queue_id": "first", "retry_lost_jobs": "1",
                           "dependencies": json.dumps([self.dependency])})

    def prepare_tail(self):
        queue = self.batch / "queue.txt"
        queue.write_text("one true\ntwo true\n")
        digest = hashlib.sha256(queue.read_bytes()).hexdigest()
        evidence = self.batch / "adaptive/verified-losses"
        evidence.mkdir()
        for job in ("one", "two"):
            claim = self.batch / "claims" / f"job_{job}.claim"
            claim.mkdir()
            owner = {"job_id": job, "reservation_id": f"pool.host.{job}", "queue_sha256": digest}
            (claim / "owner.json").write_text(json.dumps(owner))
            (evidence / f"job_{job}.json").write_text(json.dumps({
                "pool_id": "pool", "queue_id": "first", "owner": owner,
            }))
        (self.batch / "claim-cursor.json").write_text("cursor at end")
        return {**self.dependency, "queue_sha256": digest, "lock_dir": str(self.root / "queue.lock")}

    def test_only_lost_tail_is_reclaimed_and_cursor_is_reset(self):
        registration = self.prepare_tail()
        (self.batch / "done/job_one.done").write_text("success")
        self.assertEqual(MODULE.retry_verified_tail(registration, ["one", "two"]), 1)
        self.assertTrue((self.batch / "claims/job_one.claim").is_dir())
        self.assertFalse((self.batch / "claims/job_two.claim").exists())
        self.assertTrue((self.batch / "adaptive/lost-retries/job_two.claim/owner.json").is_file())
        self.assertFalse((self.batch / "claim-cursor.json").exists())
        # A second loss of the retried job cannot loop forever.
        self.assertEqual(MODULE.retry_verified_tail(registration, ["one", "two"]), 0)

    def test_unverified_failure_or_mismatched_receipt_prevents_retry(self):
        registration = self.prepare_tail()
        receipt = self.batch / "adaptive/verified-losses/job_one.json"
        original = receipt.read_text()
        receipt.unlink()
        self.assertEqual(MODULE.retry_verified_tail(registration, ["one", "two"]), 0)
        receipt.write_text(original.replace('"queue_id": "first"', '"queue_id": "wrong"'))
        with self.assertRaisesRegex(ValueError, "does not match"):
            MODULE.retry_verified_tail(registration, ["one", "two"])
        self.assertTrue((self.batch / "claim-cursor.json").exists())

    def test_busy_batch_lock_leaves_claims_untouched(self):
        registration = self.prepare_tail()
        Path(registration["lock_dir"]).mkdir()
        self.assertEqual(MODULE.retry_verified_tail(registration, ["one", "two"]), 0)
        self.assertTrue((self.batch / "claim-cursor.json").exists())

    def test_partial_move_crash_leaves_requeued_job_visible_and_retry_bounded(self):
        registration = self.prepare_tail()
        rename = MODULE.os.rename
        def interrupted(source, target):
            if source.name == "job_two.claim":
                raise OSError("power loss")
            rename(source, target)
        with mock.patch.object(MODULE.os, "rename", side_effect=interrupted):
            with self.assertRaises(OSError):
                MODULE.retry_verified_tail(registration, ["one", "two"])
        self.assertFalse((self.batch / "claim-cursor.json").exists())
        self.assertFalse((self.batch / "claims/job_one.claim").exists())
        self.assertTrue((self.batch / "claims/job_two.claim").exists())
        (self.batch / "done/job_one.done").touch()
        self.assertEqual(MODULE.retry_verified_tail(registration, ["one", "two"]), 1)

    def test_chain_binds_each_stage_to_returned_batch_path_without_shell_eval(self):
        manifest = self.root / "chain.json"
        stage = ["--model_dir", "/tmp/project $(no-shell)", "--queue", "jobs.txt", "--memory_mib_per_job", "256"]
        manifest.write_text(json.dumps([stage, stage, stage]))
        outputs = [subprocess.CompletedProcess([], 0, stdout=f"batch_dir=/tmp/batch-{i}\n") for i in (1, 2, 3)]
        with mock.patch.object(MODULE.subprocess, "run", side_effect=outputs) as run, mock.patch("builtins.print"):
            MODULE.submit_chain(Path("scheduler.sh"), manifest)
        self.assertEqual(run.call_args_list[0].args[0], ["bash", "scheduler.sh", "submit", *stage])
        self.assertEqual(run.call_args_list[1].args[0][-2:], ["--after", "/tmp/batch-1"])
        self.assertEqual(run.call_args_list[2].args[0][-2:], ["--after", "/tmp/batch-2"])

    def test_chain_stops_on_failure_and_keeps_earlier_submissions(self):
        manifest = self.root / "chain.json"
        stage = ["--model_dir", "/tmp/project", "--queue", "jobs.txt", "--memory_mib_per_job", "256"]
        manifest.write_text(json.dumps([stage] * 3))
        with mock.patch.object(MODULE.subprocess, "run", side_effect=[
            subprocess.CompletedProcess([], 0, stdout="batch_dir=/tmp/one\n"),
            subprocess.CompletedProcess([], 1, stdout=""),
        ]) as run, mock.patch("builtins.print"):
            with self.assertRaisesRegex(ValueError, "stage 2; earlier submissions are retained"):
                MODULE.submit_chain(Path("scheduler.sh"), manifest)
        self.assertEqual(run.call_count, 2)


if __name__ == "__main__":
    unittest.main()
