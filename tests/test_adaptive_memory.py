#!/usr/bin/env python3
"""Time-based memory accounting, including telemetry loss and recovery."""

import copy
import ctypes
import errno
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / "scheduler/adaptive_memory.py"
SPEC = importlib.util.spec_from_file_location("adaptive_memory", SCRIPT)
MEMORY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MEMORY)


class MemoryTests(unittest.TestCase):
    def setUp(self):
        self.history = {"schema_version": 1, "pool_id": "pool", "host_id": "host",
                        "reservations": {}}
        self.row = ["res", "queue", "running", "123", "1", "1024", "0", "1024", "0", "0"]

    def sample(self, epoch, rss):
        self.row[8] = str(rss)
        rows, self.history = MEMORY.update_usage(
            [self.row], self.history, "pool", "host", epoch, 30,
        )
        self.row = rows[0]
        return list(map(int, self.row[6:10]))

    def test_brief_spike_has_immediate_charge_but_smoothed_retention(self):
        for epoch in range(0, 31, 3):
            self.sample(epoch, 1024)
        self.assertEqual(self.sample(33, 4096)[0], 4096)
        observed, charged, current, sustained = self.sample(36, 1024)
        self.assertEqual(current, 1024)
        self.assertEqual(sustained, 1639)
        self.assertEqual(observed, charged)
        self.assertEqual(charged, 1803)
        for epoch in range(39, 364, 3):
            self.sample(epoch, 1024)
        self.assertEqual(self.row[6:10], ["1127", "1127", "1024", "1024"])

    def test_sustained_phase_retains_charge_for_five_minutes(self):
        for epoch in range(0, 61, 5):
            self.sample(epoch, 4096)
        for epoch in range(65, 361, 5):
            self.assertEqual(self.sample(epoch, 1024)[1], 4506)
        for epoch in range(365, 386, 5):
            self.sample(epoch, 1024)
        self.assertEqual(self.row[7], "1127")

    def test_irregular_intervals_are_weighted_by_elapsed_time(self):
        samples = [[0, 100], [10, 1000], [11, 100], [15, 100]]
        self.assertEqual(MEMORY.sustained_peak(samples, 15), 160)
        dense = [[t, 1000 if t == 10 else 100] for t in range(16)]
        self.assertEqual(MEMORY.sustained_peak(dense, 15), 160)

    def test_peak_between_sample_times_is_retained(self):
        samples = [[0, 100], [10, 1000], [20, 100], [30, 100]]
        self.assertEqual(MEMORY.sustained_peak(samples, 30), 700)

    def test_short_warmup_does_not_invent_a_sustained_peak(self):
        self.assertEqual(self.sample(0, 4096), [4096, 4096, 4096, 0])
        self.assertEqual(self.sample(3, 1024), [1024, 1024, 1024, 0])
        for epoch in range(6, 16, 3):
            self.sample(epoch, 1024)
        self.assertEqual(self.row[9], "1639")

    def test_declaration_remains_a_floor(self):
        for epoch in range(0, 31, 5):
            self.sample(epoch, 100)
        self.assertEqual(self.row[7], "1024")

    def test_live_declaration_changes_preserve_sustained_history(self):
        for epoch in range(0, 31, 5):
            self.sample(epoch, 4096)
        original = copy.deepcopy(self.history["reservations"]["res"])
        self.row[5] = self.row[7] = "6000"
        self.assertEqual(self.sample(35, 1024)[1], 6000)
        self.row[5] = "512"
        self.row[7] = self.row[6]
        self.assertEqual(self.sample(40, 1024)[1], 4506)
        record = self.history["reservations"]["res"]
        self.assertEqual(record["samples"][:len(original["samples"])], original["samples"])
        self.assertEqual(record["fresh_since"], original["fresh_since"])
        self.assertEqual(record["identity"], ["queue", "123", "1", "512"])
        for epoch in range(45, 361, 5):
            self.sample(epoch, 1024)
        self.assertEqual(self.row[7], "1127")

    def test_uncertain_declaration_raise_preserves_history_and_charge(self):
        for epoch in range(0, 31, 5):
            self.sample(epoch, 4096)
        original = copy.deepcopy(self.history["reservations"]["res"])
        self.row[2] = "uncertain"
        self.row[5] = self.row[7] = "6000"
        self.assertEqual(self.sample(900, 0)[1], 6000)
        record = self.history["reservations"]["res"]
        self.assertEqual(record["samples"], original["samples"])
        self.assertEqual(record["held_memory_mib"], original["held_memory_mib"])
        self.assertEqual(record["identity"][-1], "6000")

    def test_long_initial_lull_does_not_delay_charging_a_new_heavy_phase(self):
        for epoch in range(0, 201, 5):
            self.sample(epoch, 100)
        self.assertEqual(self.sample(205, 4096)[1], 4096)
        for epoch in range(210, 226, 5):
            self.sample(epoch, 4096)
        self.assertEqual(self.row[7], "4506")

    def test_restart_preserves_history_and_eventually_releases(self):
        for epoch in range(0, 31, 5):
            self.sample(epoch, 4096)
        self.history = copy.deepcopy(self.history)
        self.assertEqual(self.sample(35, 1024)[1], 4506)
        for epoch in range(40, 361, 5):
            self.sample(epoch, 1024)
        self.assertEqual(self.row[7], "1127")

    def test_missing_history_and_gaps_hold_previous_charge_until_fresh(self):
        for reset in ("missing", "gap", "clock"):
            with self.subTest(reset=reset):
                self.setUp()
                for epoch in range(0, 31, 5):
                    self.sample(epoch, 4096)
                if reset == "missing":
                    self.history["reservations"] = {}
                start = 100 if reset != "clock" else 10
                self.assertEqual(self.sample(start, 1024)[1], 4506)
                for epoch in range(start + 5, start + 315, 5):
                    self.assertEqual(self.sample(epoch, 1024)[1], 4506)
                self.assertEqual(self.sample(start + 315, 1024)[1], 1127)

    def test_uncertain_reservation_keeps_charge_even_without_a_pid(self):
        for epoch in range(0, 31, 5):
            self.sample(epoch, 4096)
        self.row[2] = "uncertain"
        self.assertEqual(self.sample(900, 0)[1], 4506)
        self.row[3] = "0"
        # Changing the identity of an existing record fails loudly.
        with self.assertRaisesRegex(ValueError, "identity changed"):
            self.sample(905, 0)
        self.history["reservations"] = {}
        self.assertEqual(self.sample(910, 0)[1], 4506)

    def test_repeated_timestamp_does_not_accumulate_duration(self):
        self.sample(0, 1024)
        for _ in range(100):
            self.sample(0, 4096)
        self.assertEqual(len(self.history["reservations"]["res"]["samples"]), 1)
        self.assertEqual(self.row[9], "0")

    def test_completed_history_is_pruned_and_live_history_is_bounded(self):
        for epoch in range(1000):
            self.sample(epoch, 1024)
        self.assertLessEqual(len(self.history["reservations"]["res"]["samples"]), 317)
        rows, history = MEMORY.update_usage([], self.history, "pool", "host", 1000, 30)
        self.assertEqual(rows, [])
        self.assertEqual(history["reservations"], {})

    def test_corrupt_history_fails_loudly(self):
        self.sample(0, 1024)
        for mutation in ("pool", "rss", "time", "identity", "declaration"):
            history = copy.deepcopy(self.history)
            record = history["reservations"]["res"]
            if mutation == "pool":
                history["pool_id"] = "other"
            elif mutation == "rss":
                record["samples"][0][1] = -1
            elif mutation == "time":
                record["samples"].append([0, 1024])
            elif mutation == "declaration":
                record["identity"][3] = "invalid"
            else:
                record["identity"][0] = "other-queue"
            with self.assertRaises(ValueError):
                MEMORY.update_usage([self.row], history, "pool", "host", 5, 30)


class CalibrationMetricsTests(unittest.TestCase):
    def setUp(self):
        self.history = {"schema_version": 1, "pool_id": "pool", "host_id": "host",
                        "reservations": {}}
        self.row = ["res", "queue", "running", "123", "1", "5000", "0", "5000", "0", "0"]

    def sample(self, epoch, memory):
        self.row[8] = str(memory)
        rows, self.history = MEMORY.update_usage(
            [self.row], self.history, "pool", "host", epoch, 30,
        )
        self.row = rows[0]

    def metadata(self, outcome="succeeded", job="job:1", started=0):
        reservation = {"schema_version": "1", "pool_id": "pool", "host_id": "host",
                       "reservation_id": self.row[0], "queue_id": self.row[1],
                       "generation": "1", "cpu_units": "1", "memory_mib": self.row[5],
                       "shared_io_units": "0", "pid": self.row[3],
                       "state": self.row[2], "started_epoch": str(started)}
        result = {"reservation_id": self.row[0], "claimed_job_id": job, "outcome": outcome,
                  "job_exit_code": "0" if outcome == "succeeded" else "1"}
        return reservation, result

    def complete(self, finished=25, outcome="succeeded", job="job:1", started=0):
        self.history = MEMORY.complete_reservation(
            self.history, *self.metadata(outcome, job, started), pool_id="pool", host_id="host",
            finished_epoch=finished, max_gap=30,
        )
        return MEMORY.calibration_report(self.history, "pool", "host")[self.row[1]]

    def representative_samples(self):
        for epoch, value in ((0, 100), (5, 500), (20, 100)):
            self.sample(epoch, value)

    def test_completed_mean_is_time_weighted_without_declared_floor_or_final_rss(self):
        self.representative_samples()
        report = self.complete()
        self.assertEqual(report["completed"], [{"reservation_id": "res", "job_id": "job:1",
                         "finished_epoch": 25, "mean_mib": 400, "seconds": 20.0}])
        self.assertEqual(report["running"], [])
        self.assertEqual(self.history["reservations"], {})
        _, pruned = MEMORY.update_usage([], self.history, "pool", "host", 30, 30)
        self.assertEqual(pruned["calibration"], self.history["calibration"])

    def test_cumulative_stats_survive_safety_pruning_and_restart(self):
        for epoch in range(0, 901, 10):
            self.sample(epoch, 100 if epoch < 600 else 200)
            if epoch == 450:
                self.history = copy.deepcopy(self.history)
        record = self.history["reservations"]["res"]
        self.assertLess(len(record["samples"]), 40)
        running = MEMORY.calibration_report(self.history, "pool", "host")["queue"]["running"][0]
        self.assertEqual(running, {"reservation_id": "res", "first_epoch": 0, "last_epoch": 900,
                         "mean_mib": 134, "seconds": 900.0, "samples": 91})
        self.assertEqual(self.complete(900)["completed"][0]["mean_mib"], 134)

    def test_repeated_timestamp_does_not_inflate_duration_or_sample_count(self):
        self.sample(0, 100)
        self.sample(5, 200)
        for _ in range(10):
            self.sample(5, 500)
        self.sample(20, 100)
        stats = self.history["reservations"]["res"]["statistics"]
        self.assertEqual((stats["samples"], stats["seconds"], stats["area"]), (3, 20, 8000))
        self.assertEqual(self.complete()["completed"][0]["mean_mib"], 400)

    def test_gaps_clock_reversals_and_uncertainty_disqualify_whole_job(self):
        for disruption in ("gap", "clock", "uncertain"):
            with self.subTest(disruption=disruption):
                self.setUp()
                self.representative_samples()
                if disruption == "gap":
                    self.sample(100, 100)
                elif disruption == "clock":
                    self.sample(10, 100)
                else:
                    self.row[2] = "uncertain"
                    self.sample(25, 0)
                    self.row[2] = "running"
                for epoch in range(110, 811, 10):
                    self.sample(epoch, 100)
                self.assertEqual(MEMORY.calibration_report(self.history, "pool", "host"), {})
                self.assertEqual(self.complete(810)["completed"], [])

    def test_short_sparse_unobserved_and_zero_memory_jobs_never_learn(self):
        for samples, finish in (([(0, 100), (10, 100)], 10),
                                ([(0, 100), (5, 100), (10, 100)], 10),
                                ([(0, 100), (5, 100), (20, 100)], 26),
                                ([(0, 0), (5, 0), (20, 0)], 20), ([], 25)):
            with self.subTest(samples=samples, finish=finish):
                self.setUp()
                for epoch, value in samples:
                    self.sample(epoch, value)
                self.assertEqual(self.complete(finish)["completed"], [])
        self.setUp()
        self.representative_samples()
        del self.history["reservations"]["res"]["statistics"]
        self.assertEqual(self.complete()["completed"], [])

    def test_duplicate_completion_is_immutable_and_idempotent(self):
        self.representative_samples()
        self.complete()
        saved = copy.deepcopy(self.history)
        self.complete()
        self.assertEqual(self.history, saved)
        for change in ("job", "time", "queue", "pid"):
            reservation, result = self.metadata()
            finished = 25
            if change == "job":
                result["claimed_job_id"] = "different"
            elif change == "time":
                finished = 26
            elif change == "queue":
                reservation["queue_id"] = "different"
            else:
                reservation["pid"] = "456"
            with self.subTest(change=change), self.assertRaises(ValueError):
                MEMORY.complete_reservation(saved, reservation, result, pool_id="pool",
                                            host_id="host", finished_epoch=finished, max_gap=30)

    def test_failure_clears_cohort_and_blocks_pre_failure_running_samples(self):
        self.representative_samples()
        self.complete()
        self.row[0] = "res-next"
        for epoch in (30, 40, 50):
            self.sample(epoch, 100)
        self.row[0] = "failed"
        report = self.complete(55, outcome="job_failed", job="failed-job", started=30)
        self.assertEqual(report, {"completed": [], "running": [], "failure_epoch": 55})
        self.complete(55, outcome="job_failed", job="failed-job", started=30)
        self.assertEqual(self.history["calibration"]["queue"]["completed"], [])

    def test_empty_and_unknown_job_identity_do_not_become_successful_samples(self):
        for outcome, job in (("empty", ""), ("succeeded", ""), ("infrastructure_failed", "")):
            self.setUp()
            self.representative_samples()
            report = self.complete(outcome=outcome, job=job)
            self.assertEqual(report["completed"], [])
            self.assertEqual(report["failure_epoch"], 25 if outcome == "infrastructure_failed" else 0)

    def test_completed_and_queue_cohorts_are_bounded(self):
        for index in range(55):
            self.row[0] = f"res-{index}"
            started = index * 30
            for epoch in (started, started + 10, started + 20):
                self.sample(epoch, 100)
            self.complete(started + 20, job=f"job-{index}", started=started)
        cohort = self.history["calibration"]["queue"]
        self.assertEqual(len(cohort["completed"]), 50)
        self.assertEqual(len(cohort["receipts"]), 50)
        self.assertEqual(cohort["completed"][0]["reservation_id"], "res-5")
        for index in range(130):
            self.row[0], self.row[1] = f"other-res-{index}", f"queue-{index}"
            self.complete(2000 + index, outcome="empty", job="", started=2000)
        self.assertEqual(len(self.history["calibration"]), 128)

    def test_corrupt_statistics_and_completion_identity_fail_loudly(self):
        self.representative_samples()
        for key, value in (("area", float("nan")), ("samples", True),
                           ("seconds", -1), ("last_mib", 999)):
            saved = copy.deepcopy(self.history)
            saved["reservations"]["res"]["statistics"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                MEMORY.calibration_report(saved, "pool", "host")
        reservation, result = self.metadata()
        reservation["pool_id"] = "other-pool"
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            MEMORY.complete_reservation(self.history, reservation, result, pool_id="pool",
                                        host_id="host", finished_epoch=25, max_gap=30)

    def test_cli_completion_uses_result_mtime_and_report_filters_plan_queues(self):
        self.representative_samples()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            history, reservation, result = (root / name for name in ("history.json", "reservation.env", "result.env"))
            history.write_text(json.dumps(self.history))
            reservation_data, result_data = self.metadata()
            for path, data in ((reservation, reservation_data), (result, result_data)):
                path.write_text("".join(f"{key}={value}\n" for key, value in data.items()))
            os.utime(result, (25, 25))
            common = ["--history-file", str(history), "--pool-id", "pool", "--host-id", "host"]
            with mock.patch.object(sys, "argv", [str(SCRIPT), "--complete-reservation", str(reservation),
                                   "--result-file", str(result), "--max-gap-seconds", "30", *common]):
                self.assertEqual(MEMORY.main(), 0)
            plan = root / "allocations.tsv"
            plan.write_text("queue_id\ttarget_running\nqueue\t0\n")
            arguments = [str(SCRIPT), "--calibration-report", "--calibration-queues-file", str(plan), *common]
            with mock.patch.object(sys, "argv", arguments), \
                    mock.patch.object(sys, "stdout", new_callable=io.StringIO) as output:
                self.assertEqual(MEMORY.main(), 0)
            queue, payload = output.getvalue().strip().split("\t")
            self.assertEqual(queue, "queue")
            self.assertEqual(json.loads(payload)["completed"][0]["finished_epoch"], 25)
            plan.write_text("queue_id\ttarget_running\nother\t0\n")
            with mock.patch.object(sys, "argv", arguments), \
                    mock.patch.object(sys, "stdout", new_callable=io.StringIO) as output:
                self.assertEqual(MEMORY.main(), 0)
                self.assertEqual(output.getvalue(), "")
            history.unlink()
            with mock.patch.object(sys, "argv", [str(SCRIPT), "--calibration-report", *common]), \
                    mock.patch.object(sys, "stdout", new_callable=io.StringIO) as output:
                self.assertEqual(MEMORY.main(), 0)
                self.assertEqual(output.getvalue(), "")
            self.assertFalse(history.exists())


class ProcessSnapshotTests(unittest.TestCase):
    snapshot = "  123   1  82.5   8192 /R --file='a b.R'  --args x\n"

    def augment(self, owners=None, footprint=None, gone=None):
        return MEMORY.augment_process_snapshot(
            self.snapshot, {123: 501} if owners is None else owners, 501,
            (lambda pid: 2 * 1024**3) if footprint is None else footprint,
            gone=(lambda pid: False) if gone is None else gone,
        )

    def test_compressed_private_memory_exceeds_rss_without_changing_argv(self):
        self.assertEqual(
            self.augment(),
            "  123   1  82.5   2097152 /R --file='a b.R'  --args x\n",
        )

    def test_smaller_footprint_is_reported_for_the_sampler_to_compare(self):
        self.assertIn(" 1 /R", self.augment(footprint=lambda pid: 1024))
        self.assertIn(" 0 /R", self.augment(footprint=lambda pid: 0))

    def test_footprint_rounds_up_to_kib(self):
        output = self.augment(footprint=lambda pid: 8192 * 1024 + 1)
        self.assertIn(" 8193 /R", output)

    def test_other_user_keeps_rss_without_requesting_privileged_data(self):
        reader = mock.Mock(side_effect=PermissionError(errno.EPERM, "denied"))
        self.assertEqual(self.augment(owners={123: 0}, footprint=reader), self.snapshot)
        reader.assert_not_called()

    def test_unreadable_live_owned_process_fails_closed(self):
        for code in (errno.EPERM, errno.EACCES, errno.EIO, errno.ESRCH):
            with self.subTest(code=code):
                reader = mock.Mock(side_effect=OSError(code, "cannot read"))
                with self.assertRaises(OSError):
                    self.augment(footprint=reader)

    def test_proven_vanished_process_keeps_original_rss(self):
        reader = mock.Mock(side_effect=ProcessLookupError(errno.ESRCH, "gone"))
        self.assertEqual(self.augment(footprint=reader, gone=lambda pid: True), self.snapshot)
        denied = mock.Mock(side_effect=PermissionError(errno.EPERM, "denied"))
        with self.assertRaises(PermissionError):
            self.augment(footprint=denied, gone=lambda pid: True)

    def test_missing_uid_needs_proof_of_exit(self):
        with self.assertRaisesRegex(ValueError, "missing from process UID"):
            self.augment(owners={})
        reader = mock.Mock()
        self.assertEqual(self.augment(owners={}, footprint=reader, gone=lambda pid: True),
                         self.snapshot)
        reader.assert_not_called()

    def test_invalid_readings_and_malformed_rows_fail_closed(self):
        for value in (-1, True, 0.5, None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.augment(footprint=lambda pid: value)
        for snapshot in ("", "123 1 0.0 -1 /R\n", "123 1 bad 12 /R\n",
                         "0 1 0.0 12 /R\n", self.snapshot * 2):
            with self.subTest(snapshot=snapshot), self.assertRaises(ValueError):
                MEMORY.augment_process_snapshot(snapshot, {}, 501, None)

    def test_uid_snapshot_rejects_duplicates_and_partial_output(self):
        for output in ("", "123 501\n123 501\n", "123 unknown\n"):
            result = mock.Mock(returncode=0, stdout=output)
            with mock.patch.object(MEMORY.subprocess, "run", return_value=result):
                with self.assertRaises(ValueError):
                    MEMORY._process_uids()

    def test_uid_snapshot_accepts_darwin_nobody_negative_uid(self):
        result = mock.Mock(returncode=0, stdout=" 123 501\n 1522 -2\n")
        with mock.patch.object(MEMORY.subprocess, "run", return_value=result):
            self.assertEqual(MEMORY._process_uids(), {123: 501, 1522: -2})

    def test_rusage_abi_and_native_error_are_explicit(self):
        self.assertEqual(ctypes.sizeof(MEMORY._DarwinRusageInfoV0), 96)
        self.assertEqual(MEMORY._DarwinRusageInfoV0.ri_phys_footprint.offset, 72)

        def denied(pid, flavor, buffer):
            ctypes.set_errno(errno.EPERM)
            return -1

        library = mock.Mock()
        library.proc_pid_rusage.side_effect = denied
        with mock.patch.object(MEMORY.ctypes, "CDLL", return_value=library):
            reader = MEMORY._darwin_footprint_reader()
        with self.assertRaises(PermissionError):
            reader(123)

    def test_snapshot_cli_is_atomic_and_linux_does_not_load_darwin_api(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot"
            path.write_text(self.snapshot)
            arguments = [str(SCRIPT), "--augment-ps-snapshot", str(path)]
            with mock.patch.object(sys, "argv", arguments), \
                    mock.patch.object(sys, "platform", "linux"), \
                    mock.patch.object(MEMORY, "_darwin_footprint_reader") as reader:
                self.assertEqual(MEMORY.main(), 0)
                reader.assert_not_called()
            self.assertEqual(path.read_text(), self.snapshot)
            with mock.patch.object(sys, "argv", arguments), \
                    mock.patch.object(sys, "platform", "darwin"), \
                    mock.patch.object(MEMORY, "_process_uids", return_value={123: 501}), \
                    mock.patch.object(MEMORY.os, "getuid", return_value=501), \
                    mock.patch.object(MEMORY, "_darwin_footprint_reader",
                                      return_value=lambda pid: 2 * 1024**3):
                self.assertEqual(MEMORY.main(), 0)
            self.assertIn(" 2097152 /R", path.read_text())
            before = path.read_bytes()
            with mock.patch.object(sys, "argv", arguments), \
                    mock.patch.object(sys, "platform", "darwin"), \
                    mock.patch.object(MEMORY, "_process_uids", return_value={123: 501}), \
                    mock.patch.object(MEMORY.os, "getuid", return_value=501), \
                    mock.patch.object(MEMORY, "_darwin_footprint_reader",
                                      side_effect=PermissionError(errno.EPERM, "denied")), \
                    mock.patch.object(sys, "stderr", new_callable=io.StringIO) as error:
                self.assertEqual(MEMORY.main(), 1)
                self.assertIn("adaptive memory:", error.getvalue())
            self.assertEqual(path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
