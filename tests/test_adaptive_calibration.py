#!/usr/bin/env python3
"""Safety and estimator behavior for opt-in queue memory calibration."""

import copy
import importlib.util
import json
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "scheduler/adaptive_calibration.py"
SPEC = importlib.util.spec_from_file_location("adaptive_calibration", SCRIPT)
CALIBRATION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CALIBRATION)


class CalibrationTests(unittest.TestCase):
    def setUp(self):
        self.policy = {"jobs": 20, "seconds": 0, "minimum_mib": 64, "generation": 1}
        self.state = self.call(None, now=1000)

    def call(self, previous, *, now=1100, reports=None, baseline=1000, policy=None):
        return CALIBRATION.calibrate(previous, baseline_mib=baseline,
                                     policy=self.policy if policy is None else policy,
                                     reports=[] if reports is None else reports, now=now)

    def completed(self, count=20, *, mean=100, start=1001, prefix="job"):
        return [{"reservation_id": f"{prefix}-res-{i}", "job_id": f"{prefix}:{i}",
                 "finished_epoch": start + i, "mean_mib": mean, "seconds": 30.0}
                for i in range(count)]

    def report(self, *, completed=None, running=None, failure=0, host="host"):
        return {"host_id": host, "completed": [] if completed is None else completed,
                "running": [] if running is None else running, "failure_epoch": failure}

    def running(self, *, first=1000, last=1600, mean=100, seconds=None, samples=61, reservation="res"):
        return {"reservation_id": reservation, "first_epoch": first, "last_epoch": last,
                "mean_mib": mean, "seconds": last - first if seconds is None else seconds,
                "samples": samples}

    def learned(self):
        return self.call(self.state, reports=[self.report(completed=self.completed())])

    def test_disabled_policy_never_learns(self):
        policy = {**self.policy, "jobs": 0}
        state = self.call(None, policy=policy)
        state = self.call(state, now=2000, policy=policy,
                          reports=[self.report(completed=self.completed())])
        self.assertEqual((state["status"], state["effective_memory_mib"], state["completed"]),
                         ("disabled", 1000, []))

    def test_initialization_ignores_existing_history_and_future_completions(self):
        self.assertEqual(self.state["reason"], "new_generation")
        rows = self.completed(20, start=980)
        rows += self.completed(1, start=1000, prefix="boundary")
        rows += self.completed(1, start=1200, prefix="future")
        state = self.call(self.state, now=1100, reports=[self.report(completed=rows)])
        self.assertEqual((state["effective_memory_mib"], state["completed"]), (1000, []))

    def test_completed_trigger_requires_twenty_distinct_jobs(self):
        state = self.call(self.state, reports=[self.report(completed=self.completed(19))])
        self.assertEqual((state["sample_jobs"], state["reason"]), (19, "insufficient_jobs"))
        state = self.call(state, reports=[self.report(completed=self.completed())])
        self.assertEqual((state["status"], state["sample_jobs"], state["candidate_mib"],
                          state["effective_memory_mib"]), ("calibrated", 20, 115, 800))

    def test_p90_uses_per_job_means_and_ignores_isolated_outliers(self):
        rows = self.completed()
        for i, mean in enumerate([400, 800, 900], start=17):
            rows[i]["mean_mib"] = mean
            rows[i]["seconds"] = 10000
        state = self.call(self.state, reports=[self.report(completed=rows)])
        self.assertEqual(state["candidate_mib"], 460)
        rows = self.completed()
        rows[-1]["mean_mib"] = 10000
        state = self.call(self.state, reports=[self.report(completed=rows)])
        self.assertEqual(state["candidate_mib"], 115)

    def test_headroom_rounds_up_and_minimum_is_respected(self):
        state = self.call(self.state, reports=[self.report(completed=self.completed(mean=101))])
        self.assertEqual(state["candidate_mib"], 117)
        policy = {**self.policy, "minimum_mib": 80}
        state = self.call(None, now=1000, policy=policy, baseline=100)
        state = self.call(state, policy=policy, baseline=100,
                          reports=[self.report(completed=self.completed(mean=1))])
        self.assertEqual((state["candidate_mib"], state["effective_memory_mib"]), (80, 80))

    def test_reductions_are_twenty_percent_per_five_minutes(self):
        reports = [self.report(completed=self.completed())]
        state = self.call(self.state, reports=reports)
        self.assertEqual(state["effective_memory_mib"], 800)
        state = self.call(state, now=1399, reports=reports)
        self.assertEqual((state["effective_memory_mib"], state["reason"]), (800, "cooldown"))
        state = self.call(state, now=1400, reports=reports)
        self.assertEqual(state["effective_memory_mib"], 640)
        odd = self.call(None, baseline=1001)
        odd = self.call(odd, now=1200, baseline=1001,
                        reports=[self.report(completed=self.completed(start=1101))])
        self.assertEqual(odd["effective_memory_mib"], 801)

    def test_small_changes_do_not_reduce_floor(self):
        state = self.call(self.state, reports=[self.report(completed=self.completed(mean=696))])
        self.assertEqual((state["candidate_mib"], state["effective_memory_mib"], state["reason"]),
                         (801, 1000, "material_change"))
        state = self.call(self.state, reports=[self.report(completed=self.completed(mean=695))])
        self.assertEqual((state["candidate_mib"], state["effective_memory_mib"]), (800, 800))

    def test_higher_candidates_raise_immediately_but_never_exceed_baseline(self):
        state = self.learned()
        reports = [self.report(completed=self.completed(mean=750, start=1101, prefix="larger"))]
        state = self.call(state, now=1200, reports=reports)
        self.assertEqual((state["effective_memory_mib"], state["reason"]), (863, "raised"))
        reports = [self.report(completed=self.completed(mean=2000, start=1201, prefix="large"))]
        state = self.call(state, now=1300, reports=reports)
        self.assertEqual(state["effective_memory_mib"], 1000)
        self.assertEqual(state["last_lowered_epoch"], 1100)

    def test_missing_or_empty_reports_never_lower_cached_estimate_after_restart(self):
        for reports in [[], [self.report()]]:
            with self.subTest(reports=reports):
                state = json.loads(json.dumps(self.learned()))
                state = self.call(state, now=2000, reports=reports)
                self.assertEqual((state["effective_memory_mib"], state["reason"]),
                                 (800, "missing_evidence"))
                self.assertNotIn("candidate_mib", state)

    def test_two_pass_refresh_only_lowers_once_when_evidence_arrives(self):
        state = self.call(self.learned(), now=1400)
        self.assertEqual(state["effective_memory_mib"], 800)
        reports = [self.report(completed=self.completed())]
        state = self.call(state, now=1400, reports=reports)
        self.assertEqual(state["effective_memory_mib"], 640)
        state = self.call(state, now=1400)
        state = self.call(state, now=1400, reports=reports)
        self.assertEqual((state["effective_memory_mib"], state["reason"]), (640, "cooldown"))

    def test_replayed_reports_do_not_inflate_completed_count(self):
        rows = self.completed(10)
        reports = [self.report(completed=rows)]
        state = self.call(self.state, reports=reports)
        state = self.call(json.loads(json.dumps(state)), now=1200, reports=reports)
        self.assertEqual((state["sample_jobs"], state["effective_memory_mib"]), (10, 1000))
        changed = copy.deepcopy(rows)
        changed[0]["mean_mib"] += 1
        with self.assertRaisesRegex(ValueError, "changed on replay"):
            self.call(state, now=1300, reports=[self.report(completed=changed)])

    def test_job_ids_are_deduplicated_across_hosts_and_reservations(self):
        first = self.completed(20)
        for row in first:
            row["job_id"] = "same:job"
        newer = self.completed(1, mean=200, start=1050, prefix="new")
        newer[0]["job_id"] = "same:job"
        state = self.call(self.state, reports=[self.report(completed=first),
                                              self.report(completed=newer, host="other")])
        self.assertEqual(state["sample_jobs"], 1)
        self.assertEqual(state["completed"][0]["mean_mib"], 200)

    def test_global_cohort_retains_only_newest_fifty_even_after_old_replays(self):
        old, new = self.completed(50), self.completed(20, start=1051, prefix="new")
        state = self.call(self.state, reports=[self.report(completed=old),
                                              self.report(completed=new, host="other")])
        self.assertEqual(len(state["completed"]), 50)
        self.assertEqual(state["completed"][0]["finished_epoch"], 1021)
        saved = copy.deepcopy(state["completed"])
        state = self.call(state, now=1200, reports=[self.report(completed=old)])
        self.assertEqual(state["completed"], saved)

    def test_failure_resets_baseline_and_cohort_without_reusing_old_running_data(self):
        policy = {**self.policy, "seconds": 600}
        state = self.call(None, policy=policy)
        state = self.call(state, now=1200, policy=policy,
                          reports=[self.report(completed=self.completed(start=1101))])
        self.assertEqual(state["effective_memory_mib"], 800)
        state = self.call(state, now=1300, policy=policy, reports=[self.report(failure=1250)])
        self.assertEqual((state["effective_memory_mib"], state["completed"], state["reason"]),
                         (1000, [], "recent_failure"))
        state = self.call(state, now=2000, policy=policy,
                          reports=[self.report(running=[self.running(first=1000, last=2000)], failure=1250)])
        self.assertEqual(state["effective_memory_mib"], 1000)
        state = self.call(state, now=2001, policy=policy,
                          reports=[self.report(running=[self.running(first=1250, last=2001)], failure=1250)])
        self.assertEqual((state["effective_memory_mib"], state["status"]), (800, "provisional"))

    def test_failure_replay_does_not_clear_successful_post_failure_cohort(self):
        state = self.call(self.state, now=1100, reports=[self.report(failure=1050)])
        rows = self.completed(start=1051)
        state = self.call(state, now=1200, reports=[self.report(completed=rows, failure=1050)])
        self.assertEqual((state["sample_jobs"], state["effective_memory_mib"]), (20, 800))
        state = self.call(state, now=1300, reports=[self.report(completed=rows, failure=1050)])
        self.assertEqual((state["sample_jobs"], state["effective_memory_mib"]), (20, 800))

    def test_new_generation_or_manual_baseline_resets_even_when_value_is_same(self):
        for baseline, policy in [(1000, {**self.policy, "generation": 2}),
                                 (2000, self.policy), (1000, {**self.policy, "jobs": 0})]:
            state = self.call(self.learned(), now=1200, baseline=baseline, policy=policy,
                              reports=[self.report(completed=self.completed())])
            self.assertEqual((state["effective_memory_mib"], state["completed"], state["started_epoch"]),
                             (baseline, [], 1200))

    def test_running_trigger_requires_duration_and_supports_exact_start_boundary(self):
        policy = {**self.policy, "jobs": 0, "seconds": 600}
        state = self.call(None, policy=policy)
        state = self.call(state, now=1699, policy=policy,
                          reports=[self.report(running=[self.running(first=1100, last=1699)])])
        self.assertEqual((state["effective_memory_mib"], state["reason"]), (1000, "insufficient_running"))
        state = self.call(state, now=1700, policy=policy,
                          reports=[self.report(running=[self.running(first=1100, last=1700)])])
        self.assertEqual((state["effective_memory_mib"], state["status"]), (800, "provisional"))

    def test_running_jobs_started_before_opt_in_are_not_reused(self):
        policy = {**self.policy, "jobs": 0, "seconds": 600}
        state = self.call(None, policy=policy)
        state = self.call(state, now=1800, policy=policy,
                          reports=[self.report(running=[self.running(first=1000, last=1800)])])
        self.assertEqual(state["effective_memory_mib"], 1000)

    def test_stale_future_gappy_or_sparse_running_data_never_lowers(self):
        policy = {**self.policy, "jobs": 0, "seconds": 600}
        cases = [(1800, self.running(first=1100, last=1700)),
                 (1699, self.running(first=1100, last=1700)),
                 (2000, self.running(first=1100, last=2000, seconds=600)),
                 (1700, self.running(first=1100, last=1700, samples=2))]
        for now, row in cases:
            with self.subTest(now=now, row=row):
                state = self.call(None, policy=policy)
                state = self.call(state, now=now, policy=policy,
                                  reports=[self.report(running=[row])])
                self.assertEqual(state["effective_memory_mib"], 1000)

    def test_completed_trigger_takes_precedence_over_provisional_running_estimate(self):
        policy = {**self.policy, "seconds": 600}
        state = self.call(None, now=1000, policy=policy)
        state = self.call(state, now=1700, policy=policy, reports=[self.report(
            completed=self.completed(), running=[self.running(last=1700, mean=2000)])])
        self.assertEqual((state["status"], state["candidate_mib"]), ("calibrated", 115))

    def test_clock_reversal_resets_baseline_and_observation_window(self):
        state = self.call(self.learned(), now=1050,
                          reports=[self.report(completed=self.completed())])
        self.assertEqual((state["effective_memory_mib"], state["completed"], state["started_epoch"],
                          state["reason"]), (1000, [], 1050, "clock_reversal"))

    def test_inputs_are_immutable_and_state_roundtrips_json(self):
        state, policy = copy.deepcopy(self.state), copy.deepcopy(self.policy)
        reports = [self.report(completed=self.completed())]
        before = copy.deepcopy((state, policy, reports))
        result = self.call(state, policy=policy, reports=reports)
        self.assertEqual((state, policy, reports), before)
        self.assertEqual(json.loads(json.dumps(result)), result)

    def test_strict_policy_report_and_state_validation(self):
        bad_policies = [{**self.policy, "jobs": 19}, {**self.policy, "jobs": 51},
                        {**self.policy, "seconds": 599}, {**self.policy, "seconds": 86401},
                        {**self.policy, "minimum_mib": 1001}, {**self.policy, "generation": 0},
                        {**self.policy, "jobs": True}, {**self.policy, "unknown": 1}]
        for policy in bad_policies:
            with self.subTest(policy=policy), self.assertRaises(ValueError):
                self.call(None, policy=policy)
        for value in [True, -1, float("nan"), float("inf"), 10 ** 1000]:
            with self.subTest(now=value), self.assertRaises(ValueError):
                self.call(None, now=value)
        for field, value in [("mean_mib", 0), ("mean_mib", True), ("seconds", 14),
                             ("finished_epoch", float("nan")), ("job_id", "job/1")]:
            rows = self.completed()
            rows[0][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.call(self.state, reports=[self.report(completed=rows)])
        for field, value in [("effective_memory_mib", 0), ("effective_memory_mib", 1001),
                             ("updated_epoch", 999), ("status", []), ("reason", "invented"),
                             ("last_lowered_epoch", 1001), ("sample_jobs", True)]:
            state = copy.deepcopy(self.state)
            state[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                self.call(state)
        with self.assertRaises(ValueError):
            self.call(self.state, reports=[self.report(), self.report()])
        with self.assertRaises(ValueError):
            self.call(self.state, reports=[self.report(running=[self.running(first=1200, last=1100)])])
        with self.assertRaises(ValueError):
            self.call(self.state, reports=[self.report(completed=self.completed(51))])


if __name__ == "__main__":
    unittest.main()
