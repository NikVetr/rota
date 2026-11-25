#!/usr/bin/env python3
"""Contract tests for the pure adaptive allocator."""

from __future__ import annotations

from contextlib import redirect_stdout
import csv
from datetime import datetime
import importlib.util
import hashlib
import io
import json
import os
from pathlib import Path
import random
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Optional
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
COORDINATOR = ROOT / "scheduler" / "adaptive_coordinator.py"
sys.path.insert(0, str(COORDINATOR.parent))
SPEC = importlib.util.spec_from_file_location("adaptive_coordinator", COORDINATOR)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
allocate = MODULE.allocate
SnapshotError = MODULE.SnapshotError
STATUS_HEADER = MODULE.STATUS_HEADER


def host(host_id: str, cpu: int = 4, memory: int = 8192, **state: object) -> dict:
    return {
        "host_id": host_id,
        "cpu_units": cpu,
        "memory_mib": memory,
        **state,
    }


def queue(queue_id: str, priority: int = 10, **values: object) -> dict:
    result = {
        "queue_id": queue_id,
        "cpu_units_per_job": 1,
        "memory_mib_per_job": 1024,
        "shared_io_units_per_job": 0,
        "schedule_priority": priority,
        "max_workers": 0,
        "pending_lines": 20,
        "admission": "enabled",
    }
    result.update(values)
    return result


def snapshot(hosts: list[dict], queues: list[dict], **values: object) -> dict:
    result = {
        "schema_version": 1,
        "shared_io_capacity": 20,
        "hosts": hosts,
        "queues": queues,
        "reservations": [],
        "prior_targets": [],
        "cursor": {},
    }
    result.update(values)
    return result


class AllocatorTests(unittest.TestCase):
    def test_one_early_slot_respects_pending_grants_and_start_pacing(self):
        qs = [queue("urgent", priority=90), queue("sample", early_sample_remaining=2),
              queue("second", early_sample_remaining=2)]
        data = snapshot([host("h", cpu=3)], qs)
        plan = allocate(data)
        self.assertEqual(plan["targets"]["h"], {"urgent": 2, "sample": 1, "second": 0})
        profiles = {q["queue_id"]: q for q in qs}
        grants = MODULE._start_grants(plan, profiles, {"h"})
        states = {"h": {"generation": 1, "start_pacing": {"slots": 0, "next_epoch": 99, "cursor": ""}}}
        MODULE._pace_start_grants(plan, grants, profiles, states, "pool", 100, 4)
        self.assertEqual(grants["h"], {"urgent": 0, "sample": 1, "second": 0})
        data["prior_targets"] = [{"host_id": "h", "queue_id": "sample", "target_running": 1}]
        self.assertNotIn("early_sample", allocate(data))
        data["prior_targets"] = []
        qs[1]["early_sample_remaining"] = 0
        self.assertEqual(allocate(data)["early_sample"]["queue_id"], "second")

    def test_early_sample_needs_an_eligible_free_slot(self):
        for h, opts in [(host("h", drain_only=True), {}), (host("h", blocked_queues=["sample"]), {}),
                        (host("h"), {"admission": "paused"}), (host("h"), {"host_priorities": {"h": "off"}}),
                        (host("h", live_memory_mib=512), {}), (host("h"), {"shared_io_units_per_job": 1})]:
            with self.subTest(h=h, opts=opts):
                plan = allocate(snapshot([h], [queue("sample", early_sample_remaining=1, **opts)],
                                         shared_io_capacity=0, allow_unschedulable_queues=True))
                self.assertNotIn("early_sample", plan)
                self.assertEqual(plan["targets"]["h"]["sample"], 0)

    def test_failure_hold_is_per_host_and_queue(self) -> None:
        plan = allocate(snapshot(
            [host("h1", cpu=2, blocked_queues=["bad"]), host("h2", cpu=2)],
            [queue("bad", priority=20), queue("good", priority=10)],
        ))
        self.assertEqual(plan["targets"]["h1"], {"bad": 0, "good": 2})
        self.assertEqual(plan["targets"]["h2"]["bad"], 2)

    def test_failure_hold_preserves_running_charges(self) -> None:
        plan = allocate(snapshot(
            [host("h", cpu=2, blocked_queues=["bad"])],
            [queue("bad"), queue("good")],
            reservations=[{"host_id": "h", "queue_id": "bad", "count": 1}],
        ))
        self.assertEqual(plan["targets"]["h"]["bad"], 0)
        self.assertEqual(plan["reservation_envelopes"]["h"]["bad"], 1)
        self.assertEqual(plan["targets"]["h"]["good"], 1)
        self.assertEqual(plan["limits"]["bad"], ["failure_hold"])

    def test_malformed_failure_holds_are_rejected(self) -> None:
        for blocked in ("bad", [1], [""]):
            with self.assertRaises(SnapshotError):
                allocate(snapshot([host("h", blocked_queues=blocked)], [queue("bad")]))

    def test_cpu_memory_and_global_io_limits(self) -> None:
        data = snapshot(
            [host("h1"), host("h2")],
            [queue("q", cpu_units_per_job=2, memory_mib_per_job=4096,
                   shared_io_units_per_job=1)],
            shared_io_capacity=3,
        )
        plan = allocate(data)
        self.assertEqual(sum(row["q"] for row in plan["targets"].values()), 3)
        self.assertTrue(all(row["q"] <= 2 for row in plan["targets"].values()))

    def test_live_capacity_limits_placement_but_not_static_fit(self) -> None:
        plan = allocate(snapshot(
            [host("h", cpu=8, memory=8192, live_cpu_units=2, live_memory_mib=3072)],
            [queue("q", cpu_units_per_job=1, memory_mib_per_job=1024)],
        ))
        self.assertEqual(plan["targets"]["h"]["q"], 2)

        unavailable = allocate(snapshot(
            [host("h", cpu=8, memory=8192, live_cpu_units=0, live_memory_mib=0)],
            [queue("q", cpu_units_per_job=2, memory_mib_per_job=4096)],
        ))
        self.assertEqual(unavailable["targets"]["h"]["q"], 0)

    def test_budget_shrink_below_job_size_preserves_running_envelope(self) -> None:
        data = snapshot(
            [host("h1", cpu=1, memory=512)], [queue("q1", cpu_units_per_job=2)],
            reservations=[{"host_id": "h1", "queue_id": "q1", "count": 1}],
            allow_unschedulable_queues=True,
        )
        plan = allocate(data)
        self.assertEqual(plan["reservation_envelopes"]["h1"]["q1"], 1)
        self.assertEqual(plan["targets"]["h1"]["q1"], 0)
        self.assertEqual(plan["limits"]["q1"], ["cpu", "memory"])

    def test_configured_capacity_remains_a_hard_ceiling(self) -> None:
        plan = allocate(snapshot(
            [host("h", cpu=2, memory=2048, live_cpu_units=20, live_memory_mib=20000)],
            [queue("q", memory_mib_per_job=1024)],
        ))
        self.assertEqual(plan["targets"]["h"]["q"], 2)

    def test_live_capacity_drop_is_nonpreemptive(self) -> None:
        plan = allocate(snapshot(
            [host("h", cpu=4, memory=4096, live_cpu_units=1, live_memory_mib=1024)],
            [queue("q", memory_mib_per_job=1024)],
            reservations=[{"host_id": "h", "queue_id": "q", "count": 2}],
        ))
        self.assertEqual(plan["reservation_envelopes"]["h"]["q"], 2)
        self.assertEqual(plan["targets"]["h"]["q"], 0)

    def test_observed_reservation_memory_replaces_declared_charge(self) -> None:
        plan = allocate(snapshot(
            [host("h", cpu=8, memory=8192)],
            [queue("q", memory_mib_per_job=1024)],
            reservations=[{
                "host_id": "h", "queue_id": "q", "count": 1,
                "memory_mib": 7500,
            }],
        ))
        self.assertEqual(plan["targets"]["h"]["q"], 1)

    def test_observed_reservation_memory_cannot_undercharge_declaration(self) -> None:
        with self.assertRaisesRegex(SnapshotError, "below declared cost"):
            allocate(snapshot(
                [host("h", memory=8192)],
                [queue("q", memory_mib_per_job=1024)],
                reservations=[{
                    "host_id": "h", "queue_id": "q", "count": 1,
                    "memory_mib": 512,
                }],
            ))

    def test_strict_priority_takes_all_immediately_free_capacity(self) -> None:
        plan = allocate(snapshot(
            [host("h", cpu=3)],
            [queue("low", 10), queue("high", 20)],
        ))
        self.assertEqual(plan["targets"]["h"], {"high": 3, "low": 0})

    def test_equal_priority_rotates_and_is_stable(self) -> None:
        data = snapshot(
            [host("h", cpu=2)],
            [queue("a"), queue("b"), queue("c")],
            cursor={"10": "a"},
        )
        first = allocate(data)
        self.assertEqual(first["targets"]["h"], {"a": 0, "b": 1, "c": 1})
        self.assertEqual(first["cursor"], {"10": "c"})
        self.assertEqual(first, allocate(data))
        data["cursor"] = first["cursor"]
        second = allocate(data)
        self.assertEqual(second["targets"]["h"], {"a": 1, "b": 1, "c": 0})

    def test_pause_and_maximum(self) -> None:
        plan = allocate(snapshot(
            [host("h", cpu=4)],
            [queue("capped", max_workers=1), queue("paused", admission="paused")],
        ))
        self.assertEqual(plan["targets"]["h"], {"capped": 1, "paused": 0})

        draining = snapshot(
            [host("h", cpu=4)],
            [queue("q", max_workers=2)],
            reservations=[{"host_id": "h", "queue_id": "q", "count": 3}],
        )
        self.assertEqual(allocate(draining)["targets"]["h"]["q"], 2)

    def test_stale_and_closed_hosts_retain_envelopes_but_get_no_targets(self) -> None:
        data = snapshot(
            [host("stale", reachable=False), host("closed", drain_only=True), host("open")],
            [queue("q")],
            reservations=[{"host_id": "stale", "queue_id": "q", "count": 2}],
        )
        plan = allocate(data)
        self.assertEqual(plan["reservation_envelopes"]["stale"]["q"], 2)
        self.assertEqual(plan["targets"]["stale"]["q"], 0)
        self.assertEqual(plan["targets"]["closed"]["q"], 0)
        self.assertGreater(plan["targets"]["open"]["q"], 0)

    def test_maximum_charges_unreachable_reservations(self) -> None:
        data = snapshot(
            [host("stale", reachable=False), host("open")],
            [queue("q", max_workers=3)],
            reservations=[{"host_id": "stale", "queue_id": "q", "count": 2}],
        )
        self.assertEqual(allocate(data)["targets"]["open"]["q"], 1)

    def test_conservative_prior_target_uses_max_not_sum(self) -> None:
        data = snapshot(
            [host("h", cpu=4)],
            [queue("q")],
            reservations=[{"host_id": "h", "queue_id": "q", "count": 2}],
            prior_targets=[{
                "host_id": "h", "queue_id": "q", "target_running": 3,
                "acknowledged": False, "lease_valid": False,
            }],
        )
        plan = allocate(data)
        self.assertEqual(plan["reservation_envelopes"]["h"]["q"], 3)
        self.assertEqual(plan["targets"]["h"]["q"], 4)

        data["prior_targets"][0].update(acknowledged=True, lease_valid=False)
        self.assertEqual(allocate(data)["reservation_envelopes"]["h"]["q"], 2)

    def test_lower_memory_keeps_old_unknown_grants_charged(self) -> None:
        data = snapshot(
            [host("h", cpu=8, memory=1200)],
            [queue("q", memory_mib_per_job=200)],
            prior_targets=[{
                "host_id": "h", "queue_id": "q", "target_running": 2,
                "memory_mib_per_job": 500, "acknowledged": False, "lease_valid": False,
            }],
        )
        self.assertEqual(allocate(data)["targets"]["h"]["q"], 3)
        data["reservations"] = [{
            "host_id": "h", "queue_id": "q", "count": 1, "memory_mib": 600,
        }]
        self.assertEqual(allocate(data)["targets"]["h"]["q"], 2)

        data["prior_targets"][0].update(acknowledged=True, lease_valid=False)
        self.assertEqual(allocate(data)["targets"]["h"]["q"], 4)

    def test_restating_reserved_targets_does_not_advance_cursor(self) -> None:
        data = snapshot(
            [host("h", cpu=2)],
            [queue("a"), queue("b")],
            reservations=[
                {"host_id": "h", "queue_id": "a", "count": 1},
                {"host_id": "h", "queue_id": "b", "count": 1},
            ],
            cursor={"10": "a"},
        )
        plan = allocate(data)
        self.assertEqual(plan["targets"]["h"], {"a": 1, "b": 1})
        self.assertEqual(plan["cursor"], {"10": "a"})

    def test_pending_lines_cap_only_new_positions(self) -> None:
        data = snapshot(
            [host("h", cpu=5)],
            [queue("q", pending_lines=1)],
            reservations=[{"host_id": "h", "queue_id": "q", "count": 2}],
        )
        self.assertEqual(allocate(data)["targets"]["h"]["q"], 3)
        data["queues"][0]["pending_lines"] = 0
        self.assertEqual(allocate(data)["targets"]["h"]["q"], 0)

    def test_large_high_priority_job_reserves_one_drain_host(self) -> None:
        data = snapshot(
            [host("h1", memory=8192), host("h2", memory=8192)],
            [
                queue("high", 20, memory_mib_per_job=4096, pending_lines=1),
                queue("low", 10, memory_mib_per_job=3072),
            ],
            reservations=[
                {"host_id": "h1", "queue_id": "low", "count": 2},
                {"host_id": "h2", "queue_id": "low", "count": 2},
            ],
        )
        plan = allocate(data)
        self.assertEqual(plan["targets"]["h1"]["high"], 0)
        self.assertEqual(plan["drain_reservations"], [{"host_id": "h1", "queue_id": "high"}])
        self.assertEqual(plan["targets"]["h1"]["low"], 1)
        self.assertEqual(plan["targets"]["h2"]["low"], 2)

    def test_large_job_drains_only_a_host_where_it_can_fit(self) -> None:
        data = snapshot(
            [host("small", memory=3072), host("large", memory=8192)],
            [
                queue("high", 20, memory_mib_per_job=4096, pending_lines=1),
                queue("low", 10, memory_mib_per_job=1024),
            ],
            reservations=[
                {"host_id": "small", "queue_id": "low", "count": 3},
                {"host_id": "large", "queue_id": "low", "count": 4},
            ],
        )
        plan = allocate(data)
        self.assertEqual(
            plan["drain_reservations"], [{"host_id": "large", "queue_id": "high"}]
        )
        self.assertEqual(plan["targets"]["small"]["low"], 3)
        self.assertEqual(plan["targets"]["large"]["low"], 3)

    def test_common_nine_per_host_takeover_starts_and_drains(self) -> None:
        hosts = [host(f"h{number}", cpu=10) for number in range(1, 6)]
        reservations = [
            {"host_id": item["host_id"], "queue_id": "a", "count": 9}
            for item in hosts
        ]
        plan = allocate(snapshot(
            hosts,
            [queue("a", 10, memory_mib_per_job=1),
             queue("b", 20, memory_mib_per_job=1, pending_lines=45)],
            reservations=reservations,
        ))
        self.assertEqual(sum(row["b"] for row in plan["targets"].values()), 5)
        self.assertEqual(plan["drain_reservations"], [{"host_id": "h1", "queue_id": "b"}])
        self.assertEqual(sum(row["a"] for row in plan["targets"].values()), 44)

    def test_invalid_snapshot_fails_loudly(self) -> None:
        with self.assertRaisesRegex(SnapshotError, "cannot fit"):
            allocate(snapshot(
                [host("h", cpu=1, memory=1024)],
                [queue("q", cpu_units_per_job=2)],
            ))

    def test_simulate_cli_emits_stable_json(self) -> None:
        data = snapshot([host("h", cpu=1)], [queue("q")])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "snapshot.json"
            path.write_text(json.dumps(data), encoding="utf-8")
            command = [sys.executable, str(COORDINATOR), "--simulate", str(path)]
            first = subprocess.run(command, check=True, capture_output=True, text=True)
            second = subprocess.run(command, check=True, capture_output=True, text=True)
        self.assertEqual(first.stdout, second.stdout)
        self.assertEqual(json.loads(first.stdout), allocate(data))

    def test_negative_priorities_are_ordered_numerically(self) -> None:
        plan = allocate(snapshot(
            [host("h", cpu=1)],
            [queue("lower", -20), queue("higher", -10)],
            cursor={"-10": "higher"},
        ))
        self.assertEqual(plan["targets"]["h"], {"higher": 1, "lower": 0})

    def test_start_grants_wait_for_cross_host_target_movement(self) -> None:
        plan = {
            "targets": {"a": {"q": 1}, "b": {"q": 1}},
            "reservation_envelopes": {"a": {"q": 2}, "b": {"q": 0}},
        }
        grants = MODULE._start_grants(
            plan, {"q": queue("q", max_workers=2)}, {"a", "b"}
        )
        self.assertEqual(grants, {"a": {"q": 0}, "b": {"q": 0}})

    def test_memory_constrained_host_does_not_block_safe_starts_elsewhere(self) -> None:
        profile = queue("q")
        plan = allocate(snapshot(
            [host("a", cpu=4, memory=4096, live_memory_mib=1536),
             host("b", cpu=2, memory=2048)],
            [profile],
            reservations=[{"host_id": "a", "queue_id": "q", "count": 2}],
        ))
        self.assertEqual(plan["targets"], {"a": {"q": 0}, "b": {"q": 2}})
        self.assertEqual(plan["reservation_envelopes"]["a"]["q"], 2)
        self.assertEqual(
            MODULE._start_grants(plan, {"q": profile}, {"a", "b"}),
            {"a": {"q": 0}, "b": {"q": 2}},
        )

    def test_cross_host_starts_still_obey_global_pending_and_worker_cap(self) -> None:
        plan = {
            "targets": {"a": {"q": 0}, "b": {"q": 2}, "c": {"q": 2}},
            "reservation_envelopes": {"a": {"q": 2}, "b": {"q": 0}, "c": {"q": 0}},
        }
        for profile in (queue("q", pending_lines=1), queue("q", max_workers=3)):
            with self.subTest(profile=profile):
                self.assertEqual(
                    MODULE._start_grants(plan, {"q": profile}, {"a", "b", "c"}),
                    {"a": {"q": 0}, "b": {"q": 1}, "c": {"q": 0}},
                )

    def test_cross_host_starts_keep_draining_workers_shared_io_charged(self) -> None:
        profile = queue("q", shared_io_units_per_job=1)
        plan = allocate(snapshot(
            [host("a", live_memory_mib=1536), host("b")],
            [profile], shared_io_capacity=3,
            reservations=[{"host_id": "a", "queue_id": "q", "count": 2}],
        ))
        self.assertEqual(
            MODULE._start_grants(plan, {"q": profile}, {"a", "b"}),
            {"a": {"q": 0}, "b": {"q": 1}},
        )

    def test_start_grants_obey_pending_and_conservative_envelopes(self) -> None:
        plan = {
            "targets": {"a": {"q": 2}, "b": {"q": 1}},
            "reservation_envelopes": {"a": {"q": 2}, "b": {"q": 0}},
        }
        self.assertEqual(
            MODULE._start_grants(
                plan, {"q": queue("q", pending_lines=1)}, {"a", "b"}
            ),
            {"a": {"q": 0}, "b": {"q": 1}},
        )
        self.assertEqual(
            MODULE._start_grants(
                plan, {"q": queue("q", pending_lines=0)}, {"a", "b"}
            ),
            {"a": {"q": 0}, "b": {"q": 0}},
        )

    def test_idle_pool_accepts_an_empty_queue_list(self) -> None:
        plan = allocate(snapshot([host("h")], []))
        self.assertEqual(plan["targets"], {"h": {}})
        self.assertEqual(plan["cursor"], {})


def random_snapshot(seed: int) -> dict:
    """Varied scalar-priority snapshot; uses only random(), whose stream is stable."""
    rng = random.Random(seed)

    def pick(count: int) -> int:
        return int(rng.random() * count)

    def chance(probability: float) -> bool:
        return rng.random() < probability

    hosts = []
    for index in range(1 + pick(4)):
        cpu, memory = 1 + pick(8), 1024 * (1 + pick(8))
        state: dict = {}
        if chance(0.2):
            state["live_cpu_units"] = pick(cpu + 1)
        if chance(0.2):
            state["live_memory_mib"] = pick(memory + 1)
        for key, probability in (("reachable", 0.1), ("enabled", 0.05)):
            if chance(probability):
                state[key] = False
        if chance(0.1):
            state["drain_only"] = True
        hosts.append(host(f"h{index}", cpu, memory, **state))
    queues = [
        queue(
            f"q{index}", [-5, 10, 10, 20, 50][pick(5)],
            cpu_units_per_job=1 + pick(2), memory_mib_per_job=256 * (1 + pick(8)),
            shared_io_units_per_job=pick(2), max_workers=pick(6), pending_lines=pick(12),
            admission="paused" if chance(0.15) else "enabled",
        )
        for index in range(pick(6))
    ]
    reservations, prior_targets = [], []
    for item in hosts:
        if queues and chance(0.15):
            item["blocked_queues"] = [queues[pick(len(queues))]["queue_id"]]
        for profile in queues:
            if chance(0.2):
                count = 1 + pick(2)
                reservation = {"host_id": item["host_id"], "queue_id": profile["queue_id"],
                               "count": count}
                if chance(0.3):
                    reservation["memory_mib"] = count * profile["memory_mib_per_job"] + pick(512)
                reservations.append(reservation)
            if chance(0.15):
                prior_targets.append({
                    "host_id": item["host_id"], "queue_id": profile["queue_id"],
                    "target_running": pick(4), "acknowledged": chance(0.5),
                    "lease_valid": chance(0.5), "memory_mib_per_job": 256 * (1 + pick(8)),
                })
    cursor = {
        str(profile["schedule_priority"]): profile["queue_id"]
        for profile in queues if chance(0.4)
    }
    return snapshot(
        hosts, queues, shared_io_capacity=pick(16), reservations=reservations,
        prior_targets=prior_targets, cursor=cursor, allow_unschedulable_queues=True,
    )


def scheduling_digest(module, data: dict) -> str:
    """Hash the plan, start grants and paced grants a coordinator would derive."""
    data = json.loads(json.dumps(data))
    plan = module.allocate(data)
    queues = {profile["queue_id"]: profile for profile in data["queues"]}
    open_hosts = {
        item["host_id"] for item in data["hosts"]
        if item.get("enabled", True) and item.get("reachable", True)
        and not item.get("drain_only", False)
    }
    grants = module._start_grants(plan, queues, open_hosts)
    states = {
        host_id: {"generation": 3, "start_pacing": {"slots": 1, "next_epoch": 50, "cursor": ""}}
        for host_id in plan["targets"]
    }
    module._pace_start_grants(plan, grants, queues, states, "pool", 100.0, 4)
    text = json.dumps([plan, grants, states], sort_keys=True)
    return hashlib.sha256(text.encode()).hexdigest()


class HostPriorityTests(unittest.TestCase):
    # Aggregate of scheduling_digest over random_snapshot(0..199), computed with
    # the scalar-only allocator before per-host priorities existed. Regenerate
    # only for a reviewed, intentional change to scalar scheduling.
    SCALAR_DIGEST = "619ace7840d44f830c209e7e05b1cff607194e04305ac916c8c1c5d5a29d9a00"

    def test_scalar_plans_match_the_pre_override_allocator(self) -> None:
        digests = [scheduling_digest(MODULE, random_snapshot(seed)) for seed in range(200)]
        aggregate = hashlib.sha256("".join(digests).encode()).hexdigest()
        self.assertEqual(aggregate, self.SCALAR_DIGEST)

    def test_overrides_equal_to_the_default_change_nothing(self) -> None:
        for seed in range(200):
            data = random_snapshot(seed)
            expected = scheduling_digest(MODULE, data)
            for profile in data["queues"]:
                profile["host_priorities"] = {
                    item["host_id"]: profile["schedule_priority"] for item in data["hosts"]
                }
            with self.subTest(seed=seed):
                self.assertEqual(scheduling_digest(MODULE, data), expected)

    def test_each_host_ranks_queues_by_their_level_on_that_host(self) -> None:
        hosts = [host("mine", cpu=2), host("friend", cpu=2)]
        scalar = allocate(snapshot(hosts, [queue("guest", 50), queue("owner", 10)]))
        self.assertEqual(scalar["targets"]["friend"], {"guest": 2, "owner": 0})
        plan = allocate(snapshot(
            hosts, [queue("guest", 50, host_priorities={"friend": 1}), queue("owner", 10)],
        ))
        self.assertEqual(plan["targets"], {
            "friend": {"guest": 0, "owner": 2}, "mine": {"guest": 2, "owner": 0},
        })
        self.assertEqual(plan["cursor"], {"10": "owner", "50": "guest"})

    def test_low_priority_guest_yields_a_node_without_preemption(self) -> None:
        reservations = [{"host_id": "friend", "queue_id": "guest", "count": 2}]
        scalar = allocate(snapshot(
            [host("friend", cpu=2)], [queue("guest", 50), queue("owner", 10)],
            reservations=reservations,
        ))
        self.assertEqual(scalar["targets"]["friend"], {"guest": 2, "owner": 0})
        self.assertEqual(scalar["drain_reservations"], [])
        plan = allocate(snapshot(
            [host("friend", cpu=2)],
            [queue("guest", 50, host_priorities={"friend": 1}), queue("owner", 10)],
            reservations=reservations,
        ))
        # Both running guest jobs continue; the next freed slot goes to the owner.
        self.assertEqual(plan["reservation_envelopes"]["friend"]["guest"], 2)
        self.assertEqual(plan["targets"]["friend"], {"guest": 1, "owner": 0})
        self.assertEqual(plan["drain_reservations"], [{"host_id": "friend", "queue_id": "owner"}])

    def test_off_keeps_a_queue_off_a_host_like_a_failure_hold(self) -> None:
        hosts = [host("mine", cpu=2), host("friend", cpu=2)]
        reservations = [{"host_id": "friend", "queue_id": "private", "count": 1}]
        plan = allocate(snapshot(
            hosts, [queue("private", 50, max_workers=2, host_priorities={"friend": "off"})],
            reservations=reservations,
        ))
        held = allocate(snapshot(
            [hosts[0], host("friend", cpu=2, blocked_queues=["private"])],
            [queue("private", 50, max_workers=2)], reservations=reservations,
        ))
        self.assertEqual(plan["targets"], held["targets"])
        self.assertEqual(plan["targets"], {"friend": {"private": 0}, "mine": {"private": 1}})
        self.assertEqual(plan["reservation_envelopes"]["friend"]["private"], 1)

        nowhere = allocate(snapshot(
            [host("friend")], [queue("private", host_priorities={"friend": "off"})],
        ))
        self.assertEqual(nowhere["targets"], {"friend": {"private": 0}})
        self.assertEqual(nowhere["limits"]["private"], ["host_unavailable"])

    def test_drain_reservation_uses_the_blocked_hosts_level(self) -> None:
        data = snapshot(
            [host("h1", memory=8192), host("h2", memory=8192)],
            [
                queue("high", 20, memory_mib_per_job=4096, pending_lines=1,
                      host_priorities={"h1": 5}),
                queue("low", 10, memory_mib_per_job=3072),
            ],
            reservations=[
                {"host_id": "h1", "queue_id": "low", "count": 2},
                {"host_id": "h2", "queue_id": "low", "count": 2},
            ],
        )
        plan = allocate(data)
        # Without the override this reserves h1 (see the scalar drain test).
        self.assertEqual(plan["drain_reservations"], [{"host_id": "h2", "queue_id": "high"}])
        self.assertEqual(plan["targets"]["h1"]["low"], 2)
        self.assertEqual(plan["targets"]["h2"]["low"], 1)

    def test_round_robin_cursor_is_kept_per_effective_level(self) -> None:
        data = snapshot(
            [host("h1", cpu=2), host("h2", cpu=1)],
            [queue("a", host_priorities={"h2": 20}), queue("b"), queue("c")],
            cursor={"10": "a"},
        )
        first = allocate(data)
        self.assertEqual(first["targets"], {
            "h1": {"a": 0, "b": 1, "c": 1}, "h2": {"a": 1, "b": 0, "c": 0},
        })
        self.assertEqual(first["cursor"], {"10": "c", "20": "a"})
        data["cursor"] = first["cursor"]
        second = allocate(data)
        self.assertEqual(second["targets"]["h1"], {"a": 1, "b": 1, "c": 0})

    def test_start_pacing_orders_grants_by_each_hosts_level(self) -> None:
        for overrides, winner in (({}, "guest"), ({"h": 1}, "owner")):
            queues = {"guest": queue("guest", 50, host_priorities=overrides),
                      "owner": queue("owner", 10)}
            states = {"h": {"generation": 0,
                            "start_pacing": {"slots": 0, "next_epoch": 100, "cursor": ""}}}
            plan = {"targets": {"h": {"guest": 1, "owner": 1}},
                    "reservation_envelopes": {"h": {"guest": 0, "owner": 0}}}
            grants = MODULE._start_grants(plan, queues, {"h"})
            MODULE._pace_start_grants(plan, grants, queues, states, "pool", 100, 4)
            with self.subTest(overrides=overrides):
                self.assertEqual(grants["h"], {"guest": int(winner == "guest"),
                                               "owner": int(winner == "owner")})

    def test_invalid_snapshot_overrides_fail_loudly(self) -> None:
        for overrides in ({"other": 1}, {"h": "high"}, {"h": True}, {"h": 1.5}, ["h"]):
            with self.subTest(overrides=overrides), self.assertRaises(SnapshotError):
                allocate(snapshot([host("h")], [queue("q", host_priorities=overrides)]))

    def test_priority_spec_grammar_and_canonical_form(self) -> None:
        hosts = {"small", "large"}
        resolve = MODULE.resolve_priority_spec
        self.assertEqual(resolve("70", hosts), (70, {}))
        self.assertEqual(resolve("small=5,large=off,70", hosts), (70, {"small": 5, "large": "off"}))
        self.assertEqual(resolve("large=-3", hosts), (50, {"large": -3}))
        self.assertEqual(resolve("-5,small=off", hosts), (-5, {"small": "off"}))
        self.assertEqual(MODULE.format_host_priorities({"small": 5, "large": "off"}),
                         "large=off,small=5")
        for spec, message in (
            ("", "nonempty"), ("70,", "empty item"), (",small=1", "empty item"),
            ("small=1,,large=2", "empty item"), ("1,2,small=3", "more than one default"),
            ("small=1,small=2", "duplicate host"), ("small=1, large=2", "whitespace"),
            ("small =1", "whitespace"), ("1.5,small=1", "malformed"), ("x,small=1", "malformed"),
            ("small=x", "integer or off"), ("small=", "integer or off"), ("small=1=2", "integer or off"),
            ("small=OFF", "integer or off"), ("=1", "invalid host"),
            ("tiny=1", "unknown host ID.*valid host IDs: large, small"),
        ):
            with self.subTest(spec=spec), self.assertRaisesRegex(SnapshotError, message):
                resolve(spec, hosts)


class StartPacingTests(unittest.TestCase):
    def setUp(self):
        self.states = {"h": {"generation": 0}}
        self.queues = {"q": queue("q")}

    def pace(self, now, occupied=0, desired=10, interval=4):
        plan = {"targets": {"h": {"q": desired}},
                "reservation_envelopes": {"h": {"q": occupied}}}
        grants = MODULE._start_grants(plan, self.queues, {"h"})
        MODULE._pace_start_grants(plan, grants, self.queues, self.states, "pool", now, interval)
        self.states["h"]["generation"] += 1
        self.assertLessEqual(plan["targets"]["h"]["q"], occupied + grants["h"]["q"])
        return grants["h"]["q"]

    def test_initial_ramp_fills_ten_slots_with_jittered_spacing(self):
        self.assertEqual(self.pace(100), 0)
        now = self.states["h"]["start_pacing"]["next_epoch"]
        self.assertTrue(100 <= now <= 102)
        for occupied in range(10):
            self.assertEqual(self.pace(now - 0.001, occupied), 0)
            self.assertEqual(self.pace(now, occupied), 1)
            following = self.states["h"]["start_pacing"]["next_epoch"]
            self.assertTrue(3 <= following - now <= 5)
            now = following
        self.assertLessEqual(now, 152)

    def test_replacements_are_prompt_even_when_all_old_jobs_finish(self):
        self.states["h"]["start_pacing"] = {"slots": 10, "next_epoch": 200, "cursor": "q"}
        self.assertEqual(self.pace(100, occupied=8), 2)
        self.assertEqual(self.pace(101, occupied=0), 10)

    def test_provisioning_increase_is_paced_but_old_slots_can_refill(self):
        self.states["h"]["start_pacing"] = {"slots": 3, "next_epoch": 104, "cursor": "q"}
        self.assertEqual(self.pace(100, occupied=2), 1)
        self.assertEqual(self.pace(104, occupied=2), 2)
        self.assertEqual(self.states["h"]["start_pacing"]["slots"], 4)

    def test_pause_and_drain_do_not_issue_deferred_starts(self):
        self.pace(100)
        self.assertEqual(self.pace(200, desired=0), 0)
        self.assertEqual(self.states["h"]["start_pacing"]["slots"], 0)
        self.assertEqual(self.states["h"]["start_pacing"]["next_epoch"], 0)
        self.assertEqual(self.pace(201), 0)
        self.assertEqual(self.pace(300, occupied=2, desired=0), 0)

    def test_host_offsets_differ_and_all_queues_share_one_expansion_slot(self):
        queues = {"a": queue("a"), "b": queue("b")}
        states = {host: {"generation": 0} for host in ("h1", "h2")}
        plan = {"targets": {host: {"a": 5, "b": 5} for host in states},
                "reservation_envelopes": {host: {"a": 0, "b": 0} for host in states}}
        grants = MODULE._start_grants(plan, queues, set(states))
        MODULE._pace_start_grants(plan, grants, queues, states, "pool", 100, 4)
        self.assertNotEqual(states["h1"]["start_pacing"]["next_epoch"],
                            states["h2"]["start_pacing"]["next_epoch"])
        plan["targets"] = {host: {"a": 5, "b": 5} for host in states}
        grants = MODULE._start_grants(plan, queues, set(states))
        MODULE._pace_start_grants(plan, grants, queues, states, "pool", 102, 4)
        self.assertTrue(all(sum(host.values()) == 1 for host in grants.values()))
        self.assertTrue(all(host["a"] == 1 for host in grants.values()))
        plan["targets"] = {host: {"a": 5, "b": 5} for host in states}
        plan["reservation_envelopes"] = {host: {"a": 1, "b": 0} for host in states}
        grants = MODULE._start_grants(plan, queues, set(states))
        MODULE._pace_start_grants(plan, grants, queues, states, "pool", 107, 4)
        self.assertTrue(all(host["b"] == 1 for host in grants.values()))

    def test_disabled_pacing_retains_full_grants(self):
        self.assertEqual(self.pace(100, interval=0), 10)

    def test_priority_replacement_can_expand_while_incumbent_finishes(self):
        queues = {"old": queue("old", priority=20), "pilot": queue("pilot", priority=50, max_workers=1)}
        self.states["h"]["start_pacing"] = {"slots": 1, "next_epoch": 104, "cursor": "old"}
        for now, expected in ((103, 0), (104, 1)):
            plan = {"targets": {"h": {"old": 0, "pilot": 1}},
                    "reservation_envelopes": {"h": {"old": 1, "pilot": 0}}}
            grants = MODULE._start_grants(plan, queues, {"h"})
            self.assertEqual(grants["h"]["pilot"], 1)
            MODULE._pace_start_grants(plan, grants, queues, self.states, "pool", now, 4)
            self.assertEqual(grants["h"], {"old": 0, "pilot": expected})
            self.assertEqual(plan["targets"]["h"], {"old": 0, "pilot": expected})
            self.assertEqual(plan["reservation_envelopes"]["h"]["old"], 1)


class CoordinatorRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.state = self.root / "state"
        self.remote_home = self.root / "remote-home"
        self.batch = self.root / "batch"
        self.queue_id = "queue-1"
        self.pool_id = "pool-1"
        (self.state / "queues").mkdir(parents=True)
        plan_dir = self.state / "plans" / "host-1"
        plan_dir.mkdir(parents=True)
        (plan_dir / "state.json").write_text(
            '{"controls":{},"generation":0,"pending":false,'
            '"targets":{},"envelope_targets":{}}\n',
            encoding="utf-8",
        )
        (self.batch / "adaptive").mkdir(parents=True)
        (self.batch / "done").mkdir()
        (self.batch / "claims").mkdir()
        (self.root / "model").mkdir()
        (self.batch / "run_meta.env").write_text("total_jobs=2\n", encoding="utf-8")
        remote_state = self.remote_home / ".parallel-workers" / "adaptive" / self.pool_id
        remote_state.mkdir(parents=True)
        (remote_state / "agent.env").write_text(
            f"schema_version=1\npool_id={self.pool_id}\nhost_id=host-1\n", encoding="utf-8",
        )
        remote_bin = remote_state / "bin"
        remote_bin.mkdir()
        self.fake_agent_script = remote_bin / "mmadaptiveagent.sh"
        self.fake_agent_script.write_text(
            "#!/usr/bin/env bash\n"
            "trap 'kill \"$child\" 2>/dev/null || true; wait \"$child\" 2>/dev/null || true; exit 0' TERM\n"
            "sleep 300 &\nchild=$!\nwait \"$child\"\n",
            encoding="utf-8",
        )
        self.fake_agent_script.chmod(0o755)
        self.fake_agent = subprocess.Popen(
            ["bash", str(self.fake_agent_script), "--state-dir", str(remote_state)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        (remote_state / "agent.lock").write_text(
            f"pid={self.fake_agent.pid}\nscript={self.fake_agent_script}\n"
            f"pool_id={self.pool_id}\nhost_id=host-1\nstate_dir={remote_state}\n",
            encoding="utf-8",
        )

        self.resources = self.root / "hosts.tsv"
        self.resources.write_text(
            "host_id\tssh_target\tcpu_units\tmemory_mib\tenabled\n"
            "host-1\tfake-target\t2\t2048\t1\n",
            encoding="utf-8",
        )
        self.editable = self.batch / "queue-edit.txt"
        self.editable.write_text("job_1 true\njob_2 true\n", encoding="utf-8")
        queue_hash = hashlib.sha256(self.editable.read_bytes()).hexdigest()
        (self.root / "original.txt").write_text("job_1 true\njob_2 true\n", encoding="utf-8")
        self.resource_env = self.batch / "adaptive" / "resources.env"
        self.control_env = self.batch / "adaptive" / "control.env"
        self.resource_env.write_text(
            "schema_version=1\nqueue_id=queue-1\ncpu_units_per_job=1\n"
            "memory_mib_per_job=256\nshared_io_units_per_job=0\n",
            encoding="utf-8",
        )
        resource_hash = hashlib.sha256(self.resource_env.read_bytes()).hexdigest()
        self.write_control(1, "enabled")
        registration = self.state / "queues" / f"{self.queue_id}.env"
        registration.write_text(
            "schema_version=1\npool_id=pool-1\nqueue_id=queue-1\n"
            f"batch_dir={self.batch}\n"
            f"original_queue_file={self.root / 'original.txt'}\n"
            f"editable_queue_file={self.editable}\n"
            f"model_dir={self.root / 'model'}\n"
            f"log_dir={self.batch}\n"
            f"lock_dir={self.root / 'lock'}\n"
            "tag_clean=test-tag\n"
            f"resources_file={self.resource_env}\n"
            f"resources_sha256={resource_hash}\n"
            f"queue_sha256={queue_hash}\n"
            f"control_file={self.control_env}\n"
            "registered_epoch=1\n",
            encoding="utf-8",
        )
        (self.state / "pool.env").write_text(
            "schema_version=1\npool_id=pool-1\nstate=ACTIVE\n",
            encoding="utf-8",
        )
        self.write_agent_status(0)
        self.fake_ssh = self.root / "ssh"
        self.ssh_log = self.root / "ssh.log"
        self.ssh_log.write_text("", encoding="utf-8")
        self.fake_ssh.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "while [ \"$#\" -gt 0 ]; do\n"
            "  case \"$1\" in -o) shift 2 ;; -n) shift ;; *) break ;; esac\n"
            "done\n"
            "[ \"$#\" -eq 2 ]\n"
            "target=\"$1\"; shift\n"
            "printf '%s\\n' \"$target\" >> \"$FAKE_SSH_LOG\"\n"
            "[[ \"$1\" != *'status='* ]] || exit 97\n"
            "if [ \"${FAKE_FAIL_PUBLISH:-0}\" = 1 ] && [[ \"$1\" == *'allocation='* ]]; then\n"
            "  exit 19\n"
            "fi\n"
            "HOME=\"$FAKE_REMOTE_HOME\" bash -c \"$1\"\n",
            encoding="utf-8",
        )
        self.fake_ssh.chmod(0o755)

    def tearDown(self) -> None:
        self.fake_agent.terminate()
        self.fake_agent.wait(timeout=5)
        self.temporary.cleanup()

    @property
    def remote_state(self) -> Path:
        return self.remote_home / ".parallel-workers" / "adaptive" / self.pool_id

    def write_control(
        self, generation: int, admission: str, memory: int | None = None,
    ) -> None:
        self.control_env.write_text(
            f"schema_version={2 if memory is not None else 1}\nqueue_id=queue-1\n"
            f"generation={generation}\nadmission={admission}\n"
            "schedule_priority=10\nmax_workers=0\nupdated_epoch=1\n"
            + (f"memory_mib_per_job={memory}\n" if memory is not None else ""),
            encoding="utf-8",
        )

    def test_memory_override_preserves_immutable_profile_and_reports_both_values(self) -> None:
        original = self.resource_env.read_bytes()
        self.write_control(2, "paused", memory=128)
        loaded = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.assertEqual(loaded[self.queue_id]["initial_memory_mib_per_job"], 256)
        self.assertEqual(loaded[self.queue_id]["allocator"]["memory_mib_per_job"], 128)
        self.assertEqual(self.run_once().returncode, 0)
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status, "initial_memory_mib_per_job"), "256")
        self.assertEqual(self.env_value(status, "effective_memory_mib_per_job"), "128")
        self.assertEqual(self.env_value(status, "admission"), "paused")
        self.assertEqual(self.resource_env.read_bytes(), original)
        profiles = json.loads((self.state / "queue-profiles.json").read_text())
        self.assertEqual(profiles["profiles"][self.queue_id]["memory_mib_per_job"], 256)

    def test_memory_override_requires_positive_value_and_new_control_schema(self) -> None:
        for memory in (0, -1):
            self.write_control(2, "enabled", memory=memory)
            with self.assertRaisesRegex(SnapshotError, "memory_mib_per_job"):
                MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.write_control(2, "enabled", memory=128)
        self.control_env.write_text(self.control_env.read_text().replace("schema_version=2", "schema_version=1"))
        with self.assertRaisesRegex(SnapshotError, "unsupported queue schema"):
            MODULE._load_queues(self.state, self.pool_id, "ACTIVE")

    def write_host_priorities(self, text: str, schema: int = 2) -> None:
        self.control_env.write_text(
            f"schema_version={schema}\nqueue_id=queue-1\ngeneration=2\nadmission=enabled\n"
            f"schedule_priority=10\nhost_priorities={text}\nmax_workers=0\nupdated_epoch=1\n",
            encoding="utf-8",
        )

    def test_host_priorities_reach_the_allocator_and_queue_status(self) -> None:
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.run_once().returncode, 0)
        self.assertNotIn("host_priorities", status.read_text())
        self.assertEqual(self.env_value(self.state / "coordinator.ready", "host_priority_version"), "1")
        self.write_host_priorities("host-1=off")
        loaded = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.assertEqual(loaded[self.queue_id]["allocator"]["host_priorities"], {"host-1": "off"})
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.env_value(status, "schedule_priority"), "10")
        self.assertEqual(self.env_value(status, "host_priorities"), "host-1=off")
        self.assertEqual(self.env_value(status, "target_workers"), "0")
        self.assertEqual(self.env_value(status, "blocking_reason"), "host_unavailable")

    def test_host_priority_controls_fail_closed(self) -> None:
        for text, schema, message in (
            ("host-1=1", 1, "unsupported queue schema"),
            ("host-1=05", 2, "canonical"),
            ("7", 2, "canonical"),
            ("host-2=1,host-1=1", 2, "canonical"),
            ("host-1=x", 2, "integer or off"),
        ):
            self.write_host_priorities(text, schema)
            with self.subTest(text=text), self.assertRaisesRegex(SnapshotError, message):
                MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.write_host_priorities("", 1)
        loaded = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.assertEqual(loaded[self.queue_id]["allocator"]["host_priorities"], {})

        self.register_queue("queue-2")
        self.write_host_priorities("ghost=1")
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        reason = "host_priorities name hosts outside the pool: ghost"
        self.assertIn(f"quarantined queue queue-1: {reason}", result.stderr)
        allocation = self.remote_state / self.env_value(self.remote_state / "lease.env", "allocations_file")
        self.assertNotIn("queue-1\t", allocation.read_text())
        self.assertIn("queue-2\t", allocation.read_text())
        self.assertIn(reason, json.loads((self.state / "quarantine.json").read_text())["queues"]["queue-1"])

    def enable_calibration(self) -> dict:
        self.write_control(2, "enabled", memory=256)
        with self.control_env.open("a") as output:
            output.write("memory_calibration_jobs=20\nmemory_calibration_seconds=0\n"
                         "memory_min_mib_per_job=64\nmemory_calibration_generation=2\n")
        with (self.state / "pool.env").open("a") as output:
            output.write("memory_calibration_version=1\n")
        policy = {"jobs": 20, "seconds": 0, "minimum_mib": 64, "generation": 2}
        state = MODULE.calibrate(None, baseline_mib=256, policy=policy,
                                 reports=[], now=time.time() - 120)
        (self.state / "memory-calibration.json").write_text(json.dumps({
            "schema_version": 1, "pool_id": self.pool_id, "queues": {self.queue_id: state},
        }))
        return policy

    def append_calibration(self, failure: float = 0, held: bool = False) -> None:
        if not hasattr(self, "calibration_finished_epoch"):
            self.calibration_finished_epoch = time.time() - 1
        report = {"completed": [
            {"reservation_id": f"finished-{i}", "job_id": f"job-{i}",
             "finished_epoch": self.calibration_finished_epoch, "mean_mib": 64, "seconds": 30}
            for i in range(20)
        ], "running": [], "failure_epoch": failure}
        row = [""] * MODULE.STATUS_COLUMNS
        row[0:3] = ["calibration", self.pool_id, "host-1"]
        row[6], row[13] = self.queue_id, json.dumps(report, separators=(",", ":"))
        with (self.remote_state / "status.tsv").open("a") as output:
            output.write("\t".join(row) + "\n")
            if held:
                row[0], row[13] = "blocked_queue", "rapid_job_failures"
                output.write("\t".join(row) + "\n")

    def test_calibration_reprices_only_after_ack_and_failure_restores_requested_floor(self) -> None:
        self.enable_calibration()
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1, reservations=1)
        self.append_calibration()
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        path = self.state / "plans/host-1/state.json"
        state = json.loads(path.read_text())
        self.assertEqual(state["memory_mib_per_job"][self.queue_id], 205)
        self.assertEqual(state["envelope_memory_mib_per_job"][self.queue_id], 256)
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status, "requested_memory_mib_per_job"), "256")
        self.assertEqual(self.env_value(status, "effective_memory_mib_per_job"), "205")
        self.assertEqual(self.env_value(status, "propagation"), "pending")
        self.write_agent_status(state["generation"], reservations=1)
        remote_status = self.remote_state / "status.tsv"
        rows = remote_status.read_text().splitlines()
        row = rows[2].split("\t")
        row[10] = row[12] = "205"
        remote_status.write_text("\n".join(rows[:2] + ["\t".join(row)]) + "\n")
        self.append_calibration()
        self.assertEqual(self.run_once().returncode, 0)
        state = json.loads(path.read_text())
        self.assertEqual(state["envelope_memory_mib_per_job"][self.queue_id], 205)
        self.assertEqual(self.env_value(status, "propagation"), "complete")
        self.write_agent_status(state["generation"], reservations=1)
        self.append_calibration(failure=time.time() - 0.1, held=True)
        self.assertEqual(self.run_once().returncode, 0)
        state = json.loads(path.read_text())
        self.assertEqual(state["memory_mib_per_job"][self.queue_id], 256)
        self.assertEqual(self.env_value(status, "memory_calibration_reason"), "recent_failure")

    def test_stale_calibration_cannot_lower_and_manual_override_disables_learning(self) -> None:
        self.enable_calibration()
        self.write_agent_status(0, sample_epoch=int(time.time()) - 60)
        self.append_calibration()
        self.assertEqual(self.run_once().returncode, 0)
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status, "effective_memory_mib_per_job"), "256")
        self.write_control(3, "paused", memory=128)
        self.write_agent_status(1)
        self.assertEqual(self.run_once().returncode, 0)
        self.assertEqual(self.env_value(status, "memory_calibration_state"), "off")
        self.assertEqual(self.env_value(status, "effective_memory_mib_per_job"), "128")
        self.assertEqual(self.env_value(status, "admission"), "paused")

    def test_calibration_change_preserves_offline_and_uncertain_reservations(self) -> None:
        self.enable_calibration()
        for reachable, uncertain in ((False, True), (True, True)):
            for old_charge, new_floor, expected in ((128, 512, 512), (700, 512, 700), (700, 64, 700)):
                with self.subTest(reachable=reachable, old_charge=old_charge, new_floor=new_floor):
                    coordinator = MODULE.Coordinator(
                        self.state, self.pool_id, self.resources, 4, 1, 10,
                        start_interval_seconds=0,
                    )
                    status = {
                        "generation": 0, "reservations": {self.queue_id: 1},
                        "reservation_memory_mib": {self.queue_id: old_charge},
                        "reservation_ids": {}, "admission": "drain-only",
                        "uncertain": uncertain, "reachable": reachable,
                        "live_cpu_units": 0, "live_memory_mib": 0,
                        "smbd_cpu_tenths": 0, "telemetry_state": "unavailable",
                    }
                    def calibrate(queues, polled=None):
                        queues[self.queue_id]["allocator"]["memory_mib_per_job"] = (
                            128 if polled is None else new_floor
                        )
                    with mock.patch.object(coordinator, "_poll", return_value=status), \
                            mock.patch.object(coordinator, "_calibrate_memory", side_effect=calibrate), \
                            mock.patch.object(coordinator, "_publish", side_effect=lambda *args: args[-1]):
                        coordinator.tick()
                    self.assertEqual(status["reservation_memory_mib"][self.queue_id], expected)

    def test_calibration_requires_complete_policy_and_compatible_pool(self) -> None:
        self.enable_calibration()
        text = self.control_env.read_text()
        self.control_env.write_text(text.replace("memory_calibration_seconds=0\n", ""))
        with self.assertRaisesRegex(SnapshotError, "incomplete memory calibration"):
            MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.control_env.write_text(text)
        pool = self.state / "pool.env"
        pool.write_text(pool.read_text().replace("memory_calibration_version=1\n", ""))
        self.assertEqual(self.run_once().returncode, 0)
        self.assertIn("does not support memory calibration",
                      (self.state / "quarantine.json").read_text())

    def test_memory_transition_accepts_only_authorized_floors_and_clamps_increases(self) -> None:
        self.write_control(2, "enabled", memory=512)
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.write_agent_status(0, reservations=1)
        path = self.remote_state / "status.tsv"
        text = path.read_text()
        parsed = MODULE._parse_status(text, self.pool_id, "host-1", queues)
        self.assertEqual(parsed["reservation_memory_mib"], {self.queue_id: 512})
        rows = text.splitlines()
        reservation = rows[2].split("\t")
        reservation[10] = reservation[12] = "128"
        old = "\n".join(rows[:2] + ["\t".join(reservation)]) + "\n"
        with self.assertRaisesRegex(SnapshotError, "profile mismatch"):
            MODULE._parse_status(old, self.pool_id, "host-1", queues)
        parsed = MODULE._parse_status(old, self.pool_id, "host-1", queues, {self.queue_id: [128]})
        self.assertEqual(parsed["reservation_memory_mib"], {self.queue_id: 512})
        reservation[12] = "1"
        bad = "\n".join(rows[:2] + ["\t".join(reservation)]) + "\n"
        with self.assertRaisesRegex(SnapshotError, "memory charge"):
            MODULE._parse_status(bad, self.pool_id, "host-1", queues, {self.queue_id: [128]})

    def test_lower_memory_retains_old_envelope_until_new_plan_acknowledged(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_control(2, "enabled", memory=128)
        self.assertEqual(self.run_once().returncode, 0)
        state_path = self.state / "plans/host-1/state.json"
        state = json.loads(state_path.read_text())
        self.assertEqual(state["memory_mib_per_job"][self.queue_id], 128)
        self.assertEqual(state["envelope_memory_mib_per_job"][self.queue_id], 256)
        self.assertEqual(state["memory_history"][self.queue_id], [128, 256])
        self.write_agent_status(state["generation"])
        self.assertEqual(self.run_once().returncode, 0)
        state = json.loads(state_path.read_text())
        self.assertEqual(state["envelope_memory_mib_per_job"][self.queue_id], 128)

    def test_corrupt_memory_history_fails_loudly(self) -> None:
        path = self.state / "plans/host-1/state.json"
        state = json.loads(path.read_text())
        state["memory_history"] = {self.queue_id: [128, 0]}
        path.write_text(json.dumps(state))
        result = self.run_once()
        self.assertEqual(result.returncode, 2)
        self.assertIn("invalid memory_history", result.stderr)

    def test_failure_feedback_blocks_grants_and_is_visible(self) -> None:
        path = self.remote_state / "status.tsv"
        row = [""] * MODULE.STATUS_COLUMNS
        row[0:3] = ["blocked_queue", self.pool_id, "host-1"]
        row[6] = self.queue_id
        row[13] = "infrastructure_failed"
        with path.open("a") as output:
            output.write("\t".join(row) + "\n")
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        allocations = (self.remote_state / "allocations.1.tsv").read_text().splitlines()
        self.assertEqual(allocations[1].split("\t")[1:3], ["0", "0"])
        status = (self.batch / "adaptive/status.env").read_text()
        self.assertIn("blocking_reason=failure_hold", status)
        self.assertIn("failure_holds=host-1:infrastructure_failed", status)

    def test_invalid_failure_feedback_fails_closed(self) -> None:
        path = self.remote_state / "status.tsv"
        row = [""] * MODULE.STATUS_COLUMNS
        row[0:3] = ["blocked_queue", self.pool_id, "host-1"]
        row[6] = self.queue_id
        row[13] = "arbitrary_reason"
        text = path.read_text() + "\t".join(row) + "\n"
        with self.assertRaises(SnapshotError):
            MODULE._parse_status(text, self.pool_id, "host-1", {self.queue_id: {"allocator": queue(self.queue_id)}})

    def write_editable(self, contents: str) -> None:
        self.editable.write_text(contents, encoding="utf-8")
        registration = self.state / "queues" / f"{self.queue_id}.env"
        if registration.exists():
            digest = hashlib.sha256(self.editable.read_bytes()).hexdigest()
            registration.write_text(
                re.sub(
                    r"^queue_sha256=.*$", f"queue_sha256={digest}",
                    registration.read_text(encoding="utf-8"), flags=re.MULTILINE,
                ),
                encoding="utf-8",
            )

    def enable_workflow(self, dependencies=None, retry=False):
        self.resource_env.write_text(self.resource_env.read_text().replace("schema_version=1", "schema_version=2")
                                     + f"dependencies={json.dumps(dependencies or [])}\nretry_lost_jobs={int(retry)}\n")
        registration = self.state / "queues" / f"{self.queue_id}.env"
        registration.write_text(re.sub(r"^resources_sha256=.*$", "resources_sha256=" +
            hashlib.sha256(self.resource_env.read_bytes()).hexdigest(), registration.read_text(), flags=re.MULTILINE))
        pool = self.state / "pool.env"
        pool.write_text(pool.read_text() + "workflow_version=1\n")

    def test_dependency_withholds_grants_until_success(self):
        predecessor = self.root / "predecessor"
        (predecessor / "adaptive").mkdir(parents=True)
        (predecessor / "run_meta.env").write_text(
            "pool_id=pool-1\nrun_token=before\nexecution_mode=adaptive-shared-queue\n")
        status = predecessor / "adaptive/status.env"
        status.write_text("queue_id=before\nlifecycle=PENDING\npropagation=complete\nrunning_workers=0\ntarget_workers=0\n")
        self.enable_workflow([{"pool_id": "pool-1", "queue_id": "before", "batch_dir": str(predecessor)}])
        self.assertEqual(self.run_once().returncode, 0)
        current = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(current, "lifecycle"), "PENDING")
        self.assertEqual(self.env_value(current, "blocking_reason"), "dependency_wait")
        self.assertEqual(self.env_value(current, "target_workers"), "0")
        self.write_agent_status(1)
        status.write_text(status.read_text().replace("PENDING", "COMPLETED"))
        self.assertEqual(self.run_once().returncode, 0)
        self.assertEqual(self.env_value(current, "target_workers"), "2")

    def prepare_verified_tail(self):
        self.enable_workflow(retry=True)
        digest = hashlib.sha256(self.editable.read_bytes()).hexdigest()
        for job in ("job_1", "job_2"):
            claim = self.batch / "claims" / f"job_{job}.claim"
            claim.mkdir()
            owner = {"reservation_id": "pool-1.host-1.1", "job_id": job, "queue_sha256": digest}
            (claim / "owner.json").write_text(json.dumps(owner))
        (self.batch / "done/job_job_1.done").touch()
        evidence = self.batch / "adaptive/verified-losses"
        evidence.mkdir()
        (evidence / "job_job_2.json").write_text(json.dumps({
            "pool_id": self.pool_id, "queue_id": self.queue_id, "owner": owner,
        }))

    def test_verified_tail_is_requeued_then_allocated_on_next_tick(self):
        self.prepare_verified_tail()
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1)
        self.assertEqual(self.run_once().returncode, 0)
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "PENDING")
        self.assertEqual(self.env_value(status, "blocking_reason"), "lost_jobs_requeued")
        self.assertFalse((self.batch / "claims/job_job_2.claim").exists())
        self.assertTrue((self.batch / "claims/job_job_1.claim").exists())
        self.write_agent_status(2)
        self.assertEqual(self.run_once().returncode, 0)
        self.assertEqual(self.env_value(status, "target_workers"), "1")

    def test_verified_tail_preserves_pause_until_resume(self):
        self.prepare_verified_tail()
        self.write_control(1, "paused")
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1)
        self.assertEqual(self.run_once().returncode, 0)
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "PENDING")
        self.assertEqual(self.env_value(status, "admission"), "paused")
        self.assertTrue((self.batch / "claims/job_job_2.claim").exists())
        self.assertTrue((self.state / "queues" / f"{self.queue_id}.env").exists())
        self.write_control(2, "enabled")
        self.write_agent_status(2)
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(3)
        self.assertEqual(self.run_once().returncode, 0)
        self.assertFalse((self.batch / "claims/job_job_2.claim").exists())

    def test_verified_tail_busy_lock_does_not_retire_registration(self):
        self.prepare_verified_tail()
        (self.root / "lock").mkdir()
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1)
        self.assertEqual(self.run_once().returncode, 0)
        self.assertEqual(self.env_value(self.batch / "adaptive/status.env", "lifecycle"), "PENDING")
        self.assertTrue((self.state / "queues" / f"{self.queue_id}.env").exists())
        self.assertTrue((self.batch / "claims/job_job_2.claim").exists())

    def test_only_first_two_claims_of_opted_in_large_queues_qualify(self):
        for size, opt_in in ((19, True), (20, True), (20, False)):
            q = f"q-{size}-{int(opt_in)}"
            batch = self.register_queue(q, size=size, early_sample=opt_in)
            claims = batch / "claims"
            claims.mkdir()
            for claimed in range(4):
                _, info = MODULE._load_queue(self.state / "queues" / f"{q}.env", self.pool_id, "ACTIVE", None)
                expected = 2 - claimed if size >= 20 and opt_in and claimed <= 2 else None
                self.assertEqual(info["allocator"].get("early_sample_remaining"), expected)
                (claims / f"job_job_{claimed + 1}.claim").mkdir()

    def register_queue(self, queue_id: str, shared_io: int = 0, size: int = 2, early_sample: bool = False) -> Path:
        batch = self.root / f"{queue_id}-batch"
        (batch / "adaptive").mkdir(parents=True)
        (batch / "done").mkdir()
        (batch / "run_meta.env").write_text(f"total_jobs={size}\n", encoding="utf-8")
        original = self.root / f"{queue_id}-original.txt"
        editable = batch / "queue-edit.txt"
        original.write_text("".join(f"job_{i+1} true\n" for i in range(size)), encoding="utf-8")
        editable.write_text(original.read_text(encoding="utf-8"), encoding="utf-8")
        resources = batch / "adaptive" / "resources.env"
        control = batch / "adaptive" / "control.env"
        resources.write_text(
            ("schema_version=2\ndependencies=[]\nretry_lost_jobs=0\nearly_eta_sample=1\n"
             if early_sample else "schema_version=1\n") + f"queue_id={queue_id}\ncpu_units_per_job=1\n"
            f"memory_mib_per_job=256\nshared_io_units_per_job={shared_io}\n",
            encoding="utf-8",
        )
        control.write_text(
            f"schema_version=1\nqueue_id={queue_id}\ngeneration=1\nadmission=enabled\n"
            "schedule_priority=10\nmax_workers=0\nupdated_epoch=1\n",
            encoding="utf-8",
        )
        digest = hashlib.sha256(resources.read_bytes()).hexdigest()
        queue_digest = hashlib.sha256(editable.read_bytes()).hexdigest()
        (self.state / "queues" / f"{queue_id}.env").write_text(
            f"schema_version=1\npool_id={self.pool_id}\nqueue_id={queue_id}\n"
            f"batch_dir={batch}\noriginal_queue_file={original}\n"
            f"editable_queue_file={editable}\nmodel_dir={self.root / 'model'}\n"
            f"log_dir={batch}\nlock_dir={self.root / f'{queue_id}.lock'}\n"
            f"tag_clean={queue_id}-tag\nresources_file={resources}\n"
            f"resources_sha256={digest}\nqueue_sha256={queue_digest}\n"
            f"control_file={control}\nregistered_epoch=1\n",
            encoding="utf-8",
        )
        return batch

    def write_agent_status(
        self, generation: int, admission: str = "drain-only", reservations: int = 0,
        live_cpu: int = 2, live_memory: int = 2048, smbd_cpu_tenths: int = 0,
        telemetry_state: str = "ok",
        sample_epoch: int | None = None, smb_bytes: int = -1,
    ) -> None:
        agent = [
            "agent", self.pool_id, "host-1", str(generation), admission,
            "", "", "", "", "", "", "", "", "ready", str(live_cpu),
            str(live_memory), str(smbd_cpu_tenths), telemetry_state,
            str(int(time.time()) if sample_epoch is None else sample_epoch), str(smb_bytes),
            "", "",
        ]
        text = STATUS_HEADER + "\n" + "\t".join(agent) + "\n"
        for index in range(reservations):
            reservation = [
                "reservation", self.pool_id, "host-1", str(generation), admission,
                f"res-{index}", self.queue_id, "running", str(self.fake_agent.pid),
                "1", "256", "0", "256", "", "", "", "", "", "", "",
                "0", "0",
            ]
            text += "\t".join(reservation) + "\n"
        (self.remote_state / "status.tsv").write_text(text, encoding="utf-8")

    def run_once(
        self, io_pressure_host: str | None = None, smbd_cpu_limit_pct: int = 400,
        **extra_environment: str,
    ) -> subprocess.CompletedProcess[str]:
        environment = os.environ.copy()
        environment.update(
            ADAPTIVE_SSH_BIN=str(self.fake_ssh),
            FAKE_REMOTE_HOME=str(self.remote_home),
            FAKE_SSH_LOG=str(self.ssh_log),
        )
        environment.update(extra_environment)
        command = [
            sys.executable, str(COORDINATOR), "serve",
            "--state-root", str(self.state), "--pool-id", self.pool_id,
            "--resources-file", str(self.resources),
            "--shared-io-capacity", "4", "--tick-seconds", "1",
            # Keep unrelated lifecycle tests independent of startup timing.
            "--lease-seconds", "10", "--start-interval-seconds", "0", "--once",
        ]
        if io_pressure_host is not None:
            command.extend([
                "--io-pressure-host", io_pressure_host,
                "--smbd-cpu-limit-pct", str(smbd_cpu_limit_pct),
            ])
        return subprocess.run(
            command,
            text=True, capture_output=True, check=False, env=environment,
        )

    @staticmethod
    def env_value(path: Path, key: str) -> str:
        values = dict(line.split("=", 1) for line in path.read_text().splitlines())
        return values[key]

    def test_paced_publication_recovery_pause_and_drain(self) -> None:
        environment = {"ADAPTIVE_SSH_BIN": str(self.fake_ssh),
                       "FAKE_REMOTE_HOME": str(self.remote_home),
                       "FAKE_SSH_LOG": str(self.ssh_log)}
        now = time.time()
        with mock.patch.dict(os.environ, environment), mock.patch.object(MODULE.time, "time", return_value=now):
            coordinator = MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 1, 10)
            self.assertFalse(coordinator.tick())
        state_path = self.state / "plans/host-1/state.json"
        state = json.loads(state_path.read_text())
        self.assertEqual(state["targets"], {self.queue_id: 0})
        self.assertEqual(state["envelope_targets"], {self.queue_id: 0})
        status_path = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status_path, "target_workers"), "0")
        self.assertEqual(self.env_value(status_path, "effective_admission"), "blocked")
        self.assertEqual(self.env_value(status_path, "blocking_reason"), "start_pacing")
        deadline = state["start_pacing"]["next_epoch"]
        self.assertGreater(deadline, now)
        self.assertEqual(coordinator.next_tick_seconds, 1)
        self.write_agent_status(1, "enabled", sample_epoch=int(deadline))
        with mock.patch.dict(os.environ, environment), mock.patch.object(MODULE.time, "time", return_value=deadline + 0.01):
            # Recovery carries the pending ramp rather than restarting its delay.
            coordinator = MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 1, 10)
            self.assertFalse(coordinator.tick())
        state = json.loads(state_path.read_text())
        self.assertEqual(state["targets"], {self.queue_id: 1})
        self.assertEqual(state["envelope_targets"], {self.queue_id: 1})
        self.assertEqual(self.env_value(status_path, "target_workers"), "1")
        self.assertEqual(self.env_value(status_path, "blocking_reason"), "none")
        allocation = self.remote_state / "allocations.2.tsv"
        self.assertEqual(allocation.read_text().splitlines()[1].split("\t")[1:3], ["1", "1"])
        self.write_agent_status(2, "enabled", reservations=1, sample_epoch=int(deadline))
        self.write_control(2, "paused")
        with mock.patch.dict(os.environ, environment), mock.patch.object(MODULE.time, "time", return_value=deadline + 0.1):
            self.assertFalse(coordinator.tick())
        self.assertEqual((self.remote_state / "allocations.3.tsv").read_text().splitlines()[1].split("\t")[1:3], ["0", "0"])
        self.write_agent_status(3, "enabled", reservations=1, sample_epoch=int(deadline))
        (self.state / "pool.env").write_text(f"pool_id={self.pool_id}\nstate=DRAINING\n")
        with mock.patch.dict(os.environ, environment), mock.patch.object(MODULE.time, "time", return_value=deadline + 0.2):
            self.assertFalse(coordinator.tick())
        self.assertEqual((self.remote_state / "allocations.4.tsv").read_text().splitlines()[1].split("\t")[1:3], ["0", "0"])
        self.assertIsNone(self.fake_agent.poll())

    def test_poll_reports_a_stopped_agent_only_when_the_host_answers(self) -> None:
        environment = {"ADAPTIVE_SSH_BIN": str(self.fake_ssh),
                       "FAKE_REMOTE_HOME": str(self.remote_home),
                       "FAKE_SSH_LOG": str(self.ssh_log)}
        coordinator = MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 1, 10)
        with mock.patch.dict(os.environ, environment):
            status = coordinator._poll("host-1", {})
            self.assertTrue(status["reachable"])
            self.assertFalse(status["agent_stopped"])
            self.fake_agent.terminate()
            self.fake_agent.wait(timeout=5)
            status = coordinator._poll("host-1", {})
            self.assertFalse(status["reachable"])
            self.assertTrue(status["agent_stopped"])
            (self.remote_state / "agent.lock").unlink()
            self.assertTrue(coordinator._poll("host-1", {})["agent_stopped"])
        unreachable = self.root / "ssh-unreachable"
        unreachable.write_text("#!/usr/bin/env bash\nexit 255\n", encoding="utf-8")
        unreachable.chmod(0o755)
        with mock.patch.dict(os.environ, {**environment, "ADAPTIVE_SSH_BIN": str(unreachable)}):
            status = coordinator._poll("host-1", {})
        self.assertFalse(status["reachable"])
        self.assertFalse(status["agent_stopped"])

    def test_a_stopped_agent_is_restarted_after_a_lease_with_backoff(self) -> None:
        coordinator = MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 1, 10)
        stopped = {"host-1": {"agent_stopped": True}}
        process = mock.Mock()
        process.poll.return_value = None
        with mock.patch.object(MODULE.subprocess, "Popen", return_value=process) as popen, \
                mock.patch("builtins.print"):
            coordinator._restart_stopped_agents(stopped, 1000)
            coordinator._restart_stopped_agents(stopped, 1009)
            popen.assert_not_called()
            coordinator._restart_stopped_agents(stopped, 1010)
            arguments = popen.call_args.args[0]
            self.assertEqual(arguments[:6], [
                str(coordinator.scheduler_script), "restart-agent", "--host_id", "host-1",
                "--expected-pool-id", self.pool_id,
            ])
            self.assertTrue(popen.call_args.kwargs["start_new_session"])
            self.assertIn("no agent on reachable host host-1 for 10 s",
                          (self.state / "agent-restarts.log").read_text())
            # One attempt runs at a time, and a failed one is retried only after the interval.
            coordinator._restart_stopped_agents(stopped, 1011)
            process.poll.return_value = 1
            coordinator._restart_stopped_agents(stopped, 1012)
            coordinator._restart_stopped_agents(stopped, 1010 + MODULE.AGENT_RESTART_INTERVAL_SECONDS - 1)
            self.assertEqual(popen.call_count, 1)
            coordinator._restart_stopped_agents(stopped, 1010 + MODULE.AGENT_RESTART_INTERVAL_SECONDS)
            self.assertEqual(popen.call_count, 2)
            # A host that answers again starts a fresh lease wait if its agent later stops.
            process.poll.return_value = 0
            coordinator._restart_stopped_agents({"host-1": {"agent_stopped": False}}, 3000)
            self.assertNotIn("host-1", coordinator.agent_stopped_since)
            coordinator._restart_stopped_agents(stopped, 3001)
            self.assertEqual(popen.call_count, 2)

    def test_invalid_pacing_state_and_settings_fail_loudly(self) -> None:
        coordinator = MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 1, 10)
        path = self.state / "plans/host-1/state.json"
        state = json.loads(path.read_text())
        state["start_pacing"] = {"slots": -1, "next_epoch": 0, "cursor": ""}
        path.write_text(json.dumps(state))
        with self.assertRaisesRegex(SnapshotError, "invalid start pacing"):
            coordinator._host_state("host-1")
        for interval in (-1, float("nan"), float("inf")):
            with self.assertRaisesRegex(SnapshotError, "start interval"):
                MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 1, 10,
                                   start_interval_seconds=interval)

    def test_current_and_sustained_memory_must_fit_the_observed_charge(self) -> None:
        self.write_agent_status(0, reservations=1)
        path = self.remote_state / "status.tsv"
        lines = path.read_text().splitlines()
        row = lines[2].split("\t")
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        for observed, current, sustained, valid in (
            (1024, 4096, 0, False),
            (4096, 1024, 4096, False),
            (4506, 1024, 4096, True),
        ):
            row[11:13] = [str(observed), str(observed)]
            row[20:22] = [str(current), str(sustained)]
            text = "\n".join(lines[:2] + ["\t".join(row)]) + "\n"
            if valid:
                status = MODULE._parse_status(text, self.pool_id, "host-1", queues)
                self.assertEqual(status["reservation_memory_mib"], {self.queue_id: 4506})
            else:
                with self.assertRaisesRegex(SnapshotError, "memory charge"):
                    MODULE._parse_status(text, self.pool_id, "host-1", queues)

    def test_service_publishes_hash_then_lease_and_tracks_propagation(self) -> None:
        first = self.run_once()
        self.assertEqual(first.returncode, 0, first.stderr)
        lease = self.remote_state / "lease.env"
        self.assertEqual(self.env_value(lease, "generation"), "1")
        allocation = self.remote_state / self.env_value(lease, "allocations_file")
        digest = hashlib.sha256(allocation.read_bytes()).hexdigest()
        self.assertEqual(self.env_value(lease, "allocations_sha256"), digest)
        self.assertIn("queue-1\t2\t2\t1\t256\t0\t", allocation.read_text())
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "target_workers"), "2")
        self.assertEqual(self.env_value(status, "propagation"), "pending")
        self.assertEqual(self.env_value(status, "admission"), "enabled")
        self.assertEqual(self.env_value(status, "schedule_priority"), "10")
        self.assertEqual(self.env_value(status, "max_workers"), "0")
        self.assertEqual(self.env_value(status, "running_by_host"), "host-1:0")
        # The host has not acknowledged yet, but an enabled queue with work waits as PENDING.
        self.assertEqual(self.env_value(status, "lifecycle"), "PENDING")

        self.write_agent_status(1, "enabled")
        second = self.run_once()
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.env_value(self.remote_state / "lease.env", "generation"), "2")
        self.assertEqual(self.env_value(status, "propagation"), "complete")

        self.write_control(2, "paused")
        self.write_agent_status(2, "enabled")
        third = self.run_once()
        self.assertEqual(third.returncode, 0, third.stderr)
        self.assertEqual(self.env_value(status, "target_workers"), "0")
        self.assertEqual(self.env_value(status, "propagation"), "pending")
        self.write_agent_status(3)
        fourth = self.run_once()
        self.assertEqual(fourth.returncode, 0, fourth.stderr)
        self.assertEqual(self.env_value(status, "propagation"), "complete")
        retained = sorted(path.name for path in self.remote_state.glob("allocations.*.tsv"))
        self.assertEqual(retained, ["allocations.3.tsv", "allocations.4.tsv"])

    def test_live_capacity_limits_published_targets(self) -> None:
        self.write_agent_status(0, live_cpu=1, live_memory=256)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        lease = self.remote_state / "lease.env"
        allocation = self.remote_state / self.env_value(lease, "allocations_file")
        with allocation.open(encoding="utf-8") as handle:
            row = next(csv.DictReader(handle, dialect="excel-tab"))
        self.assertEqual((row["target_running"], row["start_grants"]), ("1", "1"))
        ready = self.state / "coordinator.ready"
        self.assertEqual(self.env_value(ready, "resource_state"), "ok")
        self.assertEqual(self.env_value(ready, "live_cpu_units"), "1")
        self.assertEqual(self.env_value(ready, "live_memory_mib"), "256")

    def test_unavailable_telemetry_publishes_zero_grants(self) -> None:
        self.write_agent_status(
            0, live_cpu=2, live_memory=2048, telemetry_state="unavailable"
        )
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        lease = self.remote_state / "lease.env"
        allocation = self.remote_state / self.env_value(lease, "allocations_file")
        with allocation.open(encoding="utf-8") as handle:
            row = next(csv.DictReader(handle, dialect="excel-tab"))
        self.assertEqual((row["target_running"], row["start_grants"]), ("0", "0"))
        ready = self.state / "coordinator.ready"
        self.assertEqual(self.env_value(ready, "resource_state"), "unavailable")

    def test_smb_recovery_requires_distinct_fresh_samples(self) -> None:
        coordinator = MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 5, 20, start_interval_seconds=0)
        pressure = {"reachable": True, "uncertain": False, "telemetry_state": "ok",
                    "smbd_cpu_tenths": 0, "resource_sample_epoch": 100}
        with mock.patch.object(MODULE.time, "time", return_value=110):
            for _ in range(5):
                self.assertEqual(coordinator._io_pressure(pressure)[0], "waiting")
            pressure["resource_sample_epoch"] = 101
            self.assertEqual(coordinator._io_pressure(pressure)[0], "waiting")
            pressure["resource_sample_epoch"] = 102
            self.assertEqual(coordinator._io_pressure(pressure)[0], "ok")
        with mock.patch.object(MODULE.time, "time", return_value=123):
            self.assertEqual(coordinator._io_pressure(pressure)[0], "unavailable")
        pressure["resource_sample_epoch"] = 130
        with mock.patch.object(MODULE.time, "time", return_value=129):
            self.assertEqual(coordinator._io_pressure(pressure)[0], "unavailable")

    def test_republished_status_cannot_hide_stale_resource_sample(self) -> None:
        self.write_agent_status(0, sample_epoch=int(time.time()) - 60)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "target_workers"), "0")
        self.assertEqual(self.env_value(status, "blocking_reason"), "host_unavailable")

    def test_live_host_budgets_reach_leases_and_allow_oversized_waiting_jobs(self) -> None:
        pool_file = self.state / "pool.env"
        pool_file.write_text(pool_file.read_text() + f"resources_file={self.resources}\n"
                             f"resources_sha256={hashlib.sha256(self.resources.read_bytes()).hexdigest()}\n")
        with mock.patch("builtins.print"):
            MODULE.set_host_budget(self.state, self.pool_id, "host-1", 4, 4096)
        self.assertEqual(MODULE.main([
            "profile-fits", "--state-root", str(self.state), "--pool-id", self.pool_id,
            "--cpu-units", "3", "--memory-mib", "3000",
        ]), 0)
        self.write_editable("".join(f"job_{i} true\n" for i in range(10)))
        self.write_agent_status(0, live_cpu=4, live_memory=4096)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        lease = self.remote_state / "lease.env"
        self.assertEqual(self.env_value(lease, "cpu_units"), "4")
        self.assertEqual(self.env_value(lease, "memory_mib"), "4096")
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "target_workers"), "4")
        with mock.patch("builtins.print"):
            MODULE.set_host_budget(self.state, self.pool_id, "host-1", 1, 128)
        self.write_agent_status(1, "enabled", reservations=2, live_cpu=4, live_memory=4096)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.env_value(lease, "memory_mib"), "128")
        self.assertEqual(self.env_value(status, "running_workers"), "2")
        self.assertEqual(self.env_value(status, "target_workers"), "0")
        self.assertEqual(self.env_value(status, "effective_admission"), "blocked")
        self.assertEqual(self.env_value(status, "blocking_reason"), "cpu+memory")

    def test_fast_cadence_includes_unacknowledged_grants(self) -> None:
        coordinator = MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 5, 20, start_interval_seconds=0)
        environment = {"ADAPTIVE_SSH_BIN": str(self.fake_ssh),
                       "FAKE_REMOTE_HOME": str(self.remote_home), "FAKE_SSH_LOG": str(self.ssh_log)}
        with mock.patch.dict(os.environ, environment):
            coordinator.tick()
            self.assertEqual(coordinator.next_tick_seconds, 1)
            # No acknowledgement or observed worker yet: a grant can still start.
            coordinator.tick()
            self.assertEqual(coordinator.next_tick_seconds, 1)
            generation = int(self.env_value(self.remote_state / "lease.env", "generation"))
            self.write_agent_status(generation, "enabled")
            self.write_control(2, "paused")
            coordinator.tick()
            self.assertEqual(coordinator.next_tick_seconds, 5)

    def test_smb_pressure_blocks_only_new_shared_io_grants(self) -> None:
        self.resource_env.write_text(
            self.resource_env.read_text(encoding="utf-8").replace(
                "shared_io_units_per_job=0", "shared_io_units_per_job=1"
            ),
            encoding="utf-8",
        )
        registration = self.state / "queues" / f"{self.queue_id}.env"
        digest = hashlib.sha256(self.resource_env.read_bytes()).hexdigest()
        registration.write_text(
            re.sub(
                r"^resources_sha256=.*$", f"resources_sha256={digest}",
                registration.read_text(encoding="utf-8"), flags=re.MULTILINE,
            ),
            encoding="utf-8",
        )
        sampled = int(time.time()) - 6
        local_batch = self.register_queue("queue-local", shared_io=0)
        self.write_agent_status(
            0, reservations=1, live_cpu=2, live_memory=2048,
            smbd_cpu_tenths=4000, sample_epoch=sampled,
        )

        result = self.run_once(io_pressure_host="host-1", smbd_cpu_limit_pct=400)
        self.assertEqual(result.returncode, 0, result.stderr)
        lease = self.remote_state / "lease.env"
        allocation = self.remote_state / self.env_value(lease, "allocations_file")
        with allocation.open(encoding="utf-8") as handle:
            rows = {row["queue_id"]: row for row in csv.DictReader(handle, dialect="excel-tab")}
        self.assertEqual(
            (rows[self.queue_id]["target_running"], rows[self.queue_id]["start_grants"]),
            ("0", "0"),
        )
        self.assertEqual(
            (rows["queue-local"]["target_running"], rows["queue-local"]["start_grants"]),
            ("1", "1"),
        )
        ready = self.state / "coordinator.ready"
        self.assertEqual(self.env_value(ready, "io_pressure_state"), "high")
        self.assertEqual(self.env_value(ready, "smbd_cpu_tenths"), "4000")
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "admission"), "enabled")
        self.assertEqual(self.env_value(status, "effective_admission"), "blocked")
        self.assertEqual(self.env_value(status, "blocking_reason"), "smb_high")
        self.assertEqual(self.env_value(status, "running_by_host"), "host-1:1")

        local_control = local_batch / "adaptive" / "control.env"
        local_control.write_text(
            local_control.read_text(encoding="utf-8")
            .replace("generation=1", "generation=2")
            .replace("admission=enabled", "admission=paused"),
            encoding="utf-8",
        )
        self.write_agent_status(
            1, "enabled", reservations=1, live_cpu=2, live_memory=2048,
            smbd_cpu_tenths=3999, sample_epoch=sampled + 1,
        )
        first_clear = self.run_once(io_pressure_host="host-1", smbd_cpu_limit_pct=400)
        self.assertEqual(first_clear.returncode, 0, first_clear.stderr)
        self.assertEqual(self.env_value(ready, "io_pressure_state"), "high")
        self.write_agent_status(
            2, "enabled", reservations=1, live_cpu=2, live_memory=2048,
            smbd_cpu_tenths=4000, sample_epoch=sampled + 2,
        )
        relapse = self.run_once(io_pressure_host="host-1", smbd_cpu_limit_pct=400)
        self.assertEqual(relapse.returncode, 0, relapse.stderr)
        self.assertEqual(self.env_value(ready, "io_pressure_state"), "high")
        self.write_agent_status(
            3, "enabled", reservations=1, live_cpu=2, live_memory=2048,
            smbd_cpu_tenths=3999, sample_epoch=sampled + 3,
        )
        second_clear = self.run_once(io_pressure_host="host-1", smbd_cpu_limit_pct=400)
        self.assertEqual(second_clear.returncode, 0, second_clear.stderr)
        self.assertEqual(self.env_value(ready, "io_pressure_state"), "high")
        self.write_agent_status(
            4, "enabled", reservations=1, live_cpu=2, live_memory=2048,
            smbd_cpu_tenths=3999, sample_epoch=sampled + 4,
        )
        third_clear = self.run_once(io_pressure_host="host-1", smbd_cpu_limit_pct=400)
        self.assertEqual(third_clear.returncode, 0, third_clear.stderr)
        self.assertEqual(self.env_value(ready, "io_pressure_state"), "high")
        self.write_agent_status(
            5, "enabled", reservations=1, live_cpu=2, live_memory=2048,
            smbd_cpu_tenths=3999, sample_epoch=sampled + 5,
        )
        recovered = self.run_once(io_pressure_host="host-1", smbd_cpu_limit_pct=400)
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        lease = self.remote_state / "lease.env"
        allocation = self.remote_state / self.env_value(lease, "allocations_file")
        with allocation.open(encoding="utf-8") as handle:
            rows = {row["queue_id"]: row for row in csv.DictReader(handle, dialect="excel-tab")}
        self.assertGreater(int(rows[self.queue_id]["start_grants"]), 0)
        self.assertEqual(self.env_value(ready, "io_pressure_state"), "ok")

    def test_short_low_traffic_sample_does_not_diagnose_smb_stall(self) -> None:
        self.resource_env.write_text(
            self.resource_env.read_text(encoding="utf-8").replace(
                "shared_io_units_per_job=0", "shared_io_units_per_job=1"
            ),
            encoding="utf-8",
        )
        registration = self.state / "queues" / f"{self.queue_id}.env"
        digest = hashlib.sha256(self.resource_env.read_bytes()).hexdigest()
        registration.write_text(
            re.sub(
                r"^resources_sha256=.*$", f"resources_sha256={digest}",
                registration.read_text(encoding="utf-8"), flags=re.MULTILINE,
            ),
            encoding="utf-8",
        )
        ready = self.state / "coordinator.ready"
        status = self.batch / "adaptive" / "status.env"
        now = int(time.time())
        states = []
        # Six cores of smbd: first sample has no rate, then ~33 B/s, then ~3 MiB/s.
        for generation, (offset, smb_bytes) in enumerate(
            [(-6, 1000), (-3, 1100), (0, 1100 + 10 * 1048576)]
        ):
            self.write_agent_status(
                generation, smbd_cpu_tenths=6000, sample_epoch=now + offset, smb_bytes=smb_bytes,
            )
            result = self.run_once(io_pressure_host="host-1", smbd_cpu_limit_pct=400)
            self.assertEqual(result.returncode, 0, result.stderr)
            states.append((self.env_value(ready, "io_pressure_state"),
                           self.env_value(status, "blocking_reason")))
            self.assertNotIn("possible stall", result.stderr)
            self.assertEqual(self.env_value(status, "target_workers"), "0")
        self.assertEqual(states, [
            ("high", "smb_high"), ("high", "smb_high"), ("high", "smb_high"),
        ])

    def test_smb_stall_requires_sustained_distinct_fresh_samples(self) -> None:
        coordinator = MODULE.Coordinator(
            self.state, self.pool_id, self.resources, 4, 1, 10, io_pressure_host="host-1",
            start_interval_seconds=0,
        )
        epoch = int(time.time())

        def sample(offset: int, cpu: int = 6000, smb_bytes: int = 1000) -> str:
            pressure = {
                "reachable": True, "uncertain": False, "telemetry_state": "ok",
                "smbd_cpu_tenths": cpu, "resource_sample_epoch": epoch + offset,
                "smb_bytes": smb_bytes,
            }
            with mock.patch.object(MODULE.time, "time", return_value=epoch + offset):
                return coordinator._io_pressure(pressure)[0]

        self.assertEqual(sample(0), "high")
        for offset in range(5, 60, 5):
            self.assertEqual(sample(offset), "high")
        before = json.loads((self.state / "io-pressure.json").read_text())
        for _ in range(10):
            self.assertEqual(sample(55), "high")
        self.assertEqual(json.loads((self.state / "io-pressure.json").read_text()), before)
        with mock.patch.object(MODULE.sys, "stderr", io.StringIO()) as stderr:
            self.assertEqual(sample(60), "stuck")
        self.assertIn("possible stall", stderr.getvalue())
        self.assertNotIn("needs a restart", stderr.getvalue())
        self.assertEqual(sample(65, smb_bytes=10 * 1048576), "high")
        for offset in range(70, 125, 5):
            self.assertEqual(sample(offset, smb_bytes=10 * 1048576), "high")
        self.assertEqual(sample(125, smb_bytes=10 * 1048576), "stuck")
        self.assertEqual(sample(130, smb_bytes=10 * 1048576), "stuck")
        self.assertEqual(sample(135, cpu=0, smb_bytes=10 * 1048576), "stuck")
        self.assertEqual(sample(140, cpu=0, smb_bytes=10 * 1048576), "stuck")
        self.assertEqual(sample(145, cpu=0, smb_bytes=10 * 1048576), "ok")

    def test_smb_stall_history_resets_on_gaps_and_unknown_byte_counters(self) -> None:
        coordinator = MODULE.Coordinator(
            self.state, self.pool_id, self.resources, 4, 1, 10, io_pressure_host="host-1",
            start_interval_seconds=0,
        )
        epoch = int(time.time())
        for offset, smb_bytes in [(0, 1000), (5, 1000), (10, 1000), (70, 1000),
                                  (75, -1), (80, 1000), (85, 1000), (90, 0)]:
            pressure = {
                "reachable": True, "uncertain": False, "telemetry_state": "ok",
                "smbd_cpu_tenths": 6000, "resource_sample_epoch": epoch + offset,
                "smb_bytes": smb_bytes,
            }
            with mock.patch.object(MODULE.time, "time", return_value=epoch + offset):
                self.assertEqual(coordinator._io_pressure(pressure)[0], "high")

    def test_pending_plan_republish_has_no_replay_grants(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        state_path = self.state / "plans" / "host-1" / "state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["pending"] = True
        state_path.write_text(json.dumps(state), encoding="utf-8")

        recovered = self.run_once()
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        lease = self.remote_state / "lease.env"
        allocation = self.remote_state / self.env_value(lease, "allocations_file")
        with allocation.open(encoding="utf-8") as handle:
            row = next(csv.DictReader(handle, dialect="excel-tab"))
        self.assertEqual((row["target_running"], row["start_grants"]), ("2", "0"))

    def test_mid_run_priority_mutation_uses_one_shot_handoff(self) -> None:
        self.fake_agent.terminate()
        self.fake_agent.wait(timeout=5)
        (self.remote_state / "agent.lock").unlink()
        (self.remote_state / "status.tsv").unlink()

        self.write_editable("low\n" * 4)
        (self.batch / "run_meta.env").write_text("total_jobs=4\n", encoding="utf-8")
        self.control_env.write_text(
            "schema_version=1\nqueue_id=queue-1\ngeneration=1\nadmission=enabled\n"
            "schedule_priority=20\nmax_workers=0\nupdated_epoch=1\n",
            encoding="utf-8",
        )

        high_id = "queue-high"
        high_batch = self.root / "high-batch"
        (high_batch / "adaptive").mkdir(parents=True)
        (high_batch / "done").mkdir()
        (high_batch / "claims").mkdir()
        (high_batch / "run_meta.env").write_text("total_jobs=4\n", encoding="utf-8")
        high_original = self.root / "high-original.txt"
        high_editable = high_batch / "queue-edit.txt"
        high_original.write_text("high\n" * 4, encoding="utf-8")
        high_editable.write_text("high\n" * 4, encoding="utf-8")
        high_resources = high_batch / "adaptive" / "resources.env"
        high_control = high_batch / "adaptive" / "control.env"
        high_resources.write_text(
            "schema_version=1\nqueue_id=queue-high\ncpu_units_per_job=1\n"
            "memory_mib_per_job=256\nshared_io_units_per_job=0\n",
            encoding="utf-8",
        )
        high_control.write_text(
            "schema_version=1\nqueue_id=queue-high\ngeneration=1\nadmission=enabled\n"
            "schedule_priority=10\nmax_workers=0\nupdated_epoch=1\n",
            encoding="utf-8",
        )
        high_hash = hashlib.sha256(high_resources.read_bytes()).hexdigest()
        high_queue_hash = hashlib.sha256(high_editable.read_bytes()).hexdigest()
        (self.state / "queues" / f"{high_id}.env").write_text(
            "schema_version=1\npool_id=pool-1\nqueue_id=queue-high\n"
            f"batch_dir={high_batch}\noriginal_queue_file={high_original}\n"
            f"editable_queue_file={high_editable}\nmodel_dir={self.root / 'model'}\n"
            f"log_dir={high_batch}\nlock_dir={self.root / 'high.lock'}\n"
            "tag_clean=high-tag\n"
            f"resources_file={high_resources}\nresources_sha256={high_hash}\n"
            f"queue_sha256={high_queue_hash}\n"
            f"control_file={high_control}\nregistered_epoch=1\n",
            encoding="utf-8",
        )

        shutil.copy2(ROOT / "scheduler" / "mmadaptiveagent.sh", self.fake_agent_script)
        runner = self.remote_state / "bin" / "fake-runner.sh"
        runner.write_text(
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n"
            "original=$2; reservation_id=$8; result=$9\n"
            "if [ \"$original\" = \"$LOW_ORIGINAL\" ]; then\n"
            "  queue=low\n"
            "else\n"
            "  queue=high\n"
            "fi\n"
            "printf '%s\\n' \"$queue\" >> \"$START_LOG\"\n"
            "if [ \"$queue\" = low ]; then\n"
            "  deadline=$((SECONDS + 5))\n"
            "  while [ ! -e \"$RELEASE_LOW\" ] && [ \"$SECONDS\" -lt \"$deadline\" ]; do\n"
            "    sleep 0.02\n"
            "  done\n"
            "  [ -e \"$RELEASE_LOW\" ] || exit 98\n"
            "fi\n"
            "temporary=${result}.tmp.$$\n"
            "printf 'reservation_id=%s\\noutcome=succeeded\\n' \"$reservation_id\" > \"$temporary\"\n"
            "mv -f \"$temporary\" \"$result\"\n",
            encoding="utf-8",
        )
        runner.chmod(0o755)
        claim_helper = runner.parent / "adaptive_claim.py"
        shutil.copyfile(COORDINATOR.with_name("adaptive_claim.py"), claim_helper)
        claim_hash = hashlib.sha256(claim_helper.read_bytes()).hexdigest()
        memory_helper = runner.parent / "adaptive_memory.py"
        shutil.copyfile(COORDINATOR.with_name("adaptive_memory.py"), memory_helper)
        memory_hash = hashlib.sha256(memory_helper.read_bytes()).hexdigest()
        runner_hash = hashlib.sha256(runner.read_bytes()).hexdigest()
        (self.remote_state / "agent.env").write_text(
            "schema_version=1\npool_id=pool-1\nhost_id=host-1\n"
            "cpu_units=2\nmemory_mib=2048\n"
            f"runner_path={runner}\nrunner_sha256={runner_hash}\nclaim_sha256={claim_hash}\n"
            f"memory_sha256={memory_hash}\n",
            encoding="utf-8",
        )
        starts = self.root / "starts.log"
        release_low = self.root / "release-low"
        probe_bin = self.root / "probe-bin"
        probe_bin.mkdir()
        probe_ps = probe_bin / "ps"
        real_ps = shutil.which("ps")
        self.assertIsNotNone(real_ps)
        # Host processes outside this test stay anonymous, so a cluster node's own
        # workers cannot read as fixed controllers that consume the agent budget.
        probe_ps.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ \"$*\" == *'pid=,ppid=,%cpu=,rss=,command='* ]]; then\n"
            f"  awk -v root={shlex.quote(str(self.root))} "
            "'index($0, root) == 0 {print $1, $2, 0, 0, \"/usr/bin/host-process\"; next} "
            "{$3=0; $4=0; print}' "
            f"<({real_ps} -ww -axo pid=,ppid=,%cpu=,rss=,command=)\n"
            "  exit 0\n"
            "fi\n"
            f"exec {real_ps} \"$@\"\n",
            encoding="utf-8",
        )
        probe_ps.chmod(0o755)
        agent_environment = os.environ.copy()
        agent_environment.update(
            LOW_ORIGINAL=str(self.root / "original.txt"),
            START_LOG=str(starts),
            RELEASE_LOW=str(release_low),
            PATH=f"{probe_bin}:{agent_environment['PATH']}",
        )
        self.fake_agent = subprocess.Popen(
            [
                "bash", str(self.fake_agent_script), "--state-dir", str(self.remote_state),
                "--poll-seconds", "0.05",
            ],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=agent_environment,
        )

        def wait_for(predicate, message: str) -> None:
            for _ in range(200):
                if predicate():
                    return
                time.sleep(0.02)
            agent_status = self.remote_state / "status.tsv"
            detail = agent_status.read_text() if agent_status.exists() else "missing status"
            self.fail(f"{message}\n{detail}")

        def status(generation: int, reservations: int) -> bool:
            path = self.remote_state / "status.tsv"
            if not path.exists():
                return False
            rows = path.read_text(encoding="utf-8").splitlines()
            agent_rows = [row.split("\t") for row in rows if row.startswith("agent\t")]
            return (
                len(agent_rows) == 1 and int(agent_rows[0][3]) == generation
                and sum(row.startswith("reservation\t") for row in rows) == reservations
            )

        def allocation() -> dict[str, dict[str, str]]:
            lease = self.remote_state / "lease.env"
            path = self.remote_state / self.env_value(lease, "allocations_file")
            with path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle, dialect="excel-tab"))
            return {row["queue_id"]: row for row in rows}

        wait_for(lambda: status(0, 0), "real agent did not publish its initial status")
        first = self.run_once()
        self.assertEqual(first.returncode, 0, first.stderr)
        wait_for(lambda: status(1, 2), "low-priority jobs did not occupy both slots")
        self.assertEqual(allocation()[self.queue_id]["start_grants"], "2")

        high_control.write_text(
            high_control.read_text(encoding="utf-8").replace(
                "generation=1", "generation=2"
            ).replace(
                "schedule_priority=10", "schedule_priority=90"
            ),
            encoding="utf-8",
        )
        handoff = self.run_once()
        self.assertEqual(handoff.returncode, 0, handoff.stderr)
        wait_for(lambda: status(2, 2), "agent did not acknowledge the handoff plan")
        plan = allocation()
        self.assertEqual(
            (plan[self.queue_id]["target_running"], plan[self.queue_id]["start_grants"]),
            ("1", "0"),
        )

        repeated = self.run_once()
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        wait_for(lambda: status(3, 2), "agent did not acknowledge the repeated shrink plan")
        repeated_plan = allocation()
        self.assertEqual(
            (repeated_plan[self.queue_id]["target_running"],
             repeated_plan[self.queue_id]["start_grants"]),
            ("1", "0"),
        )

        release_low.touch()
        wait_for(lambda: status(3, 0), "low-priority jobs did not finish")
        time.sleep(0.2)
        self.assertEqual(starts.read_text(encoding="utf-8").splitlines(), ["low", "low"])

        freed = self.run_once()
        self.assertEqual(freed.returncode, 0, freed.stderr)
        wait_for(
            lambda: starts.exists() and len(starts.read_text().splitlines()) == 4,
            "high-priority jobs did not receive the freed slots",
        )
        time.sleep(0.2)
        self.assertEqual(
            starts.read_text(encoding="utf-8").splitlines(),
            ["low", "low", "high", "high"],
        )

    def test_starting_handshake_waits_for_active_marker(self) -> None:
        pool = self.state / "pool.env"
        pool.write_text("schema_version=1\npool_id=pool-1\nstate=STARTING\n", encoding="utf-8")
        environment = os.environ.copy()
        environment.update(
            ADAPTIVE_SSH_BIN=str(self.fake_ssh),
            FAKE_REMOTE_HOME=str(self.remote_home),
            FAKE_SSH_LOG=str(self.ssh_log),
        )
        command = [
            sys.executable, str(COORDINATOR), "serve",
            "--state-root", str(self.state), "--pool-id", self.pool_id,
            "--resources-file", str(self.resources), "--shared-io-capacity", "4",
            "--tick-seconds", "0.1", # Keep unrelated lifecycle tests independent of startup timing.
            "--lease-seconds", "10", "--start-interval-seconds", "0", "--once",
        ]
        process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=environment)
        ready = self.state / "coordinator.ready"
        for _ in range(40):
            if ready.exists() and self.env_value(ready, "state") == "waiting":
                break
            time.sleep(0.05)
        else:
            process.terminate()
            self.fail("coordinator did not publish its STARTING handshake")
        self.assertEqual(self.ssh_log.read_text(), "")
        pool.write_text("schema_version=1\npool_id=pool-1\nstate=ACTIVE\n", encoding="utf-8")
        stdout, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0, stdout + stderr)
        self.assertEqual(self.env_value(ready, "state"), "ready")

    def test_draining_requires_acknowledged_zero_plan(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1)
        (self.state / "pool.env").write_text(
            "schema_version=1\npool_id=pool-1\nstate=DRAINING\n", encoding="utf-8"
        )
        first_drain = self.run_once()
        self.assertEqual(first_drain.returncode, 0, first_drain.stderr)
        self.assertFalse((self.state / "drained.env").exists())
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "DRAINING")
        self.assertEqual(self.env_value(status, "propagation"), "pending")
        self.write_agent_status(2)
        second_drain = self.run_once()
        self.assertEqual(second_drain.returncode, 0, second_drain.stderr)
        self.assertEqual(self.env_value(self.state / "drained.env", "pool_id"), self.pool_id)
        self.assertEqual(self.env_value(status, "lifecycle"), "FINISHED_INCOMPLETE")

    def test_paused_idle_queue_stays_pending_during_lease_renewal(self) -> None:
        self.write_control(2, "paused")
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1)
        self.assertEqual(self.run_once().returncode, 0)
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "propagation"), "complete")

        # Status still reports the acknowledged zero plan during its renewal,
        # which repeats an applied control and so cannot undo its propagation.
        renewal = self.run_once()
        self.assertEqual(renewal.returncode, 0, renewal.stderr)
        self.assertEqual(self.env_value(status, "lifecycle"), "PENDING")
        self.assertEqual(self.env_value(status, "admission"), "paused")
        self.assertEqual(self.env_value(status, "running_workers"), "0")
        self.assertEqual(self.env_value(status, "target_workers"), "0")
        self.assertEqual(self.env_value(status, "propagation"), "complete")
        self.assertTrue((self.state / "queues" / f"{self.queue_id}.env").exists())
        self.assertFalse((self.state / "drained.env").exists())

    def test_paused_queue_drains_unacknowledged_grants_and_running_workers(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_control(2, "paused")
        unacknowledged = self.run_once()
        self.assertEqual(unacknowledged.returncode, 0, unacknowledged.stderr)
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "DRAINING")
        self.assertEqual(self.env_value(status, "running_workers"), "0")
        self.assertEqual(self.env_value(status, "target_workers"), "0")
        state = json.loads((self.state / "plans" / "host-1" / "state.json").read_text())
        self.assertEqual(state["envelope_targets"], {self.queue_id: 2})

        self.write_agent_status(state["generation"], reservations=1)
        running = self.run_once()
        self.assertEqual(running.returncode, 0, running.stderr)
        self.assertEqual(self.env_value(status, "lifecycle"), "DRAINING")
        self.assertEqual(self.env_value(status, "running_workers"), "1")

    def test_terminal_status_is_durable_before_registration_cleanup(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_editable("")
        (self.batch / "done" / "job_1.done").write_text("ok\n", encoding="utf-8")
        (self.batch / "done" / "job_2.done").write_text("ok\n", encoding="utf-8")
        self.write_agent_status(1)
        self.assertEqual(self.run_once().returncode, 0)
        registration = self.state / "queues" / f"{self.queue_id}.env"
        self.assertTrue(registration.exists())
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "DRAINING")
        self.write_agent_status(2)
        self.assertEqual(self.run_once().returncode, 0)
        self.assertFalse(registration.exists())
        self.assertEqual(self.env_value(status, "lifecycle"), "COMPLETED")

    def test_terminal_registration_waits_for_current_control(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_editable("")
        self.write_agent_status(1)
        self.write_control(2, "paused")
        self.assertEqual(self.run_once().returncode, 0)

        self.write_agent_status(2)
        self.write_control(3, "paused")
        pending_control = self.run_once()
        self.assertEqual(pending_control.returncode, 0, pending_control.stderr)
        registration = self.state / "queues" / f"{self.queue_id}.env"
        status = self.batch / "adaptive" / "status.env"
        self.assertTrue(registration.exists())
        self.assertEqual(self.env_value(status, "lifecycle"), "FINISHED_INCOMPLETE")
        self.assertEqual(self.env_value(status, "propagation"), "pending")

        self.write_agent_status(3)
        applied = self.run_once()
        self.assertEqual(applied.returncode, 0, applied.stderr)
        self.assertFalse(registration.exists())
        self.assertEqual(self.env_value(status, "propagation"), "complete")

    def test_terminal_cleanup_rechecks_control_under_transition_lock(self) -> None:
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.write_control(2, "paused")
        coordinator = MODULE.Coordinator(
            self.state, self.pool_id, self.resources, 4, 1, 10,
            start_interval_seconds=0,
        )
        coordinator._queue_transition(queues[self.queue_id])
        self.assertTrue((self.state / "queues" / f"{self.queue_id}.env").exists())

    def test_closed_queue_with_pending_lines_becomes_incomplete_after_zero_ack(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1)
        self.write_control(2, "closed")
        self.assertEqual(self.run_once().returncode, 0)
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "DRAINING")
        self.assertEqual(self.env_value(status, "propagation"), "pending")

        self.write_agent_status(2)
        self.assertEqual(self.run_once().returncode, 0)
        self.assertEqual(self.env_value(status, "lifecycle"), "FINISHED_INCOMPLETE")
        self.assertEqual(self.env_value(status, "propagation"), "complete")
        self.assertFalse((self.state / "queues" / f"{self.queue_id}.env").exists())

    def test_staggered_zero_plan_acknowledgements_retire_a_finished_queue(self) -> None:
        # One CPU per host places one of the two jobs on each host.
        self.resources.write_text(
            "host_id\tssh_target\tcpu_units\tmemory_mib\tenabled\n"
            "host-1\tfake-target\t1\t2048\t1\n"
            "host-2\tfake-target-2\t1\t2048\t1\n",
            encoding="utf-8",
        )
        shutil.copytree(self.state / "plans" / "host-1", self.state / "plans" / "host-2")
        coordinator = MODULE.Coordinator(
            self.state, self.pool_id, self.resources, 4, 1, 10,
            start_interval_seconds=0,
        )
        generations = {"host-1": 0, "host-2": 0}

        def poll(host_id: str, _queues: dict) -> dict:
            return {
                "generation": generations[host_id], "reservations": {},
                "reservation_memory_mib": {}, "reservation_ids": {},
                "reservation_charges_mib": {}, "admission": "enabled",
                "uncertain": False, "reachable": True,
                "live_cpu_units": 2, "live_memory_mib": 2048,
                "smbd_cpu_tenths": 0, "telemetry_state": "ok",
            }

        def tick(host_1: int, host_2: int) -> None:
            generations.update({"host-1": host_1, "host-2": host_2})
            with mock.patch.object(coordinator, "_poll", side_effect=poll), \
                    mock.patch.object(MODULE, "_ssh", return_value=""):
                coordinator.tick()

        def published(host_id: str) -> dict:
            return json.loads((self.state / "plans" / host_id / "state.json").read_text())

        tick(0, 0)
        tick(1, 1)
        self.assertEqual(published("host-1")["targets"][self.queue_id], 1)
        self.assertEqual(published("host-2")["targets"][self.queue_id], 1)
        self.write_editable("")
        (self.batch / "done" / "job_1.done").write_text("ok\n", encoding="utf-8")
        (self.batch / "done" / "job_2.done").write_text("ok\n", encoding="utf-8")
        tick(2, 2)
        zero_plan = published("host-1")["generation"]
        for host_id in ("host-1", "host-2"):
            self.assertEqual(published(host_id)["generation"], zero_plan)
            self.assertEqual(published(host_id)["targets"][self.queue_id], 0)

        # Host 2 has not seen a zero plan yet, so it could still start a job.
        tick(zero_plan, zero_plan - 1)
        registration = self.state / "queues" / f"{self.queue_id}.env"
        status = self.batch / "adaptive" / "status.env"
        self.assertTrue(registration.exists())
        self.assertEqual(self.env_value(status, "lifecycle"), "DRAINING")
        self.assertGreater(published("host-1")["generation"], zero_plan)

        # Each host has now applied a zero plan, though not the same latest one.
        tick(zero_plan, zero_plan)
        self.assertFalse(registration.exists())
        self.assertEqual(self.env_value(status, "lifecycle"), "COMPLETED")
        self.assertEqual(self.env_value(status, "propagation"), "complete")

    def test_disabled_hosts_are_not_contacted(self) -> None:
        self.resources.write_text(
            self.resources.read_text(encoding="utf-8")
            + "disabled\tmust-not-poll\t8\t8192\t0\n",
            encoding="utf-8",
        )
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("must-not-poll", self.ssh_log.read_text())

    def test_disabled_pressure_host_is_polled_but_never_published(self) -> None:
        self.resource_env.write_text(
            self.resource_env.read_text(encoding="utf-8").replace(
                "shared_io_units_per_job=0", "shared_io_units_per_job=1"
            ),
            encoding="utf-8",
        )
        registration = self.state / "queues" / f"{self.queue_id}.env"
        digest = hashlib.sha256(self.resource_env.read_bytes()).hexdigest()
        registration.write_text(
            re.sub(
                r"^resources_sha256=.*$", f"resources_sha256={digest}",
                registration.read_text(encoding="utf-8"), flags=re.MULTILINE,
            ),
            encoding="utf-8",
        )
        self.register_queue("queue-local", shared_io=0)
        self.resources.write_text(
            self.resources.read_text(encoding="utf-8")
            + "pressure\tpressure-target\t1\t1024\t0\n",
            encoding="utf-8",
        )
        coordinator = MODULE.Coordinator(
            self.state, self.pool_id, self.resources, 4, 1, 10, "pressure", 400,
            start_interval_seconds=0,
        )
        host_status = {
            "generation": 0, "reservations": {}, "reservation_memory_mib": {},
            "reservation_ids": {},
            "admission": "drain-only", "uncertain": False, "reachable": True,
            "live_cpu_units": 2, "live_memory_mib": 2048,
            "smbd_cpu_tenths": 0, "telemetry_state": "ok",
        }
        pressure_status = {
            **host_status, "live_cpu_units": 0, "live_memory_mib": 0,
            "telemetry_state": "unavailable",
        }
        polled_hosts: list[str] = []
        published: list[tuple[str, dict[str, int], dict[str, int]]] = []

        def poll(host_id: str, _queues: dict) -> dict:
            polled_hosts.append(host_id)
            return pressure_status if host_id == "pressure" else host_status

        def publish(
            host_id: str, targets: dict[str, int], grants: dict[str, int],
            _queues: dict, _polled: dict, state: dict,
        ) -> dict:
            published.append((host_id, targets, grants))
            return state

        with mock.patch.object(coordinator, "_poll", side_effect=poll), \
                mock.patch.object(coordinator, "_publish", side_effect=publish):
            coordinator.tick()

        self.assertEqual(set(polled_hosts), {"host-1", "pressure"})
        self.assertEqual([host_id for host_id, _, _ in published], ["host-1"])
        self.assertEqual(published[0][1][self.queue_id], 0)
        self.assertEqual(published[0][2][self.queue_id], 0)
        self.assertEqual(published[0][1]["queue-local"], 2)
        self.assertEqual(published[0][2]["queue-local"], 2)
        ready = self.state / "coordinator.ready"
        self.assertEqual(self.env_value(ready, "io_pressure_state"), "unavailable")

    def test_duplicate_ssh_target_fails_before_polling(self) -> None:
        self.resources.write_text(
            self.resources.read_text(encoding="utf-8")
            + "host-2\tfake-target\t2\t2048\t1\n",
            encoding="utf-8",
        )
        result = self.run_once()
        self.assertEqual(result.returncode, 2)
        self.assertIn("duplicate ssh_target: fake-target", result.stderr)
        self.assertEqual(self.ssh_log.read_text(), "")

    def test_invalid_agent_status_fails_closed_for_that_host(self) -> None:
        (self.remote_state / "status.tsv").write_text("broken\n", encoding="utf-8")
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.remote_state / "lease.env").exists())
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "propagation"), "pending")

    def assert_quarantined_while_healthy_queue_runs(self, reason: str) -> None:
        self.register_queue("queue-2")
        with self.assertRaisesRegex(SnapshotError, reason):
            MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"quarantined queue queue-1: {reason}", result.stderr)
        lease = self.remote_state / "lease.env"
        allocation = self.remote_state / self.env_value(lease, "allocations_file")
        with allocation.open(encoding="utf-8") as handle:
            rows = {row["queue_id"]: row for row in csv.DictReader(handle, dialect="excel-tab")}
        self.assertNotIn("queue-1", rows)
        self.assertGreater(int(rows["queue-2"]["start_grants"]), 0)
        self.assertEqual(self.env_value(self.state / "coordinator.ready", "quarantined_queues"), "1")
        quarantine = json.loads((self.state / "quarantine.json").read_text())
        self.assertIn(reason, quarantine["queues"]["queue-1"])

    def test_mutated_resource_record_quarantines_only_that_queue(self) -> None:
        self.resource_env.write_text(
            self.resource_env.read_text(encoding="utf-8").replace(
                "memory_mib_per_job=256", "memory_mib_per_job=128"
            ),
            encoding="utf-8",
        )
        self.assert_quarantined_while_healthy_queue_runs("resource hash mismatch for queue-1")

    def test_mutated_queue_copy_quarantines_only_that_queue(self) -> None:
        self.editable.write_text("job_1 false\n", encoding="utf-8")
        self.assert_quarantined_while_healthy_queue_runs("queue hash mismatch for queue-1")

    def test_status_write_failure_closes_only_the_bad_queue_and_recovers(self) -> None:
        self.register_queue("queue-2")
        status = self.batch / "adaptive/status.env"
        status.mkdir()
        for generation in range(2):
            self.write_agent_status(generation, "enabled")
            result = self.run_once()
            self.assertEqual(result.returncode, 0, result.stderr)
            allocations = (self.remote_state / f"allocations.{generation + 1}.tsv").read_text()
            self.assertNotIn("queue-1\t", allocations)
            self.assertIn("queue-2\t2\t2", allocations)
            self.assertEqual(self.env_value(self.state / "coordinator.ready", "quarantined_queues"), "1")
        status.rmdir()
        self.write_agent_status(2, "enabled")
        self.assertEqual(self.run_once().returncode, 0)
        self.assertIn("queue-1\t", (self.remote_state / "allocations.3.tsv").read_text())
        self.assertEqual(json.loads((self.state / "quarantine.json").read_text())["queues"], {})

    def test_completion_scan_failure_quarantines_only_that_queue(self) -> None:
        self.register_queue("queue-2")
        for job_id in ("job_1", "job_2"):
            (self.batch / "claims" / f"job_{job_id}.claim").mkdir()
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1, "enabled")
        (self.batch / "done").rmdir()
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("cannot read completion markers", result.stderr)
        self.assertNotIn("queue-1\t", (self.remote_state / "allocations.2.tsv").read_text())
        self.assertIn("queue-2\t2\t2", (self.remote_state / "allocations.2.tsv").read_text())

    def test_late_status_error_is_quarantined_before_leases_are_published(self) -> None:
        self.register_queue("queue-2")
        coordinator = MODULE.Coordinator(self.state, self.pool_id, self.resources, 4, 1, 10, start_interval_seconds=0)
        write = MODULE._atomic_text
        published = []

        def atomic(path: Path, text: str, mode: int = 0o600) -> None:
            if path == self.batch / "adaptive/status.env":
                raise PermissionError("simulated status publication denial")
            write(path, text, mode)

        def ssh(target, command, timeout, input_text=None):
            if input_text is not None:
                published.append(input_text)
                return ""
            return (self.remote_state / "status.tsv").read_text()

        with mock.patch.object(MODULE, "_atomic_text", atomic), mock.patch.object(MODULE, "_ssh", ssh):
            self.assertFalse(coordinator.tick())
        self.assertIn("simulated status publication denial", coordinator.quarantined["queue-1"])
        self.assertEqual(len(published), 1)
        self.assertNotIn("queue-1\t", published[0])
        self.assertRegex(published[0], r"queue-2\t[1-9]\d*\t[1-9]\d*\t")
        self.assertEqual(self.env_value(self.state / "coordinator.ready", "quarantined_queues"), "1")

    def test_non_utf8_control_is_quarantined(self) -> None:
        self.control_env.write_bytes(b"\xff")
        self.assert_quarantined_while_healthy_queue_runs("cannot read")

    def test_quarantined_running_queue_retains_io_charge_after_restart(self) -> None:
        self.register_queue("queue-2", shared_io=1)
        self.resource_env.write_text(self.resource_env.read_text().replace(
            "shared_io_units_per_job=0", "shared_io_units_per_job=4"))
        registration = self.state / "queues/queue-1.env"
        registration.write_text(re.sub(r"^resources_sha256=.*$", "resources_sha256=" +
            hashlib.sha256(self.resource_env.read_bytes()).hexdigest(),
            registration.read_text(), flags=re.M))
        self.write_agent_status(0, "enabled", reservations=1)
        self.assertEqual(self.run_once().returncode, 0)
        self.assertTrue((self.state / "queue-profiles.json").exists())
        # A new coordinator process must use the durable costs even if the
        # registration is now unreadable and a running job still owns all I/O.
        registration.write_bytes(b"\xff")
        self.write_agent_status(1, "enabled", reservations=1)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        allocations = (self.remote_state / "allocations.2.tsv").read_text()
        self.assertNotIn("queue-1\t", allocations)
        self.assertIn("queue-2\t0\t0", allocations)
        self.assertEqual(self.env_value(self.state / "coordinator.ready", "resource_state"), "ok")
        self.write_agent_status(2, "enabled")
        self.assertEqual(self.run_once().returncode, 0)
        self.assertIn("queue-2\t2\t2", (self.remote_state / "allocations.3.tsv").read_text())

    def test_unknown_quarantined_cost_closes_positive_io_but_allows_zero_io(self) -> None:
        self.register_queue("queue-2", shared_io=1)
        self.register_queue("queue-3", shared_io=0)
        self.resource_env.write_bytes(b"\xff")
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        allocations = (self.remote_state / "allocations.1.tsv").read_text()
        self.assertIn("queue-2\t0\t0", allocations)
        self.assertIn("queue-3\t2\t2", allocations)

    def test_quarantined_running_charge_limits_cpu_and_memory_without_closing_host(self) -> None:
        self.register_queue("queue-2")
        self.write_agent_status(0, "enabled", reservations=1, live_memory=512)
        self.assertEqual(self.run_once().returncode, 0)
        self.editable.write_text("job_1 false\n")
        self.write_agent_status(1, "enabled", reservations=1, live_memory=512)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("queue-2\t1\t1", (self.remote_state / "allocations.2.tsv").read_text())
        self.assertEqual(self.env_value(self.state / "coordinator.ready", "resource_state"), "ok")

    def test_quarantined_reporting_does_not_strand_drain_or_forget_running_jobs(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        status = self.batch / "adaptive/status.env"
        status.unlink()
        status.mkdir()
        pool = self.state / "pool.env"
        pool.write_text(pool.read_text().replace("state=ACTIVE", "state=DRAINING"))
        for generation in (1, 2):
            self.write_agent_status(generation, "enabled", reservations=1)
            result = self.run_once()
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((self.state / "drained.env").exists())
        self.write_agent_status(3, "enabled")
        self.assertEqual(self.run_once().returncode, 0)
        self.assertTrue((self.state / "drained.env").exists())

    def test_accounting_cache_with_wrong_pool_or_invalid_cost_fails_loudly(self) -> None:
        path = self.state / "queue-profiles.json"
        for pool_id, cpu in (("another-pool", 1), (self.pool_id, 0)):
            path.write_text(json.dumps({
                "schema_version": 1, "pool_id": pool_id, "profiles": {"queue-1": {
                    "cpu_units_per_job": cpu, "memory_mib_per_job": 256,
                    "shared_io_units_per_job": 1,
                }},
            }))
            result = self.run_once()
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertFalse((self.remote_state / "lease.env").exists())

    def test_cached_queue_hash_rechecks_a_replaced_copy(self) -> None:
        cache: dict = {}
        MODULE._load_queues(self.state, self.pool_id, "ACTIVE", cache)
        self.assertIn(self.queue_id, cache)
        replacement = self.editable.with_name("replacement.txt")
        replacement.write_text("job_1 false\njob_2 true\n", encoding="utf-8")
        os.replace(replacement, self.editable)
        with self.assertRaisesRegex(SnapshotError, "queue hash mismatch"):
            MODULE._load_queues(self.state, self.pool_id, "ACTIVE", cache)

    def test_pending_lines_exclude_existing_claim_paths(self) -> None:
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.assertEqual(queues[self.queue_id]["allocator"]["pending_lines"], 2)

        claims = self.batch / "claims"
        claims.mkdir(exist_ok=True)
        (claims / "job_job_1.claim").mkdir()
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.assertEqual(queues[self.queue_id]["allocator"]["pending_lines"], 1)

        # A malformed claim object is still conservative evidence of a claim.
        (claims / "job_job_2.claim").write_text("not a directory\n", encoding="utf-8")
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.assertEqual(queues[self.queue_id]["allocator"]["pending_lines"], 0)

        (claims / "job_job_2.claim").unlink()
        (claims / "job_job_2.claim").symlink_to("missing-claim-target")
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        self.assertEqual(queues[self.queue_id]["allocator"]["pending_lines"], 0)

    def test_stale_status_from_a_dead_agent_gets_no_plan(self) -> None:
        self.fake_agent.terminate()
        self.fake_agent.wait(timeout=5)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.remote_state / "lease.env").exists())

    def test_unreachable_prior_target_keeps_empty_queue_draining(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_editable("")
        self.fake_agent.terminate()
        self.fake_agent.wait(timeout=5)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "DRAINING")
        self.assertTrue((self.state / "queues" / f"{self.queue_id}.env").exists())

    def test_offline_node_without_grants_does_not_retain_finished_queue(self) -> None:
        self.write_control(1, "paused")
        self.assertEqual(self.run_once().returncode, 0)
        self.write_editable("")
        for job in (1, 2):
            (self.batch / "done" / f"job_{job}.done").write_text("ok\n")
        self.fake_agent.terminate()
        self.fake_agent.wait(timeout=5)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "COMPLETED")
        self.assertEqual(self.env_value(status, "propagation"), "complete")
        self.assertFalse((self.state / "queues" / f"{self.queue_id}.env").exists())

    def write_host_states(self, **windows: tuple[int, Optional[int]]) -> None:
        (self.state / "host-states.json").write_text(json.dumps({
            "schema_version": 1, "pool_id": self.pool_id, "generation": 1,
            "hosts": {host: {"away_from": start, "away_until": end, "reason": "meeting", "set_epoch": 1}
                      for host, (start, end) in windows.items()},
        }))

    def test_away_host_gets_no_new_work_and_is_reported(self) -> None:
        now = int(time.time())
        for window, away in (((0, None), True), ((now + 3600, None), False), ((0, now - 1), False)):
            with self.subTest(window=window):
                self.write_host_states(**{"host-1": window})
                result = self.run_once()
                self.assertEqual(result.returncode, 0, result.stderr)
                generation = self.env_value(self.remote_state / "lease.env", "generation")
                row = next(line for line in (self.remote_state / f"allocations.{generation}.tsv")
                           .read_text().splitlines() if line.startswith("queue-1\t"))
                self.assertEqual(row.split("\t")[1] == "0", away, row)
                ready = self.state / "coordinator.ready"
                self.assertEqual(self.env_value(ready, "away_hosts"), "host-1" if away else "")
                self.assertEqual(self.env_value(ready, "unavailable_hosts"), "")

    def test_set_host_state_windows_and_validation(self) -> None:
        with (self.state / "pool.env").open("a") as pool:
            pool.write(f"resources_file={self.resources}\n")
        path = self.state / "host-states.json"
        with redirect_stdout(io.StringIO()) as output:
            MODULE.set_host_state(self.state, self.pool_id, "host-1", "away", None, None, "meeting")
        self.assertIn("away_now=1", output.getvalue())
        self.assertIn("host-1", MODULE._away_hosts(json.loads(path.read_text()), time.time()))
        later = int(time.time()) + 3600
        with redirect_stdout(io.StringIO()):
            MODULE.set_host_state(self.state, self.pool_id, "host-1", "present", str(later), None, "")
        entry = json.loads(path.read_text())["hosts"]["host-1"]
        self.assertEqual(entry["away_until"], later, "a scheduled return ends the window")
        with redirect_stdout(io.StringIO()):
            MODULE.set_host_state(self.state, self.pool_id, "host-1", "present", None, None, "")
        self.assertEqual(json.loads(path.read_text())["hosts"], {})
        for host, state, at, until, reason, message in (
                ("host-9", "away", None, None, "x", "enabled pool host"),
                ("host-1", "away", None, None, "a,b", "reason"),
                ("host-1", "away", str(later), str(later - 60), "x", "until"),
                ("host-1", "present", str(later), None, "", "scheduled return"),
                ("host-1", "away", "tomorrow", None, "x", "ISO local time")):
            with self.subTest(message=message), self.assertRaisesRegex(SnapshotError, message):
                MODULE.set_host_state(self.state, self.pool_id, host, state, at, until, reason)
        self.assertEqual(MODULE._parse_time("2026-11-12T09:00"),
                         int(datetime(2026, 11, 12, 9, 0).timestamp()))

    def test_offline_node_without_grants_does_not_hold_a_closed_queue_with_pending_jobs(self) -> None:
        # An away host that never held a grant for the queue cannot start its jobs, so
        # retry and rebinding, which need propagation=complete, need not wait for it.
        self.write_control(1, "closed")
        self.assertEqual(self.run_once().returncode, 0)
        self.fake_agent.terminate()
        self.fake_agent.wait(timeout=5)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "FINISHED_INCOMPLETE")
        self.assertEqual(self.env_value(status, "propagation"), "complete")
        self.assertFalse((self.state / "queues" / f"{self.queue_id}.env").exists())

    def test_offline_zero_target_with_unacknowledged_envelope_is_retained(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1, reservations=1)
        self.write_control(2, "paused")
        self.assertEqual(self.run_once().returncode, 0)
        self.write_editable("")
        self.fake_agent.terminate()
        self.fake_agent.wait(timeout=5)
        self.assertEqual(self.run_once().returncode, 0)
        status = self.batch / "adaptive/status.env"
        self.assertEqual(self.env_value(status, "lifecycle"), "DRAINING")
        self.assertTrue((self.state / "queues" / f"{self.queue_id}.env").exists())

    def test_stale_status_from_a_live_agent_gets_no_plan(self) -> None:
        status = self.remote_state / "status.tsv"
        stale_epoch = time.time() - 20
        os.utime(status, (stale_epoch, stale_epoch))
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.remote_state / "lease.env").exists())

    def test_failed_shrink_keeps_conservative_pending_envelope(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1, reservations=2)
        self.write_control(2, "paused")
        result = self.run_once(FAKE_FAIL_PUBLISH="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("publication to host-1 failed", result.stderr)
        self.assertEqual(self.env_value(self.remote_state / "lease.env", "generation"), "1")
        state = json.loads((self.state / "plans" / "host-1" / "state.json").read_text())
        self.assertEqual(state["generation"], 2)
        self.assertTrue(state["pending"])
        self.assertEqual(state["targets"], {"queue-1": 0})
        self.assertEqual(state["envelope_targets"], {"queue-1": 2})

    def test_successful_shrink_keeps_envelope_until_acknowledged(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1, "enabled", reservations=2)
        self.write_control(2, "paused")

        shrink = self.run_once()
        self.assertEqual(shrink.returncode, 0, shrink.stderr)
        state_path = self.state / "plans" / "host-1" / "state.json"
        state = json.loads(state_path.read_text())
        self.assertEqual(state["generation"], 2)
        self.assertEqual(state["targets"], {"queue-1": 0})
        self.assertEqual(state["envelope_targets"], {"queue-1": 2})

        unacknowledged = self.run_once()
        self.assertEqual(unacknowledged.returncode, 0, unacknowledged.stderr)
        state = json.loads(state_path.read_text())
        self.assertEqual(state["generation"], 2)
        self.assertEqual(state["targets"], {"queue-1": 0})
        self.assertEqual(state["envelope_targets"], {"queue-1": 2})

        self.write_agent_status(2, "enabled")
        acknowledged = self.run_once()
        self.assertEqual(acknowledged.returncode, 0, acknowledged.stderr)
        state = json.loads(state_path.read_text())
        self.assertEqual(state["envelope_targets"], {"queue-1": 0})

    def test_crash_after_remote_commit_leaves_conservative_envelope(self) -> None:
        coordinator = MODULE.Coordinator(
            self.state, self.pool_id, self.resources, 4, 1, 10,
            start_interval_seconds=0,
        )
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        prior = {
            "generation": 1, "targets": {"queue-1": 2},
            "envelope_targets": {"queue-1": 2},
            "controls": {"queue-1": 1}, "pending": False,
        }
        real_atomic_json = MODULE._atomic_json

        def crash_before_exact(path: Path, value: dict) -> None:
            if path.name == "state.json" and not value["pending"]:
                raise OSError("simulated crash after remote commit")
            real_atomic_json(path, value)

        with mock.patch.object(MODULE, "_ssh", return_value=""), \
                mock.patch.object(MODULE, "_atomic_json", side_effect=crash_before_exact):
            with self.assertRaisesRegex(OSError, "simulated crash"):
                coordinator._publish(
                    "host-1", {"queue-1": 0}, {"queue-1": 0}, queues,
                    {"generation": 1, "reservations": {"queue-1": 2}}, prior,
                )
        state = json.loads((self.state / "plans" / "host-1" / "state.json").read_text())
        self.assertTrue(state["pending"])
        self.assertEqual(state["targets"], {"queue-1": 0})
        self.assertEqual(state["envelope_targets"], {"queue-1": 2})

    def test_missing_enabled_host_state_fails_loudly(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(1)
        (self.state / "plans" / "host-1" / "state.json").unlink()
        result = self.run_once()
        self.assertEqual(result.returncode, 2)
        self.assertIn("local plan state is missing for enabled host host-1", result.stderr)
        self.assertEqual(self.env_value(self.remote_state / "lease.env", "generation"), "1")

    def test_corrupt_host_state_fails_loudly(self) -> None:
        state_path = self.state / "plans" / "host-1" / "state.json"
        state = json.loads(state_path.read_text())
        state["targets"] = {self.queue_id: 0.5}
        state["envelope_targets"] = {self.queue_id: 1}
        state_path.write_text(json.dumps(state), encoding="utf-8")
        result = self.run_once()
        self.assertEqual(result.returncode, 2)
        self.assertIn("invalid targets in host plan state: host-1", result.stderr)

    def test_agent_generation_ahead_of_local_state_is_not_acknowledged(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        self.write_agent_status(2)
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        status = self.batch / "adaptive" / "status.env"
        self.assertEqual(self.env_value(status, "propagation"), "pending")
        state = json.loads((self.state / "plans" / "host-1" / "state.json").read_text())
        self.assertEqual(state["envelope_targets"], {"queue-1": 2})

    def test_uncertain_agent_keeps_its_last_plan_envelope(self) -> None:
        self.assertEqual(self.run_once().returncode, 0)
        lease = self.remote_state / "lease.env"
        self.assertEqual(self.env_value(lease, "generation"), "1")
        (self.remote_state / "status.tsv").write_text(
            STATUS_HEADER + "\n" + "\t".join([
                "agent", self.pool_id, "host-1", "1", "drain-only",
                "", "", "", "", "", "", "", "", "uncertain_reservation",
                "2", "2048", "0", "ok", str(int(time.time())), "-1",
                "", "",
            ]) + "\n",
            encoding="utf-8",
        )
        result = self.run_once()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.env_value(lease, "generation"), "1")
        state = json.loads((self.state / "plans" / "host-1" / "state.json").read_text())
        self.assertEqual(state["targets"], {"queue-1": 2})
        self.assertEqual(state["envelope_targets"], {"queue-1": 2})

    def test_uncertain_status_retains_last_certain_reservations(self) -> None:
        self.resource_env.write_text(
            self.resource_env.read_text(encoding="utf-8").replace(
                "shared_io_units_per_job=0", "shared_io_units_per_job=4"
            ),
            encoding="utf-8",
        )
        registration = self.state / "queues" / f"{self.queue_id}.env"
        digest = hashlib.sha256(self.resource_env.read_bytes()).hexdigest()
        registration.write_text(
            re.sub(
                r"^resources_sha256=.*$", f"resources_sha256={digest}",
                registration.read_text(encoding="utf-8"), flags=re.MULTILINE,
            ),
            encoding="utf-8",
        )
        queues = MODULE._load_queues(self.state, self.pool_id, "ACTIVE")
        coordinator = MODULE.Coordinator(
            self.state, self.pool_id, self.resources, 4, 1, 10,
            start_interval_seconds=0,
        )
        (self.remote_state / "status.tsv").write_text(
            STATUS_HEADER + "\n" + "\t".join([
                "agent", self.pool_id, "host-1", "1", "enabled",
                "", "", "", "", "", "", "", "", "ready", "2", "2048",
                "0", "ok", str(int(time.time())), "-1",
                "", "",
            ]) + "\n" + "\t".join([
                "reservation", self.pool_id, "host-1", "1", "", "res-1",
                self.queue_id, "running", "123", "1", "256", "512", "512",
                "", "", "", "", "", "", "",
                "0", "0",
            ]) + "\n",
            encoding="utf-8",
        )
        poll_environment = {
            "ADAPTIVE_SSH_BIN": str(self.fake_ssh),
            "FAKE_REMOTE_HOME": str(self.remote_home),
            "FAKE_SSH_LOG": str(self.ssh_log),
        }
        with mock.patch.dict(os.environ, poll_environment):
            certain = coordinator._poll("host-1", queues)
            self.assertEqual(certain["reservations"], {self.queue_id: 1})
            self.assertEqual(certain["reservation_memory_mib"], {self.queue_id: 512})
            (self.remote_state / "status.tsv").write_text(
                STATUS_HEADER + "\n" + "\t".join([
                    "agent", self.pool_id, "host-1", "1", "drain-only",
                    "", "", "", "", "", "", "", "",
                    "uncertain_reservation", "2", "2048", "0", "ok", str(int(time.time())), "-1",
                    "", "",
                ]) + "\n",
                encoding="utf-8",
            )
            uncertain = coordinator._poll("host-1", queues)
        self.assertTrue(uncertain["uncertain"])
        self.assertEqual(uncertain["reservations"], {self.queue_id: 1})
        self.assertEqual(uncertain["reservation_memory_mib"], {self.queue_id: 512})

        profile = dict(queues[self.queue_id]["allocator"])
        plan = allocate({
            "schema_version": 1,
            "shared_io_capacity": 4,
            "hosts": [
                {"host_id": "host-1", "cpu_units": 2, "memory_mib": 2048,
                 "enabled": True, "reachable": True, "drain_only": True},
                {"host_id": "host-2", "cpu_units": 2, "memory_mib": 2048,
                 "enabled": True, "reachable": True, "drain_only": False},
            ],
            "queues": [profile],
            "reservations": [
                {"host_id": "host-1", "queue_id": self.queue_id, "count": 1}
            ],
            "prior_targets": [],
            "cursor": {},
        })
        self.assertEqual(plan["targets"]["host-2"][self.queue_id], 0)


if __name__ == "__main__":
    unittest.main()
