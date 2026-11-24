#!/usr/bin/env python3
"""Opt-in queue memory floors from representative completed or long-running jobs."""

from __future__ import annotations

import math
import re
from typing import Any

MAX_COHORT = 50
RUNNING_MAX_AGE_SECONDS = 30
LOWER_INTERVAL_SECONDS = 300
_IDENTIFIER = re.compile(r"[A-Za-z0-9._-]+")
_JOB_ID = re.compile(r"[A-Za-z0-9._:-]+")
_POLICY_KEYS = {"jobs", "seconds", "minimum_mib", "generation"}
_COMPLETED_KEYS = {"reservation_id", "job_id", "finished_epoch", "mean_mib", "seconds"}
_RUNNING_KEYS = {"reservation_id", "first_epoch", "last_epoch", "mean_mib", "seconds", "samples"}
_STATUSES = {"disabled", "collecting", "calibrated", "provisional"}
_REASONS = {
    "disabled", "new_generation", "clock_reversal", "recent_failure", "missing_evidence",
    "insufficient_jobs", "insufficient_running", "lowered", "raised", "stable",
    "material_change", "cooldown", "baseline_cap",
}


def _fields(value: Any, required: set[str], optional: set[str] = frozenset()) -> None:
    if not isinstance(value, dict) or not required <= value.keys() or value.keys() - required - optional:
        raise ValueError("invalid calibration fields")


def _integer(value: Any, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError("invalid calibration integer")
    return value


def _number(value: Any, minimum: float = 0) -> float:
    try:
        valid = type(value) in {int, float} and math.isfinite(value) and value >= minimum
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError("invalid calibration time or duration")
    return float(value)


def _identifier(value: Any, *, job: bool = False) -> str:
    pattern = _JOB_ID if job else _IDENTIFIER
    if not isinstance(value, str) or not pattern.fullmatch(value) or (not job and value in {".", ".."}):
        raise ValueError("invalid calibration identifier")
    return value


def _policy(policy: Any, baseline: int) -> dict:
    _fields(policy, _POLICY_KEYS)
    result = {key: _integer(policy[key], 1 if key in {"minimum_mib", "generation"} else 0)
              for key in _POLICY_KEYS}
    if ((result["jobs"] and not 20 <= result["jobs"] <= 50)
            or (result["seconds"] and not 600 <= result["seconds"] <= 86400)
            or result["minimum_mib"] > baseline):
        raise ValueError("invalid calibration policy")
    return result


def _completed(value: Any, *, saved: bool = False) -> dict:
    _fields(value, _COMPLETED_KEYS | ({"host_id"} if saved else set()))
    result = {
        "reservation_id": _identifier(value["reservation_id"]),
        "job_id": _identifier(value["job_id"], job=True),
        "finished_epoch": _number(value["finished_epoch"]),
        "mean_mib": _integer(value["mean_mib"], 1),
        "seconds": _number(value["seconds"], 15),
    }
    if saved:
        result["host_id"] = _identifier(value["host_id"])
    return result


def _reports(reports: Any) -> tuple[list[dict], list[dict], list[float]]:
    if not isinstance(reports, list):
        raise ValueError("calibration reports must be a list")
    hosts, completed, running, failures = set(), [], [], []
    for report in reports:
        _fields(report, {"host_id", "completed", "running", "failure_epoch"})
        host = _identifier(report["host_id"])
        if host in hosts or not isinstance(report["completed"], list) or not isinstance(report["running"], list):
            raise ValueError("invalid or duplicate calibration host report")
        if len(report["completed"]) > MAX_COHORT:
            raise ValueError("calibration completed cohort exceeds limit")
        hosts.add(host)
        failures.append(_number(report["failure_epoch"]))
        identities = set()
        for value in report["completed"]:
            row = _completed(value)
            identity = row["reservation_id"]
            if identity in identities:
                raise ValueError("duplicate calibration reservation")
            identities.add(identity)
            completed.append({**row, "host_id": host})
        for value in report["running"]:
            _fields(value, _RUNNING_KEYS)
            row = {"host_id": host, "reservation_id": _identifier(value["reservation_id"]),
                   "first_epoch": _number(value["first_epoch"]), "last_epoch": _number(value["last_epoch"]),
                   "mean_mib": _integer(value["mean_mib"], 1), "seconds": _number(value["seconds"]),
                   "samples": _integer(value["samples"], 1)}
            if (row["reservation_id"] in identities or row["first_epoch"] > row["last_epoch"]
                    or row["seconds"] > row["last_epoch"] - row["first_epoch"] + 1e-6):
                raise ValueError("invalid or duplicate running calibration reservation")
            identities.add(row["reservation_id"])
            running.append(row)
    return completed, running, failures


def _order(row: dict) -> tuple:
    return row["finished_epoch"], row["host_id"], row["reservation_id"]


def _state(previous: Any) -> dict:
    required = {"schema_version", "key", "started_epoch", "updated_epoch", "effective_memory_mib",
                "last_lowered_epoch", "completed", "failure_epoch", "status", "reason", "sample_jobs"}
    _fields(previous, required, {"candidate_mib"})
    if type(previous["schema_version"]) is not int or previous["schema_version"] != 1:
        raise ValueError("unsupported calibration state schema")
    _fields(previous["key"], {"baseline_mib", "policy"})
    baseline = _integer(previous["key"]["baseline_mib"], 1)
    policy = _policy(previous["key"]["policy"], baseline)
    started, updated = _number(previous["started_epoch"]), _number(previous["updated_epoch"])
    failure = _number(previous["failure_epoch"])
    effective = _integer(previous["effective_memory_mib"], policy["minimum_mib"])
    lowered = previous["last_lowered_epoch"]
    if lowered is not None:
        lowered = _number(lowered)
    if (updated < started or effective > baseline
            or (lowered is not None and not started <= lowered <= updated)
            or (failure and not started <= failure <= updated)
            or not isinstance(previous["status"], str) or previous["status"] not in _STATUSES
            or not isinstance(previous["reason"], str) or previous["reason"] not in _REASONS
            or _integer(previous["sample_jobs"]) > MAX_COHORT):
        raise ValueError("invalid calibration state")
    if not isinstance(previous["completed"], list) or len(previous["completed"]) > MAX_COHORT:
        raise ValueError("invalid calibration completed state")
    completed = [_completed(row, saved=True) for row in previous["completed"]]
    if (completed != sorted(completed, key=_order)
            or len({(row["host_id"], row["reservation_id"]) for row in completed}) != len(completed)
            or len({row["job_id"] for row in completed}) != len(completed)
            or any(not max(started, failure) < row["finished_epoch"] <= updated for row in completed)):
        raise ValueError("invalid calibration completed state")
    result = {**previous, "key": {"baseline_mib": baseline, "policy": policy},
              "started_epoch": started, "updated_epoch": updated, "failure_epoch": failure,
              "last_lowered_epoch": lowered, "completed": completed}
    if "candidate_mib" in result:
        _integer(result["candidate_mib"], policy["minimum_mib"])
    return result


def _initial(baseline: int, policy: dict, now: float, reason: str) -> dict:
    enabled = bool(policy["jobs"] or policy["seconds"])
    return {"schema_version": 1, "key": {"baseline_mib": baseline, "policy": policy},
            "started_epoch": now, "updated_epoch": now, "effective_memory_mib": baseline,
            "last_lowered_epoch": None, "completed": [], "failure_epoch": 0.0,
            "status": "collecting" if enabled else "disabled",
            "reason": reason if enabled else "disabled", "sample_jobs": 0}


def _cohort(previous: list[dict], incoming: list[dict], cutoff: float, now: float) -> list[dict]:
    reservations = {}
    for row in previous + incoming:
        identity = row["host_id"], row["reservation_id"]
        if identity in reservations and reservations[identity] != row:
            raise ValueError("completed calibration reservation changed on replay")
        reservations[identity] = row
    jobs = {row["job_id"]: row for row in sorted(reservations.values(), key=_order)
            if cutoff < row["finished_epoch"] <= now}
    return sorted(jobs.values(), key=_order)[-MAX_COHORT:]


def _candidate(rows: list[dict], minimum: int) -> int:
    means = sorted(row["mean_mib"] for row in rows)
    p90 = means[(9 * len(means) + 9) // 10 - 1]
    return max(minimum, (p90 * 115 + 99) // 100)


def calibrate(previous: dict | None, *, baseline_mib: int, policy: dict,
              reports: list[dict], now: float) -> dict:
    """Return durable calibration state without mutating inputs or live controls."""
    baseline = _integer(baseline_mib, 1)
    policy = _policy(policy, baseline)
    now = _number(now)
    completed, running, failures = _reports(reports)
    state = _state(previous) if previous is not None else None
    if state is not None and now < state["updated_epoch"]:
        return _initial(baseline, policy, now, "clock_reversal")
    if state is None or state["key"] != {"baseline_mib": baseline, "policy": policy}:
        return _initial(baseline, policy, now, "new_generation")
    state["updated_epoch"] = now
    state.pop("candidate_mib", None)
    if not policy["jobs"] and not policy["seconds"]:
        return _initial(baseline, policy, state["started_epoch"], "disabled") | {"updated_epoch": now}

    latest_failure = max([state["failure_epoch"]] + [epoch for epoch in failures if epoch <= now])
    failed = latest_failure >= state["started_epoch"] and latest_failure > state["failure_epoch"]
    if failed:
        state.update(completed=[], failure_epoch=latest_failure,
                     effective_memory_mib=baseline, last_lowered_epoch=None)
    cutoff = max(state["started_epoch"], state["failure_epoch"])
    state["completed"] = _cohort(state["completed"], completed, cutoff, now)
    state["sample_jobs"] = len(state["completed"])
    if failed:
        state.update(status="collecting", reason="recent_failure")
        return state

    retained = {(row["host_id"], row["reservation_id"]) for row in state["completed"]}
    fresh_completed = any((row["host_id"], row["reservation_id"]) in retained for row in completed)
    fresh_running = sorted((row for row in running if (
        row["first_epoch"] >= cutoff
        and 0 <= now - row["last_epoch"] <= RUNNING_MAX_AGE_SECONDS
        and row["samples"] >= 3
        and math.isclose(row["seconds"], row["last_epoch"] - row["first_epoch"], abs_tol=1e-6)
    )), key=lambda row: (row["last_epoch"], row["host_id"], row["reservation_id"]))[-MAX_COHORT:]
    eligible_running = [row for row in fresh_running if row["seconds"] >= policy["seconds"]]
    if policy["jobs"] and len(state["completed"]) >= policy["jobs"] and fresh_completed:
        evidence, mode = state["completed"], "calibrated"
    elif policy["seconds"] and now - cutoff >= policy["seconds"] and eligible_running:
        evidence, mode = eligible_running, "provisional"
    else:
        reason = "missing_evidence" if not reports or not fresh_completed and not fresh_running else (
            "insufficient_jobs" if policy["jobs"] else "insufficient_running")
        state.update(status="collecting", reason=reason)
        return state

    candidate = _candidate(evidence, policy["minimum_mib"])
    current = state["effective_memory_mib"]
    state.update(status=mode, sample_jobs=len(evidence), candidate_mib=candidate)
    if candidate > current:
        state["effective_memory_mib"] = min(candidate, baseline)
        state["reason"] = "raised" if current < baseline else "baseline_cap"
    elif candidate == current:
        state["reason"] = "stable"
    elif candidate * 5 > current * 4:
        state["reason"] = "material_change"
    elif state["last_lowered_epoch"] is not None and now - state["last_lowered_epoch"] < LOWER_INTERVAL_SECONDS:
        state["reason"] = "cooldown"
    else:
        state["effective_memory_mib"] = max(candidate, (current * 4 + 4) // 5)
        state["last_lowered_epoch"] = now
        state["reason"] = "lowered"
    return state
