#!/usr/bin/env python3
"""Machine-facing scheduler controls: `rota control describe` and `rota control apply`.

describe prints the adaptive pool's state and the controls available now, built only from the
collector's cluster-status.json (its `scheduler` section comes from scheduler_snapshot.py on
the coordinator). apply reads one JSON request on stdin, re-reads that state on the coordinator,
checks the request's expectations, runs the matching adaptive_scheduler.sh command over one SSH
connection, and appends an audit line there. Every outcome is JSON on stdout; exit 0 when ok,
1 when refused or failed, 2 for a malformed call. Agent Bar and scripts drive it; it never
prompts and never depends on either of them.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

from cluster_config import load_monitor_environment

if __name__ == "__main__":
    load_monitor_environment()

PROTOCOL = 1
STALE_SEC = 120  # the collector refreshes scheduler state every 30 s
LOCK_RETRY_SEC = 10
SSH_TIMEOUT_SEC = 30
ROOT = os.environ.get("ROTA_ROOT", "")
COORDINATOR = os.environ.get("ROTA_COORDINATOR")
REMOTE_DIR = "$HOME/rota-monitor"
SNAPSHOT = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state") / "rota/cluster-status.json"
SCHEDULER = f"{ROOT}/adaptive_scheduler.sh"
CALIBRATION_JOBS = (20, 50)
CALIBRATION_SECONDS = (600, 86400)
TEXT = re.compile(r"[^\r\n\t,:]{1,200}")


class Refused(Exception):
    def __init__(self, code, message, retryable=False):
        super().__init__(message)
        self.code, self.retryable = code, retryable


# -- descriptors ---------------------------------------------------------------
def control(name, label, kind, value=None, help_text=None, danger="none", confirm=None,
            enabled=True, disabled_reason=None, effect="live", **extra):
    return {"name": name, "label": label, "kind": kind, "value": value, "help": help_text,
            "effect": effect, "danger": danger, "confirm": confirm, "enabled": enabled,
            "disabled_reason": disabled_reason if not enabled else None, **extra}


def integer(name, label, value, minimum=None, maximum=None, unit=None, help_text=None, **extra):
    return control(name, label, "integer", value, help_text, min=minimum, max=maximum, step=1,
                   unit=unit, **extra)


def text(name, label, value=None, help_text=None, optional=False):
    return control(name, label, "text", value, help_text, optional=optional)


def fact(label, value):
    return {"label": label, "value": value}


def queue_controls(queue, hosts, pool_active, commands, available):
    settings = queue["control"]
    calibration = settings["calibration"]
    admission = settings["admission"]
    off = None if available else "the scheduler is unreachable or its state is stale"

    def ok(condition, reason):
        return (available and condition), (off or reason)

    can_pause, pause_reason = ok(admission == "enabled", f"admission is {admission}")
    can_resume, resume_reason = ok(admission == "paused" and pool_active,
                                   "only a paused queue in an active pool can resume")
    can_edit, edit_reason = ok(admission != "closed", "the queue is closed")
    interrupted = queue["share_interrupted_jobs"]
    can_confirm, confirm_reason = ok(interrupted > 0 and "confirm-interrupted-jobs" in commands,
                                     "no unconfirmed share interruptions")
    return [
        control("pause", "Pause new jobs", "action", help_text="Running jobs finish; no new ones start.",
                enabled=can_pause, disabled_reason=pause_reason),
        control("resume", "Resume", "action", enabled=can_resume, disabled_reason=resume_reason),
        control("priority", "Priority", "group", {
                    "default": settings["schedule_priority"], "hosts": settings["host_priorities"]},
                "Higher runs first when queues compete for slots. Host overrides replace the default "
                "there; off keeps the queue off that host.",
                enabled=can_edit, disabled_reason=edit_reason, fields=[
                    integer("default", "Default", settings["schedule_priority"], -1000, 1000),
                    control("hosts", "Per host", "host-map", settings["host_priorities"],
                            hosts=[host["id"] for host in hosts], min=-1000, max=1000)]),
        integer("max_workers", "Worker cap", settings["max_workers"], 0, None, "jobs",
                "0 = limited only by the pool.", enabled=can_edit, disabled_reason=edit_reason),
        control("memory", "Memory per job", "group", {
                    "memory_mib_per_job": settings["memory_mib_per_job"],
                    "calibration_jobs": calibration["jobs"],
                    "calibration_seconds": calibration["seconds"],
                    "calibration_min_mib": calibration["minimum_mib"]},
                "The memory floor charged per job. Calibration learns it from finished jobs "
                "(0, or 20–50 jobs) or running ones (0, or 600–86400 s), never below the minimum. "
                "Changing calibration settings restarts its learning; a floor change alone is "
                "refused while calibration is on.",
                enabled=can_edit, disabled_reason=edit_reason, effect="next-job", fields=[
                    integer("memory_mib_per_job", "Floor", settings["memory_mib_per_job"], 1, None, "MiB"),
                    integer("calibration_jobs", "Calibrate after jobs", calibration["jobs"], 0, 50),
                    integer("calibration_seconds", "Calibrate after seconds", calibration["seconds"], 0, 86400, "s"),
                    integer("calibration_min_mib", "Calibration minimum", calibration["minimum_mib"], 1, None,
                            "MiB", optional=True)]),
        control("confirm_interrupted", "Confirm interrupted jobs", "group", {"reason": None},
                f"{interrupted} job(s) ran across a share interruption. Verify their outputs first; "
                "confirming releases dependent stages.",
                danger="irreversible", confirm="Have you verified these jobs' outputs?",
                enabled=can_confirm, disabled_reason=confirm_reason,
                fields=[text("reason", "What was checked")]),
    ]


def host_controls(host, pool_active, commands, available):
    off = None if available else "the scheduler is unreachable or its state is stale"
    window = host["away_window"] or {}
    return [
        control("budget", "Budget", "group",
                {"cpu_units": host["cpu_units"], "memory_mib": host["memory_mib"]},
                f"Reviewed table: {host['base_cpu_units']} CPU, {host['base_memory_mib']} MiB.",
                enabled=available and host["enabled"] and pool_active,
                disabled_reason=off or "only an enabled host in an active pool", fields=[
                    integer("cpu_units", "CPU units", host["cpu_units"], 1, None),
                    integer("memory_mib", "Memory", host["memory_mib"], 1, None, "MiB")]),
        control("availability", "Availability", "group",
                {"state": "away" if host["away"] else "present", "at": None,
                 "until": window.get("away_until"), "reason": window.get("reason")},
                "Away: running jobs finish and nothing new starts. Times are epoch seconds or "
                "ISO local times; with state present, at schedules the return.",
                enabled=available and host["enabled"] and "set-host-state" in commands,
                disabled_reason=off or ("only an enabled host" if not host["enabled"]
                                        else "arrives with the next fresh pool"), fields=[
                    control("state", "State", "choice", "away" if host["away"] else "present",
                            choices=[{"value": "present", "label": "Present"},
                                     {"value": "away", "label": "Away"}]),
                    text("at", "From", optional=True), text("until", "Until", optional=True),
                    text("reason", "Reason", window.get("reason"), optional=True)]),
    ]


def describe(snapshot, now=None):
    now = time.time() if now is None else now
    scheduler = snapshot.get("scheduler") or {}
    pool = scheduler.get("pool") or {}
    error = scheduler.get("error")
    stale = not scheduler.get("updated") or now - scheduler["updated"] > STALE_SEC
    if stale and not error:
        error = "scheduler state is older than 2 minutes; is the collector running?"
    reachable = bool(scheduler.get("ok")) and not stale and pool.get("coordinator_live", False)
    if scheduler.get("ok") and not stale and not pool.get("coordinator_live") and pool.get("state") != "INACTIVE":
        error = "the coordinator is not live"
    available = reachable
    commands = set(scheduler.get("commands", []))
    pool_active = pool.get("state") == "ACTIVE"
    labels = {queue["id"]: f"{queue['label']} [b{queue['batch']:05d}]" if queue.get("batch") else queue["label"]
              for queue in snapshot.get("queues", [])}
    shared_pool = (snapshot.get("shared") or {}).get("pool") or {}
    hosts = scheduler.get("hosts", [])
    queues = []
    for queue in scheduler.get("queues", []):
        if "error" in queue:
            queues.append({"id": queue["queue_id"], "snapshot_id": None, "label": queue["queue_id"],
                           "state": "UNKNOWN", "facts": [fact("Problem", queue["error"])], "controls": []})
            continue
        settings = queue["control"]
        queues.append({
            "id": queue["queue_id"], "snapshot_id": queue["tag"],
            "label": labels.get(queue["tag"]) or queue["run_name"] or queue["queue_id"],
            "project": queue["project"], "source_dir": queue["source_dir"],
            "conversation": queue["submitted_by"], "state": queue["lifecycle"],
            "facts": [fact("Mode", "adaptive shared queue"),
                      fact("Admission", settings["admission"]),
                      fact("Hold", queue["blocking_reason"] or "none"),
                      fact("Workers", f"{queue['running_workers']} running, {queue['target_workers']} planned"),
                      fact("Memory charged", f"{settings['effective_memory_mib_per_job']} MiB per job"),
                      fact("Calibration", settings["calibration"]["state"]),
                      fact("Interrupted jobs", str(queue["share_interrupted_jobs"])),
                      fact("Failed jobs", str(queue["failed_jobs_total"]) if "failed_jobs_total" in queue
                           else "unknown"),
                      fact("Host holds", ", ".join(f"{hold['host_id']}: {hold['reason']}"
                                                   for hold in queue.get("holds", [])) or "none")],
            # Claimed jobs that ended without a done marker and were not rerun (newest 200), and
            # hosts refusing this queue; null when the coordinator's failure scan was unavailable.
            "failed_jobs": queue.get("failed_jobs"), "failed_jobs_total": queue.get("failed_jobs_total"),
            "holds": queue.get("holds"),
            "controls": queue_controls(queue, hosts, pool_active, commands, available),
        })
    unavailable = pool.get("unavailable_hosts", {})
    return {
        "protocol": PROTOCOL,
        "scheduler": {"name": "rota", "version": scheduler.get("schema")},
        "as_of": scheduler.get("updated"),
        "coordinator": {"reachable": reachable, "label": (snapshot.get("shared") or {}).get("host"),
                        "error": None if reachable else error or "no scheduler state yet"},
        "pool": {
            "id": pool.get("id"), "state": pool.get("state"),
            "facts": [fact("Name", pool.get("name")), fact("Running jobs", str(pool.get("running_workers", 0))),
                      fact("Live capacity", f"{shared_pool.get('capacity')} CPU"),
                      fact("Shared I/O", shared_pool.get("io_state"))],
            "controls": [control("drain", "Drain", "action",
                                 help_text="Stops all new work; running jobs finish. A fresh pool and "
                                           "reconciliation of every unfinished queue follow.",
                                 danger="irreversible",
                                 confirm="Drain the whole pool? New work stops until a fresh pool starts.",
                                 enabled=available and pool_active and "pool-drain" in commands,
                                 disabled_reason="the scheduler is unreachable or its state is stale"
                                 if not available else "only an active pool can drain")],
        },
        "hosts": [{
            "id": host["id"], "label": host["id"], "enabled": host["enabled"],
            "facts": [fact("Budget", f"{host['cpu_units']} CPU · {host['memory_mib'] / 1024:.0f} GiB"),
                      fact("Availability", "away" if host["away"] else
                           f"not taking work ({unavailable[host['id']]})" if host["id"] in unavailable else "present")],
            "controls": host_controls(host, pool_active, commands, available),
        } for host in hosts],
        "queues": queues,
        "operations": [],
    }


# -- apply ---------------------------------------------------------------------
def ssh(arguments, script):
    """Run a shell script on the coordinator; ssh's own failures exit 255."""
    target = COORDINATOR
    if not target or not ROOT:
        raise Refused("unreachable", "Source the inventory environment or configure the collector.")
    return subprocess.run([os.environ.get("ROTA_CONTROL_SSH", "ssh"), "-o", "BatchMode=yes",
                           "-o", "ConnectTimeout=10", *arguments, target, script],
                          stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=SSH_TIMEOUT_SEC)


def fresh_state():
    script = (f"exec python3 {REMOTE_DIR}/scheduler_snapshot.py --state-root "
              f"{shlex.quote(ROOT + '/.rota/adaptive')} --scheduler {shlex.quote(SCHEDULER)}")
    result = ssh(["-n"], script)
    if result.returncode == 255:
        raise Refused("unreachable", result.stderr.strip() or "the coordinator host cannot be reached", True)
    if result.returncode:
        raise Refused("failed", result.stderr.strip() or "scheduler snapshot failed")
    try:
        return json.loads(result.stdout)
    except ValueError as error:
        raise Refused("failed", f"unreadable scheduler snapshot: {error}") from error


def need_integer(value, name, low=None, high=None, allowed_zero=False):
    if type(value) is not int or (low is not None and value < low and not (allowed_zero and value == 0)) \
            or (high is not None and value > high):
        bounds = f"{'0 or ' if allowed_zero else ''}{low if low is not None else ''}–{high if high is not None else ''}"
        raise Refused("invalid_value", f"{name} must be an integer ({bounds.strip('–')})")
    return value


def need_text(value, name, optional=False):
    if value is None and optional:
        return None
    if not isinstance(value, str) or not TEXT.fullmatch(value):
        raise Refused("invalid_value", f"{name} must be one line of 1–200 characters without tabs, commas or colons")
    return value


def priority_spec(value, hosts):
    if not isinstance(value, dict) or set(value) - {"default", "hosts"}:
        raise Refused("invalid_value", "priority takes {default, hosts}")
    spec = [str(need_integer(value.get("default"), "default priority", -1000, 1000))]
    overrides = value.get("hosts") or {}
    if not isinstance(overrides, dict):
        raise Refused("invalid_value", "hosts must map host IDs to integers or off")
    for host, priority in sorted(overrides.items()):
        if host not in hosts:
            raise Refused("invalid_value", f"unknown host in priority overrides: {host}")
        spec.append(f"{host}={'off' if priority == 'off' else need_integer(priority, host, -1000, 1000)}")
    return ",".join(spec)


def current_value(name, queue=None, host=None):
    if queue is not None:
        settings = queue["control"]
        calibration = settings["calibration"]
        return {"priority": {"default": settings["schedule_priority"], "hosts": settings["host_priorities"]},
                "max_workers": settings["max_workers"],
                "memory": {"memory_mib_per_job": settings["memory_mib_per_job"],
                           "calibration_jobs": calibration["jobs"], "calibration_seconds": calibration["seconds"],
                           "calibration_min_mib": calibration["minimum_mib"]}}.get(name)
    if host is not None:
        return {"budget": {"cpu_units": host["cpu_units"], "memory_mib": host["memory_mib"]},
                "availability": {"state": "away" if host["away"] else "present"}}.get(name)
    return None


def command_for(request, state):
    """Validate a request against fresh state; return the scheduler arguments and an audit target."""
    target, name, value = request["target"], request["control"], request.get("value")
    pool = state["pool"]
    expected = (request.get("expect") or {}).get("pool_id")
    if expected is not None and expected != pool.get("id"):
        raise Refused("conflict", f"the pool is now {pool.get('id')}, not {expected}")
    pool_args = ["--expected-pool-id", pool["id"]] if pool.get("id") else []
    commands = set(state.get("commands", []))
    if target.get("kind") == "pool":
        if name != "drain":
            raise Refused("not_supported", f"no pool control {name!r}")
        if pool.get("state") != "ACTIVE":
            raise Refused("not_supported", f"the pool is {pool.get('state')}, not ACTIVE")
        return ["pool-drain", *pool_args], "pool"
    if target.get("kind") == "host":
        host = next((h for h in state["hosts"] if h["id"] == target.get("id")), None)
        if host is None:
            raise Refused("unknown_target", f"no host {target.get('id')!r} in the pool")
        check_expected_value(request, current_value(name, host=host))
        if name == "budget":
            if not isinstance(value, dict):
                raise Refused("invalid_value", "budget takes {cpu_units, memory_mib}")
            return ["set-host-budget", "--host_id", host["id"],
                    "--cpu_units", str(need_integer(value.get("cpu_units"), "cpu_units", 1)),
                    "--memory_mib", str(need_integer(value.get("memory_mib"), "memory_mib", 1)),
                    *pool_args], f"host {host['id']}"
        if name == "availability":
            if "set-host-state" not in commands:
                raise Refused("not_supported", "host availability arrives with the next fresh pool")
            if not isinstance(value, dict) or value.get("state") not in {"away", "present"}:
                raise Refused("invalid_value", "availability takes {state: away|present, at, until, reason}")
            arguments = ["set-host-state", "--host_id", host["id"], "--state", value["state"]]
            for key in ("at", "until"):
                if value.get(key) is not None:
                    arguments += [f"--{key}", str(value[key])]
            reason = need_text(value.get("reason"), "reason", optional=value["state"] == "present")
            if reason:
                arguments += ["--reason", reason]
            return [*arguments, *pool_args], f"host {host['id']}"
        raise Refused("not_supported", f"no host control {name!r}")
    if target.get("kind") != "queue":
        raise Refused("unknown_target", "target.kind must be pool, host or queue")
    queue = next((q for q in state["queues"] if q.get("queue_id") == target.get("id") and "control" in q), None)
    if queue is None:
        raise Refused("unknown_target", f"no registered queue {target.get('id')!r}")
    check_expected_value(request, current_value(name, queue=queue))
    batch = ["--batch_dir", queue["batch_dir"]]
    label = f"queue {queue['queue_id']}"
    admission = queue["control"]["admission"]
    if name in {"pause", "resume"}:
        return [name, *batch, *pool_args], label
    if admission == "closed" and name in {"priority", "max_workers", "memory"}:
        raise Refused("not_supported", "the queue is closed")
    if name == "priority":
        return ["set-priority", *batch, "--schedule_priority",
                priority_spec(value, {h["id"] for h in state["hosts"]}), *pool_args], label
    if name == "max_workers":
        return ["set-max-workers", *batch, "--max_workers", str(need_integer(value, "max_workers", 0)),
                *pool_args], label
    if name == "memory":
        return memory_arguments(queue, value, batch, pool_args), label
    if name == "confirm_interrupted":
        if "confirm-interrupted-jobs" not in commands:
            raise Refused("not_supported", "the deployed scheduler cannot confirm interrupted jobs")
        if not queue["share_interrupted_jobs"]:
            raise Refused("not_supported", "the queue has no unconfirmed share interruptions")
        reason = need_text((value or {}).get("reason") if isinstance(value, dict) else None, "reason")
        return ["confirm-interrupted-jobs", *batch, "--reason", reason], label
    raise Refused("not_supported", f"no queue control {name!r}")


def memory_arguments(queue, value, batch, pool_args):
    if not isinstance(value, dict) or set(value) - {"memory_mib_per_job", "calibration_jobs",
                                                    "calibration_seconds", "calibration_min_mib"}:
        raise Refused("invalid_value", "memory takes {memory_mib_per_job, calibration_jobs, "
                                       "calibration_seconds, calibration_min_mib}")
    current = queue["control"]["calibration"]
    floor = need_integer(value.get("memory_mib_per_job"), "memory_mib_per_job", 1)
    jobs = need_integer(value.get("calibration_jobs", current["jobs"]), "calibration_jobs",
                        *CALIBRATION_JOBS, allowed_zero=True)
    seconds = need_integer(value.get("calibration_seconds", current["seconds"]), "calibration_seconds",
                           *CALIBRATION_SECONDS, allowed_zero=True)
    minimum = value.get("calibration_min_mib", current["minimum_mib"])
    calibrating_now = bool(current["jobs"] or current["seconds"])
    policy_changed = (jobs, seconds, minimum) != (current["jobs"], current["seconds"], current["minimum_mib"])
    if calibrating_now and not policy_changed:
        # Passing the unchanged policy restarts learning and omitting it disables calibration.
        raise Refused("not_supported", "the floor cannot change alone while calibration is on: it would "
                                       "restart or disable calibration. Change the calibration settings too, "
                                       "or turn calibration off.")
    arguments = ["set-memory", *batch, "--memory_mib_per_job", str(floor)]
    if jobs or seconds:
        minimum = need_integer(minimum, "calibration_min_mib", 1, floor)
        arguments += ["--memory_calibration_jobs", str(jobs), "--memory_calibration_seconds", str(seconds),
                      "--memory_min_mib_per_job", str(minimum)]
    return [*arguments, *pool_args]


def check_expected_value(request, current):
    expect = request.get("expect") or {}
    if "value" in expect and current is not None and expect["value"] != current:
        raise Refused("conflict", f"the current value is {json.dumps(current)}, not {json.dumps(expect['value'])}")


def run_scheduler(arguments, request, audit_target):
    command = " ".join(shlex.quote(part) for part in [SCHEDULER, *arguments])
    audit = "\t".join([str(int(time.time())), request.get("source") or "unknown", request["request_id"],
                       audit_target, request["control"], json.dumps(request.get("value"), sort_keys=True)])
    # The audit line is written only after the command succeeds, beside the pool's other controls.
    script = (f"{command} && printf '%s\\n' {shlex.quote(audit)} >> "
              f"{shlex.quote(ROOT + '/.rota/adaptive/control-audit.tsv')}")
    deadline = time.monotonic() + LOCK_RETRY_SEC
    while True:
        result = ssh(["-n"], script)
        if result.returncode == 0:
            return result.stdout
        detail = result.stderr.strip()
        if result.returncode == 255:
            raise Refused("unreachable", detail or "the coordinator host cannot be reached", True)
        if "scheduler state is locked" in detail:
            if time.monotonic() < deadline:
                time.sleep(1)
                continue
            raise Refused("lock_held", "another scheduler change is in progress", True)
        if "identity changed" in detail or "different pool" in detail:
            raise Refused("conflict", detail)
        if re.search(r"requires|invalid|must |out of range|positive", detail):
            raise Refused("invalid_value", detail)
        if "cannot resume" in detail or "not ACTIVE" in detail:
            raise Refused("not_supported", detail)
        raise Refused("failed", detail or f"exit {result.returncode}")


def apply(request):
    for key in ("protocol", "request_id", "target", "control"):
        if key not in request:
            raise ValueError(f"request lacks {key}")
    if request["protocol"] != PROTOCOL or not isinstance(request["target"], dict):
        raise ValueError("unsupported protocol or target")
    state = fresh_state()
    arguments, audit_target = command_for(request, state)
    output = run_scheduler(arguments, request, audit_target)
    lines = [line for line in output.splitlines() if line.strip()]
    return {"protocol": PROTOCOL, "ok": True, "request_id": request["request_id"],
            "value": request.get("value"), "effect": "live",
            "message": f"{request['control']} applied to {audit_target}" + (f": {lines[-1]}" if lines else "")}


def main():
    parser = argparse.ArgumentParser(prog="rota control", description=__doc__)
    parser.add_argument("command", choices=["describe", "apply"])
    parser.add_argument("--snapshot", type=Path, default=SNAPSHOT)
    args = parser.parse_args()
    if args.command == "describe":
        try:
            snapshot = json.loads(args.snapshot.read_text())
        except (OSError, ValueError) as error:
            snapshot = {"scheduler": {"ok": False, "error": f"cannot read {args.snapshot}: {error}"}}
        print(json.dumps(describe(snapshot)))
        return 0
    request_id = None
    try:
        request = json.loads(sys.stdin.read())
        request_id = request.get("request_id") if isinstance(request, dict) else None
        result = apply(request)
    except (ValueError, KeyError, TypeError, AttributeError) as error:
        print(json.dumps({"protocol": PROTOCOL, "ok": False, "request_id": request_id,
                          "error": {"code": "invalid_value", "message": f"malformed request: {error}",
                                    "retryable": False}}))
        return 2
    except Refused as refusal:
        print(json.dumps({"protocol": PROTOCOL, "ok": False, "request_id": request_id,
                          "error": {"code": refusal.code, "message": str(refusal), "retryable": refusal.retryable,
                                    **({"retry_after": 2} if refusal.retryable else {})}}))
        return 1
    except (OSError, subprocess.SubprocessError) as error:
        print(json.dumps({"protocol": PROTOCOL, "ok": False, "request_id": request_id,
                          "error": {"code": "failed", "message": str(error), "retryable": False}}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
