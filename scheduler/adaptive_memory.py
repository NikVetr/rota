#!/usr/bin/env python3
"""Track recent sustained process memory in host-local, durable state."""

from __future__ import annotations

import argparse
from bisect import bisect_right
import copy
import ctypes
import errno
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time

SMOOTH_SECONDS = 15
REMEMBER_SECONDS = 300
HEADROOM_PERCENT = 10
CALIBRATION_LIMIT = 50
CALIBRATION_QUEUES = 128
# host_rebooted: an audited reboot ended the job; it says nothing about the job's memory.
OUTCOMES = {"succeeded", "job_failed", "infrastructure_failed", "empty", "host_rebooted"}
IDENTIFIER = re.compile(r"[A-Za-z0-9._-]+")
JOB_ID = re.compile(r"[A-Za-z0-9._:-]+")


class _DarwinRusageInfoV0(ctypes.Structure):
    # SDK sys/resource.h: RUSAGE_INFO_V0, available since macOS 10.9.
    _fields_ = [("ri_uuid", ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64) for name in (
            "ri_user_time", "ri_system_time", "ri_pkg_idle_wkups",
            "ri_interrupt_wkups", "ri_pageins", "ri_wired_size",
            "ri_resident_size", "ri_phys_footprint", "ri_proc_start_abstime",
            "ri_proc_exit_abstime",
        )
    ]


def _darwin_footprint_reader():
    library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    try:
        read_usage = library.proc_pid_rusage
    except AttributeError as error:
        raise OSError("Darwin process footprint API is unavailable") from error
    read_usage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_void_p]
    read_usage.restype = ctypes.c_int

    def footprint(pid: int) -> int:
        usage = _DarwinRusageInfoV0()
        ctypes.set_errno(0)
        if read_usage(pid, 0, ctypes.byref(usage)) != 0:
            code = ctypes.get_errno() or errno.EIO
            raise OSError(code, f"cannot read memory footprint for PID {pid}: {os.strerror(code)}")
        return usage.ri_phys_footprint

    return footprint


def _process_uids() -> dict[int, int]:
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,uid="], capture_output=True, text=True, timeout=5,
        )
    except subprocess.TimeoutExpired as error:
        raise OSError("process UID snapshot timed out") from error
    if result.returncode:
        raise OSError("cannot read process UID snapshot")
    owners = {}
    for line in result.stdout.splitlines():
        if not re.fullmatch(r"\s*[1-9][0-9]*\s+-?[0-9]+\s*", line):
            raise ValueError("invalid process UID snapshot")
        pid, uid = map(int, line.split())
        if pid in owners:
            raise ValueError("duplicate PID in process UID snapshot")
        owners[pid] = uid
    if not owners:
        raise ValueError("empty process UID snapshot")
    return owners


def _process_is_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def augment_process_snapshot(
    snapshot: str, owners: dict[int, int], uid: int, footprint,
    gone=_process_is_gone,
) -> str:
    """Keep ps formatting, replacing same-UID RSS with the private footprint.

    The sampler combines both: compute trees are charged the larger value
    (compressed or swapped private memory counts), and other processes the
    smaller one (shared pages counted by every RSS do not, nor does cold
    compressed memory). System and other-user processes keep RSS because
    libproc denies their rusage to an ordinary worker account. OS headroom
    remains a separate host budget.
    """
    output = []
    seen = set()
    for line in snapshot.splitlines(keepends=True):
        match = re.fullmatch(
            r"[ \t]*([1-9][0-9]*)[ \t]+[0-9]+[ \t]+[0-9]+(?:\.[0-9]+)?"
            r"[ \t]+([0-9]+)[ \t]+\S[^\n]*(?:\n)?", line,
        )
        if match is None or int(match[1]) in seen:
            raise ValueError("invalid process resource snapshot")
        pid, rss = int(match[1]), int(match[2])
        seen.add(pid)
        if footprint is None:
            output.append(line)
            continue
        measured = rss
        if pid not in owners:
            if not gone(pid):
                raise ValueError(f"PID {pid} missing from process UID snapshot")
        elif owners[pid] == uid:
            try:
                value = footprint(pid)
            except OSError as error:
                if error.errno != errno.ESRCH or not gone(pid):
                    raise
            else:
                if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                    raise ValueError(f"invalid memory footprint for PID {pid}")
                measured = (value + 1023) // 1024
        # A proven exit keeps the original charge for this sample; never zero it.
        output.append(line if measured == rss else
                      line[:match.start(2)] + str(measured) + line[match.end(2):])
    if not seen:
        raise ValueError("empty process resource snapshot")
    return "".join(output)


def _augment_snapshot_main() -> int:
    parser = argparse.ArgumentParser(description="Augment Darwin ps RSS with current private footprint")
    parser.add_argument("--augment-ps-snapshot", type=Path, required=True)
    args = parser.parse_args()
    try:
        snapshot = args.augment_ps_snapshot.read_text(encoding="utf-8")
        if sys.platform == "darwin":
            snapshot = augment_process_snapshot(
                snapshot, _process_uids(), os.getuid(), _darwin_footprint_reader(),
            )
        elif sys.platform.startswith("linux"):
            # Linux retains RSS; validate the shared snapshot format without libproc.
            snapshot = augment_process_snapshot(snapshot, {}, os.getuid(), None)
        else:
            raise OSError(f"unsupported process memory platform: {sys.platform}")
        _atomic_text(args.augment_ps_snapshot, snapshot)
    except (OSError, ValueError) as error:
        print(f"adaptive memory: {error}", file=sys.stderr)
        return 1
    return 0


def sustained_peak(samples: list[list[float]], now: float) -> int:
    """Highest complete 15-second time-weighted mean in the last five minutes.

    Memory is held constant between observations. A moving mean can have extrema
    at either an observation or an observation shifted by the smoothing width.
    """
    times = [row[0] for row in samples]
    areas = [0.0]
    for index in range(1, len(samples)):
        areas.append(areas[-1] + (times[index] - times[index - 1]) * samples[index - 1][1])

    def integral(epoch: float) -> float:
        index = bisect_right(times, epoch) - 1
        return areas[index] + (epoch - times[index]) * samples[index][1]

    earliest = max(now - REMEMBER_SECONDS, times[0] + SMOOTH_SECONDS)
    if earliest > now:
        return 0
    ends = {earliest, now}
    ends.update(epoch for epoch in times if earliest <= epoch <= now)
    ends.update(epoch + SMOOTH_SECONDS for epoch in times
                if earliest <= epoch + SMOOTH_SECONDS <= now)
    return math.ceil(max(
        (integral(end) - integral(end - SMOOTH_SECONDS)) / SMOOTH_SECONDS
        for end in ends
    ))


def _number(value: object) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def _identifier(value: object, job: bool = False) -> bool:
    return (isinstance(value, str) and value not in {".", ".."}
            and (JOB_ID if job else IDENTIFIER).fullmatch(value) is not None)


def _statistics(value: object) -> None:
    if (not isinstance(value, dict)
            or set(value) != {"first_epoch", "last_epoch", "seconds", "area", "samples",
                              "last_mib", "continuous"}
            or any(not _number(value[key]) for key in
                   ("first_epoch", "last_epoch", "seconds", "area"))
            or type(value["samples"]) is not int or value["samples"] < 1
            or type(value["last_mib"]) is not int or value["last_mib"] < 0
            or type(value["continuous"]) is not bool
            or value["first_epoch"] > value["last_epoch"]
            or value["seconds"] > value["last_epoch"] - value["first_epoch"] + 1e-6
            or (value["seconds"] == 0 and value["area"] != 0)
            or (value["samples"] == 1 and value["seconds"] != 0)
            or (value["continuous"] and not math.isclose(
                value["seconds"], value["last_epoch"] - value["first_epoch"], abs_tol=1e-6))):
        raise ValueError("invalid cumulative memory statistics")


def _validate_history(history: dict, pool_id: str, host_id: str) -> None:
    if (not isinstance(history, dict) or type(history.get("schema_version")) is not int
            or history["schema_version"] != 1 or history.get("pool_id") != pool_id
            or history.get("host_id") != host_id
            or not _identifier(pool_id) or not _identifier(host_id)
            or not isinstance(history.get("reservations"), dict)):
        raise ValueError("invalid memory history identity or schema")
    for reservation_id, record in history["reservations"].items():
        if (not _identifier(reservation_id) or not isinstance(record, dict)
                or not isinstance(record.get("identity"), list)
                or len(record["identity"]) != 4
                or any(not isinstance(value, str) or not value for value in record["identity"])
                or not _identifier(record["identity"][0])
                or any(not value.isascii() or not value.isdigit()
                       for value in record["identity"][1:])
                or any(int(value) <= 0 for value in record["identity"][2:])
                or not _number(record.get("fresh_since"))
                or not isinstance(record.get("held_memory_mib"), int)
                or isinstance(record["held_memory_mib"], bool)
                or record["held_memory_mib"] < 0
                or not isinstance(record.get("samples"), list) or not record["samples"]):
            raise ValueError("invalid memory history record")
        last = -1.0
        for sample in record["samples"]:
            if (not isinstance(sample, list) or len(sample) != 2
                    or not _number(sample[0]) or sample[0] <= last
                    or not isinstance(sample[1], int) or isinstance(sample[1], bool)
                    or sample[1] < 0):
                raise ValueError("invalid memory history samples")
            last = sample[0]
        if record["fresh_since"] > last:
            raise ValueError("invalid memory history freshness")
        if "statistics" in record:
            _statistics(record["statistics"])
            stats = record["statistics"]
            if stats["continuous"] and (stats["last_epoch"] != record["samples"][-1][0]
                                        or stats["last_mib"] != record["samples"][-1][1]):
                raise ValueError("memory statistics do not match the last sample")
    calibration = history.get("calibration", {})
    if not isinstance(calibration, dict):
        raise ValueError("invalid memory calibration cohorts")
    for queue_id, cohort in calibration.items():
        if (not _identifier(queue_id) or not isinstance(cohort, dict)
                or set(cohort) != {"completed", "failure_epoch", "receipts"}
                or not _number(cohort["failure_epoch"])):
            raise ValueError("invalid memory calibration cohort")
        for key in ("completed", "receipts"):
            if not isinstance(cohort[key], list) or len(cohort[key]) > CALIBRATION_LIMIT:
                raise ValueError("invalid memory calibration cohort bounds")
            seen = set()
            for value in cohort[key]:
                if (not isinstance(value, dict) or not _identifier(value.get("reservation_id"))
                        or value["reservation_id"] in seen
                        or not _number(value.get("finished_epoch"))):
                    raise ValueError("invalid memory completion identity")
                seen.add(value["reservation_id"])
                if key == "completed":
                    if (set(value) != {"reservation_id", "job_id", "finished_epoch",
                                       "mean_mib", "seconds"}
                            or not _identifier(value["job_id"], job=True)
                            or type(value["mean_mib"]) is not int or value["mean_mib"] < 1
                            or not _number(value["seconds"]) or value["seconds"] < 15
                            or value["finished_epoch"] <= cohort["failure_epoch"]):
                        raise ValueError("invalid completed memory summary")
                elif (set(value) != {"reservation_id", "job_id", "finished_epoch",
                                     "outcome", "identity", "started_epoch"}
                      or (value["job_id"] != "" and not _identifier(value["job_id"], job=True))
                      or value["outcome"] not in OUTCOMES
                      or not _number(value["started_epoch"])
                      or not isinstance(value["identity"], list) or len(value["identity"]) != 3
                      or value["identity"][0] != queue_id
                      or any(not isinstance(v, str) or not v.isascii() or not v.isdigit()
                             for v in value["identity"][1:])
                      or int(value["identity"][2]) < 1):
                    raise ValueError("invalid memory completion receipt")


def _sample_statistics(previous: dict | None, now: float, current: int, max_gap: float) -> dict:
    if previous is None:
        return {"first_epoch": now, "last_epoch": now, "seconds": 0.0, "area": 0.0,
                "samples": 1, "last_mib": current, "continuous": True}
    value = previous.copy()
    delta = now - value["last_epoch"]
    if delta < 0 or delta > max_gap:
        value["continuous"] = False
    if delta > 0:
        if delta <= max_gap:
            value["seconds"] += delta
            value["area"] += delta * value["last_mib"]
        value["samples"] += 1
        value["last_epoch"] = now
        value["last_mib"] = current
    elif delta == 0:
        value["last_mib"] = max(current, value["last_mib"])
    return value


def update_usage(
    rows: list[list[str]], history: dict, pool_id: str, host_id: str,
    now: float, max_gap: float,
) -> tuple[list[list[str]], dict]:
    _validate_history(history, pool_id, host_id)
    if not _number(now) or not _number(max_gap) or max_gap <= 0:
        raise ValueError("invalid memory sample time or gap")
    previous = history["reservations"]

    records = {}
    output = []
    seen = set()
    for row in rows:
        if (len(row) != 10 or not _identifier(row[0]) or not _identifier(row[1]) or row[0] in seen
                or row[2] not in {"starting", "running", "uncertain"}
                or any(not value.isascii() or not value.isdigit() for value in row[3:])
                or int(row[4]) <= 0 or int(row[5]) <= 0
                or int(row[7]) != max(int(row[5]), int(row[6]))):
            raise ValueError("invalid reservation memory measurements")
        seen.add(row[0])
        identity = [row[1], row[3], row[4], row[5]]
        record = previous.get(row[0])
        # The lease may correct the declaration without changing the worker.
        if record is not None and record["identity"][:3] != identity[:3]:
            raise ValueError("memory history reservation identity changed")
        if record is not None:
            record = {**record, "identity": identity}
        # Uncertain reservations must never age out on absent process readings.
        if row[2] == "uncertain" or row[3] == "0":
            if record is not None:
                if "statistics" in record:
                    record["statistics"] = {**record["statistics"], "continuous": False}
                records[row[0]] = record
            output.append(row)
            continue
        current = int(row[8])
        statistics = _sample_statistics(
            record.get("statistics") if record is not None else None, now, current, max_gap,
        )
        if record is None or not 0 <= now - record["samples"][-1][0] <= max_gap:
            # Lost history, a telemetry gap, or a clock reversal cannot release
            # the previous charge without a full fresh window of evidence.
            record = {"identity": identity, "samples": [[now, current]],
                      "fresh_since": now, "held_memory_mib": int(row[6])}
        else:
            record = {**record, "samples": [sample[:] for sample in record["samples"]]}
            if now == record["samples"][-1][0]:
                # Repeated readings at one instant contribute no extra duration.
                record["samples"][-1][1] = max(current, record["samples"][-1][1])
            else:
                record["samples"].append([now, current])
        record["statistics"] = statistics
        samples = record["samples"]
        cutoff = now - REMEMBER_SECONDS - SMOOTH_SECONDS
        first = max(0, bisect_right([sample[0] for sample in samples], cutoff) - 1)
        record["samples"] = samples[first:]
        sustained = sustained_peak(record["samples"], now)
        if now - record["fresh_since"] >= REMEMBER_SECONDS + SMOOTH_SECONDS:
            record["held_memory_mib"] = 0
        observed = max(current, (sustained * (100 + HEADROOM_PERCENT) + 99) // 100,
                       record["held_memory_mib"])
        row = row[:]
        row[6:10] = [str(observed), str(max(int(row[5]), observed)),
                     str(current), str(sustained)]
        records[row[0]] = record
        output.append(row)
    updated = {**history, "reservations": records}
    if "calibration" in updated:
        _bounded_calibration(updated)
    return output, updated


def _bounded_calibration(history: dict) -> None:
    cohorts = history.get("calibration", {})
    active = {record["identity"][0] for record in history["reservations"].values()}
    newest = sorted(cohorts, key=lambda queue: max(
        [cohorts[queue]["failure_epoch"]]
        + [row["finished_epoch"] for row in cohorts[queue]["receipts"]]
        + [row["finished_epoch"] for row in cohorts[queue]["completed"]],
    ), reverse=True)[:CALIBRATION_QUEUES]
    history["calibration"] = {queue: cohort for queue, cohort in cohorts.items()
                              if queue in active or queue in newest}


def _read_env(path: Path) -> dict[str, str]:
    values = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            raise ValueError(f"invalid memory completion metadata: {path}")
        key, value = line.split("=", 1)
        if not key or key in values:
            raise ValueError(f"duplicate or empty memory completion key: {path}")
        values[key] = value
    return values


def complete_reservation(
    history: dict, reservation: dict[str, str], result: dict[str, str], *,
    pool_id: str, host_id: str, finished_epoch: float, max_gap: float,
) -> dict:
    _validate_history(history, pool_id, host_id)
    if not _number(finished_epoch) or not _number(max_gap) or max_gap <= 0:
        raise ValueError("invalid memory completion time or gap")
    reservation_id, queue_id = reservation.get("reservation_id"), reservation.get("queue_id")
    for key, minimum in (("generation", 1), ("cpu_units", 1), ("memory_mib", 1),
                         ("shared_io_units", 0), ("started_epoch", 0)):
        value = reservation.get(key, "")
        if not value.isascii() or not value.isdigit() or int(value) < minimum:
            raise ValueError(f"invalid memory completion {key}")
    pid = reservation.get("pid", "") or "0"
    if (reservation.get("schema_version") != "1" or reservation.get("pool_id") != pool_id
            or reservation.get("host_id") != host_id
            or not _identifier(reservation_id) or not _identifier(queue_id)
            or not pid.isascii() or not pid.isdigit()
            or reservation.get("state") not in {"starting", "running", "uncertain"}
            or result.get("reservation_id") != reservation_id):
        raise ValueError("memory completion identity mismatch")
    outcome = result.get("outcome")
    if outcome not in OUTCOMES:
        raise ValueError("invalid memory completion outcome")
    job_id = result.get("claimed_job_id", "")
    if job_id and not _identifier(job_id, job=True):
        raise ValueError("invalid memory completion job ID")
    exit_code = result.get("job_exit_code", "")
    if exit_code and (not exit_code.isascii() or not exit_code.isdigit()
                      or (outcome == "succeeded" and int(exit_code) != 0)):
        raise ValueError("invalid memory completion exit code")
    identity = [queue_id, pid, reservation["cpu_units"]]
    record = history["reservations"].get(reservation_id)
    if record is not None and record["identity"][:3] != identity:
        raise ValueError("memory completion reservation identity changed")
    receipt = {"reservation_id": reservation_id, "job_id": job_id,
               "finished_epoch": finished_epoch, "outcome": outcome,
               "identity": identity, "started_epoch": int(reservation["started_epoch"])}
    for old_queue, cohort in history.get("calibration", {}).items():
        for old in cohort["receipts"]:
            if old["reservation_id"] == reservation_id:
                if old_queue != queue_id or old != receipt:
                    raise ValueError("memory completion receipt changed")
                return history
        for old in cohort["completed"]:
            if old["reservation_id"] == reservation_id:
                if (old_queue != queue_id or old["job_id"] != job_id
                        or old["finished_epoch"] != finished_epoch or outcome != "succeeded"):
                    raise ValueError("completed memory summary changed")
                return history
    updated = copy.deepcopy(history)
    cohort = updated.setdefault("calibration", {}).setdefault(
        queue_id, {"completed": [], "failure_epoch": 0.0, "receipts": []},
    )
    if outcome in {"job_failed", "infrastructure_failed"}:
        if finished_epoch > cohort["failure_epoch"]:
            cohort["failure_epoch"] = finished_epoch
            cohort["completed"] = []
    elif (outcome == "succeeded" and job_id and record is not None
          and reservation["state"] != "uncertain"):
        stats = record.get("statistics")
        lifetime = finished_epoch - int(reservation["started_epoch"])
        if (stats is not None and stats["continuous"] and stats["samples"] >= 3
                and stats["seconds"] >= 15 and lifetime > 0
                and stats["seconds"] >= 0.8 * lifetime
                and stats["first_epoch"] >= int(reservation["started_epoch"])
                and 0 <= finished_epoch - stats["last_epoch"] <= max_gap
                and stats["area"] > 0 and finished_epoch > cohort["failure_epoch"]):
            cohort["completed"].append({
                "reservation_id": reservation_id, "job_id": job_id,
                "finished_epoch": finished_epoch,
                "mean_mib": math.ceil(stats["area"] / stats["seconds"]),
                "seconds": stats["seconds"],
            })
            cohort["completed"] = sorted(
                cohort["completed"], key=lambda row: (row["finished_epoch"], row["reservation_id"]),
            )[-CALIBRATION_LIMIT:]
    cohort["receipts"] = sorted(
        [*cohort["receipts"], receipt], key=lambda row: (row["finished_epoch"], row["reservation_id"]),
    )[-CALIBRATION_LIMIT:]
    updated["reservations"].pop(reservation_id, None)
    _bounded_calibration(updated)
    _validate_history(updated, pool_id, host_id)
    return updated


def calibration_report(
    history: dict, pool_id: str, host_id: str, queues: set[str] | None = None,
) -> dict[str, dict]:
    _validate_history(history, pool_id, host_id)
    output = {queue: {"completed": cohort["completed"], "running": [],
                      "failure_epoch": cohort["failure_epoch"]}
              for queue, cohort in history.get("calibration", {}).items()
              if queues is None or queue in queues}
    for reservation_id, record in history["reservations"].items():
        queue = record["identity"][0]
        if queues is not None and queue not in queues:
            continue
        stats = record.get("statistics")
        if (stats is None or not stats["continuous"] or stats["samples"] < 3
                or stats["seconds"] < 15 or stats["area"] <= 0):
            continue
        cohort = output.setdefault(queue, {"completed": [], "running": [], "failure_epoch": 0.0})
        if stats["first_epoch"] < cohort["failure_epoch"]:
            continue
        cohort["running"].append({
            "reservation_id": reservation_id, "first_epoch": stats["first_epoch"],
            "last_epoch": stats["last_epoch"],
            "mean_mib": math.ceil(stats["area"] / stats["seconds"]),
            "seconds": stats["seconds"], "samples": stats["samples"],
        })
    return output


def _atomic_text(path: Path, text: str) -> None:
    temporary = path.with_name(f"{path.name}.tmp.{os.getpid()}")
    try:
        temporary.write_text(text, encoding="utf-8")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    if any(argument.split("=", 1)[0] == "--augment-ps-snapshot" for argument in sys.argv[1:]):
        return _augment_snapshot_main()
    parser = argparse.ArgumentParser(description=__doc__)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--usage-file", type=Path)
    operation.add_argument("--complete-reservation", type=Path)
    operation.add_argument("--calibration-report", action="store_true")
    parser.add_argument("--result-file", type=Path)
    parser.add_argument("--calibration-queues-file", type=Path)
    parser.add_argument("--history-file", type=Path, required=True)
    parser.add_argument("--pool-id", required=True)
    parser.add_argument("--host-id", required=True)
    parser.add_argument("--max-gap-seconds", type=float)
    args = parser.parse_args()
    if args.complete_reservation and args.result_file is None:
        parser.error("--complete-reservation requires --result-file")
    if not args.calibration_report and args.max_gap_seconds is None:
        parser.error("sampling/completion requires --max-gap-seconds")
    try:
        if args.max_gap_seconds is not None and (
                not math.isfinite(args.max_gap_seconds) or args.max_gap_seconds <= 0):
            raise ValueError("memory sample gap must be positive")
        history = {"schema_version": 1, "pool_id": args.pool_id,
                   "host_id": args.host_id, "reservations": {}}
        if args.history_file.exists():
            history = json.loads(args.history_file.read_text(encoding="utf-8"))
        if not isinstance(history, dict):
            raise ValueError("memory history must be an object")
        if args.calibration_report:
            queues = None
            if args.calibration_queues_file is not None:
                lines = args.calibration_queues_file.read_text(encoding="utf-8").splitlines()
                if not lines or lines[0].split("\t")[0] != "queue_id":
                    raise ValueError("invalid calibration queue filter")
                queues = set()
                for line in lines[1:]:
                    queue = line.split("\t")[0]
                    if not _identifier(queue) or queue in queues:
                        raise ValueError("invalid calibration queue filter identity")
                    queues.add(queue)
            for queue, report in sorted(calibration_report(
                    history, args.pool_id, args.host_id, queues).items()):
                print(queue + "\t" + json.dumps(report, separators=(",", ":")))
            return 0
        if args.complete_reservation:
            history = complete_reservation(
                history, _read_env(args.complete_reservation), _read_env(args.result_file),
                pool_id=args.pool_id, host_id=args.host_id,
                finished_epoch=args.result_file.stat().st_mtime, max_gap=args.max_gap_seconds,
            )
            _atomic_text(args.history_file, json.dumps(history, separators=(",", ":")) + "\n")
            return 0
        rows = [line.split("\t") for line in args.usage_file.read_text(encoding="utf-8").splitlines()]
        rows, history = update_usage(rows, history, args.pool_id, args.host_id,
                                     time.time(), args.max_gap_seconds)
        _atomic_text(args.history_file, json.dumps(history, separators=(",", ":")) + "\n")
        _atomic_text(args.usage_file, "".join("\t".join(row) + "\n" for row in rows))
    except (OSError, ValueError) as error:
        print(f"adaptive memory: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
