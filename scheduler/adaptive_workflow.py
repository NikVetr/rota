#!/usr/bin/env python3
"""Immutable queue dependencies and bounded, audited tail recovery."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def read_env(path: Path) -> dict[str, str]:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"missing or unsafe workflow metadata: {path}")
    values = {}
    for line in path.read_text().splitlines():
        key, separator, value = line.partition("=")
        if not separator or not key or key in values:
            raise ValueError(f"invalid workflow metadata: {path}")
        values[key] = value
    return values


def identity(batch: Path) -> dict[str, str]:
    meta = read_env(batch / "run_meta.env")
    if meta.get("execution_mode") != "adaptive-shared-queue":
        raise ValueError(f"dependency is not an adaptive queue: {batch}")
    for key in ("pool_id", "run_token"):
        if meta.get(key) in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9._-]+", meta.get(key, "")):
            raise ValueError(f"invalid dependency identity: {batch}")
    return {"batch_dir": str(batch.resolve()), "pool_id": meta["pool_id"], "queue_id": meta["run_token"]}


def resolve_dependencies(state: Path, references: list[str]) -> list[dict[str, str]]:
    result = []
    for reference in references:
        if not reference or any(c in reference for c in "\r\n\t"):
            raise ValueError("dependency must be a batch path, queue ID, or unique run name")
        if "/" in reference:
            selected = identity(Path(reference))
        else:
            candidates = {}
            # The index survives registration retirement. Older active queues
            # are also discoverable; older completed queues require a batch path.
            paths = list((state / "queue-index").glob("*.env"))
            paths += list((state / "queues").glob("*.env"))
            for path in paths:
                record = read_env(path)
                if "run_name" in record and reference not in {record["queue_id"], record["run_name"]}:
                    continue
                batch = Path(record["batch_dir"])
                meta = read_env(batch / "run_meta.env")
                if reference not in {record["queue_id"], meta.get("run_name")}:
                    continue
                selected = identity(batch)
                if selected["queue_id"] != record["queue_id"] or selected["pool_id"] != record["pool_id"]:
                    raise ValueError(f"dependency index identity changed: {path}")
                candidates[selected["batch_dir"]] = selected
            if len(candidates) != 1:
                raise ValueError(f"dependency {reference!r} has {len(candidates)} matches; use its batch path")
            selected = next(iter(candidates.values()))
        if selected not in result:
            result.append(selected)
    return result


def policy(resources: dict[str, str]) -> tuple[list[dict[str, str]], bool]:
    if "early_eta_sample" in resources and (resources.get("schema_version") != "2"
                                             or resources["early_eta_sample"] not in {"0", "1"}):
        raise ValueError("invalid early ETA sampling policy")
    if resources.get("schema_version") == "1":
        if {"dependencies", "retry_lost_jobs"} & resources.keys():
            raise ValueError("workflow options require resource schema 2")
        return [], False
    if resources.get("schema_version") != "2" or resources.get("retry_lost_jobs") not in {"0", "1"}:
        raise ValueError("invalid workflow resource schema or retry policy")
    dependencies = json.loads(resources["dependencies"])
    if not isinstance(dependencies, list):
        raise ValueError("dependencies must be a list")
    for item in dependencies:
        if not isinstance(item, dict) or set(item) != {"batch_dir", "queue_id", "pool_id"}:
            raise ValueError("invalid dependency fields")
        if any(not isinstance(value, str) or not value or any(c in value for c in "\r\n\t")
               for value in item.values()) or not Path(item["batch_dir"]).is_absolute():
            raise ValueError("invalid dependency identity")
        if item["queue_id"] == resources["queue_id"]:
            raise ValueError("queue cannot depend on itself")
    return dependencies, resources["retry_lost_jobs"] == "1"


def unconfirmed_interruptions(batch: Path) -> list[str]:
    """Successful jobs that ran across a share interruption, not yet confirmed by an operator.

    The worker records each one under share-interrupted/; confirm-interrupted-jobs moves
    the records into share-interrupted/confirmed/. The directory, not status.env, is the
    source: a retired queue's status no longer changes when its jobs are confirmed.
    """
    try:
        with os.scandir(batch / "share-interrupted") as entries:
            return sorted(entry.name for entry in entries
                          if entry.name.startswith("job_") and entry.is_file(follow_symlinks=False))
    except FileNotFoundError:
        return []


def dependency_hold(dependencies: list[dict[str, str]]) -> str:
    waiting = unverified = False
    for dependency in dependencies:
        try:
            batch = Path(dependency["batch_dir"])
            if identity(batch) != dependency:
                return "dependency_unavailable"
            status = read_env(batch / "adaptive/status.env")
            if status.get("queue_id") != dependency["queue_id"]:
                return "dependency_unavailable"
            if status.get("lifecycle") == "FINISHED_INCOMPLETE":
                return "dependency_failed"
            waiting |= not (
                status.get("lifecycle") == "COMPLETED"
                and status.get("propagation") == "complete"
                and status.get("running_workers") == "0"
                and status.get("target_workers") == "0"
            )
            unverified |= bool(unconfirmed_interruptions(batch))
        except (OSError, ValueError, KeyError):
            return "dependency_unavailable"
    if waiting:
        return "dependency_wait"
    return "dependency_unverified" if unverified else ""


def retry_dependencies(dependencies: list[dict[str, str]],
                       replacements: list[list[str]]) -> list[dict[str, str]]:
    """Rebind only explicitly selected, retired predecessors after maintenance."""
    mapping = {}
    for old, new in replacements:
        previous = identity(Path(old))
        if previous not in dependencies or previous["batch_dir"] in mapping:
            raise ValueError(f"replacement must name one original dependency exactly once: {old}")
        status = read_env(Path(old) / "adaptive/status.env")
        if not (status.get("queue_id") == previous["queue_id"]
                and status.get("lifecycle") == "FINISHED_INCOMPLETE"
                and status.get("propagation") == "complete"
                and status.get("running_workers") == "0"
                and status.get("target_workers") == "0"):
            raise ValueError(f"replacement requires a fully retired incomplete predecessor: {old}")
        replacement = identity(Path(new))
        if replacement == previous:
            raise ValueError("replacement must be a different batch")
        mapping[previous["batch_dir"]] = replacement
    result = []
    for dependency in dependencies:
        if identity(Path(dependency["batch_dir"])) != dependency:
            raise ValueError("retry dependency identity changed")
        selected = mapping.get(dependency["batch_dir"], dependency)
        hold = dependency_hold([selected])
        if hold in {"dependency_failed", "dependency_unavailable"}:
            raise ValueError(f"cannot retry with {hold}: {selected['batch_dir']}; "
                             "reconcile the predecessor first, then use --replace_dependency OLD NEW")
        if selected not in result:
            result.append(selected)
    return result


def retry_verified_tail(registration: dict[str, str], job_ids: list[str], apply: bool = True) -> int:
    """Caller has proved zero reservations/grants and preserved admission controls.

    Move only verified claims, at most once per job. Reset the cursor BEFORE any
    move so interruption cannot make a reclaimed job invisible to new workers.
    The claim archive is also the durable retry limit, including crash recovery.
    """
    batch = Path(registration["batch_dir"])
    missing = [job for job in job_ids if not (batch / "done" / f"job_{job}.done").is_file()]
    if not missing:
        return 0
    evidence = batch / "adaptive/verified-losses"
    archive = batch / "adaptive/lost-retries"
    claims = batch / "claims"
    selected = []
    for job in missing:
        receipt_path = evidence / f"job_{job}.json"
        if not receipt_path.exists() or (archive / f"job_{job}.claim").exists():
            return 0
        receipt = json.loads(receipt_path.read_text())
        owner_path = claims / f"job_{job}.claim" / "owner.json"
        if receipt_path.is_symlink() or owner_path.is_symlink() or owner_path.parent.is_symlink():
            raise ValueError(f"unsafe verified-loss paths: {job}")
        owner = json.loads(owner_path.read_text())
        if (receipt.get("pool_id") != registration["pool_id"]
                or receipt.get("queue_id") != registration["queue_id"]
                or receipt.get("owner") != owner or owner.get("job_id") != job
                or owner.get("queue_sha256") != registration["queue_sha256"]):
            raise ValueError(f"verified loss does not match current claim: {job}")
        selected.append((job, owner_path, owner))
    if not apply:
        return len(selected)
    lock = Path(registration["lock_dir"])
    try:
        lock.mkdir()
    except FileExistsError:
        return 0
    try:
        # Workers only reap empty stale locks. Keep the audit lock nonempty.
        (lock / "recovery-owner").write_text(f"{os.getpid()}\n")
        archive.mkdir(exist_ok=True)
        for job, owner_path, owner in selected:
            if (batch / "done" / f"job_{job}.done").exists() or json.loads(owner_path.read_text()) != owner:
                raise ValueError(f"claim changed during tail recovery: {job}")
        (batch / "claim-cursor.json").unlink(missing_ok=True)
        for job, owner_path, _ in selected:
            os.rename(owner_path.parent, archive / f"job_{job}.claim")
        return len(selected)
    finally:
        (lock / "recovery-owner").unlink(missing_ok=True)
        lock.rmdir()


def submit_chain(scheduler: Path, manifest: Path) -> None:
    stages = json.loads(manifest.read_text())
    if not isinstance(stages, list) or not stages:
        raise ValueError("chain manifest must be a nonempty list of submit argument lists")
    for stage in stages:
        if not isinstance(stage, list) or not stage or any(not isinstance(arg, str) for arg in stage):
            raise ValueError("each chain stage must be a submit argument list")
        for required in ("--model_dir", "--queue", "--memory_mib_per_job"):
            if stage.count(required) != 1 or stage.index(required) + 1 >= len(stage):
                raise ValueError(f"each chain stage requires one {required}")
    previous = None
    for index, stage in enumerate(stages, 1):
        command = ["bash", str(scheduler), "submit", *stage]
        if previous is not None:
            command += ["--after", previous]
        result = subprocess.run(command, text=True, stdout=subprocess.PIPE)
        print(result.stdout, end="", flush=True)
        if result.returncode:
            raise ValueError(f"chain stopped at stage {index}; earlier submissions are retained (see batch_dir above)")
        batches = [line.partition("=")[2] for line in result.stdout.splitlines() if line.startswith("batch_dir=")]
        if len(batches) != 1:
            raise ValueError(f"stage {index} returned no unique batch_dir; inspect submission before retrying")
        previous = batches[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    resolve = commands.add_parser("resolve")
    resolve.add_argument("--state-root", type=Path, required=True)
    resolve.add_argument("references", nargs="*")
    chain = commands.add_parser("submit-chain")
    chain.add_argument("--scheduler", type=Path, required=True)
    chain.add_argument("--manifest", type=Path, required=True)
    retry = commands.add_parser("retry-args")
    retry.add_argument("--resources", type=Path, required=True)
    retry.add_argument("--replace-dependency", nargs=2, action="append", default=[])
    args = parser.parse_args()
    try:
        if args.command == "resolve":
            print(json.dumps(resolve_dependencies(args.state_root, args.references), separators=(",", ":")))
        elif args.command == "submit-chain":
            submit_chain(args.scheduler, args.manifest)
        else:
            resources = read_env(args.resources)
            dependencies, retry_lost = policy(resources)
            for dependency in retry_dependencies(dependencies, args.replace_dependency):
                print("--after\n" + dependency["batch_dir"])
            if retry_lost:
                print("--retry_lost_jobs")
            if resources.get("early_eta_sample") == "1":
                print("--early_eta_sample")
    except (OSError, ValueError, KeyError) as error:
        print(f"adaptive workflow: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
