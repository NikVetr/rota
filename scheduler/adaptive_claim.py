#!/usr/bin/env python3
"""Claim one immutable-queue job. The caller must hold the batch queue lock."""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

CURSOR_STALE_EXIT = 3


class StaleCursor(Exception):
    """The job was claimed, but the cursor could not be advanced."""

    def __init__(self, line: bytes, error: OSError) -> None:
        super().__init__(str(error))
        self.line = line


def _digest(record: dict) -> str:
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()


def _job_id(line: bytes) -> str:
    parts = line.decode("utf-8").split(None, 1)
    if len(parts) != 2 or not re.fullmatch(r"[A-Za-z0-9._:-]+", parts[0]):
        raise ValueError("invalid immutable queue record")
    return parts[0]


def claim_next(
    queue: Path, claims: Path, cursor: Path, queue_sha256: str,
    reservation_id: str | None = None,
) -> bytes:
    if not re.fullmatch(r"[0-9a-f]{64}", queue_sha256):
        raise ValueError("invalid queue hash")
    if queue.is_symlink() or claims.is_symlink() or not claims.is_dir():
        raise ValueError("queue and claims must be regular scheduler paths")
    if reservation_id is not None and not re.fullmatch(r"[A-Za-z0-9._-]+", reservation_id):
        raise ValueError("invalid claim reservation identity")
    with queue.open("rb") as handle:
        offset = 0
        if os.path.lexists(cursor):
            if cursor.is_symlink():
                raise ValueError("claim cursor must not be a symlink")
            record = json.loads(cursor.read_text())
            if not isinstance(record, dict):
                raise ValueError("invalid claim cursor")
            checksum = record.pop("checksum", None)
            if checksum != _digest(record) or set(record) != {
                "schema_version", "queue_sha256", "offset", "previous_offset", "previous_job_id",
            }:
                raise ValueError("claim cursor checksum or fields are invalid")
            if record["schema_version"] != 1 or record["queue_sha256"] != queue_sha256:
                raise ValueError("claim cursor queue identity mismatch")
            offset = record["offset"]
            previous = record["previous_offset"]
            if (type(offset) is not int or type(previous) is not int
                    or not 0 <= previous < offset <= os.fstat(handle.fileno()).st_size):
                raise ValueError("claim cursor offset is invalid")
            handle.seek(previous)
            anchor = _job_id(handle.readline())
            if (handle.tell() != offset or anchor != record["previous_job_id"]
                    or not os.path.lexists(claims / f"job_{anchor}.claim")):
                raise ValueError("claim cursor has no matching prior claim")
        handle.seek(offset)
        while True:
            previous = handle.tell()
            line = handle.readline()
            if not line:
                return b""
            if not line.strip():
                continue
            job_id = _job_id(line)
            claim = claims / f"job_{job_id}.claim"
            try:
                claim.mkdir()
            except FileExistsError:
                # Missing/rolled-back cursors are recoverable from durable claims.
                if not os.path.lexists(claim):
                    raise
                continue
            if reservation_id is not None:
                # Persist ownership before returning the command for execution.
                # A partial record leaves the claim closed for manual audit.
                with (claim / "owner.json").open("x") as owner:
                    json.dump({"reservation_id": reservation_id, "job_id": job_id,
                               "queue_sha256": queue_sha256}, owner, sort_keys=True)
                    owner.flush()
                    os.fsync(owner.fileno())
            record = {
                "schema_version": 1, "queue_sha256": queue_sha256,
                "offset": handle.tell(), "previous_offset": previous, "previous_job_id": job_id,
            }
            record["checksum"] = _digest(record)
            try:
                _publish_cursor(cursor, record)
            except OSError as error:
                # The claim directory is the dequeue record and a stale cursor only
                # costs a rescan, so run the claimed job rather than losing it.
                raise StaleCursor(line, error) from error
            # A crash before cursor publication leaves a claim that recovery skips.
            # A crash afterwards also leaves the claim; neither path replays a job.
            return line


def _publish_cursor(cursor: Path, record: dict) -> None:
    temporary = cursor.with_name(f"{cursor.name}.tmp.{os.getpid()}")
    try:
        with temporary.open("x") as output:
            json.dump(record, output, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        for attempt in range(10):
            try:
                os.replace(temporary, cursor)
                return
            except OSError as error:
                # macOS SMB clients defer closing a recently read cursor, so a
                # rename over it from another host can briefly fail with EBUSY.
                if error.errno != errno.EBUSY or attempt == 9:
                    raise
                time.sleep(0.05 * (attempt + 1))
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, required=True)
    parser.add_argument("--claims", type=Path, required=True)
    parser.add_argument("--cursor", type=Path, required=True)
    parser.add_argument("--queue-sha256", required=True)
    parser.add_argument("--reservation-id")
    args = parser.parse_args()
    try:
        line = claim_next(args.queue, args.claims, args.cursor, args.queue_sha256, args.reservation_id)
        sys.stdout.buffer.write(line)
    except StaleCursor as stale:
        sys.stdout.buffer.write(stale.line)
        print(f"adaptive claim: claimed, but the cursor was not advanced: {stale}", file=sys.stderr)
        return CURSOR_STALE_EXIT
    except (OSError, ValueError) as error:
        print(f"adaptive claim: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
