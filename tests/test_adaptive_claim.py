#!/usr/bin/env python3
"""Crash recovery and scaling contracts for adaptive dequeue."""

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SOURCE = Path(__file__).resolve().parents[1] / "scheduler/adaptive_claim.py"
spec = importlib.util.spec_from_file_location("adaptive_claim", SOURCE)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ClaimTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.queue = self.root / "queue.txt"
        self.queue.write_text("one echo α\ntwo true\nthree true", encoding="utf-8")
        self.claims = self.root / "claims"
        self.claims.mkdir()
        self.cursor = self.root / "claim-cursor.json"
        self.digest = hashlib.sha256(self.queue.read_bytes()).hexdigest()

    def claim(self):
        return module.claim_next(self.queue, self.claims, self.cursor, self.digest)

    def test_sequential_claims_preserve_queue_and_utf8_offsets(self):
        original = self.queue.read_bytes()
        self.assertEqual([self.claim() for _ in range(4)], original.splitlines(keepends=True) + [b""])
        self.assertEqual(self.queue.read_bytes(), original)
        self.assertEqual(len(list(self.claims.iterdir())), 3)

    def test_claim_binds_job_to_reservation_before_launch(self):
        line = module.claim_next(self.queue, self.claims, self.cursor, self.digest, "pool.host.1")
        self.assertEqual(line, "one echo α\n".encode())
        self.assertEqual(json.loads((self.claims / "job_one.claim/owner.json").read_text()), {
            "job_id": "one", "reservation_id": "pool.host.1", "queue_sha256": self.digest,
        })

    def test_owner_write_failure_never_releases_command(self):
        with mock.patch.object(module.json, "dump", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                module.claim_next(self.queue, self.claims, self.cursor, self.digest, "pool.host.1")
        self.assertTrue((self.claims / "job_one.claim").exists())
        self.assertFalse(self.cursor.exists())
        self.assertEqual(self.claim(), b"two true\n")

    def test_claim_operations_grow_linearly(self):
        self.queue.write_text("".join(f"job_{i} true\n" for i in range(500)))
        self.digest = hashlib.sha256(self.queue.read_bytes()).hexdigest()
        real_mkdir = Path.mkdir
        attempts = []

        def mkdir(path, *args, **kwargs):
            attempts.append(path)
            return real_mkdir(path, *args, **kwargs)

        with mock.patch.object(Path, "mkdir", mkdir):
            for _ in range(500):
                self.assertTrue(self.claim())
            self.assertEqual(self.claim(), b"")
        self.assertEqual(len(attempts), 500)

    def test_failed_cursor_publication_keeps_the_claimed_job(self):
        with mock.patch.object(module.os, "replace", side_effect=OSError("crash")):
            with self.assertRaises(module.StaleCursor) as stale:
                self.claim()
        self.assertEqual(stale.exception.line, "one echo α\n".encode())
        self.assertTrue((self.claims / "job_one.claim").is_dir())
        self.assertFalse(self.cursor.exists())
        self.assertEqual(list(self.root.glob("claim-cursor.json.tmp.*")), [])
        self.assertEqual(self.claim(), b"two true\n")

    def test_transient_busy_cursor_rename_is_retried(self):
        real_replace = module.os.replace
        failures = [OSError(module.errno.EBUSY, "Resource busy")] * 2

        def replace(source, destination):
            if failures:
                raise failures.pop()
            return real_replace(source, destination)

        with mock.patch.object(module.os, "replace", replace), \
                mock.patch.object(module.time, "sleep"):
            self.assertEqual(self.claim(), "one echo α\n".encode())
        self.assertTrue(self.cursor.exists())
        self.assertEqual(self.claim(), b"two true\n")

    def test_cli_runs_a_claim_whose_cursor_could_not_advance(self):
        # A cursor in a missing directory can never be published.
        result = subprocess.run(
            [sys.executable, str(SOURCE), "--queue", str(self.queue), "--claims", str(self.claims),
             "--cursor", str(self.root / "missing-dir" / "claim-cursor.json"),
             "--queue-sha256", self.digest],
            capture_output=True,
        )
        self.assertEqual(result.returncode, module.CURSOR_STALE_EXIT, result.stderr)
        self.assertEqual(result.stdout, "one echo α\n".encode())
        self.assertIn(b"cursor was not advanced", result.stderr)

    def test_deleted_and_rolled_back_cursors_recover_from_claims(self):
        self.claim()
        previous = self.cursor.read_bytes()
        self.claim()
        self.cursor.write_bytes(previous)
        self.assertEqual(self.claim(), b"three true")
        self.cursor.unlink()
        self.assertEqual(self.claim(), b"")

    def test_corrupt_forward_cursor_fails_loudly(self):
        self.claim()
        state = json.loads(self.cursor.read_text())
        state["offset"] = self.queue.stat().st_size
        self.cursor.write_text(json.dumps(state))
        with self.assertRaisesRegex(ValueError, "checksum"):
            self.claim()
        self.assertEqual(len(list(self.claims.iterdir())), 1)

    def test_cursor_requires_existing_anchor_and_queue_identity(self):
        self.claim()
        with self.assertRaisesRegex(ValueError, "identity"):
            module.claim_next(self.queue, self.claims, self.cursor, "0" * 64)
        (self.claims / "job_one.claim").rmdir()
        with self.assertRaisesRegex(ValueError, "matching prior claim"):
            self.claim()

    def test_claim_failure_is_not_empty_or_a_cursor_advance(self):
        with mock.patch.object(Path, "mkdir", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                self.claim()
        self.assertFalse(self.cursor.exists())
        self.assertEqual(list(self.claims.iterdir()), [])

    def test_missing_queue_or_claims_fails_loudly(self):
        self.queue.unlink()
        with self.assertRaises(FileNotFoundError):
            self.claim()
        self.queue.write_text("one true\n")
        self.claims.rmdir()
        with self.assertRaises(ValueError):
            self.claim()


if __name__ == "__main__":
    unittest.main()
