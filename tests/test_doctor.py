import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class DoctorTests(unittest.TestCase):
    def test_shared_marker_distinguishes_two_writable_copies(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            shared = base / "shared"
            shared.mkdir()
            (base / "other").mkdir()
            ssh = base / "ssh"
            ssh.write_text("""#!/usr/bin/env python3
import os, subprocess, sys
host, command = sys.argv[-2:]
if host == "offline":
    print("connection timed out", file=sys.stderr)
    sys.exit(255)
if host == "copy":
    command = command.replace(os.environ["SHARED"], os.environ["COPY"])
sys.exit(subprocess.call(["sh", "-c", command]))
""")
            ssh.chmod(0o755)
            inventory = base / "inventory.json"
            env = dict(os.environ, PATH=f"{base}:{os.environ['PATH']}", SHARED=str(shared), COPY=str(base/"other"))
            def run(other, enabled=True):
                inventory.write_text(json.dumps({"shared_root": str(shared), "coordinator": "store",
                    "hosts": [{"id": node, "ssh": node, "cpu_units": 1, "memory_mib": 512,
                               "enabled": node == "store" or enabled} for node in ("store", other)]}))
                proc = subprocess.run([str(ROOT / "doctor"), str(inventory), "--json"], env=env,
                                      capture_output=True, text=True, timeout=15)
                self.assertEqual(list(shared.iterdir()), [])
                self.assertEqual(list((base/"other").iterdir()), [])
                return proc.returncode, json.loads(proc.stdout)
            self.assertEqual(run("worker")[0], 0)
            code, result = run("copy")
            self.assertEqual(code, 1)
            checks = result["hosts"][1]["checks"]
            self.assertTrue(checks["ssh"]["ok"] and checks["write"]["ok"])
            self.assertFalse(checks["same_storage"]["ok"])
            code, result = run("offline", False)
            self.assertEqual(code, 0)
            self.assertFalse(result["hosts"][1]["checks"]["ssh"]["ok"])
