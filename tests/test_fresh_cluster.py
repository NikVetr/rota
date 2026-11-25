"""Native Linux fresh-install smoke test; SSH is strictly local and fixture-only."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


@unittest.skipUnless(sys.platform.startswith("linux"), "native Linux fixture")
class FreshClusterTests(unittest.TestCase):
    def test_render_start_submit_complete_and_drain(self):
        with tempfile.TemporaryDirectory(prefix="fresh-cluster-") as directory:
            base = Path(directory)
            share, home, binary = (base / name for name in ("shared", "home", "bin"))
            for path in (share, home, binary):
                path.mkdir()
            for source in (ROOT / "scheduler").iterdir():
                if source.is_file() and source.suffix in (".sh", ".py"):
                    shutil.copy2(source, share / source.name)
            ssh = binary / "ssh"
            ssh.write_text('''#!/usr/bin/env python3
import os, subprocess, sys
args = sys.argv[1:]
while args and args[0].startswith("-"):
    args = args[2:] if args[0] == "-o" else args[1:]
if len(args) != 2 or args[0] != "fixture":
    raise SystemExit("Refusing non-fixture SSH target")
raise SystemExit(subprocess.run(["bash", "-c", args[1]], env=os.environ).returncode)
''')
            ssh.chmod(0o755)
            config_path = base / "cluster.json"
            config_path.write_text(json.dumps({"shared_root": str(share), "coordinator": "node",
                "hosts": [{"id": "node", "ssh": "fixture", "cpu_units": 2,
                           "memory_mib": 2048, "enabled": True}]}))
            env_file = subprocess.check_output([sys.executable, str(ROOT / "setup" / "cluster_config.py"),
                                                str(config_path), "--render"], text=True).strip()
            output = subprocess.check_output(["bash", "-c", 'source "$1"; env -0', "test", str(env_file)])
            env = dict(item.decode().split("=", 1) for item in output.split(b"\0") if item)
            env.update(HOME=str(home), PATH=f"{binary}:{os.environ['PATH']}",
                       ADAPTIVE_SSH_BIN=str(ssh), ADAPTIVE_DAEMON_POLL_SECONDS="1")
            scheduler = share / "adaptive_scheduler.sh"

            def command(*args):
                result = subprocess.run(["bash", str(scheduler), *args], env=env,
                                        text=True, capture_output=True, timeout=40)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                return result.stdout

            try:
                command("pool-start", "--pool_name", "fixture", "--tick_seconds", "1",
                        "--lease_seconds", "10", "--start_interval_seconds", "0")
                model = Path(subprocess.check_output(["bash", str(ROOT / "examples/first-job/prepare.sh")],
                                                     env=env, text=True).strip())
                submitted = command("submit", "--model_dir", str(model), "--queue", "jobs.txt",
                                    "--project", "first-job", "--run_name", "squares",
                                    "--cpu_units_per_job", "1", "--memory_mib_per_job", "128", "--early_eta_sample")
                batch = Path(next(line.split("=", 1)[1] for line in submitted.splitlines()
                                  if line.startswith("batch_dir=")))
                self.assertIn("early_eta_sample=1\n", (batch / "adaptive/resources.env").read_text())
                deadline = time.monotonic() + 35
                markers = [batch / f"done/job_square{n}.done" for n in (3, 7)]
                while time.monotonic() < deadline and not all(p.exists() for p in markers):
                    time.sleep(0.2)
                self.assertTrue(all(p.exists() for p in markers),
                                (share / ".rota/adaptive/coordinator.log").read_text())
                self.assertEqual([(model / f"results/{n}.txt").read_text() for n in (3, 7)], ["9\n", "49\n"])
                command("pool-drain")
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline and (share / ".rota/adaptive/pool.env").exists():
                    time.sleep(0.2)
                self.assertFalse((share / ".rota/adaptive/pool.env").exists())
            finally:
                # Only terminate processes executing scripts inside this fixture.
                # Never use a global scheduler-name match or a network command.
                for entry in Path("/proc").iterdir():
                    if not entry.name.isdigit() or int(entry.name) == os.getpid():
                        continue
                    try:
                        argv = (entry / "cmdline").read_bytes().split(b"\0")
                        if any(arg.startswith(str(base).encode() + b"/") for arg in argv):
                            os.kill(int(entry.name), signal.SIGTERM)
                    except (OSError, ProcessLookupError):
                        pass


if __name__ == "__main__":
    unittest.main()
