"""The rota front door runs checkout tools from its checkout and scheduler commands from the root."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RotaCommandTests(unittest.TestCase):
    def run_rota(self, *args, root):
        env = dict(os.environ, ROTA_ROOT=str(root))
        return subprocess.run([str(self.link), *args], env=env, capture_output=True, text=True)

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = Path(tmp.name)
        self.link = self.tmp / "bin" / "rota"
        self.link.parent.mkdir()
        self.link.symlink_to(ROOT / "rota")  # installed as ~/.local/bin/rota
        self.root = self.tmp / "share"
        self.root.mkdir()
        for name in ("adaptive_scheduler.sh", "start_workers.sh", "kill_workers.sh", "reconcile_queue_batch.sh"):
            script = self.root / name
            script.write_text(f'#!/usr/bin/env bash\nprintf "{name} %s\\n" "$*"\n')
            script.chmod(0o755)

    def test_scheduler_commands_reach_the_shared_root(self):
        self.assertEqual(self.run_rota("pool-status", "--x", "1", root=self.root).stdout,
                         "adaptive_scheduler.sh pool-status --x 1\n")
        self.assertEqual(self.run_rota("help-scheduler", root=self.root).stdout, "adaptive_scheduler.sh --help\n")
        for command, script in (("start", "start_workers.sh"), ("kill", "kill_workers.sh"),
                                ("reconcile", "reconcile_queue_batch.sh")):
            self.assertEqual(self.run_rota(command, "--queue", "q", root=self.root).stdout, f"{script} --queue q\n")

    def test_checkout_tools_run_from_the_linked_checkout(self):
        result = self.run_rota("control", "--help", root=self.root)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("usage: rota control", result.stdout)
        self.assertIn("Usage: rota <command>", self.run_rota(root=self.root).stdout)

    def test_missing_scheduler_is_reported(self):
        result = self.run_rota("pool-status", root=self.tmp / "absent")
        self.assertEqual(result.returncode, 2)
        self.assertIn("adaptive_scheduler.sh is not deployed", result.stderr)

    def test_scheduler_commands_need_the_inventory_environment(self):
        env = {key: value for key, value in os.environ.items() if key != "ROTA_ROOT"}
        result = subprocess.run([str(self.link), "pool-status"], env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("source the inventory environment", result.stderr)


if __name__ == "__main__":
    unittest.main()
