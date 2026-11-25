"""The scheduler's SSH fallback route and its per-user installer."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
# Answers probes only for addresses listed in $OPEN; a connection prints where it went.
FAKE_NC = """#!/bin/bash
if [ "$1" = -z ]; then
  address=${@: -2:1}
  printf 'probe %s\\n' "$address" >> "$CALLS"
  case " $OPEN " in *" $address "*) exit 0 ;; *) exit 1 ;; esac
fi
printf 'connect %s %s\\n' "$1" "$2"
"""


class RouteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "bin").mkdir()
        (self.root / "bin/nc").write_text(FAKE_NC)
        (self.root / "bin/nc").chmod(0o755)

    def route(self, open_addresses, *fallbacks):
        env = dict(os.environ, PATH=f"{self.root / 'bin'}:{os.environ['PATH']}", OPEN=open_addresses,
                   CALLS=str(self.root / "calls"))
        (self.root / "calls").write_text("")
        result = subprocess.run(["bash", str(ROOT / "admin/ssh_route.sh"), "192.0.2.10", "22", *fallbacks],
                                capture_output=True, text=True, env=env)
        return result, (self.root / "calls").read_text().split()

    def test_primary_first_then_fallback(self):
        result, probes = self.route("192.0.2.10", "198.51.100.10")
        self.assertEqual((result.returncode, result.stdout), (0, "connect 192.0.2.10 22\n"))
        self.assertEqual(probes, ["probe", "192.0.2.10"])
        result, probes = self.route("198.51.100.10", "198.51.100.10")
        self.assertEqual((result.returncode, result.stdout), (0, "connect 198.51.100.10 22\n"))
        self.assertEqual(result.stderr, "", "a successful route stays quiet")
        result, _ = self.route("", "198.51.100.10")
        self.assertEqual(result.returncode, 255)
        self.assertIn("no route to 192.0.2.10:22 (tried 192.0.2.10 198.51.100.10)", result.stderr)

    def test_installer_puts_routes_first_and_keeps_existing_config(self):
        home, rendered = self.root / "home", self.root / "rendered"
        (home / ".ssh").mkdir(parents=True)
        rendered.mkdir()
        (rendered / "ssh_config").write_text("Host 192.0.2.10\n  ProxyCommand ~/.ssh/rota-route.sh %h %p 198.51.100.10\n")
        (home / ".ssh/config").write_text("Host scg\n  HostName login.example\n")
        env = dict(os.environ, HOME=str(home))
        installer = ["bash", str(ROOT / "admin/install_ssh_routes.sh")]
        check = subprocess.run(installer + ["--check", str(rendered)], capture_output=True, text=True, env=env)
        self.assertEqual(check.returncode, 1)
        self.assertIn("MISSING", check.stdout)
        for _ in range(2):
            subprocess.run(installer + [str(rendered)], check=True, capture_output=True, env=env)
        self.assertEqual((home / ".ssh/config").read_text(),
                         "Include rota.conf\nHost scg\n  HostName login.example\n")
        self.assertEqual(len(list((home / ".ssh").glob("config.bak-*"))), 1, "only the first install edits config")
        self.assertEqual((home / ".ssh/rota.conf").read_text(), (rendered / "ssh_config").read_text())
        self.assertTrue(os.access(home / ".ssh/rota-route.sh", os.X_OK))
        check = subprocess.run(installer + ["--check", str(rendered)], capture_output=True, text=True, env=env)
        self.assertEqual((check.returncode, check.stdout.split()[0]), (0, "CURRENT"))


if __name__ == "__main__":
    unittest.main()
