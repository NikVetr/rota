import copy
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import cluster_config as config


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "config.json"
        self.data = json.loads((ROOT / "examples/cluster.json").read_text())

    def load(self, data):
        self.path.write_text(json.dumps(data))
        return config.read_config(self.path)

    def test_render_one_inventory_and_safe_shell_environment(self):
        self.data["shared_root"] = str(self.root / "shared-data@site+1")
        self.data["hosts"][1]["enabled"] = False
        data = self.load(self.data)
        env_file = config.render(data, self.root / "generated files")
        self.assertEqual(config.render(data, self.root / "generated files"), env_file)
        result = subprocess.run(["bash", "-c", 'source "$1"; printf "%s\\n" "$ROTA_ROOT" "$ADAPTIVE_SHARED_IO_CAPACITY"',
                                 "test", str(env_file)], capture_output=True, text=True, check=True)
        self.assertEqual(result.stdout.splitlines(), [data["shared_root"], "0"])
        self.assertIn("user@worker 0 worker", (env_file.parent / "hosts.txt").read_text())
        self.assertIn("worker\tuser@worker\t8\t16384\t0", (env_file.parent / "adaptive-resources.tsv").read_text())
        env_file.write_text("edited")
        with self.assertRaisesRegex(ValueError, "modified"):
            config.render(data, self.root / "generated files")

    def test_opt_in_settings_render_only_when_used(self):
        self.data["shared_root"] = str(self.root)
        default = config.render(self.load(copy.deepcopy(self.data)), self.root / "default")
        table = (default.parent / "adaptive-resources.tsv").read_text()
        self.assertEqual(table.splitlines()[0], "host_id\tssh_target\tcpu_units\tmemory_mib\tenabled")
        self.assertIn("export ADAPTIVE_BACKFILL=0", default.read_text())

        self.data["backfill"] = True
        self.data["hosts"][0]["gpu_units"] = 2
        self.data["hosts"][1].update(owner_idle_seconds=300, owner_active="suspend")
        rendered = config.render(self.load(self.data), self.root / "opt-in")
        rows = (rendered.parent / "adaptive-resources.tsv").read_text().splitlines()
        self.assertEqual(rows[0].split("\t")[5:], ["gpu_units", "owner_idle_seconds", "owner_active"])
        self.assertEqual(rows[1].split("\t")[5:], ["2", "0", "hold"])
        self.assertEqual(rows[2].split("\t")[5:], ["0", "300", "suspend"])
        self.assertIn("export ADAPTIVE_BACKFILL=1", rendered.read_text())

    def test_invalid_opt_in_settings_are_rejected(self):
        for change in ({"backfill": "yes"},):
            data = copy.deepcopy(self.data)
            data.update(change)
            with self.assertRaises(ValueError):
                self.load(data)
        for host_change in ({"gpu_units": -1}, {"gpu_units": True}, {"owner_idle_seconds": 30},
                            {"owner_active": "suspend"}, {"owner_idle_seconds": 300, "owner_active": "kill"}):
            data = copy.deepcopy(self.data)
            data["hosts"][0].update(host_change)
            with self.assertRaises(ValueError, msg=str(host_change)):
                self.load(data)

    def test_render_requires_an_existing_shared_directory(self):
        shared = self.root / "data"
        self.data["shared_root"] = str(shared)
        self.load(self.data)
        command = [sys.executable, str(ROOT / "cluster_config.py"), str(self.path), "--render"]
        result = subprocess.run(command, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("shared_root is unavailable", result.stderr)
        self.assertFalse(shared.exists())
        shared.mkdir()
        env_file = Path(subprocess.check_output(command, text=True).strip())
        self.assertEqual(env_file.parent.parent, shared / "hosts/config")
        self.assertTrue(env_file.is_file())

    def test_fallback_addresses_render_ssh_routes(self):
        self.data["hosts"][1]["fallback"] = "198.51.100.2"
        data = self.load(self.data)
        generated = config.render(data, self.root / "generated").parent
        self.assertEqual((generated / "ssh_config").read_text().splitlines()[1:], [
            "Host worker", "  ProxyCommand ~/.ssh/rota-route.sh %h %p 198.51.100.2"])
        self.assertEqual((generated / "adaptive-resources.tsv").read_text().splitlines()[2],
                         "worker\tuser@worker\t8\t16384\t1", "fallbacks do not change the resource table")
        for fallback in ("198.51.100.2 -oProxyCommand=x", "user@198.51.100.2", "", 7, "storage"):
            broken = copy.deepcopy(self.data)
            broken["hosts"][1]["fallback"] = fallback
            with self.subTest(fallback=fallback), self.assertRaises(ValueError):
                self.load(broken)
        duplicate = copy.deepcopy(self.data)
        duplicate["hosts"][0]["fallback"] = "198.51.100.2"
        with self.assertRaisesRegex(ValueError, "unique"):
            self.load(duplicate)

    def test_installed_monitor_uses_inventory_without_login_shell(self):
        generated = config.render(self.load(self.data), self.root / "generated").parent
        settings = self.root / "config" / "rota"
        settings.mkdir(parents=True)
        (settings / "monitor-environment.json").write_bytes((generated / "monitor-environment.json").read_bytes())
        result = subprocess.check_output(
            [sys.executable, "-c", 'import cluster_config as c, os; c.load_monitor_environment(); '
             'print(os.environ["ROTA_ROOT"], os.environ["ROTA_COORDINATOR"])'],
            cwd=ROOT, env={"XDG_CONFIG_HOME": str(settings.parent), "ROTA_ROOT": "/explicit"},
            text=True)
        self.assertEqual(result.strip(), "/explicit user@storage")

    def test_invalid_inventory_fails_before_generation(self):
        for field, value in (("cpu_units", True), ("cpu_units", 0), ("memory_mib", -1), ("enabled", "false")):
            broken = copy.deepcopy(self.data)
            broken["hosts"][1][field] = value
            with self.subTest(field=field, value=value), self.assertRaisesRegex(ValueError, field):
                self.load(broken)
        cases = [dict(extra=True), dict(coordinator="missing"), dict(shared_root="relative"),
                 dict(shared_io_capacity=3), dict(network_interface="bad;command")]
        cases += [dict(shared_root=root) for root in
                  ("/", "//", "/srv//x", "/srv/./x", "/srv/../x", "/srv/shared data",
                   "/srv/a$b", "/srv/a'b", '/srv/a"b', "/srv/a`b", "/srv/a\\b", "/srv/a;b")]
        for overrides in cases:
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                self.load({**self.data, **overrides})
        duplicate = copy.deepcopy(self.data)
        duplicate["hosts"][1]["ssh"] = duplicate["hosts"][0]["ssh"]
        with self.assertRaises(ValueError):
            self.load(duplicate)

    def test_dashboard_uses_inventory_labels_instead_of_site_aliases(self):
        data = self.load(self.data)
        env_file = config.render(data, self.root / "generated")
        source = (ROOT / "start_tmux_monitoring.sh").read_text()
        match = re.search(r"^mk_label\(\) \{.*?^\}", source, re.M | re.S)
        assert match is not None
        function = match[0]
        script = 'HOSTS_FILE="$1"\n' + function + '\nmk_label user@storage\nmk_label user@worker\n'
        result = subprocess.run(["bash", "-c", script, "test", str(env_file.parent / "hosts.txt")],
                                capture_output=True, text=True, check=True)
        self.assertEqual(result.stdout.splitlines(), ["storage", "worker"])

    def test_process_scan_recognizes_jobs_outside_volumes(self):
        source = (ROOT / "monitor_queue_workers.sh").read_text()
        scan = source.split("REMOTE_SCAN_CMD=$(cat <<'EOF'\n", 1)[1].split("\nEOF\n", 1)[0]
        processes = "1 mmworker__srv_cluster_study 2\n42 Rscript job.R --job_tag _srv_cluster_job\n"
        script = "ps() { printf '%s' " + shlex.quote(processes) + "; }\n" + scan
        result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True)
        self.assertIn("TAG\t_srv_cluster_study", result.stdout)
        self.assertIn("TAG\t_srv_cluster_job", result.stdout)

    def test_dashboard_passes_configured_io_devices_and_spaced_root(self):
        share = self.root / "shared data"
        share.mkdir()
        (share / "io.sh").write_text('printf "%s\\n" "$@"\n')
        (share / "rates.py").touch()
        source = (ROOT / "start_tmux_monitoring.sh").read_text()
        block = source.split("  printf -v root_q", 1)[1].split("  io_cols=", 1)[0]
        block = "  printf -v root_q" + block
        setup = '''set -eu
ROTA_ROOT="$1"
ROTA_NETWORK_INTERFACE=eth0 ROTA_DISK=nvme0n1
io_script_name=io.sh io_rates_name=rates.py CLUSTER_IO_REFRESH_SEC=3 storage_file=/tmp/unused.json
MONITOR_INSTANCE=monitoring-1-2-3
'''
        result = subprocess.run(["bash", "-c", setup + block + '\nbash -c "$remote_io_cmd"', "test", str(share)],
                                capture_output=True, text=True, check=True)
        args = result.stdout.splitlines()
        self.assertEqual(args[args.index("--root")+1], str(share))
        self.assertEqual(args[args.index("--interface")+1], "eth0")
        self.assertEqual(args[args.index("--disk")+1], "nvme0n1")
        # Concurrent dashboards must not share queue-rate state.
        self.assertEqual(args[args.index("--rates_state_file")+1], "/tmp/rota-io-rates-monitoring-1-2-3.json")


if __name__ == "__main__":
    unittest.main()
