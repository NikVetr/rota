"""Launch the dashboard in snapshot mode on a private tmux server; never use the user's socket."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
sys.path.insert(1, str(ROOT / "monitor"))
from test_cluster_status_collector import TSV
import monitor_sample as sample

MAC_COUNTERS = dict(total=16 * 1024**3, page=16384, free=16416, file_backed=159933, purgeable=1454,
                    swap=256 * 1024**2, pressure_free=81)
MEMINFO = "\n".join(f"{key}: {value} kB" for key, value in {
    "MemTotal": 8388608, "MemAvailable": 5242880, "MemFree": 1048576, "Cached": 4194304,
    "Buffers": 1024, "SReclaimable": 1024, "Shmem": 2048, "SwapTotal": 1048576, "SwapFree": 786432}.items())
# Commands a snapshot pane must never run; fakes record any call.
FORBIDDEN = ("ssh", "iostat", "netstat", "ps", "df", "diskutil", "mount")


def host(host_id, kind, now, ok=True, local=False, mount="mounted", free=500e9):
    memory = sample.mac_memory(MAC_COUNTERS) if kind == "mac" else sample.linux_memory(MEMINFO, "some avg10=0.50\n")
    return {"id": host_id, "target": f"u@{host_id}", "local": local, "kind": kind, "ok": ok,
            "error": None if ok else "ssh: connect to host: No route to host; retrying in 20 s",
            "updated": now - (1 if ok else 125), "load": [1, 1, 1],
            "cpu": sample.cpu_section([(0.3, 0.1)] * 10), "memory": memory,
            "mount": {"root": "/srv/cluster", "ok": {"mounted": True, "missing": False}.get(mount),
                      "state": mount, "since": now - 300, "error": None},
            "disk": {"root_total_bytes": 1000e9, "root_free_bytes": free, "root_used_bytes": 1000e9 - free,
                     "error": None}}


def fixture(now):
    return {
        "schema": 1, "updated": now - 1,
        "collector": {"pid": 1, "started": now - 60, "hosts_source": "share", "local_host": "desktop"},
        "hosts": [host("node2", "mac", now), host("node1", "mac", now, mount="missing"),
                  host("storage", "mac", now, ok=False), host("desktop", "linux", now, local=True, free=834e9)],
        "shared": {"host": "storage", "ok": True, "error": None, "updated": now - 2, "epoch": int(now - 2),
                   "interface": "en0",
                   "pool": {"state": "draining", "capacity": 50, "capacity_memory_mib": 68062, "resource_state": "ok",
                            "io_state": "high", "smbd_cpu_pct": 42.3, "quarantined": 2, "in_use": 22},
                   "network": {"rx_bytes_per_s": 75000, "tx_bytes_per_s": 25000},
                   "smbd": {"cpu_pct": 41.0, "sessions": 6, "recv_queue_bytes": 70000, "send_queue_bytes": 0},
                   "disk": {"name": "disk4", "bytes_per_s": 420000, "read_bytes_per_s": None,
                            "write_bytes_per_s": None, "ops_per_s": 18.0, "kb_per_op": 22.67,
                            "total_bytes": 4000e9, "used_bytes": 3063e9, "free_bytes": 937e9, "error": None},
                   "registry": {"active_queues": 3, "claimed_per_min": None, "completed_per_min": 6.0,
                                "rewrite_retries_per_min": 0.0, "duplicate_retries_per_min": 0.0,
                                "stale_locks_per_min": 0.0, "age_sec": 6, "locks_held": 1, "oldest_lock_sec": 7}},
        "queues": [],
        "queue_refresh": {"ok": True, "error": None, "updated": now - 5, "attempted": now - 11, "path": "/q.tsv"}}



@unittest.skipUnless(sys.platform.startswith("linux") and shutil.which("tmux"), "Linux snapshot dashboard requires tmux")
class SnapshotDashboardTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="mmm-snapshot-")
        self.base = Path(self.directory.name)
        self.socket = self.base / "tmux.sock"
        self.real_tmux = shutil.which("tmux")

    def tearDown(self):
        subprocess.run([self.real_tmux, "-S", str(self.socket), "kill-server"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.directory.cleanup()

    def tmux(self, *args):
        return subprocess.check_output([self.real_tmux, "-S", str(self.socket), *args], text=True)

    def test_every_pane_renders_the_collector_snapshot(self):
        binary = self.base / "bin"
        binary.mkdir()
        (binary / "tmux").write_text(f'#!/bin/sh\n[ "$1" = attach-session ] && exit 0\n'
                                     f'exec {self.real_tmux} -f /dev/null -S {self.socket} "$@"\n')
        for name in FORBIDDEN:
            (binary / name).write_text(f'#!/bin/sh\necho "{name} $*" >> {self.base}/forbidden.log\nexit 99\n')
        for script in binary.iterdir():
            script.chmod(0o755)
        data = fixture(time.time() + 60)  # stays fresh for the whole test
        (self.base / "cluster-status.json").write_text(json.dumps(data))
        (self.base / "queue-progress.tsv").write_text(TSV)
        (self.base / "hosts.txt").write_text("".join(f"u@{host['id']} 4 {host['id']}\n" for host in data["hosts"]))
        env = {key: value for key, value in os.environ.items() if key != "TMUX"}
        env.update(ROTA_ROOT="/srv/cluster", PATH=f"{binary}{os.pathsep}{env['PATH']}", ROTA_HOSTS_FILE=str(self.base / "hosts.txt"),
                   ROTA_CLUSTER_SNAPSHOT=str(self.base / "cluster-status.json"),
                   ROTA_QUEUE_SNAPSHOT=str(self.base / "queue-progress.tsv"))
        result = subprocess.run(["bash", str(ROOT / "monitor" / "start_tmux_monitoring.sh"), "--session", "mmm-test"],
                                env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        deadline, panes = time.monotonic() + 20, {}
        while time.monotonic() < deadline:
            panes = dict(line.split(" ", 1) for line in self.tmux(
                "list-panes", "-t", "mmm-test:0", "-F", "#{pane_id} #{pane_title}").splitlines())
            titles = list(panes.values())
            if any(title.startswith("QUEUE PROGRESS | Updated") for title in titles) and \
                    any(title.startswith("CPU node2 40%") for title in titles):
                break
            time.sleep(0.25)
        content = {title: self.tmux("capture-pane", "-p", "-J", "-S", "-", "-t", pane)
                   for pane, title in panes.items()}
        titles = sorted(content)
        self.assertIn("CPU node2 40% ─── * = Sys, # = User ────", titles)
        self.assertIn("CPU node1 40%", titles)
        self.assertIn("CPU storage", titles, "an offline host kept a CPU total in its title")
        self.assertIn("storage unavailable: ssh: connect to host", content["CPU storage"])
        self.assertIn("Used:13.3G Cache:2.5G", content["MEM node2 ─── # = Used ────"])
        self.assertIn("PSI some: 0.50%", content["MEM desktop"])
        self.assertIn("[✔]", content["Share"])
        self.assertIn("[✗]", content["Share"])
        self.assertIn("POOL DRAINING", content["CLUSTER I/O storage"])
        self.assertIn("Boot 834GB", content["CLUSTER I/O storage"])
        queue = next(title for title in titles if title.startswith("QUEUE PROGRESS"))
        self.assertTrue(queue.startswith("QUEUE PROGRESS | Updated "), titles)
        self.assertIn("sample-main", content[queue])
        self.assertIn("12 failed 13:51", content[queue])
        self.assertFalse((self.base / "forbidden.log").exists(),
                         (self.base / "forbidden.log").read_text() if (self.base / "forbidden.log").exists() else "")

    def test_hosts_override_requires_direct_mode(self):
        result = subprocess.run(["bash", str(ROOT / "monitor" / "start_tmux_monitoring.sh"), "--hosts", "u@a"],
                                capture_output=True, text=True, timeout=30, env=dict(os.environ, ROTA_ROOT="/srv/cluster"))
        self.assertEqual(result.returncode, 2)
        self.assertIn("--hosts requires --direct", result.stderr)
        remote = (ROOT / "monitor" / "start_tmux_monitoring_remote.sh").read_text()
        self.assertEqual(remote.count("start_tmux_monitoring.sh\" --direct"), 2)


if __name__ == "__main__":
    unittest.main()
