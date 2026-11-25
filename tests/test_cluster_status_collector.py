from contextlib import redirect_stderr
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import cluster_status_collector as collector

# Columns as monitor_queue_progress.sh --data_only prints them (see render_queue_progress.sh).
ACTIVE = ["0", "0", "0", "1793743249", "DRAINING", "sample-main [b00002]", "0-1-1-0-1-1 P97 DRAIN", "3", "419",
          "5738", "-1", "0.7", "2026/11/03 | 14:00", "1793743249", "", "tmp/sample/queues/main.txt",
          "_srv_cluster_tmp_sample_main.txt__h021a__b00002", "example-study", "12", "13:51", "2"]
FIXED = ["0", "0", "0", "1793700000", "RUNNING", "fixed", "8-8-8", "1", "10", "100", "900", "10.0",
         "2026/11/03 | 03:31", "1793700000", "", "project/queues/fixed.txt", "fixed_tag", "?", "0", "", "0"]
DONE = ["1", "2", "0", "1793743233", "COMPLETED", "sample-pilot [b00001]", "0-0-0-0-0-0 P100 EMPTY", "6", "6",
        "3445", "-1", "100.0", "2026/11/03 | 14:00", "1793746678", "2026/11/03 | 14:57",
        "tmp/sample/queues/pilot.txt", "pilot_tag", "example-study", "0", "", "0"]
TSV = "".join("\t".join(row) + "\n" for row in (ACTIVE, FIXED, DONE))
SAMPLE = {"kind": "mac", "time": 1.0, "load": [1.0, 2.0, 3.0],
          "cpu": {"cores": 2, "busy_pct": 50.0, "user_pct": 40.0, "sys_pct": 10.0, "per_core": [60.0, 40.0],
                  "per_core_user_sys": [[0.5, 0.1], [0.3, 0.1]]},
          "memory": {"total_bytes": 100, "used_bytes": 60, "cache_bytes": 20, "available_bytes": 40,
                     "free_bytes": 20, "swap_used_bytes": 0, "used_pct": 60, "pressure": "normal",
                     "pressure_free_pct": 50},
          "mount": {"root": "/srv/share", "state": "missing", "error": None, "checked": 1.0},
          "disk": {"root_total_bytes": 1000, "root_free_bytes": 400, "root_used_bytes": 600, "error": None}}
IO = {"epoch": 1, "interface": "en0", "pool": {"state": "active", "capacity": 50, "slots": {"here": 3, "far": 2}},
      "network": {"rx_bytes_per_s": 1000, "tx_bytes_per_s": 2000},
      "smbd": {"cpu_pct": 4.5, "sessions": 6, "recv_queue_bytes": 0, "send_queue_bytes": 0},
      "disk": {"name": "disk4", "bytes_per_s": 5, "read_bytes_per_s": None, "write_bytes_per_s": None,
               "ops_per_s": 1.0, "kb_per_op": 4.0, "total_bytes": 4000, "used_bytes": 3000,
               "free_bytes": 1000, "error": None},
      "registry": {"active_queues": 2, "locks_held": 0, "oldest_lock_sec": 0}}
SCHEDULER = {"schema": 1, "updated": 1.0, "pool": {"id": "p1", "state": "ACTIVE"},
             "hosts": [{"id": "far", "away": True}, {"id": "storage", "away": False}],
             "queues": [{"queue_id": "batch_00002_q", "tag": ACTIVE[16], "source_dir": "/home/u/repo",
                         "submitted_by": "claude:abc", "usage": {"jobs": 2, "cpu_hours_succeeded": 1.5}}],
             "retired_queues": [{"queue_id": "batch_00001_q", "tag": DONE[16],
                                 "usage": {"jobs": 9, "cpu_hours_succeeded": 4.0}}],
             "commands": ["pool-drain"]}
FAKE_SSH = r'''#!/usr/bin/env python3
import json, os, sys, time
with open(os.environ["FAKE_SSH_LOG"], "a") as log:
    log.write(json.dumps(sys.argv[1:]) + "\n")
target, command = sys.argv[-2], sys.argv[-1]
if "monitor_cluster_io.sh" in command:
    while True:
        print(os.environ["FAKE_IO"], flush=True)
        time.sleep(0.2)
if "scheduler_snapshot.py" in command:
    print(os.environ["FAKE_SCHEDULER"])
    raise SystemExit(0)
if target.endswith("192.0.2.7"):
    print("a login banner line", flush=True)
    print(os.environ["FAKE_SAMPLE"], flush=True)
    time.sleep(0.3)
    print("Connection to 192.0.2.7 closed by remote host.", file=sys.stderr, flush=True)
    raise SystemExit(255)
print("ssh: connect to host 192.0.2.8 port 22: No route to host", file=sys.stderr)
raise SystemExit(255)
'''


class HostsTests(unittest.TestCase):
    def test_snapshot_lives_in_ram_behind_the_documented_path(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime, state = Path(directory) / "run", Path(directory) / "state"
            runtime.mkdir()
            with patch.dict(os.environ, {"XDG_RUNTIME_DIR": str(runtime)}):
                target = collector.runtime_snapshot(state)
                self.assertEqual(target, runtime / "rota" / "cluster-status.json")
                collector.atomic_write(target, "{}\n")
                link = state / "cluster-status.json"
                self.assertTrue(link.is_symlink())
                self.assertEqual(link.read_text(), "{}\n")
                self.assertEqual(collector.runtime_snapshot(state), target, "repeat calls must keep the link")
            with patch.dict(os.environ, {"XDG_RUNTIME_DIR": str(Path(directory) / "missing")}):
                self.assertEqual(collector.runtime_snapshot(state), state / "cluster-status.json")

    def test_hosts_file_labels_targets_and_errors(self):
        hosts = collector.parse_hosts("# comment\n\nu@192.0.2.11 10 node1\nu@192.0.2.15 8\nu@192.0.2.9\n")
        self.assertEqual(hosts, [{"id": "node1", "target": "u@192.0.2.11", "slots": 10},
                                 {"id": "192.0.2.15", "target": "u@192.0.2.15", "slots": 8},
                                 {"id": "192.0.2.9", "target": "u@192.0.2.9", "slots": None}])
        for text, message in (("", "no hosts"), ("-oProxyCommand=x 1 a\n", "invalid SSH target"),
                              ("u@a 1 node1\nu@b 1 node1\n", "repeats"), ("u@a -1 node1\n", "invalid slot count")):
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, message):
                collector.parse_hosts(text)

    def test_local_addresses_are_sampled_without_ssh(self):
        self.assertTrue(collector.is_local("user@127.0.0.1"))
        self.assertTrue(collector.is_local("localhost"))
        self.assertFalse(collector.is_local("user@192.0.2.1"))
        local = collector.stream_command("u@127.0.0.1", True, "monitor_sample.py", ["stream"])
        self.assertEqual(local, [sys.executable, str(ROOT / "monitor_sample.py"), "stream"])

    def test_remote_streams_use_dedicated_batch_connections(self):
        command = collector.stream_command("u@192.0.2.1", False, "monitor_sample.py", ["stream", "--root", "/a b"])
        self.assertEqual(command[0], "ssh")
        for option in ("BatchMode=yes", "ConnectTimeout=5", "ServerAliveInterval=5", "ServerAliveCountMax=2",
                       "ControlMaster=no", "ControlPath=none"):
            self.assertEqual(command[command.index(option) - 1], "-o", option)
        self.assertEqual(command[-2], "u@192.0.2.1")
        remote = shlex.split(command[-1])
        self.assertEqual(remote[:3], ["exec", "/bin/bash", "-lc"])
        self.assertIn('exec python3 "$HOME/rota-monitor/monitor_sample.py" stream --root \'/a b\'',
                      remote[3])

    def test_hosts_file_falls_back_to_cache_and_never_blocks(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(collector, "HOSTS_READ_TIMEOUT_SEC", 0.3):
            share, cache = Path(directory) / "hosts.txt", Path(directory) / "state" / "hosts.txt"
            source = collector.HostsSource(share, cache)
            with self.assertRaisesRegex(RuntimeError, "no cached copy"):
                source.read()
            share.write_text("u@192.0.2.1 4 storage\n")
            self.assertEqual(source.read(), ([{"id": "storage", "target": "u@192.0.2.1", "slots": 4}], "share"))
            self.assertEqual(stat.S_IMODE(cache.stat().st_mode), 0o600)
            share.unlink()
            hosts, origin = source.read()
            self.assertEqual(hosts[0]["id"], "storage")
            self.assertTrue(origin.startswith("cache (") and "No such file" in origin, origin)
            # A FIFO without a writer blocks open() the way a hung SMB mount does.
            os.mkfifo(share)
            started = time.monotonic()
            hosts, origin = source.read()
            self.assertLess(time.monotonic() - started, 2)
            self.assertIn("no reply within", origin)
            hosts, origin = source.read()
            self.assertIn("still blocked", origin)
            with open(share, "w"):  # release the blocked reader thread
                pass


class QueueTests(unittest.TestCase):
    def test_rows_become_queue_records(self):
        active, fixed, done = collector.parse_queue_rows(TSV)
        self.assertEqual(active, {
            "id": ACTIVE[16], "project": "example-study", "label": "sample-main", "batch": 2,
            "group": "active", "state": "DRAINING", "done": 3, "total": 419, "pct": 0.7, "eta": None,
            "started": 1793743249, "elapsed": 5738, "ended": None, "failed": 12, "failed_at": "13:51",
            "rerun": 2, "path": "tmp/sample/queues/main.txt", "workers": "0-1-1-0-1-1", "running": 4,
            "priority": 97, "priority_overrides": False, "admission": "DRAIN"})
        self.assertEqual((fixed["running"], fixed["priority"], fixed["admission"], fixed["eta"], fixed["batch"]),
                         (24, None, None, 900, None))
        self.assertEqual((done["group"], done["ended"], done["admission"]), ("recent", 1793746678, "EMPTY"))
        self.assertEqual(collector.parse_queue_rows("Queue Progress: no batch logs found.\n"), [])
        flagged, = collector.parse_queue_rows("\t".join([*ACTIVE, "recent_slower"]))
        self.assertEqual(flagged["eta_warning"], "recent_slower")
        with self.assertRaisesRegex(ValueError, "expected 21"):
            collector.parse_queue_rows("\t".join(ACTIVE[:18]) + "\n")

    def test_worker_columns_match_the_renderer(self):
        self.assertEqual(collector.parse_workers("2 run P90 PAUSED", "RUNNING")["running"], 2)
        self.assertIsNone(collector.parse_workers("1 run P40 STALE", "UNKNOWN")["running"])
        self.assertEqual(collector.parse_workers("0-. P50 FAIL", "PENDING"),
                         {"workers": "0-.", "running": 0, "priority": 50, "priority_overrides": False,
                          "admission": "FAIL"})
        self.assertEqual(collector.parse_workers("10-8 P-12", "RUNNING")["priority"], -12)
        # Per-host overrides mark the default priority with a star.
        self.assertEqual(collector.parse_workers("2-0-1 P50* PAUSED", "DRAINING"),
                         {"workers": "2-0-1", "running": 3, "priority": 50, "priority_overrides": True,
                          "admission": "PAUSED"})
        self.assertIsNone(collector.parse_workers("adaptive:n/a", "UNKNOWN")["workers"])


class StreamTests(unittest.TestCase):
    def run_stream(self, code, attempts):
        events, delays = [], []
        stream = collector.Stream("test", [sys.executable, "-c", code], events.append,
                                  lambda error: (events.append(error), len(events) >= attempts and stream.stopping.set()),
                                  lambda name, error: self.fail(error), stall_sec=30)
        stream.stopping.wait = lambda delay: delays.append(delay)
        stream.run()
        return events, delays

    def test_failures_back_off_from_five_to_sixty_seconds(self):
        events, delays = self.run_stream("print('ssh: no route'); raise SystemExit(255)", 7)
        self.assertEqual(delays, [5, 10, 20, 40, 60, 60, 60])
        self.assertEqual(events[0], "ssh: no route; retrying in 5 s")

    def test_a_stream_that_sampled_reconnects_quickly(self):
        events, delays = self.run_stream("print('{\"a\": 1}', flush=True); raise SystemExit(1)", 6)
        self.assertEqual(delays, [5, 5, 5])
        self.assertEqual(events[:2], [{"a": 1}, "stream exited with status 1; retrying in 5 s"])

    def test_a_silent_stream_is_killed(self):
        stream = collector.Stream("test", [sys.executable, "-c", "import time; time.sleep(30)"],
                                  None, None, None, stall_sec=0.2)
        result = {}
        worker = threading.Thread(target=lambda: result.setdefault("error", stream.run_once()))
        started = time.monotonic()
        worker.start()
        time.sleep(0.5)
        stream.check_stall()
        worker.join(5)
        self.assertEqual(result["error"], "no data for 0 s")
        self.assertLess(time.monotonic() - started, 5)


class CollectorTests(unittest.TestCase):
    def test_high_swap_reports_append_to_a_bounded_private_log(self):
        gib = 1024 ** 3
        report = {"processes": [{"pid": 7, "command": "leaky", "bytes": 30 * gib},
                                {"pid": 8, "command": "R", "bytes": gib}], "error": None}
        high = dict(SAMPLE, memory=dict(SAMPLE["memory"], swap_used_bytes=37 * gib), memory_top=report)
        with tempfile.TemporaryDirectory() as directory:
            events = Path(directory) / "state" / "memory-events.jsonl"
            status = collector.Collector(SimpleNamespace(queue_snapshot=Path(directory) / "q.tsv",
                                                         memory_events=events))
            status.hosts = [{"id": "storage", "ok": True, "mount": None}]
            status.host_sample("storage", SAMPLE)
            self.assertFalse(events.exists(), "a sample without a process list wrote an event")
            with redirect_stderr(io.StringIO()) as errors:
                status.host_sample("storage", high)
            self.assertIn("storage: swap 37.0G; largest: leaky (7) 30.0G, R (8) 1.0G", errors.getvalue())
            self.assertEqual(stat.S_IMODE(events.stat().st_mode), 0o600)
            first = json.loads(events.read_text())
            self.assertEqual((first["host"], first["swap_used_bytes"], first["processes"], first["error"]),
                             ("storage", 37 * gib, report["processes"], None))
            with patch.object(collector, "MEMORY_EVENTS_MAX_BYTES", 10), redirect_stderr(io.StringIO()):
                status.host_sample("storage", dict(high, time=2.0))
            self.assertEqual(json.loads(events.with_name("memory-events.jsonl.1").read_text()), first)
            self.assertEqual([json.loads(line)["time"] for line in events.read_text().splitlines()], [2.0])
            self.assertTrue(status.hosts[0]["ok"])

    def test_job_slots_come_from_the_pool_or_without_one_from_the_hosts_file(self):
        status = collector.Collector(SimpleNamespace(queue_snapshot=Path("/unused/q.tsv")))
        status.hosts = [{"id": "node1", "local": False}, {"id": "storage", "local": False}]
        status.file_slots = {"node1": 10, "storage": 6}
        status.shared = {"host": "storage", "ok": False}
        for pool, expected in (({"state": "inactive"}, [10, 6]), ({"state": "unavailable"}, [None, None]),
                               ({"state": "draining", "slots": {"node1": 1, "storage": 0}}, [1, 0]),
                               ({"state": "active", "slots": None}, [None, None])):
            with self.subTest(pool=pool):
                with redirect_stderr(io.StringIO()):
                    status.io_sample(dict(IO, pool=pool))
                snapshot = json.loads(status.snapshot())
                self.assertEqual([host["slots"] for host in snapshot["hosts"]], expected)
                self.assertNotIn("slots", snapshot["shared"]["pool"])
        for slots in ([4], {"node1": -1}, {"node1": 2.5}, {"node1": True}):
            with self.subTest(slots=slots), self.assertRaisesRegex(ValueError, "invalid pool job slots"):
                status.io_sample(dict(IO, pool={"state": "active", "slots": slots}))

    def test_snapshot_from_local_remote_down_and_queue_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            scripts, binary, state = base / "scripts", base / "bin", base / "state"
            scripts.mkdir()
            binary.mkdir()
            for name in ("cluster_status_collector.py", "cluster_config.py", "failure_events.py", "queue_eta_history.py", "monitor_sample.py", "monitor_cpu.py",
                         "monitor_mac.py", "monitor_linux.py"):
                shutil.copy(ROOT / name, scripts / name)
            (scripts / "monitor_queue_progress.sh").touch()
            (scripts / "monitor_queue_workers.sh").touch()
            fake_collect = scripts / "monitor_queue_collect.sh"
            fake_collect.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$FAKE_COLLECT_LOG"\ncat "$FAKE_TSV"\n')
            fake_collect.chmod(0o755)
            (binary / "ssh").write_text(FAKE_SSH)
            (binary / "ssh").chmod(0o755)
            (base / "rows.tsv").write_text(TSV)
            hosts = base / "hosts.txt"
            hosts.write_text("u@127.0.0.1 4 here\nu@192.0.2.7 4 far\nu@192.0.2.8 6 storage\n")
            env = dict(os.environ, PATH=f"{binary}{os.pathsep}{os.environ['PATH']}", XDG_STATE_HOME=str(base / "xdg"),
                       FAKE_SSH_LOG=str(base / "ssh.log"), FAKE_COLLECT_LOG=str(base / "collect.log"),
                       FAKE_TSV=str(base / "rows.tsv"), FAKE_SAMPLE=json.dumps(SAMPLE), FAKE_IO=json.dumps(IO),
                       FAKE_SCHEDULER=json.dumps(SCHEDULER))
            env.pop("ROTA_COORDINATOR", None)
            result = subprocess.run(
                [sys.executable, str(scripts / "cluster_status_collector.py"), "--root", "/nonexistent/share",
                 "--coordinator", "u@192.0.2.8", "--hosts_file", str(hosts), "--hosts_cache", str(state / "hosts.txt"),
                 "--snapshot", str(state / "cluster-status.json"), "--queue_snapshot", str(state / "queues.tsv"),
                 "--failure_events", str(state / "events.jsonl"), "--interval", "0.5", "--run_seconds", "4"], env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 0, result.stderr)
            time.sleep(0.3)
            leftover = subprocess.run(["pgrep", "-f", str(binary / "ssh")], capture_output=True, text=True)
            self.assertEqual(leftover.stdout, "", "a stream outlived the collector")
            self.assertFalse((base / "xdg").exists(), "explicit output paths were ignored")
            for name in ("cluster-status.json", "queues.tsv", "queues.tsv.details", "hosts.txt", "events.jsonl.state.json", "queue-eta.sqlite3"):
                self.assertEqual(stat.S_IMODE((state / name).stat().st_mode), 0o600, name)
            self.assertEqual(sorted(path.name for path in state.iterdir()),
                             ["cluster-status.json", "events.jsonl.state.json", "hosts.txt", "queue-eta.sqlite3", "queues.tsv", "queues.tsv.details"],
                             "a temporary file was left behind")
            self.assertEqual((state / "queues.tsv").read_text(), TSV)
            snapshot = json.loads((state / "cluster-status.json").read_text())
            ssh_calls = [json.loads(line) for line in (base / "ssh.log").read_text().splitlines()]
            collect_calls = (base / "collect.log").read_text().splitlines()

        # The local host never goes through SSH; each remote stream gets its own connection.
        self.assertFalse(any("127.0.0.1" in " ".join(call) for call in ssh_calls))
        self.assertTrue(all(call[call.index("ControlPath=none") - 1] == "-o" for call in ssh_calls))
        self.assertEqual(sum("monitor_cluster_io.sh" in call[-1] for call in ssh_calls), 1)
        self.assertIn("--hosts u@127.0.0.1 u@192.0.2.7 u@192.0.2.8 --fallback_host u@192.0.2.8 --mode remote",
                      collect_calls[0])
        self.assertIn("3 hosts from share", result.stderr)
        self.assertEqual(snapshot["schema"], 1)
        self.assertLess(time.time() - snapshot["updated"], 10)
        self.assertEqual(snapshot["collector"]["local_host"], "here")
        here, far, storage = snapshot["hosts"]
        self.assertEqual([host["id"] for host in snapshot["hosts"]], ["here", "far", "storage"])
        self.assertTrue(here["local"] and here["ok"])
        self.assertEqual((here["kind"], here["mount"]["state"], here["mount"]["ok"]), ("linux", "missing", False))
        self.assertTrue(here["cpu"]["per_core_user_sys"])
        # A host that drops keeps its last values, marked stale with the reason.
        self.assertFalse(far["ok"])
        self.assertIn("closed by remote host", far["error"])
        self.assertIn("retrying in 5 s", far["error"])
        self.assertEqual((far["cpu"], far["memory"]), (SAMPLE["cpu"], SAMPLE["memory"]))
        self.assertIsNotNone(far["updated"])
        self.assertEqual(far["mount"]["since"], far["updated"])
        self.assertEqual((storage["ok"], storage["cpu"], storage["updated"]), (False, None, None))
        self.assertIn("No route to host", storage["error"])
        # Pool budgets override the hosts file's counts; a host outside the pool has no limit.
        self.assertEqual([host["slots"] for host in snapshot["hosts"]], [3, 2, None])
        shared = snapshot["shared"]
        self.assertEqual((shared["host"], shared["ok"], shared["network"]), ("storage", True, IO["network"]))
        self.assertEqual(shared["pool"], {"state": "active", "capacity": 50, "in_use": 28})
        self.assertEqual([queue["label"] for queue in snapshot["queues"]], ["sample-main", "fixed", "sample-pilot"])
        self.assertTrue(snapshot["queue_refresh"]["ok"])
        # The coordinator's controls join queue rows by batch tag; hosts carry their away state.
        self.assertTrue(snapshot["scheduler"]["ok"])
        self.assertEqual(snapshot["scheduler"]["pool"]["id"], "p1")
        main, fixed, done = snapshot["queues"]
        self.assertEqual((main["control_id"], main["source_dir"], main["submitted_by"]),
                         ("batch_00002_q", "/home/u/repo", "claude:abc"))
        self.assertEqual((fixed["control_id"], fixed["submitted_by"]), (None, None))
        # CPU usage comes from the live entry, or the retired one once a queue has finished.
        self.assertEqual([queue["usage"] for queue in snapshot["queues"]],
                         [{"jobs": 2, "cpu_hours_succeeded": 1.5}, None, {"jobs": 9, "cpu_hours_succeeded": 4.0}])
        self.assertEqual([host["away"] for host in snapshot["hosts"]], [None, True, False])

    def test_collect_command_matches_the_queue_pane(self):
        with tempfile.TemporaryDirectory() as directory:
            log, tsv = Path(directory) / "log", Path(directory) / "rows.tsv"
            out = Path(directory) / "out.tsv"
            detailed = "\n".join(row + "\t" + warning for row, warning in
                                  zip(TSV.splitlines(), ("recent_slower", "-", "-"))) + "\n"
            tsv.write_text(detailed)
            fake = Path(directory) / "collect.sh"
            fake.write_text('#!/bin/sh\nfor a in "$@"; do printf "%s\\n" "$a"; done > "$LOG"\ncat "$TSV"\n')
            fake.chmod(0o755)
            args = type("Args", (), dict(root="/srv/cluster", queue_snapshot=out,
                                         no_queue_registry=False))()
            instance = collector.Collector(args)
            instance.hosts = [{"target": "u@a"}, {"target": "u@b"}]
            instance.coordinator = ("storage", "u@192.0.2.8")
            with patch.object(collector, "SCRIPT_DIR", Path(directory)), \
                    patch.dict(os.environ, LOG=str(log), TSV=str(tsv)):
                fake.rename(Path(directory) / "monitor_queue_collect.sh")
                instance.collect_queues()
            self.assertEqual(out.read_text(), TSV)
            self.assertEqual(out.with_suffix(".tsv.details").read_text(), detailed)
            assert instance.queues is not None
            self.assertEqual(instance.queues[0]["eta_warning"], "recent_slower")
            arguments = log.read_text().splitlines()
        self.assertEqual(arguments, ["--root", "/srv/cluster", "--collector_script",
                                     f"{directory}/monitor_queue_progress.sh", "--worker_script",
                                     f"{directory}/monitor_queue_workers.sh", "--hosts", "u@a u@b",
                                     "--fallback_host", "u@192.0.2.8", "--mode", "remote", "--limit", "0",
                                     "--timeout_sec", "120", "--eta_warnings"])
        self.assertEqual(instance.queue_refresh["ok"], True)
        self.assertEqual(len(instance.queues), 3)


class InstallTests(unittest.TestCase):
    def test_installer_writes_a_low_weight_user_unit(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            fake = base / "systemctl"
            fake.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$FAKE_SYSTEMCTL_LOG"\n')
            fake.chmod(0o755)
            generated = base / "rendered"
            generated.mkdir()
            settings = {"ROTA_COORDINATOR": "user@storage", "ROTA_ROOT": "/srv/sample"}
            (generated / "monitor-environment.json").write_text(json.dumps(settings))
            env = dict(os.environ, SYSTEMCTL=str(fake), XDG_CONFIG_HOME=str(base / "config"),
                       FAKE_SYSTEMCTL_LOG=str(base / "calls"), HOME=str(base / "home"),
                       ROTA_CONFIG_DIR=str(generated))
            script = str(ROOT / "admin" / "install_cluster_status_collector.sh")
            printed = subprocess.run(["bash", script, "--print"], env=env, capture_output=True, text=True)
            self.assertEqual(printed.returncode, 0, printed.stderr)
            self.assertFalse((base / "calls").exists(), "--print touched systemd")
            installed = subprocess.run(["bash", script], env=env, capture_output=True, text=True)
            self.assertEqual(installed.returncode, 0, installed.stderr)
            local_settings = base / "config/rota/monitor-environment.json"
            self.assertEqual(json.loads(local_settings.read_text()), settings)
            unit = (base / "config" / "systemd" / "user" / "rota-collector.service").read_text()
            calls = (base / "calls").read_text().splitlines()
            link = base / "home/.local/bin/rota"
            self.assertEqual(os.readlink(link), str(ROOT / "rota"))
            removed = subprocess.run(["bash", script, "--uninstall"], env=env, capture_output=True, text=True)
            self.assertFalse(link.is_symlink(), "uninstall left the rota command behind")
            self.assertEqual(removed.returncode, 0, removed.stderr)
            self.assertFalse(local_settings.exists())
            remaining = list((base / "config" / "systemd" / "user").iterdir())
            all_calls = (base / "calls").read_text().splitlines()
        self.assertEqual(unit, printed.stdout)
        self.assertNotIn("@REPO_DIR@", unit)
        self.assertIn(f"ExecStart=/usr/bin/python3 {ROOT}/cluster_status_collector.py\n", unit)
        for setting in ("Restart=always", "Nice=10", "CPUQuota=20%", "MemoryMax=256M", "WantedBy=default.target"):
            self.assertIn(setting + "\n", unit)
        self.assertEqual(calls, ["--user daemon-reload", "--user enable rota-collector.service",
                                 "--user restart rota-collector.service"])
        self.assertEqual(all_calls[3:], ["--user disable --now rota-collector.service",
                                         "--user daemon-reload"])
        self.assertEqual(remaining, [])


if __name__ == "__main__":
    unittest.main()
