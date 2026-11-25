import importlib.util
import contextlib
import io
from pathlib import Path
import sys
import os
import signal
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "monitor"))
import monitor_linux as linux
import monitor_cpu as cpu


class LinuxMonitorTests(unittest.TestCase):
    @unittest.skipUnless(sys.platform.startswith("linux"), "native Linux sampler")
    def test_memory_frame_fits_pane_without_scrolling(self):
        for height in (1, 3, 4):
            output = io.StringIO()
            with patch.object(sys, "argv", ["monitor_linux.py", "memory"]), \
                    patch.object(linux.shutil, "get_terminal_size", return_value=os.terminal_size((26, height))), \
                    patch.object(linux.time, "sleep", side_effect=SystemExit(0)), \
                    contextlib.redirect_stdout(output), self.assertRaises(SystemExit):
                linux.main()
            frame = output.getvalue()
            self.assertFalse(frame.endswith("\n"))
            self.assertEqual(len(frame.splitlines()), height)
            self.assertIn("G[", frame.splitlines()[0])

    def test_cpu_guest_not_double_counted_and_iowait_is_idle(self):
        values = cpu.linux_ticks("cpu 1 2 3 4\ncpu0 100 10 20 50 5 2 3 4 30 6\nintr 123\n")
        self.assertEqual(values, [(100, 29, 55, 10)])
        with self.assertRaises(ValueError):
            cpu.linux_ticks("cpu0 1 2 3\n")

    def test_memory_available_and_shared_cache(self):
        text = "\n".join(f"{key}: {value} kB" for key, value in {
            "MemTotal": 8388608, "MemAvailable": 5242880, "MemFree": 1048576,
            "Cached": 4194304, "Buffers": 1024, "SReclaimable": 1024,
            "Shmem": 2048, "SwapTotal": 1048576, "SwapFree": 786432}.items())
        data = linux.memory(text)
        self.assertEqual(data["used"], 3*1024**3)
        self.assertEqual(data["cache"], 4*1024**3)
        self.assertEqual(data["swap"], 256*1024**2)
        for width in (18, 26, 50):
            self.assertTrue(all(len(line) <= width for line in linux.memory_lines(data, width, "2.00%")))
        with self.assertRaises(KeyError):
            linux.memory("MemTotal: 10 kB\n")

    def test_mountinfo_exact_match_and_escaped_spaces(self):
        text = "31 20 8:1 / /srv/shared\\040data rw - ext4 /dev/sda1 rw\n"
        self.assertTrue(linux.mounted("/srv/shared data", text))
        self.assertFalse(linux.mounted("/srv/shared", text))
        with self.assertRaises(ValueError):
            linux.mounted("/srv/shared", "bad data")

    def test_network_disk_and_smb_units(self):
        route = "Iface Destination Gateway Flags RefCnt Use Metric Mask\neth0 00000000 0 0003 0 0 100 0\n"
        self.assertEqual(linux.default_interface(route), "eth0")
        self.assertEqual(linux.network("eth0: 100 0 0 0 0 0 0 0 200 0 0 0 0 0 0 0", "eth0"), (100, 200))
        self.assertEqual(linux.disk_counters("10 0 100 0 20 0 200 0 0 0 0"), (30, 153600))
        self.assertEqual(linux.smb_connections("header\n0: 00000000:01BD 00000000:1234 01 00000010:00000020\n"), (1, 32, 16))
        with self.assertRaises(ValueError):
            linux.network("", "missing")

    @unittest.skipUnless(sys.platform.startswith("linux"), "native Linux sampler")
    def test_native_io_panel_produces_a_frame(self):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, TMPDIR=directory, ADAPTIVE_STATE_ROOT=directory)
            process = subprocess.Popen(["bash", str(ROOT / "monitor" / "monitor_cluster_io.sh"),
                                        "--root", directory, "--interface", "lo", "--refresh_sec", "2"],
                                       env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, start_new_session=True)
            try:
                time.sleep(3)
            finally:
                os.killpg(process.pid, signal.SIGTERM)
            output, error = process.communicate(timeout=5)
            self.assertIn("NET Mb/s", output, error)
            self.assertIn("Ops/s", output)
            self.assertNotIn("unbound variable", error)


if __name__ == "__main__":
    unittest.main()
