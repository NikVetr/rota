import importlib.util
import fcntl
import os
from pathlib import Path
import pty
import re
import select
import signal
import struct
import subprocess
import sys
import tempfile
import termios
import time
import unittest

spec = importlib.util.spec_from_file_location("cpu", Path(__file__).resolve().parents[1] / "monitor_cpu.py")
cpu = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cpu)


class CpuMonitorTests(unittest.TestCase):
    def test_terminal_resize_redraws_without_sampling(self):
        with tempfile.TemporaryDirectory() as directory:
            counter = Path(directory) / "samples"
            code = f'''
import runpy
from pathlib import Path
m=runpy.run_path({str(Path(cpu.__file__))!r})
class FakeTicks:
    def read(self):
        p=Path({str(counter)!r})
        n=int(p.read_text())+1 if p.exists() else 1
        p.write_text(str(n))
        return [(n*3,n,n*6,0)]*10
m['main'].__globals__['CpuTicks']=FakeTicks
m['main']()
'''
            master, slave = pty.openpty()
            fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 3, 149, 0, 0))
            process = subprocess.Popen([sys.executable, "-c", code, "--interval", "5"],
                                       stdout=slave, stderr=subprocess.PIPE)
            os.close(slave)

            def frame(timeout):
                deadline = time.monotonic() + timeout
                data = b""
                while time.monotonic() < deadline:
                    if select.select([master], [], [], 0.05)[0]:
                        data += os.read(master, 65536)
                        current = data.split(b"\x1b[H\x1b[J")[-1].decode()
                        if len(re.findall(r"\d+\[", current)) == 10:
                            return current
                self.fail(f"CPU frame missing: {data!r}")

            try:
                frame(8)
                for height, width in ((2, 100), (4, 60)):
                    fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", height, width, 0, 0))
                    process.send_signal(signal.SIGWINCH)
                    text = frame(1)
                    rows = re.findall(r"\x1b\[(\d+);1H([^\x1b]*)", text)
                    self.assertTrue(all(int(row) <= height and len(line) <= width for row, line in rows))
                    self.assertEqual(counter.read_text(), "2", "Resize triggered another CPU sample")
            finally:
                process.terminate()
                process.wait(timeout=3)
                os.close(master)
            error = process.stderr.read()
            process.stderr.close()
            self.assertEqual(process.returncode, 0, error)

    def test_all_cores_fit_without_wrapping(self):
        for count in (8, 10, 12, 20):
            values = [(0.6, 0.4)] * count
            for height in range(1, 13):
                for width in (22, 40, 60, 100, 149, 300):
                    with self.subTest(count=count, width=width, height=height):
                        lines = cpu.render(values, width, height)
                        self.assertLessEqual(len(lines), height)
                        self.assertTrue(all(len(line) <= width for line in lines), lines)
                        columns = (count + height - 1) // height
                        if (width - columns + 1) // columns >= len(str(count)) + 5:
                            ids = re.findall(r"(\d+)(?:\[|:)", " ".join(lines))
                            self.assertEqual(sorted(map(int, ids)), list(range(1, count + 1)))
                        else:
                            self.assertTrue(lines[0].startswith("Resize"))

    def test_short_wide_pane_uses_more_columns(self):
        lines = cpu.render([(0.25, 0.25)] * 10, 149, 2)
        self.assertEqual(len(lines), 2)
        self.assertEqual([len(re.findall(r"\d+\[", line)) for line in lines], [5, 5])
        self.assertTrue(all(len(line) == 149 for line in lines))
        self.assertIn("#", lines[0])
        self.assertIn("*", lines[0])
        self.assertIn("50.0%", lines[0])

    def test_stream_titles_its_pane_with_the_node_total(self):
        self.assertEqual(cpu.title_sequence("CPU node1", " ─── key", [(0.3, 0.1), (0.5, 0.0)]),
                         "\033]2;CPU node1 45% ─── key\033\\")
        code = f'''
import runpy
m=runpy.run_path({str(Path(cpu.__file__))!r})
class FakeTicks:
    n = 0
    def read(self):
        FakeTicks.n += 1
        return [(FakeTicks.n*3, FakeTicks.n, FakeTicks.n*6, 0)]*10
m['main'].__globals__['CpuTicks']=FakeTicks
m['main']()
'''
        process = subprocess.Popen([sys.executable, "-c", code, "--interval", "0.2", "--width", "149",
                                    "--height", "3", "--title-prefix", "CPU fx", "--title-suffix="],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            data = b""
            deadline = time.monotonic() + 5
            while b"\x1b]2;" not in data and time.monotonic() < deadline:
                if select.select([process.stdout], [], [], 0.1)[0]:
                    data += os.read(process.stdout.fileno(), 65536)
            self.assertIn(b"\x1b]2;CPU fx 40%\x1b\\", data)
        finally:
            process.terminate()
            process.wait(timeout=3)
            process.stdout.close()
            process.stderr.close()

    def test_tick_deltas_include_nice_and_wrap(self):
        self.assertEqual(cpu.usage([(10, 20, 30, 40)], [(30, 30, 90, 50)]), [(0.3, 0.1)])
        self.assertEqual(cpu.usage([(2**32 - 2, 0, 0, 0)], [(2, 0, 4, 0)]), [(0.5, 0.0)])
        self.assertEqual(cpu.usage([(1, 1, 1, 1)], [(1, 1, 1, 1)]), [(0.0, 0.0)])
        with self.assertRaisesRegex(RuntimeError, "CPU count"):
            cpu.usage([], [(1, 2, 3, 4)])


if __name__ == "__main__":
    unittest.main()
