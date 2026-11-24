"""Exercise optional workflow argv using the actual shell source (also on Bash 3.2)."""
import os
from pathlib import Path
import re
import subprocess
import unittest

ROOT = Path(__file__).resolve().parents[1] / "scheduler"


def compatibility_script():
    scheduler = (ROOT / "adaptive_scheduler.sh").read_text()
    functions = []
    for name in ("die", "is_uint", "is_int", "env_value", "reject_record_breaks",
                 "validate_memory_calibration", "submit_queue"):
        functions.append(re.search(r"^" + name + r"\(\) \{.*?^\}", scheduler, re.M | re.S)[0])
    retry = (ROOT / "reconcile_queue_batch.sh").read_text()
    argv = re.search(r"^    ADAPTIVE_ARGS=\(.*?^    \)", retry, re.M | re.S)[0]
    # Capture the full expansion, including nested parameter expansion.
    after_line = next(line for line in (ROOT / "start_workers.sh").read_text().splitlines()
                      if '--state-root "$ADAPTIVE_STATE_ROOT"' in line and 'AFTER_QUEUES' in line)
    after_args = after_line.split('"$ADAPTIVE_STATE_ROOT" ', 1)[1].split(')"', 1)[0]
    return "set -eu\n" + "\n".join(functions) + r'''
START_WORKERS_SCRIPT=/bin/bash
STATE_ROOT=/unused
POOL_FILE="$(mktemp)"
trap 'rm -f "$POOL_FILE"' EXIT
printf 'shared_io_capacity=0\n' > "$POOL_FILE"
TRANSITION_LOCK=/unused
bash() { captured=("$@"); }
submit_queue --model_dir '/model with spaces' --queue /queue --memory_mib_per_job 128 --run_name probe
for arg in "${captured[@]}"; do [ -n "$arg" ]; done
submit_queue --model_dir /model --queue /queue --memory_mib_per_job 128 --run_name probe --after '/batch with spaces'
[ "${captured[${#captured[@]}-2]}" = --after ]
[ "${captured[${#captured[@]}-1]}" = '/batch with spaces' ]
submit_queue --model_dir /model --queue /queue --memory_mib_per_job 128 --run_name probe --retry_lost_jobs
[ "${captured[${#captured[@]}-1]}" = --retry_lost_jobs ]
MODEL_DIR=/model RETRY_QUEUE_PATH=/retry RUN_NAME=retry SCHEDULE_PRIORITY=50
CPU_UNITS=1 MEMORY_MIB=128 SHARED_IO_UNITS=0 MAX_WORKERS=0
WORKFLOW_ARGS=()
''' + argv + r'''
for arg in "${ADAPTIVE_ARGS[@]}"; do [ -n "$arg" ]; done
WORKFLOW_ARGS=(--after '/batch with spaces')
''' + argv + r'''
[ "${ADAPTIVE_ARGS[${#ADAPTIVE_ARGS[@]}-1]}" = '/batch with spaces' ]
AFTER_QUEUES=()
set -- ''' + after_args + r'''
[ "$#" -eq 0 ]
AFTER_QUEUES=('/batch with spaces')
set -- ''' + after_args + r'''
[ "$#" -eq 1 ] && [ "$1" = '/batch with spaces' ]
printf 'Workflow argv compatibility: PASS (Bash %s)\n' "$BASH_VERSION"
'''


class ShellCompatibilityTests(unittest.TestCase):
    def test_optional_workflow_arguments(self):
        result = subprocess.run([os.environ.get("TEST_BASH", "bash"), "-s"],
                                input=compatibility_script(), text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
