"""Write the square of one integer to its own result file."""
import argparse
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("number", type=int)
parser.add_argument("output", type=Path)
parser.add_argument("--job_tag", required=True, help="supplied by the worker")
args = parser.parse_args()
args.output.write_text(f"{args.number ** 2}\n")
