#!/usr/bin/env bash
export PARALLEL_WORKERS_ROOT="${PARALLEL_WORKERS_ROOT:?Source the inventory environment first}"
set -euo pipefail

MODEL_DIR="${PARALLEL_WORKERS_ROOT}/stan_projects/test"
N_JOBS=100
TARGET_MINUTES=8
RUN_ID=""
QUEUE_FILE=""

usage() {
  cat <<EOF
Creates a queue that runs cmdstanr stochastic-volatility jobs via bench_job.R.

Usage:
  $0 [--model_dir <MODEL_DIR>] [--n_jobs <N>] [--target_minutes <mins>] [--run_id <id>] [--queue_file <name>]

Defaults:
  --model_dir       $MODEL_DIR
  --n_jobs          $N_JOBS
  --target_minutes  $TARGET_MINUTES

Outputs:
  <MODEL_DIR>/queues/<queue_file>
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --model_dir) MODEL_DIR="$2"; shift 2 ;;
    --n_jobs) N_JOBS="$2"; shift 2 ;;
    --target_minutes) TARGET_MINUTES="$2"; shift 2 ;;
    --run_id) RUN_ID="$2"; shift 2 ;;
    --queue_file) QUEUE_FILE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ -z "$RUN_ID" ]]; then
  RUN_ID="svbox_$(date +%Y%m%d_%H%M%S)"
fi

mkdir -p "$MODEL_DIR/queues"
if [[ -z "$QUEUE_FILE" ]]; then
  QUEUE_FILE="job_queue_${RUN_ID}.txt"
fi

QUEUE_PATH="$MODEL_DIR/queues/$QUEUE_FILE"
BENCH_R="${PARALLEL_WORKERS_ROOT}/stan_projects/test/benchmarks/bench_job.R"

if [[ ! -f "$BENCH_R" ]]; then
  echo "Missing $BENCH_R" >&2
  exit 1
fi

if ! [[ "$N_JOBS" =~ ^[0-9]+$ ]]; then
  echo "--n_jobs must be int" >&2
  exit 2
fi

{
  for i in $(seq 1 "$N_JOBS"); do
    printf "%d env PARALLEL_WORKERS_ROOT=%q Rscript %q --job_id %d --task stan_sv_box --run_id %q --target_minutes %q\n" \
      "$i" "$PARALLEL_WORKERS_ROOT" "$BENCH_R" "$i" "$RUN_ID" "$TARGET_MINUTES"
  done
} > "$QUEUE_PATH"

echo "Wrote: $QUEUE_PATH"
echo "Run id: $RUN_ID"
echo
echo "Start (10 workers per host):"
echo "  ${PARALLEL_WORKERS_ROOT}/start_workers.sh --model_dir $MODEL_DIR --queue $QUEUE_FILE --hosts_file ${PARALLEL_WORKERS_HOSTS_FILE}"
echo
echo "Summarize:"
echo "  Rscript ${PARALLEL_WORKERS_ROOT}/stan_projects/test/benchmarks/summarize_results.R --run_id $RUN_ID"
