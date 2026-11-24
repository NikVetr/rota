#!/usr/bin/env bash

set -u

run_one() {
  local state_dir="$1"
  local tag_clean="$2"
  local job_id="$3"
  local command_file="$state_dir/commands/job_${job_id}.command"
  local log_dir="$state_dir/jobs"
  local status_dir="$state_dir/status"
  local job_command local_log status_tmp running_file exit_code

  if [ ! -f "$command_file" ]; then
    echo "Missing local command file: $command_file" >&2
    return 125
  fi

  mkdir -p "$log_dir" "$status_dir"
  local_log="$log_dir/job_${job_id}.log"
  running_file="$status_dir/job_${job_id}.running"
  job_command="$(<"$command_file")"
  printf 'COMMAND: %s\n' "$job_command" > "$local_log"
  printf '%s\n' "$(date +%s)" > "${running_file}.tmp.$$"
  mv -f "${running_file}.tmp.$$" "$running_file"

  eval "$job_command --job_tag '$tag_clean'" >> "$local_log" 2>&1
  exit_code=$?

  if [ "$exit_code" -eq 0 ]; then
    status_tmp="$status_dir/.job_${job_id}.ok.$$"
    printf '%s\n' "$(date +%s)" > "$status_tmp"
    mv -f "$status_tmp" "$status_dir/job_${job_id}.ok"
  else
    status_tmp="$status_dir/.job_${job_id}.failed.$$"
    printf 'exit_code=%s\nfinished_epoch=%s\n' "$exit_code" "$(date +%s)" > "$status_tmp"
    mv -f "$status_tmp" "$status_dir/job_${job_id}.failed"
  fi
  rm -f "$running_file"

  return "$exit_code"
}

if [ "${1:-}" = "--run-one" ]; then
  if [ "$#" -ne 4 ]; then
    echo "Usage: $0 --run-one <state_dir> <tag_clean> <job_id>" >&2
    exit 2
  fi
  run_one "$2" "$3" "$4"
  exit $?
fi

if [ "$#" -lt 6 ] || [ "$#" -gt 7 ]; then
  echo "Usage: $0 <state_dir> <jobs> <tag_clean> <host_key> <configured_host> <expected_jobs> [progress_sync_sec]" >&2
  exit 2
fi

STATE_DIR="$1"
NUM_JOBS="$2"
TAG_CLEAN="$3"
HOST_KEY="$4"
CONFIGURED_HOST="$5"
EXPECTED_JOBS="$6"
PROGRESS_SYNC_SEC="${7:-30}"
COMMAND_FILE="$STATE_DIR/commands.txt"
COMMAND_DIR="$STATE_DIR/commands"
IDS_FILE="$STATE_DIR/job_ids.txt"
LOCAL_JOB_LOG="$STATE_DIR/parallel-joblog.tsv"
LOCAL_RUNNER="$STATE_DIR/mmshardworker.sh"
LOCAL_STATUS_DIR="$STATE_DIR/status"
LOCAL_PROGRESS_DIR="$STATE_DIR/progress"
STOP_FILE="$STATE_DIR/progress.stop"
CONTROLLER_PID=""
PUBLISHER_PID=""
PARALLEL_WORKER_READY_FILE="${PARALLEL_WORKER_READY_FILE:-}"
PARALLEL_WORKER_START_FILE="${PARALLEL_WORKER_START_FILE:-}"
PARALLEL_WORKER_RUN_TOKEN="${PARALLEL_WORKER_RUN_TOKEN:-}"
PARALLEL_WORKER_RUNNER_SHA256="${PARALLEL_WORKER_RUNNER_SHA256:-}"

case "$NUM_JOBS:$EXPECTED_JOBS:$PROGRESS_SYNC_SEC" in
  *[!0-9:]*|:*|*::*|*:)
    echo "jobs, expected_jobs, and progress_sync_sec must be non-negative integers." >&2
    exit 2
    ;;
esac
if [ "$NUM_JOBS" -lt 1 ] || [ "$EXPECTED_JOBS" -lt 1 ] || [ "$PROGRESS_SYNC_SEC" -lt 1 ]; then
  echo "jobs, expected_jobs, and progress_sync_sec must all be greater than zero." >&2
  exit 2
fi
case "$HOST_KEY:$TAG_CLEAN" in
  *[!A-Za-z0-9_.:-]*)
    echo "host_key and tag_clean contain unsupported characters." >&2
    exit 2
    ;;
esac
if [ ! -f "$COMMAND_FILE" ] || [ ! -f "$LOCAL_RUNNER" ]; then
  echo "Host-local shard state is incomplete under $STATE_DIR" >&2
  exit 1
fi

export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
if ! command -v parallel >/dev/null 2>&1; then
  echo "GNU Parallel is required for host-local-shards mode." >&2
  exit 127
fi

mkdir -p "$COMMAND_DIR" "$LOCAL_STATUS_DIR" "$LOCAL_PROGRESS_DIR" "$STATE_DIR/jobs"
rm -f "$STOP_FILE"

summarize_ids() {
  local suffix="$1"
  local output_file="$2"
  local status_file job_id output_tmp="${output_file}.tmp.$$"

  : > "$output_tmp"
  for status_file in "$LOCAL_STATUS_DIR"/job_*."$suffix"; do
    [ -f "$status_file" ] || continue
    job_id="${status_file##*/job_}"
    job_id="${job_id%.$suffix}"
    printf '%s\n' "$job_id" >> "$output_tmp"
  done
  LC_ALL=C sort -u "$output_tmp" -o "$output_tmp"
  mv -f "$output_tmp" "$output_file"
}

publish_progress() {
  local shard_state="$1"
  local succeeded_file="$LOCAL_PROGRESS_DIR/succeeded_ids.txt"
  local failed_file="$LOCAL_PROGRESS_DIR/failed_ids.txt"
  local progress_file="$LOCAL_PROGRESS_DIR/progress.env"
  local progress_tmp="${progress_file}.tmp.$$"
  local succeeded failed running finished started updated_epoch

  summarize_ids ok "$succeeded_file"
  summarize_ids failed "$failed_file"
  succeeded="$(awk 'END {print NR + 0}' "$succeeded_file")"
  failed="$(awk 'END {print NR + 0}' "$failed_file")"
  running="$(find "$LOCAL_STATUS_DIR" -maxdepth 1 -type f -name 'job_*.running' -print 2>/dev/null | awk 'END {print NR + 0}')"
  finished=$((succeeded + failed))
  started=$((finished + running))
  if [ "$started" -gt "$EXPECTED_JOBS" ]; then
    started="$EXPECTED_JOBS"
  fi
  updated_epoch="$(date +%s)"

  {
    echo "state=$shard_state"
    echo "host_key=$HOST_KEY"
    echo "configured_host=$CONFIGURED_HOST"
    echo "runtime_host=$(hostname)"
    echo "worker_slots=$NUM_JOBS"
    echo "total_jobs=$EXPECTED_JOBS"
    echo "finished_jobs=$finished"
    echo "running_jobs=$running"
    echo "started_jobs=$started"
    echo "succeeded_jobs=$succeeded"
    echo "failed_jobs=$failed"
    echo "updated_epoch=$updated_epoch"
    echo "controller_pid=$$"
    echo "tag_clean=$TAG_CLEAN"
    echo "local_state_dir=$STATE_DIR"
  } > "$progress_tmp"
  mv -f "$progress_tmp" "$progress_file"

}

publish_scheduler_ready() {
  local ready_tmp

  [ -n "$PARALLEL_WORKER_READY_FILE" ] || return 0
  if [ -z "$PARALLEL_WORKER_RUN_TOKEN" ]; then
    echo "PARALLEL_WORKER_RUN_TOKEN is required when readiness is enabled." >&2
    return 1
  fi
  ready_tmp="${PARALLEL_WORKER_READY_FILE}.tmp.$$"
  {
    echo "state=ready"
    echo "run_token=$PARALLEL_WORKER_RUN_TOKEN"
    echo "tag_clean=$TAG_CLEAN"
    echo "runner_sha256=$PARALLEL_WORKER_RUNNER_SHA256"
    echo "pid=$$"
    echo "host_key=$HOST_KEY"
    echo "host=$(hostname)"
    echo "ready_epoch=$(date +%s)"
  } > "$ready_tmp" || return 1
  mv -f "$ready_tmp" "$PARALLEL_WORKER_READY_FILE"
}

wait_for_scheduler_start() {
  local gate_token gate_tag gate_epoch now_epoch

  [ -n "$PARALLEL_WORKER_START_FILE" ] || return 0
  while [ ! -f "$PARALLEL_WORKER_START_FILE" ]; do
    sleep 0.2
  done
  gate_token="$(awk -F= '$1 == "run_token" {print $2; exit}' "$PARALLEL_WORKER_START_FILE")"
  gate_tag="$(awk -F= '$1 == "tag_clean" {print $2; exit}' "$PARALLEL_WORKER_START_FILE")"
  gate_epoch="$(awk -F= '$1 == "start_epoch" {print $2; exit}' "$PARALLEL_WORKER_START_FILE")"
  if [ "$gate_token" != "$PARALLEL_WORKER_RUN_TOKEN" ] || [ "$gate_tag" != "$TAG_CLEAN" ]; then
    echo "Invalid scheduler start gate: $PARALLEL_WORKER_START_FILE" >&2
    return 1
  fi
  case "$gate_epoch" in
    ''|*[!0-9]*)
      echo "Invalid scheduler start epoch in $PARALLEL_WORKER_START_FILE" >&2
      return 1
      ;;
  esac
  now_epoch="$(date +%s)"
  while [ "$now_epoch" -lt "$gate_epoch" ]; do
    sleep 0.2
    now_epoch="$(date +%s)"
  done
}

progress_loop() {
  while [ ! -f "$STOP_FILE" ]; do
    publish_progress RUNNING
    sleep "$PROGRESS_SYNC_SEC"
  done
}

stop_children() {
  : > "$STOP_FILE"
  if [ -n "$PUBLISHER_PID" ]; then
    kill -TERM "$PUBLISHER_PID" 2>/dev/null || true
    wait "$PUBLISHER_PID" 2>/dev/null || true
  fi
  if [ -n "$CONTROLLER_PID" ] && kill -0 "$CONTROLLER_PID" 2>/dev/null; then
    kill -TERM "$CONTROLLER_PID" 2>/dev/null || true
    wait "$CONTROLLER_PID" 2>/dev/null || true
  fi
  publish_progress INTERRUPTED
  exit 143
}
trap stop_children TERM INT

if [ ! -f "$STATE_DIR/commands.prepared" ]; then
  prepared_jobs=0
  : > "$IDS_FILE"
  while IFS= read -r job_line || [ -n "$job_line" ]; do
    job_line="${job_line%$'\r'}"
    trimmed_line="${job_line#"${job_line%%[![:space:]]*}"}"
    [ -n "$trimmed_line" ] || continue
    job_id="${trimmed_line%%[[:space:]]*}"
    job_command="${trimmed_line#"$job_id"}"
    job_command="${job_command#"${job_command%%[![:space:]]*}"}"
    case "$job_id" in
      ''|*[!A-Za-z0-9_.:-]*)
        echo "Unsupported job ID '$job_id'; host-local-shards IDs may contain A-Z, a-z, 0-9, dot, colon, underscore, or hyphen." >&2
        exit 2
        ;;
    esac
    if [ -z "$job_command" ]; then
      echo "Job $job_id has no command." >&2
      exit 2
    fi
    if [ -e "$COMMAND_DIR/job_${job_id}.command" ]; then
      echo "Duplicate job ID in shard: $job_id" >&2
      exit 2
    fi
    printf '%s\n' "$job_command" > "$COMMAND_DIR/job_${job_id}.command"
    printf '%s\n' "$job_id" >> "$IDS_FILE"
    prepared_jobs=$((prepared_jobs + 1))
  done < "$COMMAND_FILE"

  if [ "$prepared_jobs" -ne "$EXPECTED_JOBS" ]; then
    echo "Shard expected $EXPECTED_JOBS jobs but prepared $prepared_jobs." >&2
    exit 2
  fi
  printf 'prepared_jobs=%s\nprepared_epoch=%s\n' "$prepared_jobs" "$(date +%s)" > "$STATE_DIR/commands.prepared"
fi

publish_progress RUNNING
if ! publish_scheduler_ready; then
  echo "Unable to publish node-local shard readiness." >&2
  publish_progress FAILED
  exit 1
fi
if ! wait_for_scheduler_start; then
  publish_progress FAILED
  exit 1
fi
progress_loop &
PUBLISHER_PID=$!

parallel --jobs "$NUM_JOBS" --joblog "$LOCAL_JOB_LOG" --halt never \
  "bash '$LOCAL_RUNNER' --run-one '$STATE_DIR' '$TAG_CLEAN' {}" :::: "$IDS_FILE" &
CONTROLLER_PID=$!
wait "$CONTROLLER_PID"
PARALLEL_RC=$?
CONTROLLER_PID=""

: > "$STOP_FILE"
kill -TERM "$PUBLISHER_PID" 2>/dev/null || true
wait "$PUBLISHER_PID" 2>/dev/null || true
PUBLISHER_PID=""

succeeded_jobs="$(find "$LOCAL_STATUS_DIR" -maxdepth 1 -type f -name 'job_*.ok' -print 2>/dev/null | awk 'END {print NR + 0}')"
failed_jobs="$(find "$LOCAL_STATUS_DIR" -maxdepth 1 -type f -name 'job_*.failed' -print 2>/dev/null | awk 'END {print NR + 0}')"
finished_jobs=$((succeeded_jobs + failed_jobs))
if [ "$failed_jobs" -eq 0 ] && [ "$finished_jobs" -eq "$EXPECTED_JOBS" ]; then
  FINAL_STATE=COMPLETED
else
  FINAL_STATE=FAILED
fi
publish_progress "$FINAL_STATE"

printf 'state=%s\nparallel_exit=%s\nfinished_epoch=%s\n' \
  "$FINAL_STATE" "$PARALLEL_RC" "$(date +%s)" > "$STATE_DIR/final.env"

if [ "$FINAL_STATE" = COMPLETED ]; then
  exit 0
fi
exit 1
