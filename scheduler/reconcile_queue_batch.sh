#!/usr/bin/env bash
export PARALLEL_WORKERS_ROOT="${PARALLEL_WORKERS_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
set -euo pipefail

MODEL_DIR=""
QUEUE_FILE="job_queue.txt"
BATCH_DIR=""
RETRY_QUEUE_FILE=""
RERUN_MISSING=0
DEPENDENCY_REPLACEMENTS=()
START_WORKERS_SCRIPT="${PARALLEL_WORKERS_ROOT}/start_workers.sh"
ADAPTIVE_SCHEDULER_SCRIPT="${ADAPTIVE_SCHEDULER_SCRIPT:-${PARALLEL_WORKERS_ROOT}/adaptive_scheduler.sh}"

read_state() {
  local file="$1" key="$2" count
  count="$(awk -F= -v key="$key" '$1 == key {n++} END {print n+0}' "$file")"
  [ "$count" -eq 1 ] || {
    echo "Error: $file must contain exactly one $key entry." >&2
    return 1
  }
  awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$file"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --model_dir)
      MODEL_DIR="$2"
      shift 2
      ;;
    --queue)
      QUEUE_FILE="$2"
      shift 2
      ;;
    --batch_dir)
      BATCH_DIR="$2"
      shift 2
      ;;
    --retry_queue)
      RETRY_QUEUE_FILE="$2"
      shift 2
      ;;
    --rerun_missing)
      RERUN_MISSING=1
      shift
      ;;
    --replace_dependency)
      [ "$#" -ge 3 ] || { echo "--replace_dependency requires OLD_BATCH NEW_BATCH" >&2; exit 1; }
      DEPENDENCY_REPLACEMENTS+=(--replace-dependency "$2" "$3")
      shift 3
      ;;
    --start_workers_script)
      START_WORKERS_SCRIPT="$2"
      shift 2
      ;;
    --adaptive_scheduler_script)
      ADAPTIVE_SCHEDULER_SCRIPT="$2"
      shift 2
      ;;
    *)
      echo "Error: Unknown argument: $1" >&2
      echo "Usage: $0 --model_dir <MODEL_DIR> [--queue <queue_file>] [--batch_dir <batch_dir>] [--retry_queue <name>] [--rerun_missing]" >&2
      exit 1
      ;;
  esac
done

if [ -z "$MODEL_DIR" ]; then
  echo "Error: --model_dir is required" >&2
  exit 1
fi

if [[ "${MODEL_DIR}" != */ ]]; then
  MODEL_DIR="${MODEL_DIR}/"
fi

if [[ "$QUEUE_FILE" = /* ]]; then
  ORIGINAL_QUEUE_FILE="$QUEUE_FILE"
  QUEUE_DIR="$(dirname "$QUEUE_FILE")"
  QUEUE_FILE="$(basename "$QUEUE_FILE")"
else
  QUEUE_DIR="${MODEL_DIR}queues"
  ORIGINAL_QUEUE_FILE="${QUEUE_DIR}/${QUEUE_FILE}"
fi

if [ -z "$BATCH_DIR" ] && [ ! -f "$ORIGINAL_QUEUE_FILE" ]; then
  echo "Error: Queue file not found: $ORIGINAL_QUEUE_FILE" >&2
  exit 1
fi

if [ -z "$BATCH_DIR" ]; then
  LOG_ROOT="${MODEL_DIR}logs"
  if [ ! -d "$LOG_ROOT" ]; then
    echo "Error: Logs directory not found: $LOG_ROOT" >&2
    exit 1
  fi
  while IFS= read -r candidate_dir; do
    [ -f "$candidate_dir/run_meta.env" ] || continue
    candidate_queue="$(awk -F= '$1 == "original_queue_file" {sub(/^[^=]*=/, ""); print; exit}' "$candidate_dir/run_meta.env")"
    if [ "$candidate_queue" = "$ORIGINAL_QUEUE_FILE" ]; then
      BATCH_DIR="$candidate_dir"
      break
    fi
  done < <(find "$LOG_ROOT" -maxdepth 1 -type d -name 'batch_*' -print | LC_ALL=C sort -r)
  if [ -z "$BATCH_DIR" ]; then
    echo "Error: No batch metadata under $LOG_ROOT matches $ORIGINAL_QUEUE_FILE" >&2
    echo "Pass --batch_dir explicitly only for a legacy batch without run_meta.env." >&2
    exit 1
  fi
fi

if [ ! -d "$BATCH_DIR" ]; then
  echo "Error: Could not determine a valid batch directory" >&2
  exit 1
fi

EXECUTION_MODE="shared-queue"
if [ -f "$BATCH_DIR/run_meta.env" ]; then
  BATCH_QUEUE="$(read_state "$BATCH_DIR/run_meta.env" original_queue_file)"
  EXECUTION_MODE="$(read_state "$BATCH_DIR/run_meta.env" execution_mode)"
  if [ "$BATCH_QUEUE" != "$ORIGINAL_QUEUE_FILE" ]; then
    echo "Error: Batch metadata points to a different queue: $BATCH_QUEUE" >&2
    exit 1
  fi
  case "$EXECUTION_MODE" in
    shared-queue|host-local-shards|adaptive-shared-queue) ;;
    *) echo "Error: unsupported execution mode in $BATCH_DIR/run_meta.env" >&2; exit 1 ;;
  esac
fi

RECONCILE_QUEUE_FILE="$ORIGINAL_QUEUE_FILE"
if [ "${#DEPENDENCY_REPLACEMENTS[@]}" -gt 0 ] && \
    { [ "$RERUN_MISSING" -ne 1 ] || [ "$EXECUTION_MODE" != adaptive-shared-queue ]; }; then
  echo "Error: --replace_dependency requires --rerun_missing for an adaptive batch." >&2
  exit 1
fi
if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  RECONCILE_QUEUE_FILE="$(read_state "$BATCH_DIR/run_meta.env" editable_queue_file)"
  EDITABLE_HASH_COUNT="$(awk -F= '$1 == "editable_queue_sha256" {n++} END {print n+0}' \
    "$BATCH_DIR/run_meta.env")"
  case "$EDITABLE_HASH_COUNT" in
    1) EXPECTED_QUEUE_HASH="$(read_state "$BATCH_DIR/run_meta.env" editable_queue_sha256)" ;;
    0) EXPECTED_QUEUE_HASH="$(read_state "$BATCH_DIR/run_meta.env" queue_sha256)" ;;
    *)
      echo "Error: adaptive batch metadata has ambiguous queue hashes." >&2
      exit 1
      ;;
  esac
  [ -f "$RECONCILE_QUEUE_FILE" ] && [ ! -L "$RECONCILE_QUEUE_FILE" ] || {
    echo "Error: adaptive batch queue is missing or ambiguous: $RECONCILE_QUEUE_FILE" >&2
    exit 1
  }
  CURRENT_QUEUE_HASH="$(shasum -a 256 "$RECONCILE_QUEUE_FILE" | awk '{print $1}')"
  [ -n "$EXPECTED_QUEUE_HASH" ] && [ "$CURRENT_QUEUE_HASH" = "$EXPECTED_QUEUE_HASH" ] || {
    echo "Error: adaptive batch queue hash does not match run metadata." >&2
    exit 1
  }
elif [ ! -f "$RECONCILE_QUEUE_FILE" ]; then
  echo "Error: Queue file not found: $RECONCILE_QUEUE_FILE" >&2
  exit 1
fi
if ! awk '
  NF {
    id=$1
    if (seen[id]++) {
      printf "Error: Duplicate job ID in reconciliation queue: %s\n", id > "/dev/stderr"
      bad=1
    }
  }
  END { exit bad ? 1 : 0 }
' "$RECONCILE_QUEUE_FILE"; then
  exit 2
fi
DONE_DIR="${BATCH_DIR}/done"
SHARD_DIR="${BATCH_DIR}/shards"
if [ "$EXECUTION_MODE" = "host-local-shards" ]; then
  if [ ! -d "$SHARD_DIR" ]; then
    echo "Error: Shard progress directory not found: $SHARD_DIR" >&2
    exit 1
  fi
elif [ ! -d "$DONE_DIR" ]; then
  echo "Error: Done marker directory not found: $DONE_DIR" >&2
  echo "Run a batch with the updated mmworker.sh (writes ${BATCH_DIR}/done/job_<ID>.done on success)." >&2
  exit 1
fi

QUEUE_BASE="${QUEUE_FILE%.*}"
QUEUE_EXT="${QUEUE_FILE##*.}"
if [ "$QUEUE_EXT" = "$QUEUE_FILE" ]; then
  QUEUE_EXT=""
fi

if [ -z "$RETRY_QUEUE_FILE" ]; then
  BATCH_NAME=$(basename "$BATCH_DIR")
  if [ -n "$QUEUE_EXT" ]; then
    RETRY_QUEUE_FILE="${QUEUE_BASE}-retry-missing-${BATCH_NAME}.${QUEUE_EXT}"
  else
    RETRY_QUEUE_FILE="${QUEUE_BASE}-retry-missing-${BATCH_NAME}"
  fi
fi

RETRY_QUEUE_PATH="${QUEUE_DIR}/${RETRY_QUEUE_FILE}"
RECONCILE_DIR="${BATCH_DIR}/reconcile"
mkdir -p "$RECONCILE_DIR"

EXPECTED_IDS_FILE="${RECONCILE_DIR}/expected_ids.txt"
DONE_IDS_FILE="${RECONCILE_DIR}/done_ids.txt"
MISSING_IDS_FILE="${RECONCILE_DIR}/missing_ids.txt"
SUMMARY_FILE="${RECONCILE_DIR}/summary.txt"

awk 'NF {print $1}' "$RECONCILE_QUEUE_FILE" | LC_ALL=C sort -u > "$EXPECTED_IDS_FILE"
if [ "$EXECUTION_MODE" = "host-local-shards" ]; then
  : > "$DONE_IDS_FILE"
  for success_file in "$SHARD_DIR"/host_*.succeeded_ids.txt; do
    [ -f "$success_file" ] || continue
    awk 'NF {print $1}' "$success_file" >> "$DONE_IDS_FILE"
  done
  LC_ALL=C sort -u "$DONE_IDS_FILE" -o "$DONE_IDS_FILE"
else
  find "$DONE_DIR" -maxdepth 1 -type f -name 'job_*.done' -print \
    | sed -nE 's#^.*/job_(.*)\.done$#\1#p' \
    | LC_ALL=C sort -u > "$DONE_IDS_FILE"
fi

comm -23 "$EXPECTED_IDS_FILE" "$DONE_IDS_FILE" > "$MISSING_IDS_FILE"
MISSING_COUNT=$(wc -l < "$MISSING_IDS_FILE" | tr -d ' ')
EXPECTED_COUNT=$(wc -l < "$EXPECTED_IDS_FILE" | tr -d ' ')
DONE_COUNT=$(wc -l < "$DONE_IDS_FILE" | tr -d ' ')

if [ "$MISSING_COUNT" -gt 0 ]; then
  awk '
    NR==FNR { missing[$1]=1; next }
    {
      id=$1
      if (id in missing) print
    }
  ' "$MISSING_IDS_FILE" "$RECONCILE_QUEUE_FILE" > "$RETRY_QUEUE_PATH"
else
  : > "$RETRY_QUEUE_PATH"
fi

RETRY_COUNT=$(wc -l < "$RETRY_QUEUE_PATH" | tr -d ' ')

{
  echo "batch_dir=$BATCH_DIR"
  echo "execution_mode=$EXECUTION_MODE"
  echo "original_queue=$ORIGINAL_QUEUE_FILE"
  echo "reconciliation_queue=$RECONCILE_QUEUE_FILE"
  if [ "$EXECUTION_MODE" = "host-local-shards" ]; then
    echo "success_records=$SHARD_DIR/host_*.succeeded_ids.txt"
  else
    echo "done_dir=$DONE_DIR"
  fi
  echo "retry_queue=$RETRY_QUEUE_PATH"
  echo "expected_count=$EXPECTED_COUNT"
  echo "done_count=$DONE_COUNT"
  echo "missing_count=$MISSING_COUNT"
  echo "retry_count=$RETRY_COUNT"
} | tee "$SUMMARY_FILE"

if [ "$MISSING_COUNT" -eq 0 ]; then
  echo "No missing jobs detected."
  exit 0
fi

echo "Missing job IDs written to: $MISSING_IDS_FILE"
echo "Retry queue written to: $RETRY_QUEUE_PATH"

if [ "$RERUN_MISSING" -eq 1 ]; then
  PROJECT_NAME=""
  if [ -f "$BATCH_DIR/run_meta.env" ] && grep -q '^project_name=' "$BATCH_DIR/run_meta.env"; then
    PROJECT_NAME="$(read_state "$BATCH_DIR/run_meta.env" project_name)" || exit 1
    [[ "$PROJECT_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] && [ "${#PROJECT_NAME}" -le 64 ] || {
      echo "Error: invalid retry project_name metadata." >&2
      exit 1
    }
  fi
  if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
    RESOURCE_FILE="$BATCH_DIR/adaptive/resources.env"
    CONTROL_FILE="$BATCH_DIR/adaptive/control.env"
    STATUS_FILE="$BATCH_DIR/adaptive/status.env"
    for required in "$RESOURCE_FILE" "$CONTROL_FILE" "$STATUS_FILE"; do
      [ -f "$required" ] && [ ! -L "$required" ] || {
        echo "Error: adaptive retry metadata is missing or ambiguous: $required" >&2
        exit 1
      }
    done
    STATUS_SCHEMA="$(read_state "$STATUS_FILE" schema_version)" || exit 1
    STATUS_QUEUE="$(read_state "$STATUS_FILE" queue_id)" || exit 1
    LIFECYCLE="$(read_state "$STATUS_FILE" lifecycle)" || exit 1
    RUNNING_WORKERS="$(read_state "$STATUS_FILE" running_workers)" || exit 1
    TARGET_WORKERS="$(read_state "$STATUS_FILE" target_workers)" || exit 1
    PROPAGATION="$(read_state "$STATUS_FILE" propagation)" || exit 1
    STATUS_GENERATION="$(read_state "$STATUS_FILE" control_generation)" || exit 1
    RUN_TOKEN="$(read_state "$BATCH_DIR/run_meta.env" run_token)" || exit 1
    if [ "$STATUS_SCHEMA" != 1 ] || [ "$STATUS_QUEUE" != "$RUN_TOKEN" ] ||
        [[ "$LIFECYCLE" != COMPLETED && "$LIFECYCLE" != FINISHED_INCOMPLETE ]] ||
        [ "$RUNNING_WORKERS" != 0 ] || [ "$TARGET_WORKERS" != 0 ] ||
        [ "$PROPAGATION" != complete ]; then
      echo "Error: adaptive retry requires exact terminal zero-admission status." >&2
      exit 1
    fi
    REGISTRATION_FILE="$(read_state "$BATCH_DIR/run_meta.env" registration_file)" || exit 1
    [ ! -e "$REGISTRATION_FILE" ] && [ ! -L "$REGISTRATION_FILE" ] || {
      echo "Error: adaptive retry is blocked while the source queue remains registered." >&2
      exit 1
    }
    RESOURCE_SNAPSHOT="$RECONCILE_DIR/adaptive-resources.snapshot.$$"
    ln "$RESOURCE_FILE" "$RESOURCE_SNAPSHOT" || {
      echo "Error: cannot snapshot adaptive retry resources." >&2
      exit 1
    }
    trap 'rm -f "$RESOURCE_SNAPSHOT"' EXIT
    RESOURCE_SCHEMA="$(read_state "$RESOURCE_SNAPSHOT" schema_version)" || exit 1
    RESOURCE_QUEUE="$(read_state "$RESOURCE_SNAPSHOT" queue_id)" || exit 1
    CONTROL_SCHEMA="$(read_state "$CONTROL_FILE" schema_version)" || exit 1
    CONTROL_QUEUE="$(read_state "$CONTROL_FILE" queue_id)" || exit 1
    CONTROL_GENERATION="$(read_state "$CONTROL_FILE" generation)" || exit 1
    RESOURCE_HASH="$(read_state "$BATCH_DIR/run_meta.env" resources_sha256)" || exit 1
    CURRENT_RESOURCE_HASH="$(shasum -a 256 "$RESOURCE_SNAPSHOT" | awk '{print $1}')" || exit 1
    [[ "$RESOURCE_SCHEMA" = 1 || "$RESOURCE_SCHEMA" = 2 ]] && [ "$RESOURCE_QUEUE" = "$RUN_TOKEN" ] && \
      [[ "$CONTROL_SCHEMA" = 1 || "$CONTROL_SCHEMA" = 2 ]] && [ "$CONTROL_QUEUE" = "$RUN_TOKEN" ] && \
      [ "$STATUS_GENERATION" = "$CONTROL_GENERATION" ] && \
      [ "$CURRENT_RESOURCE_HASH" = "$RESOURCE_HASH" ] || {
        echo "Error: adaptive retry resource/control identity is invalid." >&2
        exit 1
      }
    CPU_UNITS="$(read_state "$RESOURCE_SNAPSHOT" cpu_units_per_job)" || exit 1
    MEMORY_MIB="$(read_state "$RESOURCE_SNAPSHOT" memory_mib_per_job)" || exit 1
    if grep -q '^memory_mib_per_job=' "$CONTROL_FILE"; then
      [ "$CONTROL_SCHEMA" = 2 ] || {
        echo "Error: adaptive retry memory override requires control schema2." >&2
        exit 1
      }
      MEMORY_MIB="$(read_state "$CONTROL_FILE" memory_mib_per_job)" || exit 1
      [[ "$MEMORY_MIB" =~ ^[0-9]+$ ]] && [ "$MEMORY_MIB" -gt 0 ] || {
        echo "Error: adaptive retry memory override must be a positive integer." >&2
        exit 1
      }
    fi
    SHARED_IO_UNITS="$(read_state "$RESOURCE_SNAPSHOT" shared_io_units_per_job)" || exit 1
    # Opt-in requests are present only when the batch used them.
    GPU_UNITS=""
    MEMORY_LIMIT=""
    if grep -q '^gpu_units_per_job=' "$RESOURCE_SNAPSHOT"; then
      GPU_UNITS="$(read_state "$RESOURCE_SNAPSHOT" gpu_units_per_job)" || exit 1
    fi
    if grep -q '^memory_limit_mib=' "$RESOURCE_SNAPSHOT"; then
      MEMORY_LIMIT="$(read_state "$RESOURCE_SNAPSHOT" memory_limit_mib)" || exit 1
    fi
    SCHEDULE_PRIORITY="$(read_state "$CONTROL_FILE" schedule_priority)" || exit 1
    if grep -q '^host_priorities=' "$CONTROL_FILE"; then
      HOST_PRIORITIES="$(read_state "$CONTROL_FILE" host_priorities)" || exit 1
      if [ -n "$HOST_PRIORITIES" ]; then
        [ "$CONTROL_SCHEMA" = 2 ] || {
          echo "Error: adaptive retry host priorities require control schema2." >&2
          exit 1
        }
        # Submit revalidates the combined SPEC against the current pool hosts.
        SCHEDULE_PRIORITY="$SCHEDULE_PRIORITY,$HOST_PRIORITIES"
      fi
    fi
    MAX_WORKERS="$(read_state "$CONTROL_FILE" max_workers)" || exit 1
    ADMISSION="$(read_state "$CONTROL_FILE" admission)" || exit 1
    WORKFLOW_ARGS=()
    if [ "$RESOURCE_SCHEMA" = 2 ] || [ "${#DEPENDENCY_REPLACEMENTS[@]}" -gt 0 ]; then
      WORKFLOW_TEXT="$(python3 "$(dirname "$0")/adaptive_workflow.py" retry-args \
        --resources "$RESOURCE_SNAPSHOT" \
        ${DEPENDENCY_REPLACEMENTS[@]+"${DEPENDENCY_REPLACEMENTS[@]}"})" || exit 1
      while IFS= read -r argument; do
        [ -z "$argument" ] || WORKFLOW_ARGS+=("$argument")
      done <<< "$WORKFLOW_TEXT"
    fi
    rm -f "$RESOURCE_SNAPSHOT"
    trap - EXIT
    [[ "$ADMISSION" = enabled || "$ADMISSION" = paused || "$ADMISSION" = closed ]] || {
      echo "Error: invalid adaptive admission state: $ADMISSION" >&2
      exit 1
    }
    [ "$ADMISSION" != closed ] || ADMISSION=paused
    RUN_NAME="$(awk -F= '$1 == "run_name" {sub(/^[^=]*=/, ""); print; exit}' "$BATCH_DIR/run_meta.env")"
    ADAPTIVE_ARGS=(
      submit --model_dir "$MODEL_DIR" --queue "$RETRY_QUEUE_PATH"
      --run_name "$RUN_NAME" --schedule_priority "$SCHEDULE_PRIORITY"
      --cpu_units_per_job "$CPU_UNITS" --memory_mib_per_job "$MEMORY_MIB"
      --shared_io_units_per_job "$SHARED_IO_UNITS" --max_workers "$MAX_WORKERS"
      ${WORKFLOW_ARGS[@]+"${WORKFLOW_ARGS[@]}"}
    )
    if grep -Eq '^(memory_calibration_jobs|memory_calibration_seconds|memory_min_mib_per_job|memory_calibration_generation)=' "$CONTROL_FILE"; then
      CALIBRATION_JOBS="$(read_state "$CONTROL_FILE" memory_calibration_jobs)" || exit 1
      CALIBRATION_SECONDS="$(read_state "$CONTROL_FILE" memory_calibration_seconds)" || exit 1
      CALIBRATION_MIN="$(read_state "$CONTROL_FILE" memory_min_mib_per_job)" || exit 1
      CALIBRATION_GENERATION="$(read_state "$CONTROL_FILE" memory_calibration_generation)" || exit 1
      if [ "$CONTROL_SCHEMA" != 2 ] || ! grep -q '^memory_mib_per_job=' "$CONTROL_FILE" ||
          ! [[ "$CALIBRATION_JOBS" =~ ^[0-9]+$ && "$CALIBRATION_SECONDS" =~ ^[0-9]+$ &&
               "$CALIBRATION_MIN" =~ ^[0-9]+$ && "$CALIBRATION_GENERATION" =~ ^[0-9]+$ ]] ||
          { [ "$CALIBRATION_JOBS" -ne 0 ] && { [ "$CALIBRATION_JOBS" -lt 20 ] || [ "$CALIBRATION_JOBS" -gt 50 ]; }; } ||
          { [ "$CALIBRATION_SECONDS" -ne 0 ] && { [ "$CALIBRATION_SECONDS" -lt 600 ] || [ "$CALIBRATION_SECONDS" -gt 86400 ]; }; } ||
          { [ "$CALIBRATION_JOBS" -eq 0 ] && [ "$CALIBRATION_SECONDS" -eq 0 ]; } ||
          [ "$CALIBRATION_MIN" -le 0 ] || [ "$CALIBRATION_MIN" -gt "$MEMORY_MIB" ] ||
          [ "$CALIBRATION_GENERATION" -le 0 ] || [ "$CALIBRATION_GENERATION" -gt "$CONTROL_GENERATION" ]; then
        echo "Error: invalid adaptive retry memory calibration policy." >&2
        exit 1
      fi
      # Keep the opt-in policy; the new batch starts a fresh learning cohort.
      ADAPTIVE_ARGS+=(--memory_calibration_jobs "$CALIBRATION_JOBS"
        --memory_calibration_seconds "$CALIBRATION_SECONDS" --memory_min_mib_per_job "$CALIBRATION_MIN")
    fi
    [ -z "$GPU_UNITS" ] || ADAPTIVE_ARGS+=(--gpu_units_per_job "$GPU_UNITS")
    [ -z "$MEMORY_LIMIT" ] || ADAPTIVE_ARGS+=(--memory_limit_mib "$MEMORY_LIMIT")
    [ -z "$PROJECT_NAME" ] || ADAPTIVE_ARGS+=(--project "$PROJECT_NAME")
    [ "$ADMISSION" != paused ] || ADAPTIVE_ARGS+=(--paused)
    if [ ! -x "$ADAPTIVE_SCHEDULER_SCRIPT" ]; then
      echo "Error: adaptive scheduler not executable: $ADAPTIVE_SCHEDULER_SCRIPT" >&2
      printf 'Future submit command:' >&2
      printf ' %q' "$ADAPTIVE_SCHEDULER_SCRIPT" "${ADAPTIVE_ARGS[@]}" >&2
      printf '\n' >&2
      exit 1
    fi
    echo "Launching adaptive retry batch for missing jobs..."
    if ! "$ADAPTIVE_SCHEDULER_SCRIPT" "${ADAPTIVE_ARGS[@]}"; then
      echo "Retry queue retained; the adaptive pool did not accept it." >&2
      printf 'Future submit command:' >&2
      printf ' %q' "$ADAPTIVE_SCHEDULER_SCRIPT" "${ADAPTIVE_ARGS[@]}" >&2
      printf '\n' >&2
      exit 1
    fi
  else
    if [ ! -x "$START_WORKERS_SCRIPT" ]; then
      echo "Error: start_workers.sh not executable: $START_WORKERS_SCRIPT" >&2
      exit 1
    fi
    echo "Launching retry batch for missing jobs..."
    START_ARGS=(--model_dir "$MODEL_DIR" --queue "$RETRY_QUEUE_PATH" --execution_mode "$EXECUTION_MODE")
    [ -z "$PROJECT_NAME" ] || START_ARGS+=(--project "$PROJECT_NAME")
    bash "$START_WORKERS_SCRIPT" "${START_ARGS[@]}"
  fi
fi
