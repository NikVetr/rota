#!/usr/bin/env bash
if [ -x /Library/Frameworks/R.framework/Resources/bin/Rscript ]; then
  export PATH="/Library/Frameworks/R.framework/Resources/bin:${PATH}"
fi

# Keep one queued job to one CPU thread by default. The queue scheduler provides
# coarse-grained parallelism; package-level BLAS/OpenMP/data.table fan-out makes
# job placement much less predictable on the shared worker pool.
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-1}"
export BLIS_NUM_THREADS="${BLIS_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"
export R_DATATABLE_NUM_THREADS="${R_DATATABLE_NUM_THREADS:-1}"

NUM_JOBS=$1                  # Number of parallel jobs to run on this node
ORIGINAL_QUEUE_FILE=$2       # Path to the original job queue file
EDITABLE_QUEUE_FILE=$3       # Path to the editable job queue file
MODEL_DIR=$4                 # Model directory path
LOG_DIR=$5                   # Log directory passed from start_workers.sh
LOCK_DIR=$6                  # Lock file for the queue
TAG_CLEAN=$7                  # Lock file for the queue
JOB_LOG_DIR="$LOG_DIR/jobs" # Define the job-specific log directory
HOST_NAME="$(hostname)"
WORKER_STATE_DIR="$LOG_DIR/workers"
QUEUE_REWRITE_MAX_RETRIES="${QUEUE_REWRITE_MAX_RETRIES:-12}"
QUEUE_REWRITE_SLEEP_SEC="${QUEUE_REWRITE_SLEEP_SEC:-0.5}"
WORKER_IDLE_EXIT_AFTER="${WORKER_IDLE_EXIT_AFTER:-200}"
LOCK_STALE_SEC="${LOCK_STALE_SEC:-3600}"
LOCK_STALE_CHECK_SEC="${LOCK_STALE_CHECK_SEC:-10}"
LOCK_RETRY_MIN_TENTHS="${LOCK_RETRY_MIN_TENTHS:-5}"
LOCK_RETRY_MAX_TENTHS="${LOCK_RETRY_MAX_TENTHS:-20}"
JOB_LOG_SYNC_SEC="${JOB_LOG_SYNC_SEC:-300}"
JOB_LOG_LIVE_BYTES="${JOB_LOG_LIVE_BYTES:-262144}"
LOCAL_JOB_LOG_ROOT="${LOCAL_JOB_LOG_ROOT:-${TMPDIR:-/tmp}/parallel-workers-job-logs}"
WORKER_SLOT_OFFSET="${WORKER_SLOT_OFFSET:-0}"
PARALLEL_WORKER_READY_FILE="${PARALLEL_WORKER_READY_FILE:-}"
PARALLEL_WORKER_START_FILE="${PARALLEL_WORKER_START_FILE:-}"
PARALLEL_WORKER_RUN_TOKEN="${PARALLEL_WORKER_RUN_TOKEN:-}"
PARALLEL_WORKER_RUNNER_SHA256="${PARALLEL_WORKER_RUNNER_SHA256:-}"
ADAPTIVE_RESERVATION_ID="${8:-}"
ADAPTIVE_RESULT_FILE="${9:-}"
ADAPTIVE_RESERVATION_FILE="${10:-}"
ADAPTIVE_QUEUE_SHA256="${11:-}"
ADAPTIVE_MODE=0
ADAPTIVE_RESULT_WRITTEN=0
ADAPTIVE_CLAIMED_JOB_ID=""

publish_adaptive_result() {
  local outcome="$1"
  local job_exit_code="${2:-}"
  local result_tmp

  [ "$ADAPTIVE_MODE" -eq 1 ] || return 0
  [ "$ADAPTIVE_RESULT_WRITTEN" -eq 0 ] || return 0
  result_tmp="${ADAPTIVE_RESULT_FILE}.tmp.$$.$RANDOM"
  {
    echo "reservation_id=$ADAPTIVE_RESERVATION_ID"
    echo "claimed_job_id=$ADAPTIVE_CLAIMED_JOB_ID"
    echo "outcome=$outcome"
    echo "job_exit_code=$job_exit_code"
  } > "$result_tmp" || return 1
  mv -f "$result_tmp" "$ADAPTIVE_RESULT_FILE" || {
    rm -f "$result_tmp"
    return 1
  }
  ADAPTIVE_RESULT_WRITTEN=1
}

if [ -n "$ADAPTIVE_RESERVATION_ID" ] || [ -n "$ADAPTIVE_RESULT_FILE" ] || [ -n "$ADAPTIVE_RESERVATION_FILE" ]; then
  ADAPTIVE_MODE=1
  if [ -z "$ADAPTIVE_RESERVATION_ID" ] || [ -z "$ADAPTIVE_RESULT_FILE" ] || [ -z "$ADAPTIVE_RESERVATION_FILE" ]; then
    echo "Adaptive mode requires reservation ID, result file, and reservation file." >&2
    exit 2
  fi
  case "$ADAPTIVE_RESERVATION_ID" in
    *[!A-Za-z0-9._-]*|'')
      echo "Invalid adaptive reservation ID: $ADAPTIVE_RESERVATION_ID" >&2
      exit 2
      ;;
  esac
  if [ "$NUM_JOBS" != "1" ]; then
    echo "Adaptive one-claim mode requires NUM_JOBS=1." >&2
    exit 2
  fi
  if [ ! -d "$(dirname "$ADAPTIVE_RESULT_FILE")" ] || [ ! -w "$(dirname "$ADAPTIVE_RESULT_FILE")" ]; then
    echo "Adaptive result directory is not writable: $(dirname "$ADAPTIVE_RESULT_FILE")" >&2
    exit 1
  fi
  if [ ! -r "$ADAPTIVE_RESERVATION_FILE" ] ||
     [ "$(awk -F= -v id="$ADAPTIVE_RESERVATION_ID" '$1 == "reservation_id" && $2 == id {count++} END {print count + 0}' "$ADAPTIVE_RESERVATION_FILE")" -ne 1 ]; then
    echo "Adaptive reservation state does not match $ADAPTIVE_RESERVATION_ID." >&2
    exit 1
  fi
  trap 'rc=$?; if [ "$ADAPTIVE_RESULT_WRITTEN" -eq 0 ]; then publish_adaptive_result infrastructure_failed || true; fi; exit "$rc"' EXIT
  ADAPTIVE_CLAIM_SCRIPT="$(cd "$(dirname "$0")" && pwd)/adaptive_claim.py"
  [ -r "$ADAPTIVE_CLAIM_SCRIPT" ] || { echo "Missing adaptive claim helper" >&2; exit 1; }
  if [ -z "$ADAPTIVE_QUEUE_SHA256" ]; then
    ADAPTIVE_QUEUE_SHA256="$(shasum -a 256 "$EDITABLE_QUEUE_FILE")" || exit 1
    ADAPTIVE_QUEUE_SHA256="${ADAPTIVE_QUEUE_SHA256%% *}"
  fi
fi

function require_nonnegative_integer {
  local name="$1"
  local value="$2"
  case "$value" in
    ''|*[!0-9]*)
      echo "$name must be a non-negative integer." >&2
      exit 2
      ;;
  esac
}

require_nonnegative_integer "WORKER_SLOT_OFFSET" "$WORKER_SLOT_OFFSET"
require_nonnegative_integer "LOCK_STALE_CHECK_SEC" "$LOCK_STALE_CHECK_SEC"
require_nonnegative_integer "LOCK_RETRY_MIN_TENTHS" "$LOCK_RETRY_MIN_TENTHS"
require_nonnegative_integer "LOCK_RETRY_MAX_TENTHS" "$LOCK_RETRY_MAX_TENTHS"
require_nonnegative_integer "JOB_LOG_SYNC_SEC" "$JOB_LOG_SYNC_SEC"
require_nonnegative_integer "JOB_LOG_LIVE_BYTES" "$JOB_LOG_LIVE_BYTES"

if [ "$LOCK_STALE_CHECK_SEC" -eq 0 ]; then
  echo "LOCK_STALE_CHECK_SEC must be greater than zero." >&2
  exit 2
fi
if [ "$LOCK_RETRY_MIN_TENTHS" -eq 0 ] || [ "$LOCK_RETRY_MAX_TENTHS" -lt "$LOCK_RETRY_MIN_TENTHS" ]; then
  echo "Lock retry bounds must satisfy 0 < LOCK_RETRY_MIN_TENTHS <= LOCK_RETRY_MAX_TENTHS." >&2
  exit 2
fi
if [ "$JOB_LOG_LIVE_BYTES" -eq 0 ]; then
  echo "JOB_LOG_LIVE_BYTES must be greater than zero." >&2
  exit 2
fi

case "$(uname -s)" in
  Darwin) LOCK_STAT_FORMAT="darwin" ;;
  *) LOCK_STAT_FORMAT="gnu" ;;
esac

# Tags contain project and queue paths and can exceed a filesystem component limit.
LOCAL_LOG_KEY="$(printf '%s' "$TAG_CLEAN" | shasum -a 256)" || {
  echo "Unable to hash the node-local job log identity." >&2
  exit 1
}
LOCAL_LOG_KEY="${LOCAL_LOG_KEY%% *}"
LOCAL_BATCH_LOG_DIR="${LOCAL_JOB_LOG_ROOT%/}/$LOCAL_LOG_KEY/$(basename "$LOG_DIR")/$HOST_NAME"
if ! mkdir -p "$LOCAL_BATCH_LOG_DIR"; then
  echo "Unable to create node-local job log directory: $LOCAL_BATCH_LOG_DIR" >&2
  exit 1
fi
if ! mkdir -p "$JOB_LOG_DIR" "$WORKER_STATE_DIR"; then
  echo "Unable to initialize shared worker directories under $LOG_DIR" >&2
  exit 1
fi
if [ "$ADAPTIVE_MODE" -eq 0 ] && [ ! -r "$ORIGINAL_QUEUE_FILE" ]; then
  echo "Original queue is not readable: $ORIGINAL_QUEUE_FILE" >&2
  exit 1
fi
if [ ! -r "$EDITABLE_QUEUE_FILE" ] || \
   { [ "$ADAPTIVE_MODE" -eq 0 ] && [ ! -w "$EDITABLE_QUEUE_FILE" ]; }; then
  echo "Editable queue has invalid access for this execution mode: $EDITABLE_QUEUE_FILE" >&2
  exit 1
fi
if [ ! -d "$(dirname "$LOCK_DIR")" ] || [ ! -w "$(dirname "$LOCK_DIR")" ]; then
  echo "Queue lock parent is not writable: $(dirname "$LOCK_DIR")" >&2
  exit 1
fi
if [ "$ADAPTIVE_MODE" -eq 0 ]; then
  rm -f "$WORKER_STATE_DIR/${HOST_NAME}.all_done"
fi

# Log a message that the worker has started
WORKER_LOG_FILE="$LOG_DIR/worker_${HOST_NAME}.log"
if ! echo "$(date): Worker initialized on $HOST_NAME with $NUM_JOBS parallel jobs." >> "$WORKER_LOG_FILE"; then
  echo "Unable to append worker log: $WORKER_LOG_FILE" >&2
  exit 1
fi

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
    echo "host=$HOST_NAME"
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

if ! publish_scheduler_ready; then
  echo "Unable to publish node-local worker readiness." >&2
  exit 1
fi
if ! wait_for_scheduler_start; then
  exit 1
fi

# Acquire a lock to safely modify the queue
function acquire_lock {
  local last_stale_check=0
  local lock_mtime now_epoch lock_age retry_tenths retry_range

  while ! mkdir "$LOCK_DIR" 2>/dev/null; do
    now_epoch="$(date +%s)"
    if [ $((now_epoch - last_stale_check)) -ge "$LOCK_STALE_CHECK_SEC" ]; then
      last_stale_check="$now_epoch"
      if [ "$LOCK_STAT_FORMAT" = "darwin" ]; then
        lock_mtime="$(stat -f %m "$LOCK_DIR" 2>/dev/null || printf '0')"
      else
        lock_mtime="$(stat -c %Y "$LOCK_DIR" 2>/dev/null || printf '0')"
      fi
      case "$lock_mtime" in
        ''|*[!0-9]*)
          lock_mtime=0
          ;;
      esac
      lock_age=$((now_epoch - lock_mtime))
      if [ "$lock_mtime" -gt 0 ] && [ "$lock_age" -ge "$LOCK_STALE_SEC" ]; then
        rmdir "$LOCK_DIR" 2>/dev/null && \
          echo "$(date): Removed stale queue lock $LOCK_DIR after ${lock_age}s on $HOST_NAME." >> "$WORKER_LOG_FILE"
      fi
    fi

    retry_range=$((LOCK_RETRY_MAX_TENTHS - LOCK_RETRY_MIN_TENTHS + 1))
    retry_tenths=$((LOCK_RETRY_MIN_TENTHS + RANDOM % retry_range))
    sleep "$((retry_tenths / 10)).$((retry_tenths % 10))"
  done
}

# Release the lock after modifying the queue
function release_lock {
  rmdir "$LOCK_DIR"
}

function rewrite_queue_from_second_line {
  local action_label="$1"
  local attempt tmp_queue err_file err_text

  for attempt in $(seq 1 "$QUEUE_REWRITE_MAX_RETRIES"); do
    tmp_queue="${EDITABLE_QUEUE_FILE}.$$.$RANDOM.tmp"
    err_file="${EDITABLE_QUEUE_FILE}.$$.$RANDOM.err"

    if ! tail -n +2 "$EDITABLE_QUEUE_FILE" > "$tmp_queue" 2>"$err_file"; then
      err_text="$(tr '\n' ' ' < "$err_file" 2>/dev/null | sed 's/[[:space:]]\+/ /g')"
      rm -f "$tmp_queue" "$err_file"
      echo "$(date): Failed to stage queue rewrite while attempting to ${action_label} on $HOST_NAME (attempt ${attempt}/${QUEUE_REWRITE_MAX_RETRIES}): ${err_text:-tail failed}." >> "$WORKER_LOG_FILE"
      sleep "$QUEUE_REWRITE_SLEEP_SEC"
      continue
    fi

    : > "$err_file"
    if mv -f "$tmp_queue" "$EDITABLE_QUEUE_FILE" 2>"$err_file"; then
      rm -f "$err_file"
      return 0
    fi

    err_text="$(tr '\n' ' ' < "$err_file" 2>/dev/null | sed 's/[[:space:]]\+/ /g')"
    rm -f "$tmp_queue" "$err_file"
    echo "$(date): Failed to ${action_label} on $HOST_NAME (attempt ${attempt}/${QUEUE_REWRITE_MAX_RETRIES}): ${err_text:-rewrite failed}." >> "$WORKER_LOG_FILE"
    sleep "$QUEUE_REWRITE_SLEEP_SEC"
  done

  return 1
}

function mark_slot_done {
  local slot_done_file="$1"
  printf "%s\n" "$(date +%s)" > "$slot_done_file"
}

# Shared logs are rewritten in place. Publishing through a temporary name and
# renaming it over the log blocked the file server: CREATE requests for names that
# had just been renamed away looped forever (see TROUBLESHOOTING). Readers may
# briefly see a partial snapshot; the node-local log remains the source.
function publish_live_job_log {
  local local_log_file="$1"
  local shared_log_file="$2"
  local local_stage="${local_log_file}.live.$$"

  if {
    printf '[parallel-workers live tail snapshot; complete log is published when the job exits]\n'
    tail -c "$JOB_LOG_LIVE_BYTES" "$local_log_file"
  } > "$local_stage" && cat "$local_stage" > "$shared_log_file" && chmod 666 "$shared_log_file"; then
    rm -f "$local_stage"
    return 0
  fi

  rm -f "$local_stage"
  return 1
}

function publish_final_job_log {
  local local_log_file="$1"
  local shared_log_file="$2"

  if cat "$local_log_file" > "$shared_log_file" && chmod 666 "$shared_log_file"; then
    rm -f "$local_log_file"
    return 0
  fi
  return 1
}

# True when the canary on fd 7 no longer names an open file. macOS revokes open SMB
# handles it cannot restore after a reconnect, and fstat on a revoked handle fails.
share_handle_revoked() {
  # Separate statements: a `my` variable is not yet in scope later in its own expression.
  ! perl -e 'open(my $handle, "<&=", 7) or exit 1; stat($handle) or exit 1'
}

# One write, no rename: the record must not add rename churn to the share it describes.
record_share_interruption() {
  mkdir -p "$LOG_DIR/share-interrupted" && printf '%s\n' \
    "job_id=$JOB_ID" "host=$HOST_NAME" "exit_code=$JOB_EXIT_CODE" \
    "started_epoch=$job_started_epoch" "finished_epoch=$(date +%s)" \
    > "$LOG_DIR/share-interrupted/job_${JOB_ID}.${HOST_NAME}"
}

stop_log_sync() {
  [ -n "${log_sync_pid:-}" ] || return 0
  kill -TERM "$log_sync_pid" 2>/dev/null || true
  wait "$log_sync_pid" 2>/dev/null || true
  log_sync_pid=""
}

sync_job_log() {
  local sleeper="" size published_size=""
  trap - EXIT
  trap '[ -z "$sleeper" ] || kill "$sleeper" 2>/dev/null; exit 0' TERM INT
  while kill -0 "$job_pid" 2>/dev/null; do
    sleep "$JOB_LOG_SYNC_SEC" &
    sleeper=$!
    wait "$sleeper"
    sleeper=""
    kill -0 "$job_pid" 2>/dev/null || break
    # Quiet jobs (e.g. long Stan warmup) need no repeated writes to the share.
    size="$(wc -c < "$local_job_log_file" | tr -d ' ')"
    [ "$size" != "$published_size" ] || continue
    if publish_live_job_log "$local_job_log_file" "$JOB_LOG_FILE"; then
      published_size="$size"
    else
      echo "$(date): Failed to publish live log for job $JOB_ID; preserved $local_job_log_file." >> "$WORKER_LOG_FILE"
    fi
  done
}

# Process a single job from the editable queue
function process_job {
  local SLOT_ID="${1:-0}"
  local SLOT_DONE_FILE="$WORKER_STATE_DIR/${HOST_NAME}_slot${SLOT_ID}.done"
  local idle_loops=0
  local local_job_log_file job_pid log_sync_pid=""

  trap '
    if [ -n "${job_pid:-}" ] && kill -0 "$job_pid" 2>/dev/null; then
      if [ "$ADAPTIVE_MODE" -eq 1 ]; then
        kill -TERM -- "-$job_pid" 2>/dev/null || true
      else
        kill -TERM "$job_pid" 2>/dev/null || true
      fi
      wait "$job_pid" 2>/dev/null || true
    fi
    stop_log_sync
    if [ -n "${local_job_log_file:-}" ] && [ -f "$local_job_log_file" ] && [ -n "${JOB_LOG_FILE:-}" ]; then
      publish_final_job_log "$local_job_log_file" "$JOB_LOG_FILE" || true
    fi
    exit 143
  ' TERM INT

  while true; do
    # Acquire the lock to read the next job from the queue
    acquire_lock

    # Read the next job from the editable queue
    if [ "$ADAPTIVE_MODE" -eq 1 ]; then
      if [ ! -r "$EDITABLE_QUEUE_FILE" ]; then
        echo "$(date): Unable to open editable queue $EDITABLE_QUEUE_FILE on $HOST_NAME." >> "$WORKER_LOG_FILE"
        release_lock
        return 1
      fi
    elif [ ! -s "$EDITABLE_QUEUE_FILE" ]; then
      # If the queue is empty, release the lock and exit
      echo "$(date): Queue is empty. Exiting worker on $HOST_NAME." >> "$WORKER_LOG_FILE"
      release_lock
      mark_slot_done "$SLOT_DONE_FILE"
      return 0
    fi

    if [ "$ADAPTIVE_MODE" -eq 1 ]; then
      CLAIM_ROOT="$LOG_DIR/claims"
      if ! mkdir -p "$CLAIM_ROOT"; then
        echo "$(date): Unable to initialize claim directory $CLAIM_ROOT on $HOST_NAME." >> "$WORKER_LOG_FILE"
        release_lock
        return 1
      fi

      JOB_LINE="$(python3 "$ADAPTIVE_CLAIM_SCRIPT" --queue "$EDITABLE_QUEUE_FILE" \
          --claims "$CLAIM_ROOT" --cursor "$LOG_DIR/claim-cursor.json" \
          --queue-sha256 "$ADAPTIVE_QUEUE_SHA256" --reservation-id "$ADAPTIVE_RESERVATION_ID")"
      claim_status=$?
      if [ "$claim_status" -eq 3 ]; then
        # Claimed, but the cursor stayed behind; later claims rescan past this claim.
        echo "$(date): Claimed a job on $HOST_NAME but could not advance $LOG_DIR/claim-cursor.json; running it anyway." >> "$WORKER_LOG_FILE"
      elif [ "$claim_status" -ne 0 ]; then
        release_lock
        return 1
      fi
      if [ -z "$JOB_LINE" ]; then
        echo "$(date): Queue has no unclaimed jobs. Exiting worker on $HOST_NAME." >> "$WORKER_LOG_FILE"
        release_lock
        publish_adaptive_result empty || true
        return 1
      fi
      release_lock
    else
      JOB_LINE=$(head -n 1 "$EDITABLE_QUEUE_FILE")

      # If the first line is blank, delete it and continue (don’t treat as a fatal error).
      # Use temp-file + atomic rename to avoid truncating the queue on partial writes.
      if [ -z "${JOB_LINE//[[:space:]]/}" ]; then
        if rewrite_queue_from_second_line "drop blank queue line"; then
          idle_loops=0
          release_lock
          continue
        else
          echo "$(date): Failed to drop blank queue line on $HOST_NAME. Retrying." >> "$WORKER_LOG_FILE"
          release_lock
          sleep 0.5
          continue
        fi
      fi

      # Remove the job from the editable queue.
      # Use temp-file + atomic rename; avoid in-place overwrite because a failed write
      # can leave the queue truncated on shared/network filesystems.
      if ! rewrite_queue_from_second_line "pop queue line"; then
        echo "$(date): Failed to pop queue line on $HOST_NAME. Retrying." >> "$WORKER_LOG_FILE"
        release_lock
        sleep 0.5
        continue
      fi
      release_lock
    fi

    if [ "$ADAPTIVE_MODE" -eq 0 ]; then
      # Pause to let other fixed workers grab a job.
      PAUSE=$(awk -v seed=$RANDOM 'BEGIN { srand(seed); printf("%.2f\n", 1.0 + rand()*1.0) }')
      sleep "$PAUSE"
    fi

    # Extract the job number and command. Some queues begin directly with
    # inline env assignments like RUN_ID=... rather than a separate numeric ID.
    JOB_LINE="${JOB_LINE%$'\r'}"
    JOB_LINE="${JOB_LINE#"${JOB_LINE%%[![:space:]]*}"}"
    FIRST_TOKEN="${JOB_LINE%%[[:space:]]*}"
    if [[ "$FIRST_TOKEN" == *=* ]]; then
      JOB_ID="$FIRST_TOKEN"
      JOB_COMMAND="$JOB_LINE"
    else
      JOB_ID="$FIRST_TOKEN"
      JOB_COMMAND="${JOB_LINE#"$JOB_ID"}"
      JOB_COMMAND="${JOB_COMMAND#"${JOB_COMMAND%%[![:space:]]*}"}"
    fi
    [ "$ADAPTIVE_MODE" -eq 0 ] || ADAPTIVE_CLAIMED_JOB_ID="$JOB_ID"

    if [ "$ADAPTIVE_MODE" -eq 0 ]; then
      # Fixed workers still pop the queue, then use a claim as duplicate protection.
      CLAIM_ROOT="$LOG_DIR/claims"
      CLAIM_DIR="$CLAIM_ROOT/job_${JOB_ID}.claim"
      if ! { mkdir -p "$CLAIM_ROOT" && mkdir "$CLAIM_DIR" 2>/dev/null; }; then
        if [ ! -e "$CLAIM_DIR" ] && [ ! -L "$CLAIM_DIR" ]; then
          # The line is already popped, so this job now needs batch reconciliation.
          echo "$(date): Unable to claim popped job $JOB_ID at $CLAIM_DIR on $HOST_NAME; reconcile the batch." >&2
          echo "$(date): Unable to claim popped job $JOB_ID on $HOST_NAME; slot ${SLOT_ID} stopping." >> "$WORKER_LOG_FILE"
          return 1
        fi
        idle_loops=$((idle_loops + 1))
        if [ "$idle_loops" -ge "$WORKER_IDLE_EXIT_AFTER" ]; then
          echo "$(date): Exiting worker slot ${SLOT_ID} on $HOST_NAME after ${idle_loops} consecutive already-claimed jobs." >> "$WORKER_LOG_FILE"
          mark_slot_done "$SLOT_DONE_FILE"
          return 0
        fi
        echo "$(date): Skipping already-claimed job $JOB_ID on $HOST_NAME." >> "$WORKER_LOG_FILE"
        continue
      fi
      idle_loops=0
    fi

    # Append a tag to find the job later
    JOB_COMMAND="$JOB_COMMAND --job_tag $TAG_CLEAN"

    # Log the job and execute the command
    echo "$(date): Processing job $JOB_ID on $HOST_NAME." >> "$WORKER_LOG_FILE"

    # Define log file name
    JOB_LOG_FILE="$JOB_LOG_DIR/job_${JOB_ID}_${HOST_NAME}.log"
    local_job_log_file="$LOCAL_BATCH_LOG_DIR/$(basename "$JOB_LOG_FILE")"

    # Log the raw job line at the top of the job-specific log
    echo "COMMAND: $JOB_LINE" > "$local_job_log_file"

    # A share reconnect that loses open handles makes scripts read from the share hit a
    # silent EOF, so a job can stop early yet exit 0. Hold a canary handle on the queue
    # this mode reads (adaptive: the scheduler's immutable copy; fixed: the original).
    canary_file="$ORIGINAL_QUEUE_FILE"
    [ "$ADAPTIVE_MODE" -eq 0 ] || canary_file="$EDITABLE_QUEUE_FILE"
    exec 7< "$canary_file" || {
      echo "$(date): Cannot open $canary_file before job $JOB_ID; slot ${SLOT_ID} stopping." >> "$WORKER_LOG_FILE"
      return 1
    }
    job_started_epoch=$(date +%s)

    # Run against a node-local log. Publish a bounded tail periodically for
    # live inspection, then overwrite it in place with the complete log.
    if [ "$ADAPTIVE_MODE" -eq 1 ] && command -v setsid >/dev/null 2>&1; then
      setsid bash -c "$JOB_COMMAND" 7<&- >> "$local_job_log_file" 2>&1 &
    elif [ "$ADAPTIVE_MODE" -eq 1 ]; then
      perl -MPOSIX -e 'POSIX::setsid() >= 0 or die "setsid failed\n"; exec @ARGV' \
        bash -c "$JOB_COMMAND" 7<&- >> "$local_job_log_file" 2>&1 &
    else
      eval "$JOB_COMMAND" 7<&- >> "$local_job_log_file" 2>&1 &
    fi
    job_pid=$!
    if [ "$JOB_LOG_SYNC_SEC" -gt 0 ]; then
      sync_job_log 7<&- &
      log_sync_pid=$!
    fi
    wait "$job_pid"
    JOB_EXIT_CODE=$?
    stop_log_sync
    share_interrupted=0
    share_handle_revoked && share_interrupted=1
    exec 7<&-

    if ! publish_final_job_log "$local_job_log_file" "$JOB_LOG_FILE"; then
      echo "$(date): Failed to publish final log for job $JOB_ID on $HOST_NAME; preserved local log at $local_job_log_file." >> "$WORKER_LOG_FILE"
    fi

    if [ "$share_interrupted" -eq 1 ]; then
      echo "$(date): The share connection was interrupted while job $JOB_ID ran on $HOST_NAME: its open files on the share were revoked, so a script read from the share may have stopped early. Verify its outputs; dependent stages wait for confirm-interrupted-jobs." >> "$WORKER_LOG_FILE"
    fi
    if [ "$JOB_EXIT_CODE" -eq 0 ] && [ "$share_interrupted" -eq 1 ] && ! record_share_interruption; then
      echo "$(date): Job $JOB_ID exited successfully across a share interruption that could not be recorded; claim retained. Audit outputs before retrying." >&2
      echo "$(date): Failed to record the share interruption for job $JOB_ID on $HOST_NAME; claim retained." >> "$WORKER_LOG_FILE"
      if [ "$ADAPTIVE_MODE" -eq 1 ]; then
        publish_adaptive_result infrastructure_failed "$JOB_EXIT_CODE" || return 1
      fi
      return 1
    fi

    if [ "$JOB_EXIT_CODE" -eq 0 ]; then
      DONE_ROOT="$LOG_DIR/done"
      if ! mkdir -p "$DONE_ROOT" || ! { : > "$DONE_ROOT/job_${JOB_ID}.done"; }; then
        echo "$(date): Job $JOB_ID exited successfully but its completion marker could not be written; claim retained. Audit outputs before retrying." >&2
        echo "$(date): Failed to publish completion marker for job $JOB_ID on $HOST_NAME; claim retained." >> "$WORKER_LOG_FILE"
        if [ "$ADAPTIVE_MODE" -eq 1 ]; then
          publish_adaptive_result infrastructure_failed "$JOB_EXIT_CODE" || return 1
        fi
        return 1
      fi
      echo "$(date): Completed job $JOB_ID on $HOST_NAME." >> "$WORKER_LOG_FILE"
    else
      echo "$(date): Job $JOB_ID failed with exit code $JOB_EXIT_CODE on $HOST_NAME." >> "$WORKER_LOG_FILE"
    fi

    if [ "$ADAPTIVE_MODE" -eq 1 ]; then
      if [ "$JOB_EXIT_CODE" -eq 0 ]; then
        publish_adaptive_result succeeded "$JOB_EXIT_CODE" || return 1
      else
        publish_adaptive_result job_failed "$JOB_EXIT_CODE" || return 1
      fi
      return "$JOB_EXIT_CODE"
    fi

  done
}

# Now mmworker.sh is running as the session leader in a brand-new session.
echo "mmworker.sh is session leader (pid $$, pgid $(ps -o pgid= $$))."
echo "Currently reading from queue file at: $ORIGINAL_QUEUE_FILE"

if [ "$ADAPTIVE_MODE" -eq 1 ]; then
  process_job "$((WORKER_SLOT_OFFSET + 1))"
  worker_status=$?
  exit "$worker_status"
else
  # Run multiple jobs in parallel
  worker_pids=()
  for i in $(seq 1 "$NUM_JOBS"); do
    SLOT_ID=$((WORKER_SLOT_OFFSET + i))
    rm -f "$WORKER_STATE_DIR/${HOST_NAME}_slot${SLOT_ID}.done"
    process_job "$SLOT_ID" &
    worker_pids+=("$!")
  done

  # Wait for all background jobs to finish
  worker_status=0
  for worker_pid in "${worker_pids[@]}"; do
    wait "$worker_pid" || worker_status=1
  done
  [ "$worker_status" -eq 0 ] || exit "$worker_status"

  # Log that all jobs are complete
  echo "$(date): All jobs processed on $HOST_NAME." >> "$WORKER_LOG_FILE"
  printf "%s\n" "$(date +%s)" > "$WORKER_STATE_DIR/${HOST_NAME}.all_done"
fi
