#!/usr/bin/env bash
export PARALLEL_WORKERS_ROOT="${PARALLEL_WORKERS_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
MODEL_DIR=""
QUEUE_FILE="job_queue.txt"  # Default queue file name
HOSTS_FILE="${PARALLEL_WORKERS_HOSTS_FILE:-}"
BATCH_DIR=""
HARD_STOP=0
ADAPTIVE_CLOSE_TIMEOUT_SEC="${ADAPTIVE_CLOSE_TIMEOUT_SEC:-30}"
ADAPTIVE_SCHEDULER_SCRIPT="${ADAPTIVE_SCHEDULER_SCRIPT:-${PARALLEL_WORKERS_ROOT}/adaptive_scheduler.sh}"
VISIBLE_POOL_FILE="${ADAPTIVE_POOL_MARKER:-${ADAPTIVE_STATE_ROOT:-${PARALLEL_WORKERS_ROOT}/.parallel-workers/adaptive}/pool.env}"

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --model_dir|--queue|--hosts_file|--batch_dir|--adaptive_close_timeout_sec|\
        --adaptive_scheduler_script)
            [ "$#" -ge 2 ] || { echo "Error: $1 requires a value" >&2; exit 2; }
            ;;
    esac
    case "$1" in
        --model_dir)
            MODEL_DIR="$2"
            shift 2
            ;;
        --queue)
            QUEUE_FILE="$2"
            shift 2
            ;;
        --hosts_file)
            HOSTS_FILE="$2"
            shift 2
            ;;
        --batch_dir)
            BATCH_DIR="$2"
            shift 2
            ;;
        --hard)
            HARD_STOP=1
            shift
            ;;
        --adaptive_close_timeout_sec)
            ADAPTIVE_CLOSE_TIMEOUT_SEC="$2"
            shift 2
            ;;
        --adaptive_scheduler_script)
            ADAPTIVE_SCHEDULER_SCRIPT="$2"
            shift 2
            ;;
        *)
            echo "Error: Unknown argument: $1"
            echo "Usage: $0 --model_dir <dir> [--queue <file>] [--hosts_file <file>] [--batch_dir <dir>] [--hard]"
            exit 1
            ;;
    esac
done

if [ -z "$MODEL_DIR" ]; then
  echo "Error: --model_dir is required"
  echo "Usage: $0 --model_dir <dir> [--queue <file>] [--hosts_file <file>] [--batch_dir <dir>] [--hard]"
  exit 1
fi

# Specify user paths
# Check if the path ends with a '/' using parameter expansion
if [[ "${MODEL_DIR}" != */ ]]; then
    MODEL_DIR="${MODEL_DIR}/"
fi

env_value() {
  local file="$1" key="$2" count
  count="$(awk -F= -v key="$key" '$1 == key {n++} END {print n+0}' "$file")"
  [ "$count" -eq 1 ] || {
    echo "Error: $file must contain exactly one $key entry." >&2
    return 1
  }
  awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$file"
}

optional_env_value() {
  local file="$1" key="$2" count
  count="$(awk -F= -v key="$key" '$1 == key {n++} END {print n+0}' "$file")"
  [ "$count" -le 1 ] || {
    echo "Error: $file contains duplicate $key entries." >&2
    return 1
  }
  [ "$count" -eq 1 ] || return 0
  awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$file"
}

path_identity() {
  local path="${1%/}" parent leaf
  parent="$(dirname "$path")"
  leaf="$(basename "$path")"
  [ -d "$parent" ] || return 1
  printf '%s/%s\n' "$(cd "$parent" && pwd -P)" "$leaf"
}

QUEUE_BASENAME="$(basename "$QUEUE_FILE")"
if [[ "$QUEUE_FILE" = /* ]]; then
  REQUESTED_QUEUE_FILE="$QUEUE_FILE"
else
  REQUESTED_QUEUE_FILE="${MODEL_DIR}queues/${QUEUE_FILE}"
fi
TAG_BASE="${MODEL_DIR}${QUEUE_FILE}"
TAG_CLEAN="${TAG_BASE//\//_}"
if [[ -n "$HOSTS_FILE" ]]; then
  CONFIG_FILE="$HOSTS_FILE"
else
  CONFIG_FILE=""
fi
LOCK_PARENT_DIR="${MODEL_DIR}lock/"
LOCK_FILE="${QUEUE_BASENAME%.txt}.lock"
LOCK_DIR="${LOCK_PARENT_DIR}${LOCK_FILE}"
ACTIVE_RUN_DIR="${LOCK_DIR}.active"

EXECUTION_MODE=""
RECORDED_TAG=""
RECORDED_LOG_DIR=""
BATCH_VALIDATED=0
if [ -f "$ACTIVE_RUN_DIR/owner.env" ]; then
  EXECUTION_MODE="$(env_value "$ACTIVE_RUN_DIR/owner.env" execution_mode)" || exit 1
  RECORDED_TAG="$(env_value "$ACTIVE_RUN_DIR/owner.env" tag_clean)" || exit 1
  RECORDED_LOG_DIR="$(optional_env_value "$ACTIVE_RUN_DIR/owner.env" log_dir)" || exit 1
  case "$EXECUTION_MODE" in
    ''|shared-queue|host-local-shards|adaptive-shared-queue) ;;
    *) echo "Error: invalid execution mode in $ACTIVE_RUN_DIR/owner.env" >&2; exit 1 ;;
  esac
  [ -z "$RECORDED_TAG" ] || TAG_CLEAN="$RECORDED_TAG"
  [ -n "$BATCH_DIR" ] || BATCH_DIR="$RECORDED_LOG_DIR"
fi

if [ -n "$BATCH_DIR" ]; then
  [ -d "$BATCH_DIR" ] || { echo "Error: batch directory not found: $BATCH_DIR" >&2; exit 1; }
  [ -f "$BATCH_DIR/run_meta.env" ] || {
    echo "Error: batch metadata not found: $BATCH_DIR/run_meta.env" >&2
    exit 1
  }
  BATCH_MODE="$(env_value "$BATCH_DIR/run_meta.env" execution_mode)"
  BATCH_TAG="$(env_value "$BATCH_DIR/run_meta.env" tag_clean)"
  BATCH_QUEUE_FILE="$(env_value "$BATCH_DIR/run_meta.env" original_queue_file)"
  case "$BATCH_MODE" in
    shared-queue|host-local-shards|adaptive-shared-queue) ;;
    *) echo "Error: invalid execution mode in $BATCH_DIR/run_meta.env" >&2; exit 1 ;;
  esac
  [ -n "$BATCH_TAG" ] && [ -n "$BATCH_QUEUE_FILE" ] || {
    echo "Error: incomplete batch identity in $BATCH_DIR/run_meta.env" >&2
    exit 1
  }
  REQUESTED_QUEUE_ID="$(path_identity "$REQUESTED_QUEUE_FILE")" || {
    echo "Error: cannot resolve requested queue path: $REQUESTED_QUEUE_FILE" >&2
    exit 1
  }
  BATCH_QUEUE_ID="$(path_identity "$BATCH_QUEUE_FILE")" || {
    echo "Error: cannot resolve recorded queue path: $BATCH_QUEUE_FILE" >&2
    exit 1
  }
  [ "$REQUESTED_QUEUE_ID" = "$BATCH_QUEUE_ID" ] || {
    echo "Error: --batch_dir belongs to a different queue: $BATCH_QUEUE_FILE" >&2
    exit 1
  }
  if [ -f "$ACTIVE_RUN_DIR/owner.env" ]; then
    [ "$EXECUTION_MODE" = "$BATCH_MODE" ] && [ "$RECORDED_TAG" = "$BATCH_TAG" ] && \
      [ "$(path_identity "$RECORDED_LOG_DIR")" = "$(path_identity "$BATCH_DIR")" ] || {
        echo "Error: batch metadata does not match the active queue guard." >&2
        exit 1
      }
  fi
  EXECUTION_MODE="$BATCH_MODE"
  TAG_CLEAN="$BATCH_TAG"
  BATCH_VALIDATED=1
fi

if { [ -e "$VISIBLE_POOL_FILE" ] || [ -L "$VISIBLE_POOL_FILE" ]; } && \
    [ "$EXECUTION_MODE" != adaptive-shared-queue ] && [ "$BATCH_VALIDATED" -eq 0 ]; then
  echo "Error: queue mode is unknown or not bound to validated batch metadata while an adaptive pool marker exists; specify --batch_dir." >&2
  exit 1
fi

if [ "$EXECUTION_MODE" != "adaptive-shared-queue" ] && [ -z "$CONFIG_FILE" ]; then
  if [[ -f ${PARALLEL_WORKERS_ROOT}/hosts/hosts-r.txt ]]; then
    CONFIG_FILE=${PARALLEL_WORKERS_ROOT}/hosts/hosts-r.txt
  else
    CONFIG_FILE=${PARALLEL_WORKERS_ROOT}/hosts-r.txt
  fi
fi

if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  [ "$HARD_STOP" -eq 1 ] || {
    echo "Error: adaptive jobs are non-preemptive; pass --hard for an explicit tagged stop." >&2
    exit 1
  }
  if [ -n "$RECORDED_LOG_DIR" ] && \
      [ "$(path_identity "$BATCH_DIR")" != "$(path_identity "$RECORDED_LOG_DIR")" ]; then
    echo "Error: --batch_dir does not match the guarded adaptive batch: $RECORDED_LOG_DIR" >&2
    exit 1
  fi
  [ -d "$BATCH_DIR" ] || { echo "Error: adaptive batch directory not found: $BATCH_DIR" >&2; exit 1; }
  [ -x "$ADAPTIVE_SCHEDULER_SCRIPT" ] || {
    echo "Error: adaptive scheduler not executable: $ADAPTIVE_SCHEDULER_SCRIPT" >&2
    exit 1
  }
  case "$ADAPTIVE_CLOSE_TIMEOUT_SEC" in ''|*[!0-9]*)
    echo "Error: --adaptive_close_timeout_sec must be a non-negative integer." >&2
    exit 2 ;;
  esac
  [ -f "$BATCH_DIR/run_meta.env" ] || {
    echo "Error: adaptive batch metadata not found: $BATCH_DIR/run_meta.env" >&2
    exit 1
  }
  REGISTRATION_FILE="$(env_value "$BATCH_DIR/run_meta.env" registration_file)"
  [ -n "$REGISTRATION_FILE" ] || {
    echo "Error: adaptive registration path is missing from $BATCH_DIR/run_meta.env" >&2
    exit 1
  }
  POOL_FILE="${ADAPTIVE_POOL_MARKER:-$(dirname "$(dirname "$REGISTRATION_FILE")")/pool.env}"
  [ -f "$POOL_FILE" ] && [ ! -L "$POOL_FILE" ] || {
    echo "Error: adaptive pool marker not found or ambiguous: $POOL_FILE" >&2
    exit 1
  }
  POOL_STATE="$(awk -F= '$1 == "state" {print $2; exit}' "$POOL_FILE")"
  case "$POOL_STATE" in
    ACTIVE) "$ADAPTIVE_SCHEDULER_SCRIPT" close-for-hard-stop --batch_dir "$BATCH_DIR" || exit 1 ;;
    DRAINING) ;;
    *) echo "Error: adaptive pool cannot be hard-stopped from state '$POOL_STATE'." >&2; exit 1 ;;
  esac
  "$ADAPTIVE_SCHEDULER_SCRIPT" wait-closed --batch_dir "$BATCH_DIR" \
    --timeout_sec "$ADAPTIVE_CLOSE_TIMEOUT_SEC" || {
      echo "Error: adaptive admission closure was not confirmed; no jobs were signaled." >&2
      exit 1
    }
fi

# Ensure the hosts file exists
if [ "$EXECUTION_MODE" != "adaptive-shared-queue" ] && [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Hosts file not found at $CONFIG_FILE"
  exit 1
fi

if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  RESOURCES_FILE="$(awk -F= '$1 == "resources_file" {sub(/^[^=]*=/, ""); print; exit}' "$POOL_FILE")"
  [ -f "$RESOURCES_FILE" ] || { echo "Error: adaptive resource file not found: $RESOURCES_FILE" >&2; exit 1; }
  RESOURCES_HASH="$(awk -F= '$1 == "resources_sha256" {print $2; exit}' "$POOL_FILE")"
  CURRENT_RESOURCES_HASH="$(shasum -a 256 "$RESOURCES_FILE" 2>/dev/null | awk '{print $1}')"
  [ -n "$RESOURCES_HASH" ] && [ "$CURRENT_RESOURCES_HASH" = "$RESOURCES_HASH" ] || {
    echo "Error: adaptive resource file differs from the pool record; no jobs were signaled." >&2
    exit 1
  }
  HOST_LIST="$(awk -F '\t' 'NR > 1 && $5 == 1 {print $2}' "$RESOURCES_FILE")"
  ADAPTIVE_STATE_ROOT="$(dirname "$POOL_FILE")"
  QUEUE_ID="$(env_value "$BATCH_DIR/run_meta.env" run_token)"
else
  # Match start_workers.sh: zero-slot (disabled) hosts never received workers.
  HOST_LIST="$(awk 'NF && $1 !~ /^#/ && $2 + 0 > 0 {print $1}' "$CONFIG_FILE")"
fi
case "$TAG_CLEAN" in *"'"*|*$'\n'*|*$'\r'*)
  echo "Error: unsafe job tag; no jobs were signaled." >&2
  exit 1 ;;
esac

# After a confirmed closure no host starts this queue's jobs, so a host SSH cannot reach
# holds none when its last-known agent status lists none of them.
unreachable_without_batch_jobs() {
  local host_id status
  [ "$EXECUTION_MODE" = "adaptive-shared-queue" ] && [ -n "$QUEUE_ID" ] || return 1
  host_id="$(awk -F '\t' -v target="$1" 'NR > 1 && $2 == target {print $1; exit}' "$RESOURCES_FILE")"
  status="$ADAPTIVE_STATE_ROOT/last-known/$host_id.tsv"
  [ -n "$host_id" ] && [ -f "$status" ] && [ ! -L "$status" ] || return 1
  awk -F '\t' -v queue="$QUEUE_ID" '
    NR == 1 { for (i = 1; i <= NF; i++) column[$i] = i; next }
    $column["record_type"] == "reservation" && $column["queue_id"] == queue { found = 1 }
    END { exit (found || !("queue_id" in column) || !("record_type" in column)) }
  ' "$status"
}

STOP_FAILED=0
for HOST in $HOST_LIST; do
  [ -n "${HOST:-}" ] || continue
  echo "Killing tagged processes on $HOST with --job_tag $TAG_CLEAN (including descendants)"

  # Force bash on the remote host (zsh doesn't split words by default).
  if ssh "$HOST" "TAG_CLEAN='$TAG_CLEAN' bash -lc '
    set -euo pipefail

    descend() {
      local p=\$1
      local kids
      kids=\$(pgrep -P \"\$p\" || true)
      for k in \$kids; do
        echo \"\$k\"
        descend \"\$k\"
      done
    }

    SKIP_PIDS=\" \$\$ \"
    ANCESTOR=\$PPID
    while [[ \"\$ANCESTOR\" =~ ^[0-9]+\$ ]] && [ \"\$ANCESTOR\" -gt 0 ]; do
      SKIP_PIDS=\"\$SKIP_PIDS\$ANCESTOR \"
      NEXT=\$(ps -o ppid= -p \"\$ANCESTOR\" 2>/dev/null | tr -d \" \" || true)
      [ \"\$NEXT\" != \"\$ANCESTOR\" ] || break
      ANCESTOR=\$NEXT
    done
    tagged_pids() {
      local candidate command
      while read -r candidate command; do
        [[ \"\$candidate\" =~ ^[0-9]+\$ ]] || continue
        case \"\$SKIP_PIDS\" in *\" \$candidate \"*) continue ;; esac
        case \" \$command \" in
          *\" \$TAG_CLEAN \"*) printf \"%s\\n\" \"\$candidate\" ;;
        esac
      done < <(ps -ww -axo pid=,command=)
    }
    PIDS=\$(tagged_pids)
    TARGET_PIDS=""
    for PID in \$PIDS; do
      [ -z \"\$PID\" ] && continue
      echo \"Found tagged PID \$PID on \$(hostname)\"

      # Kill descendants first (children → grandchildren → ...)
      DESC=\$(descend \"\$PID\" | awk \"NF\" | sort -u || true)
      if [ -n \"\$DESC\" ]; then
        echo \"Killing descendants of \$PID: \$DESC\"
        for d in \$DESC; do
          kill -TERM \"\$d\" 2>/dev/null || true
        done
      fi

      echo \"Killing PID \$PID on \$(hostname)\"
      kill -TERM \"\$PID\" 2>/dev/null || true
      TARGET_PIDS=\"\$TARGET_PIDS \$DESC \$PID\"
    done
    TARGET_PIDS=\$(printf \"%s\\n\" \$TARGET_PIDS | awk \"NF\" | sort -un || true)
    live_targets() {
      local candidate
      for candidate in \$TARGET_PIDS; do
        kill -0 \"\$candidate\" 2>/dev/null && printf \"%s\\n\" \"\$candidate\"
      done
      return 0
    }
    SURVIVORS=\$(live_targets)
    for ATTEMPT in 1 2 3 4 5; do
      [ -z \"\$SURVIVORS\" ] && break
      sleep 1
      SURVIVORS=\$(live_targets)
    done
    [ -z \"\$SURVIVORS\" ] || {
      echo \"Tagged processes survived TERM on \$(hostname) (including descendants): \$SURVIVORS\" >&2
      exit 8
    }
  '"; then
    :
  elif [ $? -eq 255 ] && unreachable_without_batch_jobs "$HOST"; then
    echo "Skipped $HOST: unreachable, and its last-known agent status holds no job from this batch"
  else
    echo "Failed to stop processes on $HOST" >&2
    STOP_FAILED=1
  fi
done

if [ "$STOP_FAILED" -ne 0 ]; then
  if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
    echo "Error: adaptive hard stop was incomplete; the batch guard was retained." >&2
  else
    echo "Error: worker stop was incomplete; the queue guard was retained." >&2
  fi
  exit 1
fi

# A host-local-shards batch has a storage-local progress coordinator. The
# selected worker host file may omit the coordinator, so stop that exact recorded PID here
# when this command is run on the coordinator host.
if [ -f "$ACTIVE_RUN_DIR/owner.env" ]; then
  COORDINATOR_PID="$(awk -F= '$1 == "coordinator_pid" {value=$2} END {print value}' "$ACTIVE_RUN_DIR/owner.env")"
  COORDINATOR_TAG="$(awk -F= '$1 == "tag_clean" {print $2; exit}' "$ACTIVE_RUN_DIR/owner.env")"
  if [[ "$COORDINATOR_PID" =~ ^[0-9]+$ ]] && [ -n "$COORDINATOR_TAG" ]; then
    COORDINATOR_COMMAND="$(ps -ww -p "$COORDINATOR_PID" -o command= 2>/dev/null || true)"
    if [[ " $COORDINATOR_COMMAND " == *" $COORDINATOR_TAG "* ]]; then
      echo "Stopping shard progress coordinator PID $COORDINATOR_PID on $(hostname)"
      kill -TERM "$COORDINATOR_PID" 2>/dev/null || true
    fi
  fi
fi


# Remove any stale lock files
if [ -d "$LOCK_DIR" ]; then
  echo "Removing stale lock file."
  rmdir "$LOCK_DIR" || echo "Failed to remove lock directory. Check permissions or active processes."
else
  echo "No lock file found. Nothing to clean up."
fi

echo "All workers and related processes stopped."
