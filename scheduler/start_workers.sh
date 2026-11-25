#!/usr/bin/env bash
export ROTA_ROOT="${ROTA_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
MODEL_DIR=""
QUEUE_FILE="job_queue.txt"  # Default queue file name
HOSTS_FILE="${ROTA_HOSTS_FILE:-}"
RUN_NAME=""
PROJECT_NAME=""
PROJECT_OPTION_SET=0
RECOVER_INACTIVE=0
ALLOW_NETWORK_COORDINATOR=0
EXECUTION_MODE="shared-queue"
SCHEDULE_PRIORITY=""
HOST_PRIORITIES=""
CPU_UNITS_PER_JOB=""
MEMORY_MIB_PER_JOB=""
MEMORY_CALIBRATION_JOBS=0
MEMORY_CALIBRATION_SECONDS=0
MEMORY_MIN_MIB_PER_JOB=""
MEMORY_CALIBRATION_ENABLED=0
SHARED_IO_UNITS_PER_JOB=""
GPU_UNITS_PER_JOB=0
MEMORY_LIMIT_MIB=""
MAX_WORKERS=""
ADAPTIVE_ADMISSION="enabled"
AFTER_QUEUES=()
DEPENDENCIES='[]'
RETRY_LOST_JOBS=0
EARLY_ETA_SAMPLE=0
WORKFLOW_ENABLED=0
SHARD_PROGRESS_SYNC_SEC="${SHARD_PROGRESS_SYNC_SEC:-30}"
SSH_CONNECT_TIMEOUT_SEC="${SSH_CONNECT_TIMEOUT_SEC:-8}"
SSH_LAUNCH_TIMEOUT_SEC="${SSH_LAUNCH_TIMEOUT_SEC:-20}"
WORKER_READY_TIMEOUT_SEC="${WORKER_READY_TIMEOUT_SEC:-12}"
WORKER_RELEASE_DELAY_SEC="${WORKER_RELEASE_DELAY_SEC:-15}"
BATCH_ALLOC_RETRY_SEC="${BATCH_ALLOC_RETRY_SEC:-1}"
BATCH_ALLOC_MAX_ATTEMPTS="${BATCH_ALLOC_MAX_ATTEMPTS:-60}"
SSH_COMMON_OPTS=(
  -o BatchMode=yes
  -o ConnectTimeout="$SSH_CONNECT_TIMEOUT_SEC"
  -o ConnectionAttempts=1
)

usage() {
  echo "Usage: $0 --model_dir <MODEL_DIR> [--queue <queue_file_name>] [--hosts_file <hosts_file>] [--run_name <label>] [--project <source-repo-label>] [--execution_mode shared-queue|host-local-shards|adaptive-shared-queue] [--shard_progress_sync_sec <seconds>] [--recover_inactive] [--allow_network_coordinator]"
}

valid_project_name() {
  [[ "$1" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] && [ "${#1}" -le 64 ]
}

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --model_dir|--queue|--hosts_file|--run_name|--project|--execution_mode|--schedule_priority|\
        --cpu_units_per_job|--memory_mib_per_job|--shared_io_units_per_job|--max_workers|\
        --gpu_units_per_job|--memory_limit_mib|\
        --memory_calibration_jobs|--memory_calibration_seconds|--memory_min_mib_per_job|\
        --adaptive_admission|--shard_progress_sync_sec|--after)
            [ "$#" -ge 2 ] || { echo "Error: $1 requires a value" >&2; exit 2; }
            ;;
    esac
    case "$1" in
        --after) AFTER_QUEUES+=("$2"); WORKFLOW_ENABLED=1; shift 2 ;;
        --retry_lost_jobs) RETRY_LOST_JOBS=1; WORKFLOW_ENABLED=1; shift ;;
        --early_eta_sample) EARLY_ETA_SAMPLE=1; WORKFLOW_ENABLED=1; shift ;;
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
        --run_name)
            RUN_NAME="$2"
            shift 2
            ;;
        --project)
            PROJECT_NAME="$2"
            PROJECT_OPTION_SET=1
            shift 2
            ;;
        --execution_mode)
            EXECUTION_MODE="$2"
            shift 2
            ;;
        --schedule_priority)
            SCHEDULE_PRIORITY="$2"
            shift 2
            ;;
        --cpu_units_per_job)
            CPU_UNITS_PER_JOB="$2"
            shift 2
            ;;
        --memory_mib_per_job)
            MEMORY_MIB_PER_JOB="$2"
            shift 2
            ;;
        --memory_calibration_jobs)
            MEMORY_CALIBRATION_JOBS="$2"
            shift 2
            ;;
        --memory_calibration_seconds)
            MEMORY_CALIBRATION_SECONDS="$2"
            shift 2
            ;;
        --memory_min_mib_per_job)
            MEMORY_MIN_MIB_PER_JOB="$2"
            shift 2
            ;;
        --shared_io_units_per_job)
            SHARED_IO_UNITS_PER_JOB="$2"
            shift 2
            ;;
        --gpu_units_per_job)
            GPU_UNITS_PER_JOB="$2"
            shift 2
            ;;
        --memory_limit_mib)
            MEMORY_LIMIT_MIB="$2"
            shift 2
            ;;
        --max_workers)
            MAX_WORKERS="$2"
            shift 2
            ;;
        --adaptive_admission)
            ADAPTIVE_ADMISSION="$2"
            shift 2
            ;;
        --host_local_shards)
            EXECUTION_MODE="host-local-shards"
            shift
            ;;
        --shard_progress_sync_sec)
            SHARD_PROGRESS_SYNC_SEC="$2"
            shift 2
            ;;
        --recover_inactive)
            RECOVER_INACTIVE=1
            shift
            ;;
        --allow_network_coordinator)
            ALLOW_NETWORK_COORDINATOR=1
            shift
            ;;
        *)
            echo "Error: Unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

if { [ "$GPU_UNITS_PER_JOB" != 0 ] || [ -n "$MEMORY_LIMIT_MIB" ]; } &&
    [ "$EXECUTION_MODE" != adaptive-shared-queue ]; then
  echo "Error: --gpu_units_per_job and --memory_limit_mib require adaptive mode." >&2
  exit 2
fi
if [ "$WORKFLOW_ENABLED" -eq 1 ] && [ "$EXECUTION_MODE" != adaptive-shared-queue ]; then
  echo "Error: --after, --retry_lost_jobs and --early_eta_sample require adaptive mode." >&2
  exit 2
fi

if [ -z "$MODEL_DIR" ]; then
  echo "Error: --model_dir is required"
  usage
  exit 1
fi
if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ] && [[ "$MODEL_DIR" != /* ]]; then
  echo "Error: adaptive --model_dir must be an absolute path." >&2
  exit 2
fi
if [ "$PROJECT_OPTION_SET" -eq 1 ]; then
  valid_project_name "$PROJECT_NAME" || {
    echo "Error: --project must be 1-64 characters: an ASCII letter or digit followed by letters, digits, '.', '_' or '-'." >&2
    exit 2
  }
elif PROJECT_ROOT="$(git -C "$MODEL_DIR" rev-parse --show-toplevel 2>/dev/null)"; then
  PROJECT_NAME="$(basename "$PROJECT_ROOT")"
  if ! valid_project_name "$PROJECT_NAME"; then
    echo "Warning: Git checkout name is not a supported --project label; leaving project_name unset." >&2
    PROJECT_NAME=""
  fi
fi

# Check if the path ends with a '/' using parameter expansion
if [[ "${MODEL_DIR}" != */ ]]; then
    MODEL_DIR="${MODEL_DIR}/"
fi

QUEUE_ARG="$QUEUE_FILE"
QUEUE_BASENAME="$(basename "$QUEUE_FILE")"
if [[ "$QUEUE_FILE" = /* ]]; then
  ORIGINAL_QUEUE_FILE="$QUEUE_FILE"
  QUEUE_FILE="$QUEUE_BASENAME"
else
  ORIGINAL_QUEUE_FILE="${MODEL_DIR}queues/${QUEUE_FILE}"
fi

# Update or specify other paths to use the overall path
if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  CONFIG_FILE=""
elif [[ -n "$HOSTS_FILE" ]]; then
  CONFIG_FILE="$HOSTS_FILE"
elif [[ -f ${ROTA_ROOT}/hosts/hosts-r.txt ]]; then
  CONFIG_FILE=${ROTA_ROOT}/hosts/hosts-r.txt
else
  CONFIG_FILE=${ROTA_ROOT}/hosts-r.txt
fi
WORKER_SCRIPT="${WORKER_SCRIPT:-${ROTA_ROOT}/mmworker.sh}"
ADAPTIVE_COORDINATOR_SCRIPT="${ADAPTIVE_COORDINATOR_SCRIPT:-$(dirname "$0")/adaptive_coordinator.py}"
SHARD_WORKER_SCRIPT="${SHARD_WORKER_SCRIPT:-${ROTA_ROOT}/mmshardworker.sh}"
SHARD_COORDINATOR_SCRIPT="${SHARD_COORDINATOR_SCRIPT:-${ROTA_ROOT}/mmshardcoordinator.sh}"
STATE_ROOT="${ROTA_STATE_ROOT:-${ROTA_ROOT}/.rota}"
ADAPTIVE_STATE_ROOT="${ADAPTIVE_STATE_ROOT:-$STATE_ROOT/adaptive}"
ADAPTIVE_POOL_MARKER="${ADAPTIVE_POOL_MARKER:-$ADAPTIVE_STATE_ROOT/pool.env}"
MODE_TRANSITION_LOCK="${MODE_TRANSITION_LOCK:-$(dirname "$ADAPTIVE_STATE_ROOT")/mode-transition.lock}"
[ "$ADAPTIVE_POOL_MARKER" = "$ADAPTIVE_STATE_ROOT/pool.env" ] || {
  echo "Error: ADAPTIVE_POOL_MARKER must be $ADAPTIVE_STATE_ROOT/pool.env" >&2
  exit 2
}
[ "$MODE_TRANSITION_LOCK" = "$(dirname "$ADAPTIVE_STATE_ROOT")/mode-transition.lock" ] || {
  echo "Error: MODE_TRANSITION_LOCK must be $(dirname "$ADAPTIVE_STATE_ROOT")/mode-transition.lock" >&2
  exit 2
}
HELD_GLOBAL_LOCK=""
LOCK_PARENT_DIR="${MODEL_DIR}lock/"
LOCK_FILE="${QUEUE_BASENAME%.txt}.lock"
LOCK_DIR="${LOCK_PARENT_DIR}${LOCK_FILE}"
ACTIVE_RUN_DIR="${LOCK_DIR}.active"
LEGACY_TAG_BASE="${MODEL_DIR}${QUEUE_ARG}"
LEGACY_TAG_CLEAN="${LEGACY_TAG_BASE//\//_}"

if [ ! -f "$ORIGINAL_QUEUE_FILE" ]; then
  echo "Error: queue file not found: $ORIGINAL_QUEUE_FILE" >&2
  exit 1
fi
if [ "$EXECUTION_MODE" != "adaptive-shared-queue" ] && [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: hosts file not found: $CONFIG_FILE" >&2
  exit 1
fi
case "$EXECUTION_MODE" in
  shared-queue|host-local-shards|adaptive-shared-queue) ;;
  *)
    echo "Error: unsupported --execution_mode: $EXECUTION_MODE" >&2
    exit 2
    ;;
esac
SHARED_WORKER_HASH=""
if [ "$EXECUTION_MODE" = "shared-queue" ]; then
  if [ ! -r "$WORKER_SCRIPT" ] || ! SHARED_WORKER_HASH="$(shasum -a 256 "$WORKER_SCRIPT" 2>/dev/null | awk '{print $1}')" \
      || [ -z "$SHARED_WORKER_HASH" ]; then
    echo "Error: unable to read and hash the shared worker runner on the coordinator: $WORKER_SCRIPT" >&2
    echo "No batch or launch guard has been allocated." >&2
    exit 1
  fi
fi
if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  # A per-host SPEC is validated against the pool host table once it is verified.
  [[ "$SCHEDULE_PRIORITY" =~ ^-?[0-9]+$ || "$SCHEDULE_PRIORITY" == *[,=]* ]] || {
    echo "Error: --schedule_priority must be an integer or a per-host SPEC for adaptive mode." >&2; exit 2;
  }
  [[ "$CPU_UNITS_PER_JOB" =~ ^[0-9]+$ ]] && [ "$CPU_UNITS_PER_JOB" -gt 0 ] && \
    [[ "$MEMORY_MIB_PER_JOB" =~ ^[0-9]+$ ]] && [ "$MEMORY_MIB_PER_JOB" -gt 0 ] || {
      echo "Error: adaptive CPU and memory declarations must be positive integers." >&2; exit 2;
    }
  [[ "$SHARED_IO_UNITS_PER_JOB" =~ ^[0-9]+$ ]] && [[ "$MAX_WORKERS" =~ ^[0-9]+$ ]] || {
    echo "Error: adaptive shared-I/O units and max workers must be non-negative integers." >&2; exit 2;
  }
  [[ "$GPU_UNITS_PER_JOB" =~ ^[0-9]+$ ]] || {
    echo "Error: --gpu_units_per_job must be a non-negative integer." >&2; exit 2;
  }
  [ -z "$MEMORY_LIMIT_MIB" ] || { [[ "$MEMORY_LIMIT_MIB" =~ ^[1-9][0-9]*$ ]] &&
      [ "$MEMORY_LIMIT_MIB" -ge "$MEMORY_MIB_PER_JOB" ]; } || {
    echo "Error: --memory_limit_mib must be an integer no smaller than --memory_mib_per_job." >&2; exit 2;
  }
  [[ "$ADAPTIVE_ADMISSION" = enabled || "$ADAPTIVE_ADMISSION" = paused ]] || {
    echo "Error: adaptive admission must be enabled or paused." >&2; exit 2;
  }
  [[ "$MEMORY_CALIBRATION_JOBS" =~ ^[0-9]+$ ]] && \
    { [ "$MEMORY_CALIBRATION_JOBS" -eq 0 ] || { [ "$MEMORY_CALIBRATION_JOBS" -ge 20 ] && [ "$MEMORY_CALIBRATION_JOBS" -le 50 ]; }; } || {
      echo "Error: memory calibration jobs must be 0 or an integer from 20 to 50." >&2; exit 2;
    }
  [[ "$MEMORY_CALIBRATION_SECONDS" =~ ^[0-9]+$ ]] && \
    { [ "$MEMORY_CALIBRATION_SECONDS" -eq 0 ] || { [ "$MEMORY_CALIBRATION_SECONDS" -ge 600 ] && [ "$MEMORY_CALIBRATION_SECONDS" -le 86400 ]; }; } || {
      echo "Error: memory calibration seconds must be 0 or an integer from 600 to 86400." >&2; exit 2;
    }
  if [ "$MEMORY_CALIBRATION_JOBS" -gt 0 ] || [ "$MEMORY_CALIBRATION_SECONDS" -gt 0 ]; then
    MEMORY_CALIBRATION_ENABLED=1
    MEMORY_MIN_MIB_PER_JOB="${MEMORY_MIN_MIB_PER_JOB:-64}"
    [[ "$MEMORY_MIN_MIB_PER_JOB" =~ ^[0-9]+$ ]] && [ "$MEMORY_MIN_MIB_PER_JOB" -gt 0 ] && \
      [ "$MEMORY_MIN_MIB_PER_JOB" -le "$MEMORY_MIB_PER_JOB" ] || {
        echo "Error: memory minimum must be a positive integer no larger than requested memory." >&2; exit 2;
      }
  elif [ -n "$MEMORY_MIN_MIB_PER_JOB" ]; then
    echo "Error: memory minimum requires an enabled calibration trigger." >&2; exit 2
  fi
elif [ "$MEMORY_CALIBRATION_JOBS" != 0 ] || [ "$MEMORY_CALIBRATION_SECONDS" != 0 ] || \
    [ -n "$MEMORY_MIN_MIB_PER_JOB" ]; then
  echo "Error: memory calibration requires adaptive mode." >&2
  exit 2
fi
case "$SHARD_PROGRESS_SYNC_SEC" in
  ''|*[!0-9]*)
    echo "Error: --shard_progress_sync_sec must be a positive integer." >&2
    exit 2
    ;;
esac
if [ "$SHARD_PROGRESS_SYNC_SEC" -lt 1 ]; then
  echo "Error: --shard_progress_sync_sec must be at least 1." >&2
  exit 2
fi
case "$WORKER_READY_TIMEOUT_SEC:$WORKER_RELEASE_DELAY_SEC" in
  *[!0-9:]*|:*|*::*|*:)
    echo "Error: worker readiness and release delays must be positive integers." >&2
    exit 2
    ;;
esac
if [ "$WORKER_READY_TIMEOUT_SEC" -lt 1 ] || [ "$WORKER_RELEASE_DELAY_SEC" -lt 1 ]; then
  echo "Error: worker readiness and release delays must be at least 1." >&2
  exit 2
fi
# The tag is interpolated into job commands and exec -a names unquoted.
case "$LEGACY_TAG_CLEAN" in
  *[!A-Za-z0-9._+=@,:%~^#-]*)
    echo "Error: queue paths may contain only letters, digits and ._+=@,:%~^#-/" \
      "(no spaces, quotes or shell metacharacters): $LEGACY_TAG_BASE" >&2
    exit 1
    ;;
esac

case "$(uname -s)" in
  Darwin)
    COORDINATOR_MOUNT="$(df -P "$MODEL_DIR" 2>/dev/null | awk 'NR == 2 {print $6}')"
    COORDINATOR_FS="$(mount | awk -v mountpoint="$COORDINATOR_MOUNT" '$3 == mountpoint {gsub(/[(),]/, "", $4); print $4; exit}')"
    COORDINATOR_FS="${COORDINATOR_FS:-unknown}"
    ;;
  *) COORDINATOR_FS="$(stat -f -c %T "$MODEL_DIR" 2>/dev/null || printf unknown)" ;;
esac
case "$COORDINATOR_FS" in
  cifs|smb*|nfs*|fuse*|osxfuse*)
    if [ "$ALLOW_NETWORK_COORDINATOR" -ne 1 ]; then
      echo "Error: refusing to coordinate a queue from network filesystem type '$COORDINATOR_FS'." >&2
      echo "Run this command on the storage host, where ${ROTA_ROOT} is directly attached." >&2
      echo "Use --allow_network_coordinator only for an explicitly audited exception." >&2
      exit 1
    fi
    echo "Warning: coordinating from network filesystem type '$COORDINATOR_FS' by explicit override." >&2
    ;;
  unknown|"")
    echo "Error: unable to verify that the coordinator filesystem is local." >&2
    exit 1
    ;;
esac

HOST_LIST=""
if [ "$EXECUTION_MODE" != "adaptive-shared-queue" ]; then
  HOST_LIST="$(awk 'NF && $1 !~ /^#/ && $2 + 0 > 0 {print $1}' "$CONFIG_FILE")"
  if [ -z "$HOST_LIST" ]; then
    echo "Error: no positive worker allocations found in $CONFIG_FILE" >&2
    exit 1
  fi
fi

validate_queue_file() {
  awk '
    NF {
      id=$1
      if (id !~ /^[A-Za-z0-9_.:-]+$/) {
        printf "Unsupported job ID on line %d: %s\n", NR, id > "/dev/stderr"
        bad=1
      }
      if (NF < 2) {
        printf "Missing command on queue line %d\n", NR > "/dev/stderr"
        bad=1
      }
      if (seen[id]++) {
        printf "Duplicate job ID: %s\n", id > "/dev/stderr"
        bad=1
      }
    }
    END { exit bad ? 1 : 0 }
  ' "$1"
}

TOTAL_JOBS=$(awk 'NF {n++} END {print n+0}' "$ORIGINAL_QUEUE_FILE")
if [ "$TOTAL_JOBS" -lt 1 ]; then
  echo "Error: queue contains no jobs: $ORIGINAL_QUEUE_FILE" >&2
  exit 1
fi
if ! validate_queue_file "$ORIGINAL_QUEUE_FILE"; then
  exit 2
fi
if [ "$EXECUTION_MODE" = "host-local-shards" ]; then
  if [ ! -f "$SHARD_WORKER_SCRIPT" ] || [ ! -f "$SHARD_COORDINATOR_SCRIPT" ]; then
    echo "Error: host-local shard scripts not found: $SHARD_WORKER_SCRIPT / $SHARD_COORDINATOR_SCRIPT" >&2
    exit 1
  fi
  for host in $HOST_LIST; do
    if ! ssh -n "${SSH_COMMON_OPTS[@]}" "$host" \
      'export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"; command -v parallel >/dev/null 2>&1' 2>/dev/null; then
      echo "Error: GNU Parallel is unavailable or $host cannot be reached; refusing host-local-shards launch." >&2
      exit 1
    fi
  done
fi

audit_tag_on_hosts() {
  local hosts="$1"
  local tag="$2"
  local host output rc

  for host in $hosts; do
    output="$(
      ssh -n "${SSH_COMMON_OPTS[@]}" "$host" \
        "ps -ww -axo pid=,command= | grep -F -- '$tag' | grep -v '[g]rep -F' || true" 2>/dev/null
    )"
    rc=$?
    if [ "$rc" -ne 0 ]; then
      echo "Error: cannot audit existing workers on $host; refusing an ambiguous launch." >&2
      return 2
    fi
    if [ -n "$output" ]; then
      echo "Error: existing processes for this queue were found on $host:" >&2
      printf '%s\n' "$output" >&2
      return 1
    fi
  done

  return 0
}

guard_value() {
  local key="$1"
  local owner_file="$ACTIVE_RUN_DIR/owner.env"
  [ -f "$owner_file" ] || return 0
  awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$owner_file"
}

archive_path() {
  local path="$1"
  local reason="$2"
  local archived="${path}.${reason}.$(date +%s).$$"
  mv "$path" "$archived"
  echo "Archived $path as $archived"
}

release_global_lock() {
  local owner_pid
  [ -n "$HELD_GLOBAL_LOCK" ] || return 0
  owner_pid="$(awk -F= '$1 == "pid" {print $2; exit}' "$HELD_GLOBAL_LOCK/owner.env" 2>/dev/null || true)"
  if [ "$owner_pid" = "$$" ]; then
    rm -f "$HELD_GLOBAL_LOCK/owner.env"
    rmdir "$HELD_GLOBAL_LOCK" 2>/dev/null || true
  fi
}
trap release_global_lock EXIT

acquire_global_lock() {
  local lock="$1"
  local owner_pid owner_host
  mkdir -p "$(dirname "$lock")"
  if ! mkdir "$lock" 2>/dev/null; then
    if [ "$RECOVER_INACTIVE" -eq 1 ] && [ -f "$lock/owner.env" ]; then
      owner_pid="$(awk -F= '$1 == "pid" {print $2; exit}' "$lock/owner.env")"
      owner_host="$(awk -F= '$1 == "host" {print $2; exit}' "$lock/owner.env")"
      if [ "$owner_host" = "$(hostname)" ] && [[ "$owner_pid" =~ ^[0-9]+$ ]] && \
          ! kill -0 "$owner_pid" 2>/dev/null; then
        rm -f "$lock/owner.env"
        rmdir "$lock" 2>/dev/null || {
          echo "Error: cannot reclaim audited scheduler transition: $lock" >&2
          exit 1
        }
        mkdir "$lock" 2>/dev/null || { echo "Error: cannot acquire $lock" >&2; exit 1; }
      else
        echo "Error: scheduler transition owner is not safely inactive: $lock" >&2
        exit 1
      fi
    else
      echo "Error: scheduler transition is locked: $lock" >&2
      [ ! -f "$lock/owner.env" ] || sed -n '1,5p' "$lock/owner.env" >&2
      echo "Audit the recorded owner; transition locks are not reclaimed by age." >&2
      exit 1
    fi
  fi
  HELD_GLOBAL_LOCK="$lock"
  {
    echo "pid=$$"
    echo "host=$(hostname)"
    echo "execution_mode=$EXECUTION_MODE"
    echo "created_epoch=$(date +%s)"
  } > "$lock/owner.env"
}

state_value() {
  local file="$1" key="$2" count
  count="$(awk -F= -v key="$key" '$1 == key {n++} END {print n+0}' "$file")"
  if [ "$count" -ne 1 ]; then
    echo "Error: $file must contain exactly one $key entry." >&2
    return 1
  fi
  awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$file"
}

handle_existing_guard() {
  local prior_hosts prior_tag prior_log_dir prior_total prior_mode prior_coordinator_pid
  local prior_registration prior_run_token prior_status prior_status_schema prior_status_queue
  local prior_lifecycle prior_running prior_target prior_propagation
  local prior_meta meta_mode meta_tag meta_run_token meta_registration
  local done_count shard_env shard_done coordinator_command

  [ -d "$ACTIVE_RUN_DIR" ] || return 0
  prior_hosts="$(guard_value hosts)"
  prior_tag="$(guard_value tag_clean)"

  if [ -z "$prior_hosts" ]; then
    echo "Error: launch guard is incomplete: $ACTIVE_RUN_DIR" >&2
    echo "Inspect it manually; it will not be reclaimed by age." >&2
    return 1
  fi
  if [ -z "$prior_tag" ]; then
    if [ "$RECOVER_INACTIVE" -ne 1 ]; then
      echo "Error: launch guard was interrupted before its batch tag was recorded: $ACTIVE_RUN_DIR" >&2
      echo "Audit the listed hosts, then use --recover_inactive to archive this incomplete allocation." >&2
      return 1
    fi
    audit_tag_on_hosts "$prior_hosts" "$LEGACY_TAG_CLEAN" || return $?
    echo "Recovering an audited allocation interrupted before worker launch." >&2
    archive_path "$ACTIVE_RUN_DIR" recovered_incomplete
    return 0
  fi

  prior_hosts="$(state_value "$ACTIVE_RUN_DIR/owner.env" hosts)" || return 1
  prior_tag="$(state_value "$ACTIVE_RUN_DIR/owner.env" tag_clean)" || return 1
  prior_log_dir="$(state_value "$ACTIVE_RUN_DIR/owner.env" log_dir)" || return 1
  prior_total="$(state_value "$ACTIVE_RUN_DIR/owner.env" total_jobs)" || return 1
  prior_mode="$(state_value "$ACTIVE_RUN_DIR/owner.env" execution_mode)" || return 1
  prior_run_token="$(state_value "$ACTIVE_RUN_DIR/owner.env" run_token)" || return 1
  case "$prior_mode" in
    shared-queue|host-local-shards|adaptive-shared-queue) ;;
    *)
      echo "Error: unsupported execution mode in prior launch guard: $prior_mode" >&2
      return 1
      ;;
  esac
  if [ -z "$prior_hosts" ] || [ -z "$prior_tag" ] || [ -z "$prior_log_dir" ] ||
      [ -z "$prior_run_token" ]; then
    echo "Error: the prior launch guard is missing durable batch identity." >&2
    return 1
  fi
  prior_meta="$prior_log_dir/run_meta.env"
  if [ ! -f "$prior_meta" ]; then
    echo "Error: the prior launch guard has no matching run metadata: $prior_meta" >&2
    return 1
  fi
  meta_mode="$(state_value "$prior_meta" execution_mode)" || return 1
  meta_tag="$(state_value "$prior_meta" tag_clean)" || return 1
  meta_run_token="$(state_value "$prior_meta" run_token)" || return 1
  if [ "$prior_mode" != "$meta_mode" ] || [ "$prior_tag" != "$meta_tag" ] ||
      [ "$prior_run_token" != "$meta_run_token" ]; then
    echo "Error: the prior launch guard does not match its run metadata: $prior_meta" >&2
    return 1
  fi
  prior_coordinator_pid="$(guard_value coordinator_pid)"
  if [ "$prior_mode" = "adaptive-shared-queue" ]; then
    prior_registration="$(state_value "$ACTIVE_RUN_DIR/owner.env" registration_file)" || return 1
    meta_registration="$(state_value "$prior_meta" registration_file)" || return 1
    if [ "$prior_registration" != "$meta_registration" ]; then
      echo "Error: the prior adaptive launch guard does not match its queue registration metadata." >&2
      return 1
    fi
    if [ -z "$prior_registration" ] || [ -z "$prior_run_token" ] || [ -z "$prior_log_dir" ]; then
      echo "Error: the prior adaptive launch guard is missing durable queue identity." >&2
      return 1
    fi
    if [ -e "$prior_registration" ] || [ -L "$prior_registration" ]; then
      echo "Error: the prior adaptive queue is still registered: $prior_registration" >&2
      return 1
    fi
    prior_status="$prior_log_dir/adaptive/status.env"
    if [ ! -f "$prior_status" ]; then
      echo "Error: the prior adaptive queue has no durable terminal status: $prior_status" >&2
      return 1
    fi
    prior_status_schema="$(state_value "$prior_status" schema_version)" || return 1
    prior_status_queue="$(state_value "$prior_status" queue_id)" || return 1
    prior_lifecycle="$(state_value "$prior_status" lifecycle)" || return 1
    prior_running="$(state_value "$prior_status" running_workers)" || return 1
    prior_target="$(state_value "$prior_status" target_workers)" || return 1
    prior_propagation="$(state_value "$prior_status" propagation)" || return 1
    if [ "$prior_status_schema" != 1 ] || [ "$prior_status_queue" != "$prior_run_token" ] ||
        [[ "$prior_lifecycle" != COMPLETED && "$prior_lifecycle" != FINISHED_INCOMPLETE ]] ||
        [ "$prior_running" != 0 ] || [ "$prior_target" != 0 ] ||
        [ "$prior_propagation" != complete ]; then
      echo "Error: the prior adaptive queue is not durably terminal and fully closed: $prior_status" >&2
      return 1
    fi
  else
    audit_tag_on_hosts "$prior_hosts" "$prior_tag" || return $?
  fi
  if [[ "$prior_coordinator_pid" =~ ^[0-9]+$ ]]; then
    coordinator_command="$(ps -ww -p "$prior_coordinator_pid" -o command= 2>/dev/null || true)"
    if [[ "$coordinator_command" == *"$prior_tag"* ]]; then
      echo "Error: shard coordinator PID $prior_coordinator_pid is still active on $(hostname)." >&2
      return 1
    fi
  fi

  done_count=0
  if [ "$prior_mode" = "host-local-shards" ] && [ -n "$prior_log_dir" ] && [ -d "$prior_log_dir/shards" ]; then
    for shard_env in "$prior_log_dir"/shards/host_*.env; do
      [ -f "$shard_env" ] || continue
      shard_done="$(awk -F= '$1 == "succeeded_jobs" {print $2; exit}' "$shard_env")"
      case "$shard_done" in
        ''|*[!0-9]*) shard_done=0 ;;
      esac
      done_count=$((done_count + shard_done))
    done
  elif [ -n "$prior_log_dir" ] && [ -d "$prior_log_dir/done" ]; then
    done_count="$(find "$prior_log_dir/done" -maxdepth 1 -type f -name 'job_*.done' -print 2>/dev/null | awk 'END {print NR + 0}')"
  fi
  case "$prior_total" in
    ''|*[!0-9]*) prior_total=0 ;;
  esac

  if [ "$prior_total" -gt 0 ] && [ "$done_count" -ge "$prior_total" ]; then
    archive_path "$ACTIVE_RUN_DIR" completed
    return 0
  fi
  if [ "$RECOVER_INACTIVE" -eq 1 ]; then
    echo "Recovering an audited inactive attempt with ${done_count}/${prior_total} success markers." >&2
    archive_path "$ACTIVE_RUN_DIR" recovered
    return 0
  fi

  echo "Error: an incomplete prior attempt is guarded at $ACTIVE_RUN_DIR (${done_count}/${prior_total} successful)." >&2
  echo "Reconcile its chain outputs first, then use --recover_inactive only after confirming the attempt is inactive." >&2
  return 1
}

mkdir -p "${LOCK_PARENT_DIR}"

if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  acquire_global_lock "$MODE_TRANSITION_LOCK"
  if [ ! -f "$ADAPTIVE_POOL_MARKER" ]; then
    echo "Error: no adaptive pool is active: $ADAPTIVE_POOL_MARKER" >&2
    exit 1
  fi
  POOL_STATE="$(state_value "$ADAPTIVE_POOL_MARKER" state)" || exit 1
  [ "$POOL_STATE" = ACTIVE ] || {
    echo "Error: adaptive pool is not admitting queues (state=$POOL_STATE)." >&2
    exit 1
  }
  POOL_ID="$(state_value "$ADAPTIVE_POOL_MARKER" pool_id)" || exit 1
  SHARED_IO_CAPACITY="$(state_value "$ADAPTIVE_POOL_MARKER" shared_io_capacity)" || exit 1
  LEASE_SECONDS="$(state_value "$ADAPTIVE_POOL_MARKER" lease_seconds)" || exit 1
  COORDINATOR_PID="$(state_value "$ADAPTIVE_POOL_MARKER" coordinator_pid)" || exit 1
  if ! [[ "$SHARED_IO_CAPACITY" =~ ^[0-9]+$ && "$LEASE_SECONDS" =~ ^[1-9][0-9]*$ &&
          "$COORDINATOR_PID" =~ ^[1-9][0-9]*$ ]] ||
      [ "$SHARED_IO_UNITS_PER_JOB" -gt "$SHARED_IO_CAPACITY" ]; then
    echo "Error: invalid adaptive pool capacity, lease, coordinator PID, or per-job shared-I/O request." >&2
    exit 2
  fi
  COORDINATOR_READY_FILE="$ADAPTIVE_STATE_ROOT/coordinator.ready"
  [ -f "$COORDINATOR_READY_FILE" ] || {
    echo "Error: adaptive coordinator readiness marker is missing: $COORDINATOR_READY_FILE" >&2
    exit 1
  }
  READY_POOL_ID="$(state_value "$COORDINATOR_READY_FILE" pool_id)" || exit 1
  READY_PID="$(state_value "$COORDINATOR_READY_FILE" pid)" || exit 1
  READY_STATE="$(state_value "$COORDINATOR_READY_FILE" state)" || exit 1
  READY_EPOCH="$(state_value "$COORDINATOR_READY_FILE" updated_epoch)" || exit 1
  NOW_EPOCH="$(date +%s)"
  COORDINATOR_COMMAND="$(ps -ww -p "$COORDINATOR_PID" -o command= 2>/dev/null || true)"
  if [ "$READY_POOL_ID" != "$POOL_ID" ] || [ "$READY_PID" != "$COORDINATOR_PID" ] ||
      [ "$READY_STATE" != ready ] || ! [[ "$READY_EPOCH" =~ ^[0-9]+$ ]] ||
      [ "$READY_EPOCH" -gt "$NOW_EPOCH" ] || [ $((NOW_EPOCH - READY_EPOCH)) -gt "$LEASE_SECONDS" ] ||
      [[ "$COORDINATOR_COMMAND" != *adaptive_coordinator.py* ]] ||
      [[ "$COORDINATOR_COMMAND" != *"--pool-id $POOL_ID"* ]]; then
    echo "Error: adaptive coordinator identity or heartbeat is not current." >&2
    exit 1
  fi
  if [ "$MEMORY_CALIBRATION_ENABLED" -eq 1 ]; then
    # Reuse the pool/daemon capability audit while retaining the launch lock.
    env ADAPTIVE_STATE_ROOT="$ADAPTIVE_STATE_ROOT" ADAPTIVE_POOL_MARKER="$ADAPTIVE_POOL_MARKER" \
      MODE_TRANSITION_LOCK="$MODE_TRANSITION_LOCK" \
      ADAPTIVE_COORDINATOR_SCRIPT="$ADAPTIVE_COORDINATOR_SCRIPT" \
      bash "$(dirname "$0")/adaptive_scheduler.sh" verify-memory-calibration || exit 1
  fi
  if [ "$WORKFLOW_ENABLED" -eq 1 ]; then
    if [ "$EARLY_ETA_SAMPLE" -eq 1 ] && [ "$(state_value "$COORDINATOR_READY_FILE" early_eta_sample_version)" != 1 ]; then
      echo "Error: --early_eta_sample requires a coordinator with early ETA sampling support." >&2
      exit 1
    fi
    [ "$(state_value "$ADAPTIVE_POOL_MARKER" workflow_version)" = 1 ] && \
      [ "$(state_value "$COORDINATOR_READY_FILE" workflow_version)" = 1 ] || {
        echo "Error: workflow options require a compatible fresh pool." >&2; exit 1;
      }
    DEPENDENCIES="$(python3 "$(dirname "$0")/adaptive_workflow.py" resolve \
      --state-root "$ADAPTIVE_STATE_ROOT" ${AFTER_QUEUES[@]+"${AFTER_QUEUES[@]}"})" || exit 1
  fi
  ADAPTIVE_RESOURCES_FILE="$(state_value "$ADAPTIVE_POOL_MARKER" resources_file)" || exit 1
  [ -f "$ADAPTIVE_RESOURCES_FILE" ] || {
    echo "Error: adaptive host resource file not found: $ADAPTIVE_RESOURCES_FILE" >&2
    exit 1
  }
  ADAPTIVE_RESOURCES_HASH="$(state_value "$ADAPTIVE_POOL_MARKER" resources_sha256)" || exit 1
  CURRENT_RESOURCES_HASH="$(shasum -a 256 "$ADAPTIVE_RESOURCES_FILE" 2>/dev/null | awk '{print $1}')"
  [ -n "$CURRENT_RESOURCES_HASH" ] && [ "$CURRENT_RESOURCES_HASH" = "$ADAPTIVE_RESOURCES_HASH" ] || {
    echo "Error: adaptive host resource file differs from the pool record." >&2
    exit 1
  }
  HOST_LIST="$(awk -F '\t' 'NR > 1 && $5 == 1 {print $2}' "$ADAPTIVE_RESOURCES_FILE")"
  [ -n "$HOST_LIST" ] || { echo "Error: adaptive pool has no enabled hosts." >&2; exit 1; }
  if ! [[ "$SCHEDULE_PRIORITY" =~ ^-?[0-9]+$ ]]; then
    PRIORITY_FIELDS="$(python3 "$ADAPTIVE_COORDINATOR_SCRIPT" priority-spec \
      --resources-file "$ADAPTIVE_RESOURCES_FILE" "--spec=$SCHEDULE_PRIORITY")" || exit 2
    IFS=$'\t' read -r SCHEDULE_PRIORITY HOST_PRIORITIES <<< "$PRIORITY_FIELDS"
    [ -z "$HOST_PRIORITIES" ] || \
      [ "$(state_value "$COORDINATOR_READY_FILE" host_priority_version 2>/dev/null)" = 1 ] || {
        echo "Error: per-host schedule priorities require a coordinator that supports them; deploy the complete scheduler and restart the pool." >&2
        exit 1
      }
  fi
  if ! python3 "$ADAPTIVE_COORDINATOR_SCRIPT" profile-fits \
      --state-root "$(dirname "$ADAPTIVE_POOL_MARKER")" --pool-id "$POOL_ID" \
      --cpu-units "$CPU_UNITS_PER_JOB" --memory-mib "$MEMORY_MIB_PER_JOB" \
      --gpu-units "$GPU_UNITS_PER_JOB"; then
    echo "Error: this job profile does not fit any enabled adaptive host." >&2
    exit 2
  fi
else
  acquire_global_lock "$MODE_TRANSITION_LOCK"
  if [ -L "$ADAPTIVE_POOL_MARKER" ] || \
      { [ -e "$ADAPTIVE_POOL_MARKER" ] && [ ! -f "$ADAPTIVE_POOL_MARKER" ]; }; then
    echo "Error: adaptive pool marker is ambiguous or is not a regular file:" >&2
    echo "$ADAPTIVE_POOL_MARKER" >&2
    exit 1
  fi
  audit_tag_on_hosts "$HOST_LIST" "$LEGACY_TAG_CLEAN" || exit $?
fi
handle_existing_guard || exit $?

if [ -d "$LOCK_DIR" ]; then
  if [ "$RECOVER_INACTIVE" -eq 1 ]; then
    archive_path "$LOCK_DIR" recovered
  else
    echo "Error: scheduler lock already exists: $LOCK_DIR" >&2
    echo "It will not be removed automatically. Audit the prior attempt before using --recover_inactive." >&2
    exit 1
  fi
fi

if ! mkdir "$ACTIVE_RUN_DIR" 2>/dev/null; then
  echo "Error: another launcher acquired $ACTIVE_RUN_DIR" >&2
  exit 1
fi
{
  echo "state=allocating"
  echo "launcher_host=$(hostname)"
  echo "launcher_pid=$$"
  echo "queue_file=$QUEUE_ARG"
  echo "execution_mode=$EXECUTION_MODE"
  echo "hosts=$(printf '%s' "$HOST_LIST" | tr '\n' ' ')"
  echo "created_epoch=$(date +%s)"
} > "$ACTIVE_RUN_DIR/owner.env"

# Determine today's date (YYYY-MM-DD) for the log file
TODAY=$(date +%F)  # e.g. 2025-01-23

# Define log directory and ensure it exists
LOG_ROOT_DIR="${MODEL_DIR}logs/"
mkdir -p "$LOG_ROOT_DIR"

allocate_batch_dir() {
  local last_num next_num next_num_padded candidate_dir attempt

  for attempt in $(seq 1 "$BATCH_ALLOC_MAX_ATTEMPTS"); do
    last_num="$(
      find "$LOG_ROOT_DIR" -maxdepth 1 -type d -name 'batch_*' -print 2>/dev/null |
      awk -F/ '
        {
          n = split($NF, parts, "_")
          if (n >= 2 && parts[2] ~ /^[0-9]+$/ && parts[2] + 0 > max) {
            max = parts[2] + 0
          }
        }
        END { print max + 0 }
      '
    )"
    last_num="${last_num:-0}"
    next_num=$((last_num + 1))
    next_num_padded=$(printf "%05d" "$next_num")
    BATCH_DIR="batch_${next_num_padded}_${TODAY}"
    candidate_dir="${LOG_ROOT_DIR}${BATCH_DIR}"

    if mkdir "$candidate_dir" 2>/dev/null; then
      LOG_DIR="$candidate_dir"
      JOB_LOG_DIR="$LOG_DIR/jobs"
      mkdir -p "$JOB_LOG_DIR"
      echo "Creating new batch directory: $LOG_DIR"
      return 0
    fi

    sleep "$BATCH_ALLOC_RETRY_SEC"
  done

  echo "Error: failed to allocate a unique batch directory under $LOG_ROOT_DIR after ${BATCH_ALLOC_MAX_ATTEMPTS} attempts" >&2
  exit 1
}

allocate_batch_dir
chmod 777 "$LOG_DIR" "$JOB_LOG_DIR"

START_EPOCH=$(date +%s)
if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  CONFIGURED_HOSTS=0
  CONFIGURED_WORKER_SLOTS=0
else
  CONFIGURED_HOSTS=$(awk 'NF && $1 !~ /^#/ && $2 + 0 > 0 {n++} END {print n+0}' "$CONFIG_FILE")
  CONFIGURED_WORKER_SLOTS=$(awk 'NF && $1 !~ /^#/ && $2 + 0 > 0 {s+=$2+0} END {print s+0}' "$CONFIG_FILE")
fi
EXPECTED_HOSTS="$CONFIGURED_HOSTS"
EXPECTED_WORKER_SLOTS="$CONFIGURED_WORKER_SLOTS"
SHARD_DIR=""

prepare_execution_inputs() {
  if [ "$EXECUTION_MODE" = "host-local-shards" ]; then
    EDITABLE_QUEUE_FILE=""
    SHARD_DIR="$LOG_DIR/shards"
    mkdir -p "$SHARD_DIR"
    chmod 777 "$SHARD_DIR"
    : > "$SHARD_DIR/allocations.tsv"
    host_index=0
    while read -r host cores _; do
      [ -n "${host:-}" ] || continue
      [[ "$host" == \#* ]] && continue
      [ "${cores:-0}" -gt 0 ] || continue
      host_index=$((host_index + 1))
      host_key="$(printf 'host_%02d' "$host_index")"
      : > "$SHARD_DIR/${host_key}.commands.txt"
      printf '%s\t%s\t%s\n' "$host" "$cores" "$host_key" >> "$SHARD_DIR/allocations.tsv"
    done < "$CONFIG_FILE"

    awk -v shard_dir="$SHARD_DIR" '
      NR == FNR {
        for (slot_i = 1; slot_i <= $2; slot_i++) slot_host[++slot_count] = $3
        next
      }
      NF {
        host_key = slot_host[(job_count % slot_count) + 1]
        print $0 >> (shard_dir "/" host_key ".commands.txt")
        job_count++
      }
    ' "$SHARD_DIR/allocations.tsv" "$ORIGINAL_QUEUE_FILE"

    EXPECTED_HOSTS=0
    EXPECTED_WORKER_SLOTS=0
    : > "$SHARD_DIR/manifest.tsv"
    while IFS=$'\t' read -r host cores host_key; do
      shard_file="$SHARD_DIR/${host_key}.commands.txt"
      shard_jobs="$(awk 'NF {n++} END {print n+0}' "$shard_file")"
      printf '%s\t%s\t%s\t%s\n' "$host_key" "$host" "$cores" "$shard_jobs" >> "$SHARD_DIR/manifest.tsv"
      if [ "$shard_jobs" -gt 0 ]; then
        EXPECTED_HOSTS=$((EXPECTED_HOSTS + 1))
        EXPECTED_WORKER_SLOTS=$((EXPECTED_WORKER_SLOTS + cores))
      fi
      chmod 666 "$shard_file"
    done < "$SHARD_DIR/allocations.tsv"
    chmod 666 "$SHARD_DIR/allocations.tsv" "$SHARD_DIR/manifest.tsv"
    echo "Created $EXPECTED_HOSTS immutable host-local shards under $SHARD_DIR"
  else
    # Each shared-queue attempt receives its own editable queue. A relaunch can
    # therefore never reset the queue file belonging to an earlier batch.
    EDITABLE_QUEUE_FILE="$LOG_DIR/queue-edit.txt"
    echo "Creating batch-specific editable queue: $EDITABLE_QUEUE_FILE"
    if ! cp "$ORIGINAL_QUEUE_FILE" "$EDITABLE_QUEUE_FILE"; then
      echo "Error: unable to create batch queue copy: $EDITABLE_QUEUE_FILE" >&2
      exit 1
    fi
    if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
      chmod 444 "$EDITABLE_QUEUE_FILE" || {
        echo "Error: unable to protect adaptive queue copy: $EDITABLE_QUEUE_FILE" >&2
        exit 1
      }
      TOTAL_JOBS="$(awk 'NF {n++} END {print n+0}' "$EDITABLE_QUEUE_FILE")"
      if [ "$TOTAL_JOBS" -lt 1 ]; then
        echo "Error: adaptive queue copy contains no jobs: $EDITABLE_QUEUE_FILE" >&2
        exit 1
      fi
      validate_queue_file "$EDITABLE_QUEUE_FILE" || exit 2
    else
      chmod 666 "$EDITABLE_QUEUE_FILE" || {
        echo "Error: unable to set queue copy permissions: $EDITABLE_QUEUE_FILE" >&2
        exit 1
      }
    fi
  fi
}

prepare_execution_inputs

sha256_file() {
  local output digest
  output="$(shasum -a 256 "$1" 2>/dev/null)" || return 1
  digest="${output%% *}"
  [ "${#digest}" -eq 64 ] || return 1
  case "$digest" in *[!0-9a-f]*) return 1 ;; esac
  printf '%s\n' "$digest"
}

QUEUE_HASH="$(sha256_file "$ORIGINAL_QUEUE_FILE")" || {
  echo "Error: unable to hash original queue: $ORIGINAL_QUEUE_FILE" >&2
  exit 1
}
ADAPTIVE_QUEUE_HASH=""
if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  ADAPTIVE_QUEUE_HASH="$(sha256_file "$EDITABLE_QUEUE_FILE")" || {
    echo "Error: unable to hash adaptive queue copy: $EDITABLE_QUEUE_FILE" >&2
    exit 1
  }
fi
BATCH_NUMBER="$(printf '%s' "$BATCH_DIR" | awk -F_ '{print $2}')"
TAG_FINGERPRINT="$(printf '%s' "$LEGACY_TAG_BASE" | shasum -a 256 | awk '{print substr($1, 1, 16)}')"
if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  TAG_CLEAN="${LEGACY_TAG_CLEAN}__h${TAG_FINGERPRINT}__b${BATCH_NUMBER}"
else
  TAG_CLEAN="${LEGACY_TAG_CLEAN}__b${BATCH_NUMBER}"
fi
RUN_TOKEN="${BATCH_DIR}_${START_EPOCH}_${TAG_FINGERPRINT}"
RUN_META_FILE="$LOG_DIR/run_meta.env"
ADAPTIVE_DIR=""
ADAPTIVE_RESOURCE_FILE=""
ADAPTIVE_CONTROL_FILE=""
ADAPTIVE_STATUS_FILE=""
REGISTRATION_FILE=""
if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  ADAPTIVE_DIR="$LOG_DIR/adaptive"
  ADAPTIVE_RESOURCE_FILE="$ADAPTIVE_DIR/resources.env"
  ADAPTIVE_CONTROL_FILE="$ADAPTIVE_DIR/control.env"
  ADAPTIVE_STATUS_FILE="$ADAPTIVE_DIR/status.env"
  REGISTRATION_FILE="$ADAPTIVE_STATE_ROOT/queues/${RUN_TOKEN}.env"
fi
{
  echo "run_name=$RUN_NAME"
  [ -z "$PROJECT_NAME" ] || echo "project_name=$PROJECT_NAME"
  echo "execution_mode=$EXECUTION_MODE"
  echo "queue_file=$QUEUE_ARG"
  echo "original_queue_file=$ORIGINAL_QUEUE_FILE"
  echo "editable_queue_file=$EDITABLE_QUEUE_FILE"
  echo "total_jobs=$TOTAL_JOBS"
  echo "expected_hosts=$EXPECTED_HOSTS"
  echo "expected_worker_slots=$EXPECTED_WORKER_SLOTS"
  echo "start_epoch=$START_EPOCH"
  echo "run_token=$RUN_TOKEN"
  echo "tag_clean=$TAG_CLEAN"
  echo "launch_guard=$ACTIVE_RUN_DIR"
  echo "queue_sha256=$QUEUE_HASH"
  echo "shard_progress_dir=$SHARD_DIR"
  echo "shard_progress_sync_sec=$SHARD_PROGRESS_SYNC_SEC"
  echo "worker_ready_timeout_sec=$WORKER_READY_TIMEOUT_SEC"
  echo "worker_release_delay_sec=$WORKER_RELEASE_DELAY_SEC"
  if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
    echo "pool_id=$POOL_ID"
    echo "resource_file=$ADAPTIVE_RESOURCE_FILE"
    echo "control_file=$ADAPTIVE_CONTROL_FILE"
    echo "registration_file=$REGISTRATION_FILE"
    echo "editable_queue_sha256=$ADAPTIVE_QUEUE_HASH"
  fi
} > "$RUN_META_FILE"
chmod 666 "$RUN_META_FILE"

{
  echo "state=launching"
  echo "launcher_host=$(hostname)"
  echo "launcher_pid=$$"
  echo "queue_file=$QUEUE_ARG"
  echo "execution_mode=$EXECUTION_MODE"
  echo "hosts=$(printf '%s' "$HOST_LIST" | tr '\n' ' ')"
  echo "created_epoch=$START_EPOCH"
  echo "batch_dir=$BATCH_DIR"
  echo "log_dir=$LOG_DIR"
  echo "total_jobs=$TOTAL_JOBS"
  echo "tag_clean=$TAG_CLEAN"
  echo "run_token=$RUN_TOKEN"
  echo "queue_sha256=$QUEUE_HASH"
  echo "registration_file=$REGISTRATION_FILE"
} > "$ACTIVE_RUN_DIR/owner.env.tmp"
mv -f "$ACTIVE_RUN_DIR/owner.env.tmp" "$ACTIVE_RUN_DIR/owner.env"

register_adaptive_queue() (
  set -e -o pipefail
  local tmp resource_hash

  mkdir -p "$ADAPTIVE_DIR" "$ADAPTIVE_STATE_ROOT/queues" "$LOG_DIR/claims" "$LOG_DIR/done"
  chmod 777 "$ADAPTIVE_DIR" "$LOG_DIR/claims" "$LOG_DIR/done"
  { [ ! -e "$REGISTRATION_FILE" ] && [ ! -L "$REGISTRATION_FILE" ]; } || {
    echo "Error: adaptive queue registration already exists: $REGISTRATION_FILE" >&2
    return 1
  }

  tmp="${ADAPTIVE_RESOURCE_FILE}.tmp.$$"
  {
    echo "schema_version=$((1 + WORKFLOW_ENABLED))"
    echo "queue_id=$RUN_TOKEN"
    echo "cpu_units_per_job=$CPU_UNITS_PER_JOB"
    echo "memory_mib_per_job=$MEMORY_MIB_PER_JOB"
    echo "shared_io_units_per_job=$SHARED_IO_UNITS_PER_JOB"
    # Opt-in costs appear only when used, so other queues keep their original profile.
    [ "$GPU_UNITS_PER_JOB" -eq 0 ] || echo "gpu_units_per_job=$GPU_UNITS_PER_JOB"
    [ -z "$MEMORY_LIMIT_MIB" ] || echo "memory_limit_mib=$MEMORY_LIMIT_MIB"
    if [ "$WORKFLOW_ENABLED" -eq 1 ]; then
      echo "dependencies=$DEPENDENCIES"
      echo "retry_lost_jobs=$RETRY_LOST_JOBS"
      [ "$EARLY_ETA_SAMPLE" -eq 0 ] || echo "early_eta_sample=1"
    fi
  } > "$tmp"
  chmod 444 "$tmp"
  mv -f "$tmp" "$ADAPTIVE_RESOURCE_FILE"
  resource_hash="$(shasum -a 256 "$ADAPTIVE_RESOURCE_FILE" | awk '{print $1}')" || return 1
  echo "resources_sha256=$resource_hash" >> "$RUN_META_FILE"

  tmp="${ADAPTIVE_CONTROL_FILE}.tmp.$$"
  {
    if [ "$MEMORY_CALIBRATION_ENABLED" -eq 1 ] || [ -n "$HOST_PRIORITIES" ]; then
      echo "schema_version=2"
    else
      echo "schema_version=1"
    fi
    echo "queue_id=$RUN_TOKEN"
    echo "generation=1"
    echo "admission=$ADAPTIVE_ADMISSION"
    echo "schedule_priority=$SCHEDULE_PRIORITY"
    [ -z "$HOST_PRIORITIES" ] || echo "host_priorities=$HOST_PRIORITIES"
    echo "max_workers=$MAX_WORKERS"
    if [ "$MEMORY_CALIBRATION_ENABLED" -eq 1 ]; then
      echo "memory_mib_per_job=$MEMORY_MIB_PER_JOB"
      echo "memory_calibration_jobs=$MEMORY_CALIBRATION_JOBS"
      echo "memory_calibration_seconds=$MEMORY_CALIBRATION_SECONDS"
      echo "memory_min_mib_per_job=$MEMORY_MIN_MIB_PER_JOB"
      echo "memory_calibration_generation=1"
    fi
    echo "updated_epoch=$(date +%s)"
  } > "$tmp"
  chmod 666 "$tmp"
  mv -f "$tmp" "$ADAPTIVE_CONTROL_FILE"

  tmp="${ADAPTIVE_STATUS_FILE}.tmp.$$"
  {
    echo "schema_version=1"
    echo "queue_id=$RUN_TOKEN"
    echo "lifecycle=PENDING"
    echo "admission=$ADAPTIVE_ADMISSION"
    echo "schedule_priority=$SCHEDULE_PRIORITY"
    [ -z "$HOST_PRIORITIES" ] || echo "host_priorities=$HOST_PRIORITIES"
    echo "max_workers=$MAX_WORKERS"
    echo "control_generation=0"
    echo "propagation=pending"
    echo "running_workers=0"
    echo "target_workers=0"
    echo "updated_epoch=$(date +%s)"
  } > "$tmp"
  chmod 666 "$tmp"
  mv -f "$tmp" "$ADAPTIVE_STATUS_FILE"

  tmp="${REGISTRATION_FILE}.tmp.$$"
  {
    echo "schema_version=1"
    echo "pool_id=$POOL_ID"
    echo "queue_id=$RUN_TOKEN"
    echo "batch_dir=$(cd "$LOG_DIR" && pwd -P)"
    echo "original_queue_file=$ORIGINAL_QUEUE_FILE"
    echo "editable_queue_file=$EDITABLE_QUEUE_FILE"
    echo "model_dir=$MODEL_DIR"
    echo "log_dir=$LOG_DIR"
    echo "lock_dir=$LOCK_DIR"
    echo "tag_clean=$TAG_CLEAN"
    echo "resources_file=$ADAPTIVE_RESOURCE_FILE"
    echo "resources_sha256=$resource_hash"
    echo "queue_sha256=$ADAPTIVE_QUEUE_HASH"
    echo "control_file=$ADAPTIVE_CONTROL_FILE"
    echo "registered_epoch=$(date +%s)"
    echo "run_name=$RUN_NAME"
  } > "$tmp"
  chmod 644 "$tmp"
  mkdir -p "$ADAPTIVE_STATE_ROOT/queue-index"
  cp "$tmp" "$ADAPTIVE_STATE_ROOT/queue-index/$RUN_TOKEN.env"
  mv -f "$tmp" "$REGISTRATION_FILE"

  {
    echo "state=active"
    echo "run_token=$RUN_TOKEN"
    echo "registered=1"
    echo "hosts_total=0"
    echo "hosts_acknowledged=0"
    echo "hosts_released=0"
    echo "updated_epoch=$(date +%s)"
  } > "$LOG_DIR/launch_status.env"
  chmod 666 "$LOG_DIR/launch_status.env"
  echo "launch_state=active" >> "$ACTIVE_RUN_DIR/owner.env"
  echo "registered=1" >> "$ACTIVE_RUN_DIR/owner.env"
  echo "batch_dir=$(cd "$LOG_DIR" && pwd -P)"
  echo "queue_id=$RUN_TOKEN"
)

if [ "$EXECUTION_MODE" = "adaptive-shared-queue" ]; then
  register_adaptive_queue
  registration_rc=$?
  [ "$registration_rc" -eq 0 ] || exit "$registration_rc"
  if ! python3 "$ADAPTIVE_COORDINATOR_SCRIPT" project-summary \
      --state-root "$ADAPTIVE_STATE_ROOT" --pool-id "$POOL_ID" --project "$PROJECT_NAME"; then
    echo "Queue registered successfully; project summary failed. Do not resubmit this queue." >&2
  fi
  echo "Job compatibility: scripts, dependencies and binaries must support every enabled worker's OS/architecture; the scheduler does not select hosts by platform."
  exit 0
fi

HOST_TOTAL=0
HOST_STARTED=0
HOST_TIMEOUTS=0
HOST_FAILED=0
HOST_UNCERTAIN=0
HOST_ISSUES=""
HOST_RELEASED=0
READY_HOSTS=""
AUTO_STOPPED=0
COORDINATOR_STARTED=0
COORDINATOR_PID=""
LAUNCH_ISSUES_FILE="$LOG_DIR/launch_issues.tsv"
printf 'host\tphase\texit_code\tdetail\n' > "$LAUNCH_ISSUES_FILE"
chmod 666 "$LAUNCH_ISSUES_FILE"

function run_with_timeout {
  local timeout_sec="$1"
  shift

  "$@" &
  local cmd_pid=$!
  local waited=0

  while kill -0 "$cmd_pid" 2>/dev/null; do
    if [ "$waited" -ge "$timeout_sec" ]; then
      kill -TERM "$cmd_pid" 2>/dev/null || true
      sleep 1
      kill -KILL "$cmd_pid" 2>/dev/null || true
      wait "$cmd_pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
    waited=$((waited + 1))
  done

  wait "$cmd_pid"
}

verify_remote_ack() {
  local host="$1"
  local runner_hash="$2"
  local output rc

  output="$(
    ssh -n "${SSH_COMMON_OPTS[@]}" "$host" \
      "state_dir=\"\$HOME/.rota/runs/$RUN_TOKEN\"; \
       state_file=\"\$state_dir/state.env\"; ready_file=\"\$state_dir/ready.env\"; \
       [ -f \"\$state_file\" ] && [ -f \"\$ready_file\" ] || exit 3; \
       grep -q '^run_token=$RUN_TOKEN$' \"\$state_file\" || exit 4; \
       grep -q '^tag_clean=$TAG_CLEAN$' \"\$state_file\" || exit 5; \
       grep -q '^run_token=$RUN_TOKEN$' \"\$ready_file\" || exit 6; \
       grep -q '^tag_clean=$TAG_CLEAN$' \"\$ready_file\" || exit 7; \
       grep -q '^runner_sha256=$runner_hash$' \"\$ready_file\" || exit 8; \
       pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$state_file\"); \
       if [ -n \"\$pid\" ] && kill -0 \"\$pid\" 2>/dev/null; then live=1; else live=0; fi; \
       echo READY pid=\$pid live=\$live" 2>/dev/null
  )"
  rc=$?
  if [ "$rc" -eq 0 ]; then
    printf '%s\n' "$output"
    return 0
  fi
  if [ "$rc" -eq 255 ]; then
    return 2
  fi
  return 1
}

record_host_issue() {
  local host="$1"
  local phase="$2"
  local exit_code="$3"
  local detail="$4"
  detail="$(printf '%s' "$detail" | tr '\t\r\n' '   ')"
  printf '%s\t%s\t%s\t%s\n' "$host" "$phase" "$exit_code" "$detail" >> "$LAUNCH_ISSUES_FILE"
}

remote_bootstrap_summary() {
  local host="$1"
  ssh -n "${SSH_COMMON_OPTS[@]}" "$host" \
    "file=\"\$HOME/.rota/runs/$RUN_TOKEN/bootstrap.log\"; \
     [ -f \"\$file\" ] && sed -n '1,12p' \"\$file\"" 2>/dev/null | tr '\t\r\n' '   '
}

release_ready_hosts() {
  local release_epoch release_tmp_dir host index release_file detail
  local -a release_hosts release_pids release_outputs
  local release_failed=0

  release_epoch=$(( $(date +%s) + WORKER_RELEASE_DELAY_SEC ))
  release_tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/rota-release.XXXXXX")"
  while IFS= read -r host; do
    [ -n "$host" ] || continue
    release_hosts+=("$host")
  done <<< "$READY_HOSTS"

  for index in "${!release_hosts[@]}"; do
    host="${release_hosts[$index]}"
    release_file="$release_tmp_dir/release_${index}.out"
    release_outputs+=("$release_file")
    ssh -n "${SSH_COMMON_OPTS[@]}" "$host" \
      "state_dir=\"\$HOME/.rota/runs/$RUN_TOKEN\"; \
       state_file=\"\$state_dir/state.env\"; ready_file=\"\$state_dir/ready.env\"; start_file=\"\$state_dir/start.env\"; \
       [ -f \"\$state_file\" ] && [ -f \"\$ready_file\" ] || exit 3; \
       grep -q '^run_token=$RUN_TOKEN$' \"\$ready_file\" || exit 4; \
       grep -q '^tag_clean=$TAG_CLEAN$' \"\$ready_file\" || exit 5; \
       pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$state_file\"); \
       case \"\$pid\" in ''|*[!0-9]*) exit 6 ;; esac; \
       kill -0 \"\$pid\" 2>/dev/null || exit 7; \
       { echo run_token=$RUN_TOKEN; echo tag_clean=$TAG_CLEAN; echo start_epoch=$release_epoch; } \
         > \"\$start_file.tmp.\$\$\"; \
       mv \"\$start_file.tmp.\$\$\" \"\$start_file\"; \
       echo RELEASED" > "$release_file" 2>&1 &
    release_pids+=("$!")
  done

  for index in "${!release_hosts[@]}"; do
    host="${release_hosts[$index]}"
    if wait "${release_pids[$index]}"; then
      HOST_RELEASED=$((HOST_RELEASED + 1))
      echo "Released ready workers on $host for epoch $release_epoch"
    else
      release_failed=1
      detail="$(sed -n '1,6p' "${release_outputs[$index]}" 2>/dev/null | tr '\t\r\n' '   ')"
      record_host_issue "$host" release 1 "${detail:-unable to publish node-local start gate}"
      HOST_ISSUES="${HOST_ISSUES}${host}: start-gate release failed\n"
    fi
  done

  for release_file in "${release_outputs[@]}"; do
    rm -f "$release_file"
  done
  rmdir "$release_tmp_dir"
  return "$release_failed"
}

stop_ready_hosts() {
  local host

  while IFS= read -r host; do
    [ -n "$host" ] || continue
    if ssh -n "${SSH_COMMON_OPTS[@]}" "$host" \
      "state_dir=\"\$HOME/.rota/runs/$RUN_TOKEN\"; \
       state_file=\"\$state_dir/state.env\"; \
       [ -f \"\$state_file\" ] || exit 3; \
       grep -q '^run_token=$RUN_TOKEN$' \"\$state_file\" || exit 4; \
       grep -q '^tag_clean=$TAG_CLEAN$' \"\$state_file\" || exit 5; \
       pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$state_file\"); \
       case \"\$pid\" in ''|*[!0-9]*) exit 6 ;; esac; \
       descend() { for child in \$(pgrep -P \"\$1\" 2>/dev/null || true); do descend \"\$child\"; echo \"\$child\"; done; }; \
       descendants=\$(descend \"\$pid\"); \
       for child in \$descendants; do kill -TERM \"\$child\" 2>/dev/null || true; done; \
       kill -TERM \"\$pid\" 2>/dev/null || true; \
       { echo state=auto-stopped; echo run_token=$RUN_TOKEN; echo tag_clean=$TAG_CLEAN; echo stopped_epoch=\$(date +%s); } > \"\$state_dir/auto-stop.env\"" \
      >/dev/null 2>&1; then
      AUTO_STOPPED=$((AUTO_STOPPED + 1))
      echo "Stopped unreleased batch controller on $host"
    else
      record_host_issue "$host" auto-stop 1 "unable to verify or stop the recorded batch controller"
    fi
  done <<< "$READY_HOSTS"
}

verify_remote_stage() {
  local host="$1"
  local shard_hash="$2"
  local runner_hash="$3"
  local rc

  ssh -n "${SSH_COMMON_OPTS[@]}" "$host" \
    "stage_file=\"\$HOME/.rota/runs/$RUN_TOKEN/staged.env\"; \
     [ -f \"\$stage_file\" ] || exit 3; \
     grep -q '^run_token=$RUN_TOKEN$' \"\$stage_file\" || exit 4; \
     grep -q '^commands_sha256=$shard_hash$' \"\$stage_file\" || exit 5; \
     grep -q '^runner_sha256=$runner_hash$' \"\$stage_file\" || exit 6; \
     echo STAGED" >/dev/null 2>&1
  rc=$?
  if [ "$rc" -eq 0 ]; then
    return 0
  fi
  if [ "$rc" -eq 255 ]; then
    return 2
  fi
  return 1
}

launch_shared_queue_hosts() {
WORKER_HASH="$SHARED_WORKER_HASH"
while read -r HOST CORES _; do
  [ -z "${HOST:-}" ] && continue
  [[ "$HOST" == \#* ]] && continue
  [ "${CORES:-0}" -gt 0 ] || continue
  HOST_TOTAL=$((HOST_TOTAL + 1))

  echo "Staging and starting $CORES workers on $HOST"
  run_with_timeout "$SSH_LAUNCH_TIMEOUT_SEC" ssh \
    "${SSH_COMMON_OPTS[@]}" \
    "$HOST" "set -e
  state_parent=\"\$HOME/.rota/runs\"
  state_dir=\"\$state_parent/$RUN_TOKEN\"
  mkdir -p \"\$state_parent\"
  if ! mkdir \"\$state_dir\" 2>/dev/null; then
    if [ -f \"\$state_dir/ready.env\" ] && \\
       grep -q '^run_token=$RUN_TOKEN$' \"\$state_dir/ready.env\" && \\
       grep -q '^tag_clean=$TAG_CLEAN$' \"\$state_dir/ready.env\" && \\
       grep -q '^runner_sha256=$WORKER_HASH$' \"\$state_dir/ready.env\"; then
      cat >/dev/null
      echo ALREADY_READY
      exit 0
    fi
    cat >/dev/null
    echo \"Ambiguous existing host-local launch state: \$state_dir\" >&2
    exit 3
  fi
  cat > \"\$state_dir/mmworker.sh.incoming\"
  set -- \$(shasum -a 256 \"\$state_dir/mmworker.sh.incoming\")
  [ \"\$1\" = '$WORKER_HASH' ] || exit 4
  mv \"\$state_dir/mmworker.sh.incoming\" \"\$state_dir/mmworker.sh\"
  chmod 700 \"\$state_dir/mmworker.sh\"
  {
    echo run_token=$RUN_TOKEN
    echo tag_clean=$TAG_CLEAN
    echo runner_sha256=$WORKER_HASH
    echo staged_epoch=\$(date +%s)
  } > \"\$state_dir/staged.env\"
  nohup env \\
    PARALLEL_WORKER_READY_FILE=\"\$state_dir/ready.env\" \\
    PARALLEL_WORKER_START_FILE=\"\$state_dir/start.env\" \\
    PARALLEL_WORKER_RUN_TOKEN='$RUN_TOKEN' \\
    PARALLEL_WORKER_RUNNER_SHA256='$WORKER_HASH' \\
    bash -c \"
    exec -a mmworker_${TAG_CLEAN} \\
    bash '\$state_dir/mmworker.sh' \\
      '$CORES' \\
      '$ORIGINAL_QUEUE_FILE' \\
      '$EDITABLE_QUEUE_FILE' \\
      '$MODEL_DIR' \\
      '$LOG_DIR' \\
      '$LOCK_DIR' \\
      '$TAG_CLEAN'
  \" < /dev/null > \"\$state_dir/bootstrap.log\" 2>&1 &
  worker_pid=\$!
  {
    echo state=bootstrapping
    echo run_token=$RUN_TOKEN
    echo tag_clean=$TAG_CLEAN
    echo queue_sha256=$QUEUE_HASH
    echo runner_sha256=$WORKER_HASH
    echo pid=\$worker_pid
    echo host=\$(hostname)
  } > \"\$state_dir/state.env.tmp.\$\$\"
  mv \"\$state_dir/state.env.tmp.\$\$\" \"\$state_dir/state.env\"
  ready_ok=0
  for ready_attempt in \$(seq 1 '$WORKER_READY_TIMEOUT_SEC'); do
    if [ -f \"\$state_dir/ready.env\" ] && \\
       grep -q '^run_token=$RUN_TOKEN$' \"\$state_dir/ready.env\" && \\
       grep -q '^tag_clean=$TAG_CLEAN$' \"\$state_dir/ready.env\" && \\
       grep -q '^runner_sha256=$WORKER_HASH$' \"\$state_dir/ready.env\"; then
      ready_ok=1
      break
    fi
    kill -0 \"\$worker_pid\" 2>/dev/null || break
    sleep 1
  done
  if [ \"\$ready_ok\" -eq 1 ]; then
    {
      echo state=ready
      echo run_token=$RUN_TOKEN
      echo tag_clean=$TAG_CLEAN
      echo queue_sha256=$QUEUE_HASH
      echo runner_sha256=$WORKER_HASH
      echo pid=\$worker_pid
      echo host=\$(hostname)
      echo ready_epoch=\$(date +%s)
    } > \"\$state_dir/state.env.tmp.\$\$\"
    mv \"\$state_dir/state.env.tmp.\$\$\" \"\$state_dir/state.env\"
    echo READY pid=\$worker_pid run_token=$RUN_TOKEN
    exit 0
  fi
  if kill -0 \"\$worker_pid\" 2>/dev/null; then bootstrap_state=unready; else bootstrap_state=failed; fi
  {
    echo state=\$bootstrap_state
    echo run_token=$RUN_TOKEN
    echo tag_clean=$TAG_CLEAN
    echo queue_sha256=$QUEUE_HASH
    echo runner_sha256=$WORKER_HASH
    echo pid=\$worker_pid
    echo host=\$(hostname)
    echo failed_epoch=\$(date +%s)
  } > \"\$state_dir/state.env.tmp.\$\$\"
  mv \"\$state_dir/state.env.tmp.\$\$\" \"\$state_dir/state.env\"
  sed -n '1,12p' \"\$state_dir/bootstrap.log\" >&2 || true
  exit 7" < "$WORKER_SCRIPT"
  SSH_RC=$?
  if [ "$SSH_RC" -eq 0 ]; then
    HOST_STARTED=$((HOST_STARTED + 1))
    READY_HOSTS="${READY_HOSTS}${HOST}"$'\n'
    echo "Readiness-confirmed $CORES workers on $HOST"
  elif ACK_OUTPUT="$(verify_remote_ack "$HOST" "$WORKER_HASH")"; then
    HOST_STARTED=$((HOST_STARTED + 1))
    READY_HOSTS="${READY_HOSTS}${HOST}"$'\n'
    [ "$SSH_RC" -eq 124 ] && HOST_TIMEOUTS=$((HOST_TIMEOUTS + 1))
    echo "Recovered readiness confirmation from $HOST: $ACK_OUTPUT"
  else
    VERIFY_RC=$?
    BOOTSTRAP_DETAIL="$(remote_bootstrap_summary "$HOST")"
    [ "$SSH_RC" -eq 124 ] && HOST_TIMEOUTS=$((HOST_TIMEOUTS + 1))
    if [ "$VERIFY_RC" -eq 2 ]; then
      HOST_UNCERTAIN=$((HOST_UNCERTAIN + 1))
      HOST_ISSUES="${HOST_ISSUES}${HOST} (${CORES} workers): ambiguous SSH launch with no readiness confirmation\n"
      record_host_issue "$HOST" bootstrap "$SSH_RC" "${BOOTSTRAP_DETAIL:-host unreachable while checking readiness}"
    else
      HOST_FAILED=$((HOST_FAILED + 1))
      HOST_ISSUES="${HOST_ISSUES}${HOST} (${CORES} workers): bootstrap failed before readiness\n"
      record_host_issue "$HOST" bootstrap "$SSH_RC" "${BOOTSTRAP_DETAIL:-worker exited or did not initialize before timeout}"
    fi
    echo "Worker bootstrap failed before readiness on $HOST (ssh exit $SSH_RC)." >&2
  fi
done < "$CONFIG_FILE"
}

launch_shard_hosts() {
  RUNNER_HASH="$(shasum -a 256 "$SHARD_WORKER_SCRIPT" | awk '{print $1}')"
  while IFS=$'\t' read -r HOST_KEY HOST CORES SHARD_JOBS; do
    [ -n "${HOST:-}" ] || continue
    [ "${SHARD_JOBS:-0}" -gt 0 ] || continue
    HOST_TOTAL=$((HOST_TOTAL + 1))
    SHARD_FILE="$SHARD_DIR/${HOST_KEY}.commands.txt"
    SHARD_HASH="$(shasum -a 256 "$SHARD_FILE" | awk '{print $1}')"
    STAGE_TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/rota-stage.XXXXXX")"
    STAGE_ARCHIVE="$STAGE_TMP_DIR/stage.tar"
    cp "$SHARD_FILE" "$STAGE_TMP_DIR/commands.txt"
    cp "$SHARD_WORKER_SCRIPT" "$STAGE_TMP_DIR/mmshardworker.sh"
    tar -cf "$STAGE_ARCHIVE" -C "$STAGE_TMP_DIR" commands.txt mmshardworker.sh
    STAGE_HASH="$(shasum -a 256 "$STAGE_ARCHIVE" | awk '{print $1}')"

    echo "Staging $SHARD_JOBS immutable commands, runner, and $CORES slots on $HOST"
    run_with_timeout "$SSH_LAUNCH_TIMEOUT_SEC" ssh \
      "${SSH_COMMON_OPTS[@]}" \
      "$HOST" "set -e
  state_parent=\"\$HOME/.rota/runs\"
  state_dir=\"\$state_parent/$RUN_TOKEN\"
  mkdir -p \"\$state_parent\"
  if ! mkdir \"\$state_dir\" 2>/dev/null; then
    if [ -f \"\$state_dir/staged.env\" ] && \
       grep -q '^run_token=$RUN_TOKEN$' \"\$state_dir/staged.env\" && \
       grep -q '^commands_sha256=$SHARD_HASH$' \"\$state_dir/staged.env\" && \
       grep -q '^runner_sha256=$RUNNER_HASH$' \"\$state_dir/staged.env\"; then
      cat >/dev/null
      echo ALREADY_STAGED
      exit 0
    fi
    echo \"Ambiguous existing host-local stage: \$state_dir\" >&2
    exit 3
  fi
  cat > \"\$state_dir/stage.tar.incoming\"
  set -- \$(shasum -a 256 \"\$state_dir/stage.tar.incoming\")
  [ \"\$1\" = '$STAGE_HASH' ] || exit 4
  tar -xf \"\$state_dir/stage.tar.incoming\" -C \"\$state_dir\"
  set -- \$(shasum -a 256 \"\$state_dir/commands.txt\")
  [ \"\$1\" = '$SHARD_HASH' ] || exit 5
  set -- \$(shasum -a 256 \"\$state_dir/mmshardworker.sh\")
  [ \"\$1\" = '$RUNNER_HASH' ] || exit 6
  chmod 700 \"\$state_dir/mmshardworker.sh\"
  rm -f \"\$state_dir/stage.tar.incoming\"
  {
    echo run_token=$RUN_TOKEN
    echo commands_sha256=$SHARD_HASH
    echo runner_sha256=$RUNNER_HASH
    echo host_key=$HOST_KEY
    echo staged_epoch=\$(date +%s)
  } > \"\$state_dir/staged.env.tmp.\$\$\"
  mv \"\$state_dir/staged.env.tmp.\$\$\" \"\$state_dir/staged.env\"
  echo STAGED" < "$STAGE_ARCHIVE"
    STAGE_RC=$?
    rm -rf "$STAGE_TMP_DIR"

    if [ "$STAGE_RC" -ne 0 ]; then
      verify_remote_stage "$HOST" "$SHARD_HASH" "$RUNNER_HASH"
      VERIFY_STAGE_RC=$?
      [ "$STAGE_RC" -eq 124 ] && HOST_TIMEOUTS=$((HOST_TIMEOUTS + 1))
      if [ "$VERIFY_STAGE_RC" -eq 0 ]; then
        echo "Recovered ambiguous shard staging acknowledgement from $HOST"
      elif [ "$VERIFY_STAGE_RC" -eq 2 ]; then
        HOST_UNCERTAIN=$((HOST_UNCERTAIN + 1))
        HOST_ISSUES="${HOST_ISSUES}${HOST} (${CORES} slots): ambiguous shard staging\n"
        record_host_issue "$HOST" staging "$STAGE_RC" "host unreachable while verifying immutable shard stage"
        echo "Failed to verify the immutable shard staged on $HOST; leaving the batch guarded." >&2
        continue
      else
        HOST_FAILED=$((HOST_FAILED + 1))
        HOST_ISSUES="${HOST_ISSUES}${HOST} (${CORES} slots): shard staging failed\n"
        record_host_issue "$HOST" staging "$STAGE_RC" "immutable shard or runner failed hash verification"
        echo "Failed to verify the immutable shard staged on $HOST; leaving the batch guarded." >&2
        continue
      fi
    fi

    echo "Starting host-local shard controller on $HOST"
    run_with_timeout "$SSH_LAUNCH_TIMEOUT_SEC" ssh -n \
      "${SSH_COMMON_OPTS[@]}" \
      "$HOST" "set -e
  state_dir=\"\$HOME/.rota/runs/$RUN_TOKEN\"
  grep -q '^run_token=$RUN_TOKEN$' \"\$state_dir/staged.env\"
  if [ -f \"\$state_dir/ready.env\" ] && \\
     grep -q '^run_token=$RUN_TOKEN$' \"\$state_dir/ready.env\" && \\
     grep -q '^tag_clean=$TAG_CLEAN$' \"\$state_dir/ready.env\" && \\
     grep -q '^runner_sha256=$RUNNER_HASH$' \"\$state_dir/ready.env\"; then
    echo ALREADY_READY
    exit 0
  fi
  nohup env \\
    PARALLEL_WORKER_READY_FILE=\"\$state_dir/ready.env\" \\
    PARALLEL_WORKER_START_FILE=\"\$state_dir/start.env\" \\
    PARALLEL_WORKER_RUN_TOKEN='$RUN_TOKEN' \\
    PARALLEL_WORKER_RUNNER_SHA256='$RUNNER_HASH' \\
    bash -c \"
    exec -a mmshardworker_${TAG_CLEAN} \\
    bash '\$state_dir/mmshardworker.sh' \\
      '\$state_dir' \\
      '$CORES' \\
      '$TAG_CLEAN' \\
      '$HOST_KEY' \\
      '$HOST' \\
      '$SHARD_JOBS' \\
      '$SHARD_PROGRESS_SYNC_SEC'
  \" < /dev/null > \"\$state_dir/bootstrap.log\" 2>&1 &
  worker_pid=\$!
  {
    echo state=bootstrapping
    echo run_token=$RUN_TOKEN
    echo tag_clean=$TAG_CLEAN
    echo queue_sha256=$QUEUE_HASH
    echo runner_sha256=$RUNNER_HASH
    echo execution_mode=host-local-shards
    echo pid=\$worker_pid
    echo host=\$(hostname)
  } > \"\$state_dir/state.env.tmp.\$\$\"
  mv \"\$state_dir/state.env.tmp.\$\$\" \"\$state_dir/state.env\"
  ready_ok=0
  for ready_attempt in \$(seq 1 '$WORKER_READY_TIMEOUT_SEC'); do
    if [ -f \"\$state_dir/ready.env\" ] && \\
       grep -q '^run_token=$RUN_TOKEN$' \"\$state_dir/ready.env\" && \\
       grep -q '^tag_clean=$TAG_CLEAN$' \"\$state_dir/ready.env\" && \\
       grep -q '^runner_sha256=$RUNNER_HASH$' \"\$state_dir/ready.env\"; then
      ready_ok=1
      break
    fi
    kill -0 \"\$worker_pid\" 2>/dev/null || break
    sleep 1
  done
  if [ \"\$ready_ok\" -eq 1 ]; then
    {
      echo state=ready
      echo run_token=$RUN_TOKEN
      echo tag_clean=$TAG_CLEAN
      echo queue_sha256=$QUEUE_HASH
      echo runner_sha256=$RUNNER_HASH
      echo execution_mode=host-local-shards
      echo pid=\$worker_pid
      echo host=\$(hostname)
      echo ready_epoch=\$(date +%s)
    } > \"\$state_dir/state.env.tmp.\$\$\"
    mv \"\$state_dir/state.env.tmp.\$\$\" \"\$state_dir/state.env\"
    echo READY pid=\$worker_pid run_token=$RUN_TOKEN
    exit 0
  fi
  if kill -0 \"\$worker_pid\" 2>/dev/null; then bootstrap_state=unready; else bootstrap_state=failed; fi
  {
    echo state=\$bootstrap_state
    echo run_token=$RUN_TOKEN
    echo tag_clean=$TAG_CLEAN
    echo queue_sha256=$QUEUE_HASH
    echo runner_sha256=$RUNNER_HASH
    echo execution_mode=host-local-shards
    echo pid=\$worker_pid
    echo host=\$(hostname)
    echo failed_epoch=\$(date +%s)
  } > \"\$state_dir/state.env.tmp.\$\$\"
  mv \"\$state_dir/state.env.tmp.\$\$\" \"\$state_dir/state.env\"
  sed -n '1,12p' \"\$state_dir/bootstrap.log\" >&2 || true
  exit 7"
    SSH_RC=$?

    if [ "$SSH_RC" -eq 0 ]; then
      HOST_STARTED=$((HOST_STARTED + 1))
      READY_HOSTS="${READY_HOSTS}${HOST}"$'\n'
      echo "Readiness-confirmed $CORES host-local slots on $HOST"
    elif ACK_OUTPUT="$(verify_remote_ack "$HOST" "$RUNNER_HASH")"; then
      HOST_STARTED=$((HOST_STARTED + 1))
      READY_HOSTS="${READY_HOSTS}${HOST}"$'\n'
      [ "$SSH_RC" -eq 124 ] && HOST_TIMEOUTS=$((HOST_TIMEOUTS + 1))
      echo "Recovered shard readiness confirmation from $HOST: $ACK_OUTPUT"
    else
      VERIFY_RC=$?
      BOOTSTRAP_DETAIL="$(remote_bootstrap_summary "$HOST")"
      [ "$SSH_RC" -eq 124 ] && HOST_TIMEOUTS=$((HOST_TIMEOUTS + 1))
      if [ "$VERIFY_RC" -eq 2 ]; then
        HOST_UNCERTAIN=$((HOST_UNCERTAIN + 1))
        HOST_ISSUES="${HOST_ISSUES}${HOST} (${CORES} slots): ambiguous shard launch without readiness\n"
        record_host_issue "$HOST" bootstrap "$SSH_RC" "${BOOTSTRAP_DETAIL:-host unreachable while checking readiness}"
      else
        HOST_FAILED=$((HOST_FAILED + 1))
        HOST_ISSUES="${HOST_ISSUES}${HOST} (${CORES} slots): shard bootstrap failed before readiness\n"
        record_host_issue "$HOST" bootstrap "$SSH_RC" "${BOOTSTRAP_DETAIL:-shard runner exited or did not initialize before timeout}"
      fi
      echo "Host-local shard bootstrap failed before readiness on $HOST; leaving the batch guarded." >&2
    fi
  done < "$SHARD_DIR/manifest.tsv"
}

if [ "$EXECUTION_MODE" = "shared-queue" ]; then
  launch_shared_queue_hosts
else
  launch_shard_hosts
fi

if [ "$HOST_STARTED" -eq "$HOST_TOTAL" ] && [ "$HOST_TOTAL" -gt 0 ]; then
  if ! release_ready_hosts; then
    echo "At least one start gate could not be released; stopping this batch before its common start epoch." >&2
    stop_ready_hosts
  fi
else
  echo "Not all requested hosts reached readiness; stopping this batch without releasing workers." >&2
  stop_ready_hosts
fi

start_shard_coordinator() {
  nohup bash -c "
    exec -a mmshardcoordinator_${TAG_CLEAN} \\
    bash '$SHARD_COORDINATOR_SCRIPT' \\
      '$LOG_DIR' \\
      '$SHARD_DIR/manifest.tsv' \\
      '$RUN_TOKEN' \\
      '$TAG_CLEAN' \\
      '$SHARD_PROGRESS_SYNC_SEC' \\
      '$SSH_CONNECT_TIMEOUT_SEC' \\
      '$SSH_LAUNCH_TIMEOUT_SEC'
  " < /dev/null > "$LOG_DIR/shard-coordinator.log" 2>&1 &
  COORDINATOR_PID=$!
  sleep 1
  if kill -0 "$COORDINATOR_PID" 2>/dev/null; then
    COORDINATOR_STARTED=1
    printf '%s\n' "$COORDINATOR_PID" > "$LOG_DIR/shard-coordinator.pid"
    chmod 666 "$LOG_DIR/shard-coordinator.pid"
    echo "Started shard progress coordinator PID $COORDINATOR_PID"
  else
    wait "$COORDINATOR_PID" 2>/dev/null || true
    echo "Failed to start the shard progress coordinator; stopping released shard controllers." >&2
    coordinator_host="${ROTA_COORDINATOR:-$(hostname)}"
    HOST_ISSUES="${HOST_ISSUES}${coordinator_host} coordinator: failed to start\n"
    record_host_issue "$coordinator_host" coordinator 1 "shard progress coordinator exited during startup"
    stop_ready_hosts
  fi
}

if [ "$EXECUTION_MODE" = "host-local-shards" ] && [ "$HOST_RELEASED" -eq "$HOST_TOTAL" ]; then
  start_shard_coordinator
fi

LAUNCH_STATE="active"
if [ "$HOST_STARTED" -ne "$HOST_TOTAL" ]; then
  LAUNCH_STATE="partial"
fi
if [ "$HOST_RELEASED" -ne "$HOST_TOTAL" ]; then
  LAUNCH_STATE="partial"
fi
if [ "$EXECUTION_MODE" = "host-local-shards" ] && [ "$COORDINATOR_STARTED" -ne 1 ]; then
  LAUNCH_STATE="partial"
fi
{
  echo "state=$LAUNCH_STATE"
  echo "run_token=$RUN_TOKEN"
  echo "hosts_total=$HOST_TOTAL"
  echo "hosts_acknowledged=$HOST_STARTED"
  echo "hosts_released=$HOST_RELEASED"
  echo "hosts_auto_stopped=$AUTO_STOPPED"
  echo "hosts_timed_out=$HOST_TIMEOUTS"
  echo "hosts_failed=$HOST_FAILED"
  echo "hosts_uncertain=$HOST_UNCERTAIN"
  echo "coordinator_pid=$COORDINATOR_PID"
  echo "issues_file=$LAUNCH_ISSUES_FILE"
  echo "updated_epoch=$(date +%s)"
} > "$LOG_DIR/launch_status.env"
chmod 666 "$LOG_DIR/launch_status.env"
echo "launch_state=$LAUNCH_STATE" >> "$ACTIVE_RUN_DIR/owner.env"
echo "coordinator_pid=$COORDINATOR_PID" >> "$ACTIVE_RUN_DIR/owner.env"

echo "Launch summary: hosts=${HOST_TOTAL} ready=${HOST_STARTED} released=${HOST_RELEASED} auto_stopped=${AUTO_STOPPED} timed_out=${HOST_TIMEOUTS} failed=${HOST_FAILED} uncertain=${HOST_UNCERTAIN}"
if [ -n "$HOST_ISSUES" ]; then
  echo "Host launch issues:"
  printf "%b" "$HOST_ISSUES"
fi
if [ "$LAUNCH_STATE" != "active" ]; then
  echo "The partial batch remains guarded at $ACTIVE_RUN_DIR." >&2
  echo "Do not relaunch it blindly. Audit host acknowledgements and reconcile outputs first." >&2
  exit 1
fi
