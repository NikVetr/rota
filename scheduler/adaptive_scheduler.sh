#!/usr/bin/env bash
export PARALLEL_WORKERS_ROOT="${PARALLEL_WORKERS_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
STATE_ROOT="${ADAPTIVE_STATE_ROOT:-${PARALLEL_WORKERS_ROOT}/.parallel-workers/adaptive}"
POOL_FILE="${ADAPTIVE_POOL_MARKER:-$STATE_ROOT/pool.env}"
TRANSITION_LOCK="${MODE_TRANSITION_LOCK:-$(dirname "$STATE_ROOT")/mode-transition.lock}"
CLUSTER_HOSTS_FILE="${ADAPTIVE_CLUSTER_HOSTS_FILE:-${PARALLEL_WORKERS_ROOT}/hosts/hosts-ethernet.txt}"
START_WORKERS_SCRIPT="${START_WORKERS_SCRIPT:-$SCRIPT_DIR/start_workers.sh}"
COORDINATOR_SCRIPT="${ADAPTIVE_COORDINATOR_SCRIPT:-$SCRIPT_DIR/adaptive_coordinator.py}"
DAEMON_SCRIPT="${ADAPTIVE_DAEMON_SCRIPT:-$SCRIPT_DIR/mmhostdaemon.sh}"
WORKER_SCRIPT="${WORKER_SCRIPT:-$SCRIPT_DIR/mmworker.sh}"
CLAIM_SCRIPT="$SCRIPT_DIR/adaptive_claim.py"
MEMORY_SCRIPT="$SCRIPT_DIR/adaptive_memory.py"
CALIBRATION_SCRIPT="$SCRIPT_DIR/adaptive_calibration.py"
RECOVERY_SCRIPT="$SCRIPT_DIR/adaptive_recovery.py"
WORKFLOW_SCRIPT="$SCRIPT_DIR/adaptive_workflow.py"
SSH_BIN="${ADAPTIVE_SSH_BIN:-ssh}"
SSH_CONNECT_TIMEOUT="${ADAPTIVE_SSH_CONNECT_TIMEOUT:-8}"
READY_TIMEOUT="${ADAPTIVE_READY_TIMEOUT:-15}"
DAEMON_POLL_SECONDS="${ADAPTIVE_DAEMON_POLL_SECONDS:-1}"
HELD_LOCKS=()
TEMP_DIR=""
MEMORY_CONTROL_VERSION=""
MEMORY_CALIBRATION_VERSION=""

usage() {
  cat <<'EOF'
Usage: adaptive_scheduler.sh <command> [options]

Commands:
  submit-chain --manifest <json>   Submit an ordered list of submit argument lists
  pool-start --resources_file <file> [--pool_name <name>] [--shared_io_capacity <units>]
             [--io_pressure_host <host_id>] [--smbd_cpu_limit_pct <percent>]
             [--backfill 0|1]
  pool-start --recover-inactive [--expected-pool-id <pool_id>]
  pool-status
  set-host-budget --host_id <id> --cpu_units <int> --memory_mib <MiB>
                  [--expected-pool-id <pool_id>]
  set-host-state --host_id <id> --state away|present [--at <time>] [--until <time>]
                 [--reason <note>] [--expected-pool-id <pool_id>]
                 Away: start no new jobs there (running ones finish), from --at (default
                 now) until --until or until set present. Times: epoch or ISO local time
  pool-drain [--recover-inactive] [--expected-pool-id <pool_id>]
  submit --model_dir <dir> --queue <file> --memory_mib_per_job <MiB> [options]
  set-priority --batch_dir <dir> --schedule_priority <SPEC>  Replaces all host overrides
  set-max-workers --batch_dir <dir> --max_workers <non-negative integer>
  set-memory --batch_dir <dir> --memory_mib_per_job <positive MiB>
             [--expected-pool-id <pool_id>]
  pause|resume --batch_dir <dir>
  close-for-hard-stop --batch_dir <dir>
  queue-status --batch_dir <dir>
  wait-closed --batch_dir <dir> [--timeout_sec <seconds>]
  reset-failures --batch_dir <dir> --host_id <id> --reason <audit note>
                 [--expected-pool-id <pool_id>]
  resolve-uncertain --batch_dir <dir> --host_id <id> --reservation_id <id>
                    [--verify_lost_job]  Audit a loss for opt-in tail retry
                    --reason <audit note> [--expected-pool-id <pool_id>]
  restart-daemon --host_id <id> [--reason <audit note>] [--expected-pool-id <pool_id>]
                Restart one host's dead daemon; releases work its reboot ended
  confirm-interrupted-jobs --batch_dir <dir> --reason <audit note>
                Accept verified outputs of jobs that ran across a share interruption

Submit options:
  --run_name <label>                 Default: empty
  --project <source-repo-label>       Optional: 1-64 ASCII letters/digits/._-
  --schedule_priority <SPEC>         Default: 50. SPEC is an integer, or comma items:
                                      [default,]HOST=<integer>|HOST=off (pool host IDs)
  --cpu_units_per_job <integer>      Default: 1
  --shared_io_units_per_job <int>    Default: 1 if the pool has shared I/O capacity, else 0
  --gpu_units_per_job <int>          Default: 0. Devices per job, on hosts with gpu_units
  --memory_limit_mib <MiB>           Optional: end a job whose processes exceed this (sampled)
  --max_workers <integer>            Default: 0 (pool-limited)
  --paused                           Start with new-job admission paused
  --memory_calibration_jobs <0|20..50>  Opt in after successful jobs (default 0)
  --after <batch-dir|queue-id|unique-run-name>  Hold until successful completion; repeat for AND
  --retry_lost_jobs                Retry audited node-loss tail jobs once (default off)
  --early_eta_sample               Let the first two jobs in a queue of 20+ sample free slots early
  --memory_calibration_seconds <0|600..86400>  Opt in for long jobs (default 0)
  --memory_min_mib_per_job <MiB>       Calibration floor (default 64 when enabled)
  --recover_inactive                 Recover an audited stale queue guard

The three calibration options also apply to set-memory. Without them,
set-memory disables calibration and restores the requested memory floor.

Pool-start tuning:
  --cpu_tolerance_pct <0..100>        Background CPU tolerance, percent of one core (default 10)
  --tick_seconds <seconds>           Idle cadence (default 5); active work polls at most 1s apart
  --start_interval_seconds <seconds> Added worker spacing per host (default 4; 0 disables)

Pool-drain options:
  --recover-inactive                 Recover a dead coordinator directly into DRAINING
  --expected-pool-id <pool_id>       Refuse if the locked pool identity differs
                                      (also valid with pool-start --recover-inactive)
EOF
}

die() {
  echo "Error: $*" >&2
  exit 1
}

is_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

is_int() {
  [[ "$1" =~ ^-?[0-9]+$ ]]
}

validate_memory_calibration() {
  local jobs="$1" seconds="$2" minimum="$3" memory="$4"
  is_uint "$jobs" && { [ "$jobs" -eq 0 ] || { [ "$jobs" -ge 20 ] && [ "$jobs" -le 50 ]; }; } || \
    die "memory calibration jobs must be 0 or an integer from 20 to 50"
  is_uint "$seconds" && { [ "$seconds" -eq 0 ] || { [ "$seconds" -ge 600 ] && [ "$seconds" -le 86400 ]; }; } || \
    die "memory calibration seconds must be 0 or an integer from 600 to 86400"
  if [ "$jobs" -eq 0 ] && [ "$seconds" -eq 0 ]; then
    [ -z "$minimum" ] || die "memory minimum requires an enabled calibration trigger"
  else
    minimum="${minimum:-64}"
    is_uint "$minimum" && [ "$minimum" -gt 0 ] && [ "$minimum" -le "$memory" ] || \
      die "memory minimum must be a positive integer no larger than requested memory"
  fi
}

reject_record_breaks() {
  local value
  for value in "$@"; do
    [[ "$value" != *$'\n'* && "$value" != *$'\r'* && "$value" != *$'\t'* ]] || \
      die "tabs and newlines are not supported in scheduler values"
  done
}

env_value() {
  local file="$1" key="$2" result count
  [ -f "$file" ] || die "state file not found: $file"
  count="$(awk -F= -v key="$key" '$1 == key {n++} END {print n+0}' "$file")"
  [ "$count" -eq 1 ] || die "$file must contain exactly one $key entry"
  result="$(awk -F= -v key="$key" '$1 == key {sub(/^[^=]*=/, ""); print; exit}' "$file")"
  printf '%s\n' "$result"
}

state_record() {
  local file="$1" keys="$2"
  [ -f "$file" ] || return 1
  awk -F= -v keys="$keys" '
    BEGIN {
      count=split(keys, ordered, " ")
      for (i=1; i<=count; i++) wanted[ordered[i]]=1
    }
    $1 in wanted {
      if (seen[$1]++) bad=1
      value[$1]=substr($0, length($1) + 2)
    }
    END {
      for (i=1; i<=count; i++) if (seen[ordered[i]] != 1) bad=1
      if (bad) exit 1
      for (i=1; i<=count; i++)
        printf "%s%s", value[ordered[i]], (i == count ? "\n" : "\t")
    }
  ' "$file"
}

release_locks() {
  local index lock owner_pid
  for ((index=${#HELD_LOCKS[@]}-1; index>=0; index--)); do
    lock="${HELD_LOCKS[$index]}"
    owner_pid="$(awk -F= '$1 == "pid" {print $2; exit}' "$lock/owner.env" 2>/dev/null || true)"
    [ "$owner_pid" = "$$" ] || continue
    rm -f "$lock/owner.env"
    rmdir "$lock" 2>/dev/null || true
  done
  if [ -n "$TEMP_DIR" ] && [ -d "$TEMP_DIR" ]; then
    rm -f "$TEMP_DIR/stage.tar"
    rmdir "$TEMP_DIR" 2>/dev/null || true
  fi
}
trap release_locks EXIT

release_lock() {
  local lock="$1" owner_pid index held_index=-1 held_count
  owner_pid="$(awk -F= '$1 == "pid" {print $2; exit}' "$lock/owner.env" 2>/dev/null || true)"
  [ "$owner_pid" = "$$" ] || die "cannot release transition lock not owned by this process: $lock"
  held_count="${#HELD_LOCKS[@]}"
  for ((index=0; index<held_count; index++)); do
    if [ "${HELD_LOCKS[$index]}" = "$lock" ]; then
      [ "$held_index" -eq -1 ] || die "transition lock is recorded more than once: $lock"
      held_index="$index"
    fi
  done
  [ "$held_index" -ge 0 ] || die "transition lock is not recorded as held: $lock"
  rm -f "$lock/owner.env"
  rmdir "$lock" 2>/dev/null || die "cannot release transition lock: $lock"
  for ((index=held_index; index+1<held_count; index++)); do
    HELD_LOCKS[$index]="${HELD_LOCKS[$((index + 1))]}"
  done
  unset "HELD_LOCKS[$((held_count - 1))]"
}

acquire_lock() {
  local lock="$1" recover="${2:-0}" owner_pid owner_host
  mkdir -p "$(dirname "$lock")"
  if ! mkdir "$lock" 2>/dev/null; then
    if [ "$recover" -eq 1 ] && [ -f "$lock/owner.env" ]; then
      owner_pid="$(awk -F= '$1 == "pid" {print $2; exit}' "$lock/owner.env")"
      owner_host="$(awk -F= '$1 == "host" {print $2; exit}' "$lock/owner.env")"
      if [ "$owner_host" = "$(hostname)" ] && is_uint "$owner_pid" && \
          ! kill -0 "$owner_pid" 2>/dev/null; then
        rm -f "$lock/owner.env"
        rmdir "$lock" 2>/dev/null || die "cannot reclaim audited transition lock: $lock"
        mkdir "$lock" 2>/dev/null || die "cannot acquire transition lock: $lock"
      else
        sed -n '1,5p' "$lock/owner.env" >&2
        die "the recorded transition-lock owner is not safely inactive"
      fi
    else
      echo "Error: scheduler state is locked: $lock" >&2
      [ ! -f "$lock/owner.env" ] || sed -n '1,5p' "$lock/owner.env" >&2
      die "audit the recorded owner; locks are not reclaimed by age"
    fi
  fi
  {
    echo "pid=$$"
    echo "host=$(hostname)"
    echo "command=$COMMAND"
    echo "created_epoch=$(date +%s)"
  } > "$lock/owner.env"
  HELD_LOCKS+=("$lock")
}

absolute_path() {
  printf '%s/%s\n' "$(cd "$(dirname "$1")" && pwd -P)" "$(basename "$1")"
}

assert_local_state_root() {
  local probe="$STATE_ROOT" filesystem mountpoint
  while [ ! -e "$probe" ] && [ "$probe" != / ]; do probe="$(dirname "$probe")"; done
  if [ "$(uname -s)" = Darwin ]; then
    mountpoint="$(df -P "$probe" 2>/dev/null | awk 'NR == 2 {print $6}')"
    filesystem="$(mount | awk -v point="$mountpoint" '$3 == point {gsub(/[(),]/, "", $4); print $4; exit}')"
  else
    filesystem="$(stat -f -c %T "$probe" 2>/dev/null || printf unknown)"
  fi
  case "${filesystem:-unknown}" in
    cifs|smb*|nfs*|fuse*|osxfuse*)
      die "adaptive coordinator state must be on a local filesystem, not $filesystem" ;;
    unknown|"") die "cannot verify the adaptive coordinator state filesystem" ;;
  esac
}

hash_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

validate_resources() {
  # The opt-in host columns (devices, owner activity) are present only when an inventory sets one.
  awk -F '\t' '
    NR == 1 {
      columns=5
      if ($0 == "host_id\tssh_target\tcpu_units\tmemory_mib\tenabled\tgpu_units\towner_idle_seconds\towner_active")
        columns=8
      else if ($0 != "host_id\tssh_target\tcpu_units\tmemory_mib\tenabled") bad=1
      next
    }
    NF {
      if (NF != columns || $1 !~ /^[A-Za-z0-9_.-]+$/ || $1 == "." || $1 == ".." ||
          $2 == "" || $2 ~ /[\r\n\t]/ ||
          $3 !~ /^[1-9][0-9]*$/ || $4 !~ /^[1-9][0-9]*$/ || $5 !~ /^[01]$/ || seen[$1]++) bad=1
      if (columns == 8 && ($6 !~ /^[0-9]+$/ || $7 !~ /^[0-9]+$/ || ($7 != 0 && ($7 < 60 || $7 > 86400)) ||
          ($8 != "hold" && $8 != "suspend"))) bad=1
      if (seen_target[$2]++) bad=1
      if ($5 == 1) enabled++
    }
    END {exit bad || enabled < 1}
  ' "$1" || die "invalid adaptive resources TSV: $1"
}

write_pool() {
  local state="$1" coordinator_pid="$2" target_state="${3:-$1}" tmp="${POOL_FILE}.tmp.$$"
  {
    echo "schema_version=1"
    echo "pool_id=$POOL_ID"
    echo "pool_name=$POOL_NAME"
    echo "state=$state"
    echo "target_state=$target_state"
    echo "resources_file=$RESOURCES_FILE"
    echo "resources_sha256=$RESOURCES_HASH"
    echo "shared_io_capacity=$SHARED_IO_CAPACITY"
    echo "io_pressure_host=$IO_PRESSURE_HOST"
    echo "smbd_cpu_limit_pct=$SMBD_CPU_LIMIT_PCT"
    echo "cpu_tolerance_pct=$CPU_TOLERANCE_PCT"
    echo "tick_seconds=$TICK_SECONDS"
    echo "start_interval_seconds=$START_INTERVAL_SECONDS"
    echo "backfill=$BACKFILL"
    echo "lease_seconds=$LEASE_SECONDS"
    echo "daemon_sha256=$DAEMON_HASH"
    echo "worker_sha256=$WORKER_HASH"
    echo "claim_sha256=$CLAIM_HASH"
    echo "memory_sha256=$MEMORY_HASH"
    echo "calibration_sha256=$CALIBRATION_HASH"
    echo "recovery_sha256=$RECOVERY_HASH"
    echo "coordinator_sha256=$COORDINATOR_HASH"
    echo "workflow_sha256=$WORKFLOW_HASH"
    echo "workflow_version=1"
    [ -z "$MEMORY_CONTROL_VERSION" ] || echo "memory_control_version=$MEMORY_CONTROL_VERSION"
    [ -z "$MEMORY_CALIBRATION_VERSION" ] || echo "memory_calibration_version=$MEMORY_CALIBRATION_VERSION"
    echo "coordinator_pid=$coordinator_pid"
    echo "created_epoch=$CREATED_EPOCH"
    echo "updated_epoch=$(date +%s)"
  } > "$tmp"
  chmod 644 "$tmp"
  mv -f "$tmp" "$POOL_FILE"
}

ssh_no_input() {
  "$SSH_BIN" -n -o BatchMode=yes -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" "$1" "$2"
}

audit_host_adaptive_clear() {
  local target="$1" recover="$2" pattern audit remote_hostname snapshot output ancestor_pids
  pattern='[.]parallel-workers/adaptive/[^ ]+/bin/[m]mworker[.]sh|[m]madaptivedaemon[.]sh|[a]daptive_coordinator[.]py|(^|[ /])[a]daptive_scheduler[.]sh([ ]|$)'
  audit="$(ssh_no_input "$target" "hostname; ps -ww -axo pid=,command= | grep -E '$pattern' || true")" || \
    die "cannot audit scheduler processes on $target"
  remote_hostname="${audit%%$'\n'*}"
  if [[ "$audit" == *$'\n'* ]]; then output="${audit#*$'\n'}"; else output=""; fi
  if [ "$remote_hostname" = "$(hostname)" ]; then
    snapshot="$(ps -ww -axo pid=,command=)" || die "cannot audit local scheduler processes"
    output="$(printf '%s\n' "$snapshot" | grep -E "$pattern" || true)"
    ancestor_pids="$(ps -axo pid=,ppid= | awk -v pid="$$" '
      {parent[$1]=$2}
      END {
        while (pid ~ /^[1-9][0-9]*$/ && !seen[pid]++) {
          printf " %s", pid
          if (!(pid in parent) || parent[pid] == pid) break
          pid=parent[pid]
        }
        print " "
      }
    ')" || die "cannot identify the local pool-start process ancestry"
    output="$(printf '%s\n' "$output" | awk -v ancestors="$ancestor_pids" -v self="$$" '
      $1 == self {next}
      index(ancestors, " " $1 " ") &&
        $0 ~ /(^|[ \/])adaptive_scheduler[.]sh (pool-start|pool-drain --recover-inactive)( |$)/ {next}
      {print}
    ')"
  fi
  output="$(printf '%s\n' "$output" | awk '
    function base(path) {sub(/^.*\//, "", path); return path}
    # mmadaptiveagent: the host daemon name in pools started before its rename.
    function scheduler(path, name) {
      name=base(path)
      return name ~ /^(mmhostdaemon[.]sh|mmadaptiveagent[.]sh|adaptive_coordinator[.]py|adaptive_scheduler[.]sh)$/ ||
        path ~ /[.]parallel-workers\/adaptive\/[^ \/]+\/bin\/mmworker[.]sh$/
    }
    $1 ~ /^[0-9]+$/ {
      script=2
      executable=base($script)
      if (!scheduler($script)) {
        shell=(executable ~ /^(bash|sh|zsh)$/)
        python=(executable ~ /^[Pp]ython([0-9]+([.][0-9]+)*)?$/)
        if (!shell && !python && executable !~ /^(mmworker_|mmhostdaemon_|mmadaptiveagent_)/) next
        script++
        while ($script ~ /^-/) {
          if ($script == "--") {script++; break}
          if ($script ~ /^-[^-]*c/ || $script == "--command" || (python && $script ~ /^-m/)) next
          if ((shell && $script ~ /^(-o|-O|--rcfile|--init-file)$/) ||
              (python && $script ~ /^(-W|-X)$/)) script++
          script++
        }
      }
      if (!scheduler($script)) next
      if (base($script) == "adaptive_scheduler.sh" &&
          $(script+1) ~ /^(pool-status|queue-status|wait-closed|--help|-h)$/) next
      print
    }
  ')" || die "cannot classify scheduler processes on $target"
  if [ "$recover" -eq 1 ] && [ -n "$output" ]; then
    output="$(printf '%s\n' "$output" | awk -v pool="$POOL_ID" '
      index($0, pool) && ($0 ~ /mmhostdaemon[.]sh/ || $0 ~ /mmworker[.]sh/) {next}
      {print}
    ')"
  fi
  [ -z "$output" ] || {
    printf 'Active scheduler processes on %s:\n%s\n' "$target" "$output" >&2
    return 1
  }
}

stage_host() {
  local host_id="$1" target="$2" cpu="$3" memory="$4" archive="$5"
  local gpu="${6:-0}" owner_idle="${7:-0}" owner_active="${8:-hold}"
  "$SSH_BIN" -o BatchMode=yes -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" "$target" "set -e
state_dir=\"\$HOME/.parallel-workers/adaptive/$POOL_ID\"
incoming=\"\$state_dir/bin.incoming.\$\$\"
mkdir -p \"\$state_dir\" \"\$incoming\"
python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 9))'
tar -xf - -C \"\$incoming\"
[ \"\$(shasum -a 256 \"\$incoming/mmhostdaemon.sh\" | awk '{print \$1}')\" = '$DAEMON_HASH' ]
[ \"\$(shasum -a 256 \"\$incoming/mmworker.sh\" | awk '{print \$1}')\" = '$WORKER_HASH' ]
[ \"\$(shasum -a 256 \"\$incoming/adaptive_claim.py\" | awk '{print \$1}')\" = '$CLAIM_HASH' ]
[ \"\$(shasum -a 256 \"\$incoming/adaptive_memory.py\" | awk '{print \$1}')\" = '$MEMORY_HASH' ]
[ \"\$(shasum -a 256 \"\$incoming/adaptive_recovery.py\" | awk '{print \$1}')\" = '$RECOVERY_HASH' ]
if [ -d \"\$state_dir/bin\" ]; then
  cmp -s \"\$incoming/mmhostdaemon.sh\" \"\$state_dir/bin/mmhostdaemon.sh\"
  cmp -s \"\$incoming/mmworker.sh\" \"\$state_dir/bin/mmworker.sh\"
  cmp -s \"\$incoming/adaptive_claim.py\" \"\$state_dir/bin/adaptive_claim.py\"
  cmp -s \"\$incoming/adaptive_memory.py\" \"\$state_dir/bin/adaptive_memory.py\"
  cmp -s \"\$incoming/adaptive_recovery.py\" \"\$state_dir/bin/adaptive_recovery.py\"
  rm -f \"\$incoming/mmhostdaemon.sh\" \"\$incoming/mmworker.sh\" \"\$incoming/adaptive_claim.py\"
  rm -f \"\$incoming/adaptive_recovery.py\"
  rm -f \"\$incoming/adaptive_memory.py\"
  rmdir \"\$incoming\"
else
  mv \"\$incoming\" \"\$state_dir/bin\"
  chmod 700 \"\$state_dir/bin/mmhostdaemon.sh\" \"\$state_dir/bin/mmworker.sh\"
fi
daemon=\"\$state_dir/daemon.env\"
if [ -f \"\$daemon\" ]; then
  grep -Fqx 'schema_version=1' \"\$daemon\"
  grep -Fqx 'pool_id=$POOL_ID' \"\$daemon\"
  grep -Fqx 'host_id=$host_id' \"\$daemon\"
  grep -Fqx 'cpu_units=$cpu' \"\$daemon\"
  grep -Fqx 'memory_mib=$memory' \"\$daemon\"
  grep -Fqx \"runner_path=\$state_dir/bin/mmworker.sh\" \"\$daemon\"
  grep -Fqx 'runner_sha256=$WORKER_HASH' \"\$daemon\"
  grep -Fqx 'claim_sha256=$CLAIM_HASH' \"\$daemon\"
  grep -Fqx 'memory_sha256=$MEMORY_HASH' \"\$daemon\"
  grep -Fqx 'gpu_units=$gpu' \"\$daemon\"
  grep -Fqx 'owner_idle_seconds=$owner_idle' \"\$daemon\"
  grep -Fqx 'owner_active=$owner_active' \"\$daemon\"
else
  { echo schema_version=1; echo pool_id='$POOL_ID'; echo host_id='$host_id';
    echo cpu_units='$cpu'; echo memory_mib='$memory';
    echo \"runner_path=\$state_dir/bin/mmworker.sh\"; echo runner_sha256='$WORKER_HASH';
    echo claim_sha256='$CLAIM_HASH'; echo memory_sha256='$MEMORY_HASH';
    echo gpu_units='$gpu'; echo owner_idle_seconds='$owner_idle'; echo owner_active='$owner_active';
  } > \"\$daemon.tmp.\$\$\"
  chmod 600 \"\$daemon.tmp.\$\$\"
  mv \"\$daemon.tmp.\$\$\" \"\$daemon\"
fi" < "$archive" || die "daemon staging failed on $target"
}

start_daemon() {
  local host_id="$1" target="$2" sample_seconds=$((LEASE_SECONDS / 3))
  [ "$sample_seconds" -ge 1 ] || sample_seconds=1
  [ "$sample_seconds" -le 5 ] || sample_seconds=5
  ssh_no_input "$target" "set -e
state_dir=\"\$HOME/.parallel-workers/adaptive/$POOL_ID\"
owner=\"\$state_dir/daemon.lock\"
if [ -f \"\$owner\" ]; then
  pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$owner\")
  script=\$(awk -F= '\$1 == \"script\" {print \$2; exit}' \"\$owner\")
  owner_state=\$(awk -F= '\$1 == \"state_dir\" {print \$2; exit}' \"\$owner\")
  grep -Fqx 'pool_id=$POOL_ID' \"\$owner\"
  grep -Fqx 'host_id=$host_id' \"\$owner\"
  [ \"\$script\" = \"\$state_dir/bin/mmhostdaemon.sh\" ]
  [ \"\$owner_state\" = \"\$state_dir\" ]
  command=\$(ps -ww -p \"\$pid\" -o command= 2>/dev/null || true)
  if [ -n \"\$command\" ] && [[ \" \$command \" == *\" \$script \"* ]] &&
     [[ \" \$command \" == *\" --state-dir \$owner_state \"* ]] &&
     [ -f \"\$state_dir/status.tsv\" ]; then echo ALREADY_READY; exit 0; fi
  [ -z \"\$command\" ] || exit 8
fi
# A restarted daemon must not erase the log of the one that died.
[ ! -s \"\$state_dir/daemon.log\" ] || mv -f \"\$state_dir/daemon.log\" \"\$state_dir/daemon.log.\$(date +%s)\"
nohup bash -c \"exec -a 'mmhostdaemon_${POOL_ID}_${host_id}' bash '\$state_dir/bin/mmhostdaemon.sh' --state-dir '\$state_dir' --poll-seconds '$DAEMON_POLL_SECONDS' --sample-seconds '$sample_seconds'\" < /dev/null > \"\$state_dir/daemon.log\" 2>&1 &
pid=\$!
for attempt in \$(seq 1 '$READY_TIMEOUT'); do
  lock_pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$state_dir/daemon.lock\" 2>/dev/null || true)
  if [ \"\$lock_pid\" = \"\$pid\" ] &&
     grep -Fqx 'pool_id=$POOL_ID' \"\$state_dir/daemon.lock\" &&
     grep -Fqx 'host_id=$host_id' \"\$state_dir/daemon.lock\" &&
     grep -Fqx \"script=\$state_dir/bin/mmhostdaemon.sh\" \"\$state_dir/daemon.lock\" &&
     grep -Fqx \"state_dir=\$state_dir\" \"\$state_dir/daemon.lock\" &&
     kill -0 \"\$pid\" 2>/dev/null && [ -f \"\$state_dir/status.tsv\" ] &&
     awk -F '\t' -v pool='$POOL_ID' -v host='$host_id' '\$1 == \"daemon\" && \$2 == pool && \$3 == host {ok=1} END {exit !ok}' \"\$state_dir/status.tsv\"; then
    echo READY
    exit 0
  fi
  sleep 1
done
exit 7" >/dev/null || die "adaptive daemon failed readiness on $target"
}

daemon_lock_state() {
  local target="$1"
  ssh_no_input "$target" "
lock=\"\$HOME/.parallel-workers/adaptive/$POOL_ID/daemon.lock\"
if [ -L \"\$lock\" ]; then
  exit 8
elif [ -f \"\$lock\" ]; then
  echo PRESENT
elif [ -e \"\$lock\" ]; then
  exit 8
else
  echo ABSENT
fi"
}

audit_unowned_daemon() {
  local action="$1" host_id="$2" target="$3" require_idle=0 token
  [ "$action" != finalize ] || require_idle=1
  # Auditing the coordinator itself goes over SSH to localhost, so the local ssh
  # client (not an ancestor of the remote shell) carries this script, including
  # the patterns searched for; the token lets the audit skip its own processes.
  token="parallel-workers-audit-$$-$RANDOM-$(date +%s)"
  ssh_no_input "$target" "set -e
: $token
state_dir=\"\$HOME/.parallel-workers/adaptive/$POOL_ID\"
lock=\"\$state_dir/daemon.lock\"
[ ! -e \"\$lock\" ] && [ ! -L \"\$lock\" ]
if [ '$require_idle' = 1 ]; then
  [ -d \"\$state_dir/reservations\" ]
  [ -z \"\$(find \"\$state_dir/reservations\" -type f -name '*.env' -print)\" ] || exit 8
fi
processes=\$(ps -ww -axo pid=,command=)
skip=\" \$\$ \"
ancestor=\$PPID
while [[ \"\$ancestor\" =~ ^[0-9]+\$ ]] && [ \"\$ancestor\" -gt 0 ]; do
  skip=\"\$skip\$ancestor \"
  next=\$(ps -o ppid= -p \"\$ancestor\" 2>/dev/null | tr -d ' ' || true)
  [ \"\$next\" != \"\$ancestor\" ] || break
  ancestor=\$next
done
if printf '%s\n' \"\$processes\" | awk -v skip=\"\$skip\" -v token='$token' \
    -v daemon=\"\$state_dir/bin/mmhostdaemon.sh\" \
    -v reservation='$POOL_ID.$host_id.' -v require_idle='$require_idle' '
      index(skip, \" \" \$1 \" \") || index(\$0, token) {next}
      index(\$0, daemon) || (require_idle && index(\$0, reservation)) {found=1}
      END {exit !found}
    '; then
  exit 8
fi
[ ! -e \"\$lock\" ] && [ ! -L \"\$lock\" ]" >/dev/null
}

audit_recovery_workers() {
  local host_id="$1" target="$2"
  ssh_no_input "$target" "set -e
state_dir=\"\$HOME/.parallel-workers/adaptive/$POOL_ID\"
processes=\$(ps -ww -axo pid=,command=)
while IFS=' ' read -r pid reservation_id; do
  [ -n \"\$pid\" ] || continue
  record=\"\$state_dir/reservations/\$reservation_id.env\"
  [ -f \"\$record\" ] && [ ! -L \"\$record\" ]
  awk -F= -v pool='$POOL_ID' -v host='$host_id' -v id=\"\$reservation_id\" -v pid=\"\$pid\" '
    BEGIN {
      required=\"schema_version pool_id host_id reservation_id pid state\"
      split(required, key, \" \")
      for (i in key) wanted[key[i]]=1
    }
    \$1 in wanted {if (seen[\$1]++) bad=1; value[\$1]=substr(\$0, length(\$1) + 2)}
    END {
      for (i in key) if (seen[key[i]] != 1) bad=1
      if (value[\"schema_version\"] != 1 || value[\"pool_id\"] != pool ||
          value[\"host_id\"] != host || value[\"reservation_id\"] != id ||
          value[\"pid\"] != pid || value[\"state\"] !~ /^(starting|running|uncertain)\$/) bad=1
      exit bad
    }
  ' \"\$record\"
done < <(printf '%s\\n' \"\$processes\" | awk -v prefix='$POOL_ID.$host_id.' '
  {
    pid=\$1
    for (i=2; i<=NF; i++) {
      if (index(\$i, prefix) != 1) continue
      suffix=substr(\$i, length(prefix) + 1)
      if (suffix ~ /^[0-9]+\$/ && !seen[\$i]++) print pid, \$i
    }
  }
')" >/dev/null || die "adaptive worker reservation audit failed on $target"
}

quiesce_daemon() {
  local host_id="$1" target="$2" lock_state
  lock_state="$(daemon_lock_state "$target")" || die "cannot audit adaptive daemon ownership on $target"
  if [ "$lock_state" = ABSENT ]; then
    audit_unowned_daemon recover "$host_id" "$target" || \
      die "an unowned adaptive daemon process remains on $target"
    return
  fi
  [ "$lock_state" = PRESENT ] || die "invalid adaptive daemon lock state on $target"
  ssh_no_input "$target" "set -e
state_dir=\"\$HOME/.parallel-workers/adaptive/$POOL_ID\"
lock=\"\$state_dir/daemon.lock\"
[ -f \"\$lock\" ] || exit 8
pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$lock\")
script=\$(awk -F= '\$1 == \"script\" {print \$2; exit}' \"\$lock\")
grep -Fqx 'pool_id=$POOL_ID' \"\$lock\"
grep -Fqx 'host_id=$host_id' \"\$lock\"
grep -Fqx \"state_dir=\$state_dir\" \"\$lock\"
[ \"\$script\" = \"\$state_dir/bin/mmhostdaemon.sh\" ]
command=\$(ps -ww -p \"\$pid\" -o command= 2>/dev/null || true)
[ -z \"\$command\" ] && exit 0
[[ \" \$command \" == *\" \$script \"* ]]
[[ \" \$command \" == *\" --state-dir \$state_dir \"* ]]
kill -TERM \"\$pid\"
for attempt in \$(seq 1 '$READY_TIMEOUT'); do
  kill -0 \"\$pid\" 2>/dev/null || exit 0
  sleep 1
done
exit 7" >/dev/null || die "cannot quiesce old adaptive daemon on $target"
}

withhold_daemon_lease() {
  local target="$1"
  ssh_no_input "$target" \
    "rm -f \"\$HOME/.parallel-workers/adaptive/$POOL_ID/lease.env\"" || \
    die "cannot close stale adaptive admission on $target"
}

daemon_running_state() {
  local target="$1"
  ssh_no_input "$target" "set -e
state_dir=\"\$HOME/.parallel-workers/adaptive/$POOL_ID\"
lock=\"\$state_dir/daemon.lock\"
[ -f \"\$state_dir/daemon.env\" ] || exit 9
[ ! -L \"\$lock\" ] || exit 8
if [ -f \"\$lock\" ]; then
  pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$lock\")
  command=\$(ps -ww -p \"\$pid\" -o command= 2>/dev/null || true)
  if [[ \" \$command \" == *\" \$state_dir/bin/mmhostdaemon.sh \"* ]] &&
     [[ \" \$command \" == *\" --state-dir \$state_dir \"* ]]; then
    echo RUNNING
    exit 0
  fi
elif [ -e \"\$lock\" ]; then
  exit 8
fi
echo STOPPED"
}

# After a reboot the dead daemon's lock may name a PID that another process now
# holds, which start_daemon refuses. Keep it as evidence instead.
retire_stale_daemon_lock() {
  local target="$1"
  ssh_no_input "$target" "set -e
state_dir=\"\$HOME/.parallel-workers/adaptive/$POOL_ID\"
lock=\"\$state_dir/daemon.lock\"
[ -f \"\$lock\" ] || exit 0
pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$lock\")
command=\$(ps -ww -p \"\$pid\" -o command= 2>/dev/null || true)
[[ \" \$command \" != *\" \$state_dir/bin/mmhostdaemon.sh \"* ]] || exit 8
mv -f \"\$lock\" \"\$state_dir/daemon.lock.stale.\$(date +%s)\"" || \
    die "cannot retire the stale adaptive daemon lock on $target"
}

recovery_helper() {
  local target="$1" pool="$2" payload="$3" helper_hash
  helper_hash="$(env_value "$POOL_FILE" recovery_sha256)"
  [[ "$helper_hash" =~ ^[0-9a-f]{64}$ ]] || die "invalid recovery helper hash"
  printf '%s\n' "$payload" | "$SSH_BIN" -o BatchMode=yes -o ConnectTimeout="$SSH_CONNECT_TIMEOUT" "$target" "set -e
state=\"\$HOME/.parallel-workers/adaptive/$pool\"
helper=\"\$state/bin/adaptive_recovery.py\"
[ \"\$(shasum -a 256 \"\$helper\" | awk '{print \$1}')\" = '$helper_hash' ]
python3 \"\$helper\" --state-dir \"\$state\""
}

make_stage_archive() {
  TEMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/adaptive-stage.XXXXXX")"
  tar -cf "$TEMP_DIR/stage.tar" -C "$SCRIPT_DIR" \
    "$(basename "$DAEMON_SCRIPT")" "$(basename "$WORKER_SCRIPT")" "$(basename "$CLAIM_SCRIPT")" \
    "$(basename "$MEMORY_SCRIPT")" "$(basename "$RECOVERY_SCRIPT")"
}

restart_daemon() {
  local host_id="" expected="" reason="operator restart-daemon" state row target cpu memory enabled
  local daemon_state payload outcome
  while [ $# -gt 0 ]; do
    [ $# -ge 2 ] || die "$1 requires a value"
    case "$1" in
      --host_id) host_id="$2" ;;
      --expected-pool-id) expected="$2" ;;
      --reason) reason="$2" ;;
      *) die "unknown restart-daemon argument: $1" ;;
    esac
    shift 2
  done
  [ -n "$host_id" ] && [ -n "${reason// /}" ] || \
    die "restart-daemon requires --host_id and a nonempty --reason"
  reject_record_breaks "$host_id" "$expected" "$reason"
  assert_local_state_root
  acquire_lock "$TRANSITION_LOCK"
  [ -f "$POOL_FILE" ] || die "no adaptive pool exists"
  POOL_ID="$(env_value "$POOL_FILE" pool_id)"
  [ -z "$expected" ] || [ "$POOL_ID" = "$expected" ] || die "adaptive pool identity changed"
  state="$(env_value "$POOL_FILE" state)"
  [[ "$state" = ACTIVE || "$state" = DRAINING ]] || \
    die "daemons restart only in an ACTIVE or DRAINING pool, not $state"
  coordinator_is_live "$(env_value "$POOL_FILE" coordinator_pid)" || \
    die "the coordinator is not running; recover the pool with pool-start --recover-inactive"
  RESOURCES_FILE="$(env_value "$POOL_FILE" resources_file)"
  [ "$(hash_file "$RESOURCES_FILE")" = "$(env_value "$POOL_FILE" resources_sha256)" ] || \
    die "resources hash mismatch"
  IO_PRESSURE_HOST="$(env_value "$POOL_FILE" io_pressure_host)"
  LEASE_SECONDS="$(env_value "$POOL_FILE" lease_seconds)"
  row="$(awk -F '\t' -v host="$host_id" 'NR > 1 && $1 == host' "$RESOURCES_FILE")"
  [ -n "$row" ] || die "unknown adaptive host: $host_id"
  IFS=$'\t' read -r _ target cpu memory enabled gpu_units owner_idle owner_active <<< "$row"
  [ "$enabled" = 1 ] || [ "$host_id" = "$IO_PRESSURE_HOST" ] || \
    die "host has no daemon in this pool: $host_id"
  # Stage only the pool's own files: the running coordinator expects them.
  DAEMON_HASH="$(env_value "$POOL_FILE" daemon_sha256)"
  WORKER_HASH="$(env_value "$POOL_FILE" worker_sha256)"
  CLAIM_HASH="$(env_value "$POOL_FILE" claim_sha256)"
  MEMORY_HASH="$(env_value "$POOL_FILE" memory_sha256)"
  RECOVERY_HASH="$(env_value "$POOL_FILE" recovery_sha256)"
  [ "$(hash_file "$DAEMON_SCRIPT")" = "$DAEMON_HASH" ] && \
    [ "$(hash_file "$WORKER_SCRIPT")" = "$WORKER_HASH" ] && \
    [ "$(hash_file "$CLAIM_SCRIPT")" = "$CLAIM_HASH" ] && \
    [ "$(hash_file "$MEMORY_SCRIPT")" = "$MEMORY_HASH" ] && \
    [ "$(hash_file "$RECOVERY_SCRIPT")" = "$RECOVERY_HASH" ] || \
    die "scheduler files differ from the running pool's; this host rejoins at the next pool start"
  daemon_state="$(daemon_running_state "$target")" || die "cannot audit the adaptive daemon on $target"
  if [ "$daemon_state" = RUNNING ]; then
    echo "host_id=$host_id"
    echo "daemon=running"
    return 0
  fi
  [ "$daemon_state" = STOPPED ] || die "invalid adaptive daemon state on $target"
  audit_recovery_workers "$host_id" "$target"
  make_stage_archive
  stage_host "$host_id" "$target" "$cpu" "$memory" "$TEMP_DIR/stage.tar" \
    "${gpu_units:-0}" "${owner_idle:-0}" "${owner_active:-hold}"
  payload="$(python3 -c 'import json,sys; print(json.dumps(dict(zip(
    ("action","pool_id","host_id","reason"),sys.argv[1:]))))' \
    resolve-rebooted "$POOL_ID" "$host_id" "$reason")"
  outcome="$(recovery_helper "$target" "$POOL_ID" "$payload")" || \
    die "the reboot audit failed on $target"
  withhold_daemon_lease "$target"
  retire_stale_daemon_lock "$target"
  start_daemon "$host_id" "$target"
  echo "host_id=$host_id"
  echo "daemon=restarted"
  python3 -c 'import json,sys; result=json.loads(sys.argv[1])
print("rebooted_resolved=" + result["rebooted_resolved"]); print("boot_epoch=" + result["boot_epoch"])' \
    "$outcome"
}

coordinator_is_live() {
  local pid="$1" command
  is_uint "$pid" || return 1
  command="$(ps -ww -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"$COORDINATOR_SCRIPT"* && "$command" == *"--pool-id $POOL_ID"* ]]
}

clear_stale_coordinator_lock() {
  local lock="$STATE_ROOT/coordinator.lock" pid
  [ -d "$lock" ] || return 0
  [ -f "$lock/owner.env" ] || die "coordinator lock has no owner: $lock"
  [ "$(env_value "$lock/owner.env" pool_id)" = "$POOL_ID" ] || die "coordinator lock pool mismatch"
  pid="$(env_value "$lock/owner.env" pid)"
  coordinator_is_live "$pid" && die "coordinator is still active with PID $pid"
  rm -f "$lock/owner.env"
  rmdir "$lock" 2>/dev/null || die "cannot clear audited coordinator lock"
}

start_coordinator() {
  local pid attempt ready_pid ready_state
  rm -f "$STATE_ROOT/coordinator.ready" "$STATE_ROOT/drained.env"
  nohup python3 "$COORDINATOR_SCRIPT" serve --state-root "$STATE_ROOT" \
    --pool-id "$POOL_ID" --resources-file "$RESOURCES_FILE" \
    --shared-io-capacity "$SHARED_IO_CAPACITY" --tick-seconds "$TICK_SECONDS" \
    --lease-seconds "$LEASE_SECONDS" --io-pressure-host "$IO_PRESSURE_HOST" \
    --smbd-cpu-limit-pct "$SMBD_CPU_LIMIT_PCT" \
    --cpu-tolerance-pct "$CPU_TOLERANCE_PCT" \
    --start-interval-seconds "$START_INTERVAL_SECONDS" --backfill "$BACKFILL" \
    < /dev/null >> "$STATE_ROOT/coordinator.log" 2>&1 &
  pid=$!
  for attempt in $(seq 1 "$READY_TIMEOUT"); do
    if [ -f "$STATE_ROOT/coordinator.ready" ]; then
      ready_pid="$(env_value "$STATE_ROOT/coordinator.ready" pid)"
      ready_state="$(env_value "$STATE_ROOT/coordinator.ready" state)"
      if [ "$(env_value "$STATE_ROOT/coordinator.ready" pool_id)" = "$POOL_ID" ] && \
          [ "$ready_pid" = "$pid" ] && [ "$ready_state" = waiting ]; then
        printf '%s\n' "$pid"
        return 0
      fi
    fi
    kill -0 "$pid" 2>/dev/null || break
    sleep 1
  done
  wait "$pid" 2>/dev/null || true
  die "adaptive coordinator failed readiness; inspect $STATE_ROOT/coordinator.log"
}

wait_coordinator_ready() {
  local pid="$1" attempt ready_pid ready_state
  for attempt in $(seq 1 "$READY_TIMEOUT"); do
    if [ -f "$STATE_ROOT/coordinator.ready" ]; then
      ready_pid="$(env_value "$STATE_ROOT/coordinator.ready" pid)"
      ready_state="$(env_value "$STATE_ROOT/coordinator.ready" state)"
      if [ "$(env_value "$STATE_ROOT/coordinator.ready" pool_id)" = "$POOL_ID" ] && \
          [ "$ready_pid" = "$pid" ] && [ "$ready_state" = ready ]; then
        return 0
      fi
    fi
    coordinator_is_live "$pid" || break
    sleep 1
  done
  die "adaptive coordinator did not become active"
}

pool_start() {
  local recover=0 expected_pool_id="" expected_pool_id_seen=0
  local final_state=ACTIVE coordinator_pid stored_pid
  local host_id target cpu memory enabled plan_state resource_source resource_tmp
  RESOURCES_FILE="${PARALLEL_WORKERS_RESOURCES_FILE:-}"
  POOL_NAME="adaptive-$(date +%Y%m%d-%H%M%S)"
  SHARED_IO_CAPACITY="${ADAPTIVE_SHARED_IO_CAPACITY:-50}"
  IO_PRESSURE_HOST="${ADAPTIVE_IO_PRESSURE_HOST:-}"
  SMBD_CPU_LIMIT_PCT="${ADAPTIVE_SMBD_CPU_LIMIT_PCT:-400}"
  CPU_TOLERANCE_PCT="${ADAPTIVE_CPU_TOLERANCE_PCT:-10}"
  TICK_SECONDS="${ADAPTIVE_TICK_SECONDS:-5}"
  START_INTERVAL_SECONDS="${ADAPTIVE_START_INTERVAL_SECONDS:-4}"
  LEASE_SECONDS="${ADAPTIVE_LEASE_SECONDS:-20}"
  BACKFILL="${ADAPTIVE_BACKFILL:-0}"
  while [ $# -gt 0 ]; do
    case "$1" in
      --resources_file) RESOURCES_FILE="$2"; shift 2 ;;
      --pool_name) POOL_NAME="$2"; shift 2 ;;
      --shared_io_capacity) SHARED_IO_CAPACITY="$2"; shift 2 ;;
      --io_pressure_host) IO_PRESSURE_HOST="$2"; shift 2 ;;
      --smbd_cpu_limit_pct) SMBD_CPU_LIMIT_PCT="$2"; shift 2 ;;
      --cpu_tolerance_pct) CPU_TOLERANCE_PCT="$2"; shift 2 ;;
      --tick_seconds) TICK_SECONDS="$2"; shift 2 ;;
      --start_interval_seconds)
        [ "$#" -ge 2 ] || die "--start_interval_seconds requires a value"
        START_INTERVAL_SECONDS="$2"; shift 2 ;;
      --lease_seconds) LEASE_SECONDS="$2"; shift 2 ;;
      --backfill)
        [ "$#" -ge 2 ] || die "--backfill requires 0 or 1"
        BACKFILL="$2"; shift 2 ;;
      --recover-inactive) recover=1; shift ;;
      --expected-pool-id)
        [ $# -ge 2 ] || die "--expected-pool-id requires a value"
        [ "$expected_pool_id_seen" -eq 0 ] || die "--expected-pool-id was provided more than once"
        expected_pool_id="$2"
        expected_pool_id_seen=1
        shift 2
        ;;
      *) die "unknown pool-start argument: $1" ;;
    esac
  done
  if [ "$expected_pool_id_seen" -eq 1 ]; then
    [ "$recover" -eq 1 ] || die "--expected-pool-id requires --recover-inactive"
    [[ "$expected_pool_id" =~ ^[A-Za-z0-9._-]+$ ]] && \
      [[ "$expected_pool_id" != . && "$expected_pool_id" != .. ]] || \
      die "invalid expected pool ID"
  fi
  assert_local_state_root
  acquire_lock "$TRANSITION_LOCK" "$recover"
  if [ "$recover" -eq 1 ]; then
    [ -f "$POOL_FILE" ] || die "no adaptive pool exists to recover"
    POOL_ID="$(env_value "$POOL_FILE" pool_id)"
    [ "$expected_pool_id_seen" -eq 0 ] || [ "$POOL_ID" = "$expected_pool_id" ] || \
      die "adaptive pool identity changed: expected $expected_pool_id, found $POOL_ID"
    POOL_NAME="$(env_value "$POOL_FILE" pool_name)"
    final_state="$(env_value "$POOL_FILE" state)"
    [[ "$final_state" = ACTIVE || "$final_state" = DRAINING || "$final_state" = STARTING ]] || \
      die "pool state cannot be recovered: $final_state"
    if [ "$final_state" = STARTING ]; then
      final_state="$(env_value "$POOL_FILE" target_state)"
      [[ "$final_state" = ACTIVE || "$final_state" = DRAINING ]] || \
        die "invalid recovery target state: $final_state"
    fi
    RESOURCES_FILE="$(env_value "$POOL_FILE" resources_file)"
    SHARED_IO_CAPACITY="$(env_value "$POOL_FILE" shared_io_capacity)"
    IO_PRESSURE_HOST="$(env_value "$POOL_FILE" io_pressure_host)"
    SMBD_CPU_LIMIT_PCT="$(env_value "$POOL_FILE" smbd_cpu_limit_pct)"
    CPU_TOLERANCE_PCT="$(env_value "$POOL_FILE" cpu_tolerance_pct)"
    TICK_SECONDS="$(env_value "$POOL_FILE" tick_seconds)"
    START_INTERVAL_SECONDS="$(env_value "$POOL_FILE" start_interval_seconds)"
    LEASE_SECONDS="$(env_value "$POOL_FILE" lease_seconds)"
    # Pools started before backfill existed have no key and recover with it off.
    BACKFILL="$(awk -F= '$1 == "backfill" {n++; value=$2}
      END {if (n > 1) exit 1; print (n ? value : 0)}' "$POOL_FILE")" || die "invalid pool backfill setting"
    CREATED_EPOCH="$(env_value "$POOL_FILE" created_epoch)"
    MEMORY_CONTROL_VERSION="$(awk -F= '$1 == "memory_control_version" {n++; value=$2}
      END {if (n > 1) exit 1; print value}' "$POOL_FILE")" || die "invalid pool memory-control capability"
    [[ "$MEMORY_CONTROL_VERSION" = "" || "$MEMORY_CONTROL_VERSION" = 1 ]] || \
      die "unsupported pool memory-control capability"
    MEMORY_CALIBRATION_VERSION="$(awk -F= '$1 == "memory_calibration_version" {n++; value=$2}
      END {if (n > 1) exit 1; print value}' "$POOL_FILE")" || die "invalid pool memory-calibration capability"
    [[ "$MEMORY_CALIBRATION_VERSION" = "" || "$MEMORY_CALIBRATION_VERSION" = 1 ]] || \
      die "unsupported pool memory-calibration capability"
    stored_pid="$(env_value "$POOL_FILE" coordinator_pid)"
    coordinator_is_live "$stored_pid" && die "coordinator is still active with PID $stored_pid"
  else
    [ ! -e "$POOL_FILE" ] && [ ! -L "$POOL_FILE" ] || \
      die "an adaptive pool marker already exists or is ambiguous: $POOL_FILE"
    [ -n "$RESOURCES_FILE" ] || die "pool-start requires --resources_file"
    RESOURCES_FILE="$(absolute_path "$RESOURCES_FILE")"
    CREATED_EPOCH="$(date +%s)"
    POOL_ID="${POOL_NAME}.${CREATED_EPOCH}.$$.$RANDOM"
  fi
  [[ "$POOL_NAME" =~ ^[A-Za-z0-9._-]+$ && "$POOL_ID" =~ ^[A-Za-z0-9._-]+$ ]] && \
    [[ "$POOL_NAME" != . && "$POOL_NAME" != .. && "$POOL_ID" != . && "$POOL_ID" != .. ]] || \
    die "pool name and ID must be identifiers"

  [ -r "$RESOURCES_FILE" ] || die "resources file is not readable: $RESOURCES_FILE"
  if [ "$recover" -eq 0 ]; then
    resource_source="$RESOURCES_FILE"
    resource_tmp="$STATE_ROOT/resources.tsv.tmp.$$"
    mkdir -p "$STATE_ROOT"
    if ! cp "$resource_source" "$resource_tmp" || ! chmod 444 "$resource_tmp" || \
        ! mv -f "$resource_tmp" "$STATE_ROOT/resources.tsv"; then
      rm -f "$resource_tmp"
      die "cannot snapshot adaptive resources"
    fi
    RESOURCES_FILE="$STATE_ROOT/resources.tsv"
  fi
  validate_resources "$RESOURCES_FILE"
  is_uint "$SHARED_IO_CAPACITY" || die "shared I/O capacity must be non-negative"
  [[ "$BACKFILL" = 0 || "$BACKFILL" = 1 ]] || die "backfill must be 0 or 1"
  is_uint "$SMBD_CPU_LIMIT_PCT" && [ "$SMBD_CPU_LIMIT_PCT" -gt 0 ] || \
    die "SMB CPU limit must be a positive integer percentage"
  is_uint "$CPU_TOLERANCE_PCT" && [ "$CPU_TOLERANCE_PCT" -le 100 ] || \
    die "CPU tolerance must be 0..100 percent of one core"
  if [ "$SHARED_IO_CAPACITY" -gt 0 ]; then
    [ -n "$IO_PRESSURE_HOST" ] || \
      die "a positive shared I/O capacity requires --io_pressure_host"
    [[ "$IO_PRESSURE_HOST" =~ ^[A-Za-z0-9._-]+$ ]] && \
      [[ "$IO_PRESSURE_HOST" != . && "$IO_PRESSURE_HOST" != .. ]] || \
      die "invalid I/O pressure host ID"
    [ "$(awk -F '\t' -v host="$IO_PRESSURE_HOST" 'NR > 1 && $1 == host {n++} END {print n+0}' "$RESOURCES_FILE")" -eq 1 ] || \
      die "I/O pressure host is not a unique resource-table host: $IO_PRESSURE_HOST"
  else
    [ -z "$IO_PRESSURE_HOST" ] || \
      die "--io_pressure_host is only valid with positive shared I/O capacity"
  fi
  awk -v tick="$TICK_SECONDS" -v lease="$LEASE_SECONDS" \
    'BEGIN {exit !(tick ~ /^[0-9]+([.][0-9]+)?$/ && tick > 0 && lease ~ /^[1-9][0-9]*$/ && lease >= 2*tick)}' || \
    die "lease seconds must be an integer at least twice the positive tick interval"
  awk -v interval="$START_INTERVAL_SECONDS" \
    'BEGIN {exit !(interval ~ /^[0-9]+([.][0-9]+)?$/ && interval >= 0)}' || \
    die "start interval must be non-negative"
  for required in "$COORDINATOR_SCRIPT" "$DAEMON_SCRIPT" "$WORKER_SCRIPT" "$CLAIM_SCRIPT" "$MEMORY_SCRIPT" "$CALIBRATION_SCRIPT" "$RECOVERY_SCRIPT" "$WORKFLOW_SCRIPT"; do
    [ -r "$required" ] || die "required scheduler file is not readable: $required"
  done
  python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 9))' 2>/dev/null || \
    die "adaptive coordination requires Python 3.9 or newer on the coordinator"
  RESOURCES_HASH="$(hash_file "$RESOURCES_FILE")"
  DAEMON_HASH="$(hash_file "$DAEMON_SCRIPT")"
  WORKER_HASH="$(hash_file "$WORKER_SCRIPT")"
  CLAIM_HASH="$(hash_file "$CLAIM_SCRIPT")"
  MEMORY_HASH="$(hash_file "$MEMORY_SCRIPT")"
  CALIBRATION_HASH="$(hash_file "$CALIBRATION_SCRIPT")"
  RECOVERY_HASH="$(hash_file "$RECOVERY_SCRIPT")"
  COORDINATOR_HASH="$(hash_file "$COORDINATOR_SCRIPT")"
  WORKFLOW_HASH="$(hash_file "$WORKFLOW_SCRIPT")"
  if [ "$recover" -eq 1 ]; then
    [ "$(env_value "$POOL_FILE" resources_sha256)" = "$RESOURCES_HASH" ] && \
      [ "$(env_value "$POOL_FILE" daemon_sha256)" = "$DAEMON_HASH" ] && \
      [ "$(env_value "$POOL_FILE" worker_sha256)" = "$WORKER_HASH" ] && \
      [ "$(env_value "$POOL_FILE" claim_sha256)" = "$CLAIM_HASH" ] && \
      [ "$(env_value "$POOL_FILE" memory_sha256)" = "$MEMORY_HASH" ] && \
      [ "$(env_value "$POOL_FILE" calibration_sha256)" = "$CALIBRATION_HASH" ] && \
      [ "$(env_value "$POOL_FILE" recovery_sha256)" = "$RECOVERY_HASH" ] && \
      [ "$(env_value "$POOL_FILE" workflow_sha256)" = "$WORKFLOW_HASH" ] && \
      [ "$(env_value "$POOL_FILE" coordinator_sha256)" = "$COORDINATOR_HASH" ] || \
      die "recovery inputs differ from the recorded pool"
    clear_stale_coordinator_lock
  else
    [ ! -d "$STATE_ROOT/coordinator.lock" ] || die "unexpected coordinator lock in fresh state"
  fi

  mkdir -p "$STATE_ROOT/queues" "$STATE_ROOT/plans" "$STATE_ROOT/last-known"
  make_stage_archive
  [ -r "$CLUSTER_HOSTS_FILE" ] || \
    die "cluster hosts file is not readable: $CLUSTER_HOSTS_FILE"
  while read -r target _; do
    [ -n "$target" ] && [[ "$target" != \#* ]] || continue
    audit_host_adaptive_clear "$target" "$recover" || \
      die "pool start found conflicting adaptive scheduler processes"
  done < "$CLUSTER_HOSTS_FILE"
  while IFS=$'\t' read -r host_id target cpu memory enabled gpu_units owner_idle owner_active; do
    [ -n "$host_id" ] || continue
    awk -v target="$target" '$1 == target {found=1} END {exit !found}' \
      "$CLUSTER_HOSTS_FILE" && continue
    audit_host_adaptive_clear "$target" "$recover" || \
      die "pool start found conflicting adaptive scheduler processes"
  done < <(sed -n '2,$p' "$RESOURCES_FILE")
  while IFS=$'\t' read -r host_id target cpu memory enabled gpu_units owner_idle owner_active; do
    [ "$enabled" = 1 ] || [ "$host_id" = "$IO_PRESSURE_HOST" ] || continue
    plan_state="$STATE_ROOT/plans/$host_id/state.json"
    if [ "$recover" -eq 1 ]; then
      [ -f "$plan_state" ] || die "recorded host plan state is missing: $plan_state"
    else
      mkdir -p "$(dirname "$plan_state")"
      [ ! -e "$plan_state" ] || die "unexpected host plan state: $plan_state"
      printf '%s\n' '{"generation":0,"targets":{},"envelope_targets":{},"controls":{},"pending":false}' \
        > "$plan_state.tmp.$$"
      chmod 600 "$plan_state.tmp.$$"
      mv -f "$plan_state.tmp.$$" "$plan_state"
    fi
  done < <(sed -n '2,$p' "$RESOURCES_FILE")
  write_pool STARTING "" "$final_state"
  while IFS=$'\t' read -r host_id target cpu memory enabled gpu_units owner_idle owner_active; do
    [ -n "$host_id" ] || continue
    [ "$enabled" = 1 ] || [ "$host_id" = "$IO_PRESSURE_HOST" ] || continue
    if [ "$recover" -eq 1 ]; then
      quiesce_daemon "$host_id" "$target"
      audit_recovery_workers "$host_id" "$target"
      withhold_daemon_lease "$target"
    fi
    stage_host "$host_id" "$target" "$cpu" "$memory" "$TEMP_DIR/stage.tar" \
    "${gpu_units:-0}" "${owner_idle:-0}" "${owner_active:-hold}"
    start_daemon "$host_id" "$target"
  done < <(sed -n '2,$p' "$RESOURCES_FILE")

  coordinator_pid="$(start_coordinator)"
  write_pool "$final_state" "$coordinator_pid" "$final_state"
  wait_coordinator_ready "$coordinator_pid"
  if [ "$final_state" = ACTIVE ]; then
    assert_memory_controls_supported 1
    assert_memory_controls_supported 1 memory_calibration_version
    MEMORY_CONTROL_VERSION=1
    MEMORY_CALIBRATION_VERSION=1
    write_pool "$final_state" "$coordinator_pid" "$final_state"
  fi
  [ "$final_state" != DRAINING ] || start_finalizer "$POOL_ID"
  echo "pool_id=$POOL_ID"
  echo "state=$final_state"
}

set_host_budget() {
  local host_id="" cpu="" memory="" expected=""
  while [ $# -gt 0 ]; do
    [ $# -ge 2 ] || die "$1 requires a value"
    case "$1" in
      --host_id) host_id="$2" ;;
      --cpu_units) cpu="$2" ;;
      --memory_mib) memory="$2" ;;
      --expected-pool-id) expected="$2" ;;
      *) die "unknown set-host-budget argument: $1" ;;
    esac
    shift 2
  done
  [ -n "$host_id" ] && is_uint "$cpu" && [ "$cpu" -gt 0 ] && \
    is_uint "$memory" && [ "$memory" -gt 0 ] || \
    die "set-host-budget requires --host_id and positive --cpu_units and --memory_mib"
  assert_local_state_root
  acquire_lock "$TRANSITION_LOCK"
  POOL_ID="$(env_value "$POOL_FILE" pool_id)"
  [ -z "$expected" ] || [ "$POOL_ID" = "$expected" ] || die "adaptive pool identity changed"
  python3 "$COORDINATOR_SCRIPT" set-host-budget --state-root "$STATE_ROOT" \
    --pool-id "$POOL_ID" --host-id "$host_id" --cpu-units "$cpu" --memory-mib "$memory"
}

set_host_state() {
  local host_id="" state="" at="" until="" reason="" expected="" arguments
  while [ $# -gt 0 ]; do
    [ $# -ge 2 ] || die "$1 requires a value"
    case "$1" in
      --host_id) host_id="$2" ;;
      --state) state="$2" ;;
      --at) at="$2" ;;
      --until) until="$2" ;;
      --reason) reason="$2" ;;
      --expected-pool-id) expected="$2" ;;
      *) die "unknown set-host-state argument: $1" ;;
    esac
    shift 2
  done
  [ -n "$host_id" ] && { [ "$state" = away ] || [ "$state" = present ]; } || \
    die "set-host-state requires --host_id and --state away|present"
  [ "$state" != away ] || [ -n "$reason" ] || die "set-host-state --state away requires --reason"
  assert_local_state_root
  acquire_lock "$TRANSITION_LOCK"
  POOL_ID="$(env_value "$POOL_FILE" pool_id)"
  [ -z "$expected" ] || [ "$POOL_ID" = "$expected" ] || die "adaptive pool identity changed"
  arguments=(--state-root "$STATE_ROOT" --pool-id "$POOL_ID" --host-id "$host_id" --state "$state")
  [ -z "$at" ] || arguments+=(--at "$at")
  [ -z "$until" ] || arguments+=(--until "$until")
  [ -z "$reason" ] || arguments+=(--reason "$reason")
  python3 "$COORDINATOR_SCRIPT" set-host-state "${arguments[@]}"
}

pool_status() {
  local pid live=0 ready=0 ready_pool ready_pid ready_state heartbeat lease now age=-1
  local capacity_record resource_state live_cpu live_memory io_pressure smbd_tenths
  if [ ! -f "$POOL_FILE" ]; then
    echo "state=INACTIVE"
    return
  fi
  cat "$POOL_FILE"
  if [ -f "$STATE_ROOT/host-budgets.json" ]; then
    printf 'host_budgets='
    cat "$STATE_ROOT/host-budgets.json"
  fi
  if [ -f "$STATE_ROOT/host-states.json" ]; then
    printf 'host_states='
    cat "$STATE_ROOT/host-states.json"
  fi
  if [ -f "$STATE_ROOT/quarantine.json" ]; then
    printf 'quarantine='
    cat "$STATE_ROOT/quarantine.json"
  fi
  POOL_ID="$(env_value "$POOL_FILE" pool_id)"
  pid="$(env_value "$POOL_FILE" coordinator_pid)"
  lease="$(env_value "$POOL_FILE" lease_seconds)"
  coordinator_is_live "$pid" && live=1
  echo "coordinator_live=$live"
  if [ -f "$STATE_ROOT/coordinator.ready" ]; then
    ready_pool="$(env_value "$STATE_ROOT/coordinator.ready" pool_id)"
    ready_pid="$(env_value "$STATE_ROOT/coordinator.ready" pid)"
    ready_state="$(env_value "$STATE_ROOT/coordinator.ready" state)"
    heartbeat="$(env_value "$STATE_ROOT/coordinator.ready" updated_epoch)"
    if [ "$ready_pool" = "$POOL_ID" ] && [ "$ready_pid" = "$pid" ] && is_uint "$heartbeat"; then
      now="$(date +%s)"
      age=$((now - heartbeat))
      if is_uint "$lease" && [ "$live" -eq 1 ] && [ "$ready_state" = ready ] && \
          [ "$age" -ge 0 ] && [ "$age" -le "$lease" ]; then ready=1; fi
    fi
  fi
  echo "coordinator_ready=$ready"
  echo "heartbeat_age_sec=$age"
  if capacity_record="$(state_record "$STATE_ROOT/coordinator.ready" \
      'resource_state live_cpu_units live_memory_mib io_pressure_state smbd_cpu_tenths' 2>/dev/null)"; then
    IFS=$'\t' read -r resource_state live_cpu live_memory io_pressure smbd_tenths <<< "$capacity_record"
    echo "resource_state=$resource_state"
    echo "live_cpu_units=$live_cpu"
    echo "live_memory_mib=$live_memory"
    echo "io_pressure_state=$io_pressure"
    echo "smbd_cpu_tenths=$smbd_tenths"
  fi
  if [ -f "$STATE_ROOT/drained.env" ] && \
      [ "$(env_value "$STATE_ROOT/drained.env" pool_id)" = "$POOL_ID" ]; then
    echo "drain_complete=1"
  else
    echo "drain_complete=0"
  fi
}

daemon_idle() {
  local action="$1" host_id="$2" target="$3" lock_state
  lock_state="$(daemon_lock_state "$target")" || return 1
  if [ "$lock_state" = ABSENT ]; then
    audit_unowned_daemon finalize "$host_id" "$target"
    return
  fi
  [ "$lock_state" = PRESENT ] || return 1
  ssh_no_input "$target" "set -e
state_dir=\"\$HOME/.parallel-workers/adaptive/$POOL_ID\"
lock=\"\$state_dir/daemon.lock\"
[ -f \"\$lock\" ] && [ -f \"\$state_dir/status.tsv\" ]
pid=\$(awk -F= '\$1 == \"pid\" {print \$2; exit}' \"\$lock\")
script=\$(awk -F= '\$1 == \"script\" {print \$2; exit}' \"\$lock\")
grep -Fqx 'pool_id=$POOL_ID' \"\$lock\"
grep -Fqx 'host_id=$host_id' \"\$lock\"
grep -Fqx \"state_dir=\$state_dir\" \"\$lock\"
[ \"\$script\" = \"\$state_dir/bin/mmhostdaemon.sh\" ]
command=\$(ps -ww -p \"\$pid\" -o command= 2>/dev/null)
[[ \" \$command \" == *\" \$script \"* ]]
[[ \" \$command \" == *\" --state-dir \$state_dir \"* ]]
awk -F '\t' -v pool='$POOL_ID' -v host='$host_id' '
  \$1 == \"daemon\" && \$2 == pool && \$3 == host && \$5 ~ /^(enabled|drain-only)\$/ {daemon++}
  \$1 == \"reservation\" {reservations++}
  END {exit !(daemon == 1 && reservations == 0)}
' \"\$state_dir/status.tsv\"
[ '$action' = verify ] && exit 0
kill -TERM \"\$pid\"
for attempt in \$(seq 1 '$READY_TIMEOUT'); do
  if [ ! -e \"\$lock\" ]; then
    command=\$(ps -ww -p \"\$pid\" -o command= 2>/dev/null || true)
    [[ \" \$command \" == *\" \$script \"* ]] || exit 0
  fi
  sleep 1
done
exit 7" >/dev/null
}

finalize_drain() {
  local expected_id="" host_id target cpu memory enabled pid attempt archive expected_hash announced=0
  while [ $# -gt 0 ]; do
    case "$1" in --pool-id) expected_id="$2"; shift 2 ;; *) die "invalid drain finalizer argument" ;; esac
  done
  [ -n "$expected_id" ] || die "drain finalizer requires --pool-id"
  assert_local_state_root
  POOL_ID="$expected_id"
  [ -f "$POOL_FILE" ] && [ "$(env_value "$POOL_FILE" pool_id)" = "$POOL_ID" ] && \
    [ "$(env_value "$POOL_FILE" state)" = DRAINING ] || die "pool changed before drain finalization"
  pid="$(env_value "$POOL_FILE" coordinator_pid)"
  # Running jobs can take hours; only the final stop/archive needs the lock.
  while :; do
    [ -f "$POOL_FILE" ] || die "pool marker disappeared before drain completed"
    [ "$(env_value "$POOL_FILE" pool_id)" = "$POOL_ID" ] && \
      [ "$(env_value "$POOL_FILE" state)" = DRAINING ] && \
      [ "$(env_value "$POOL_FILE" coordinator_pid)" = "$pid" ] || die "draining pool identity changed"
    if [ -f "$STATE_ROOT/drained.env" ] && \
        [ "$(env_value "$STATE_ROOT/drained.env" pool_id)" = "$expected_id" ]; then
      break
    fi
    if ! coordinator_is_live "$pid"; then
      # The coordinator publishes its receipt before exiting. It may do both
      # between our first receipt check and the process check.
      [ -f "$STATE_ROOT/drained.env" ] && \
        [ "$(env_value "$STATE_ROOT/drained.env" pool_id)" = "$expected_id" ] && break
      die "coordinator stopped before drain completed; recover the pool"
    fi
    sleep 2
  done
  for attempt in $(seq 1 "$READY_TIMEOUT"); do
    coordinator_is_live "$pid" || break
    sleep 1
  done
  coordinator_is_live "$pid" && die "drained coordinator did not exit"
  # The coordinator has exited, so giving up would strand idle daemons; waiting
  # blocks nothing because the lock is not held meanwhile.
  until mkdir "$TRANSITION_LOCK" 2>/dev/null; do
    if [ "$announced" -eq 0 ]; then
      echo "$(date): waiting for transition lock $TRANSITION_LOCK to finalize drained pool $POOL_ID" >&2
      sed -n '1,5p' "$TRANSITION_LOCK/owner.env" >&2 2>/dev/null || true
      announced=1
    fi
    sleep 1
  done
  {
    echo "pid=$$"
    echo "host=$(hostname)"
    echo "command=finalize-drain"
    echo "created_epoch=$(date +%s)"
  } > "$TRANSITION_LOCK/owner.env"
  HELD_LOCKS+=("$TRANSITION_LOCK")
  [ "$(env_value "$POOL_FILE" pool_id)" = "$POOL_ID" ] && \
    [ "$(env_value "$POOL_FILE" state)" = DRAINING ] && \
    [ "$(env_value "$POOL_FILE" coordinator_pid)" = "$pid" ] || die "pool changed before drain finalization"
  [ "$(env_value "$STATE_ROOT/drained.env" pool_id)" = "$POOL_ID" ] || \
    die "drain marker changed before drain finalization"
  coordinator_is_live "$pid" && die "coordinator restarted before drain finalization"
  RESOURCES_FILE="$(env_value "$POOL_FILE" resources_file)"
  IO_PRESSURE_HOST="$(env_value "$POOL_FILE" io_pressure_host)"
  [ -r "$RESOURCES_FILE" ] || die "adaptive resources file is not readable: $RESOURCES_FILE"
  expected_hash="$(env_value "$POOL_FILE" resources_sha256)"
  [ "$(hash_file "$RESOURCES_FILE")" = "$expected_hash" ] || \
    die "adaptive resources file differs from the pool record"
  while IFS=$'\t' read -r host_id target cpu memory enabled gpu_units owner_idle owner_active; do
    [ "$enabled" = 1 ] || [ "$host_id" = "$IO_PRESSURE_HOST" ] || continue
    daemon_idle verify "$host_id" "$target" || die "cannot verify idle adaptive daemon on $target"
  done < <(sed -n '2,$p' "$RESOURCES_FILE")
  while IFS=$'\t' read -r host_id target cpu memory enabled gpu_units owner_idle owner_active; do
    [ "$enabled" = 1 ] || [ "$host_id" = "$IO_PRESSURE_HOST" ] || continue
    daemon_idle stop "$host_id" "$target" || die "cannot stop drained adaptive daemon on $target"
  done < <(sed -n '2,$p' "$RESOURCES_FILE")
  archive="${STATE_ROOT}.drained.${POOL_ID}.$(date +%s)"
  [ ! -e "$archive" ] || die "drain archive already exists: $archive"
  mv "$STATE_ROOT" "$archive"
  mkdir -p "$STATE_ROOT"
}

start_finalizer() {
  local pool_id="$1" watcher_pid watcher_command tmp
  watcher_pid="$(awk -F= '$1 == "pid" {print $2; exit}' "$STATE_ROOT/finalizer.env" 2>/dev/null || true)"
  watcher_command=""
  is_uint "$watcher_pid" && watcher_command="$(ps -ww -p "$watcher_pid" -o command= 2>/dev/null || true)"
  [[ "$watcher_command" == *"$SCRIPT_DIR/adaptive_scheduler.sh finalize-drain --pool-id $pool_id"* ]] && return
  nohup env ADAPTIVE_STATE_ROOT="$STATE_ROOT" ADAPTIVE_POOL_MARKER="$POOL_FILE" \
    MODE_TRANSITION_LOCK="$TRANSITION_LOCK" ADAPTIVE_SSH_BIN="$SSH_BIN" \
    ADAPTIVE_COORDINATOR_SCRIPT="$COORDINATOR_SCRIPT" \
    ADAPTIVE_SSH_CONNECT_TIMEOUT="$SSH_CONNECT_TIMEOUT" ADAPTIVE_READY_TIMEOUT="$READY_TIMEOUT" \
    "$SCRIPT_DIR/adaptive_scheduler.sh" finalize-drain --pool-id "$pool_id" \
    < /dev/null >> "$STATE_ROOT/finalizer.log" 2>&1 &
  watcher_pid=$!
  tmp="$STATE_ROOT/finalizer.env.tmp.$$"
  { echo "pool_id=$pool_id"; echo "pid=$watcher_pid"; } > "$tmp"
  mv -f "$tmp" "$STATE_ROOT/finalizer.env"
}

pool_drain() {
  local recover=0 expected_pool_id="" expected_pool_id_seen=0 tmp state pool_id pid
  while [ $# -gt 0 ]; do
    case "$1" in
      --recover-inactive) recover=1; shift ;;
      --expected-pool-id)
        [ $# -ge 2 ] || die "--expected-pool-id requires a value"
        [ "$expected_pool_id_seen" -eq 0 ] || die "--expected-pool-id was provided more than once"
        expected_pool_id="$2"
        expected_pool_id_seen=1
        shift 2
        ;;
      *) die "unknown pool-drain argument: $1" ;;
    esac
  done
  if [ "$expected_pool_id_seen" -eq 1 ]; then
    [[ "$expected_pool_id" =~ ^[A-Za-z0-9._-]+$ ]] && \
      [[ "$expected_pool_id" != . && "$expected_pool_id" != .. ]] || \
      die "invalid expected pool ID"
  fi
  assert_local_state_root
  acquire_lock "$TRANSITION_LOCK" "$recover"
  [ -f "$POOL_FILE" ] || die "no adaptive pool is active"
  POOL_ID="$(env_value "$POOL_FILE" pool_id)"
  [ "$expected_pool_id_seen" -eq 0 ] || [ "$POOL_ID" = "$expected_pool_id" ] || \
    die "adaptive pool identity changed: expected $expected_pool_id, found $POOL_ID"
  state="$(env_value "$POOL_FILE" state)"
  [[ "$state" = ACTIVE || "$state" = DRAINING || "$state" = STARTING ]] || \
    die "pool state is $state"
  pid="$(env_value "$POOL_FILE" coordinator_pid)"
  tmp="${POOL_FILE}.tmp.$$"
  awk -F= -v now="$(date +%s)" '
    $1 == "state" {$0="state=DRAINING"}
    $1 == "target_state" {$0="target_state=DRAINING"}
    $1 == "updated_epoch" {$0="updated_epoch=" now}
    {print}
  ' "$POOL_FILE" > "$tmp"
  chmod 644 "$tmp"
  mv -f "$tmp" "$POOL_FILE"
  pool_id="$POOL_ID"
  if ! coordinator_is_live "$pid"; then
    if [ "$recover" -eq 0 ]; then
      die "adaptive coordinator is inactive; pool is latched DRAINING; rerun: $SCRIPT_DIR/adaptive_scheduler.sh pool-drain --recover-inactive"
    fi
    release_lock "$TRANSITION_LOCK"
    pool_start --recover-inactive --expected-pool-id "$pool_id"
    return
  fi
  start_finalizer "$pool_id"
  echo "state=DRAINING"
}

submit_queue() {
  local model_dir="" queue="" run_name="" priority=50 cpu=1 memory="" io="" maximum=0 admission=enabled gpu=0 memory_limit=""
  local recover=0 allow_network=0 project="" project_option_set=0
  local calibration_jobs=0 calibration_seconds=0 calibration_min=""
  local workflow_args=()
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --project|--memory_calibration_jobs|--memory_calibration_seconds|--memory_min_mib_per_job|--after|--gpu_units_per_job|--memory_limit_mib)
        [ $# -ge 2 ] || die "$1 requires a value" ;;
    esac
    case "$1" in
      --after) workflow_args+=(--after "$2"); shift 2 ;;
      --retry_lost_jobs|--early_eta_sample) workflow_args+=("$1"); shift ;;
      --model_dir) model_dir="$2"; shift 2 ;;
      --queue) queue="$2"; shift 2 ;;
      --run_name) run_name="$2"; shift 2 ;;
      --project) project="$2"; project_option_set=1; shift 2 ;;
      --schedule_priority) priority="$2"; shift 2 ;;
      --cpu_units_per_job) cpu="$2"; shift 2 ;;
      --memory_mib_per_job) memory="$2"; shift 2 ;;
      --memory_calibration_jobs) calibration_jobs="$2"; shift 2 ;;
      --memory_calibration_seconds) calibration_seconds="$2"; shift 2 ;;
      --memory_min_mib_per_job) calibration_min="$2"; shift 2 ;;
      --shared_io_units_per_job) io="$2"; shift 2 ;;
      --gpu_units_per_job) gpu="$2"; shift 2 ;;
      --memory_limit_mib) memory_limit="$2"; shift 2 ;;
      --max_workers) maximum="$2"; shift 2 ;;
      --paused) admission=paused; shift ;;
      --recover_inactive) recover=1; shift ;;
      --allow_network_coordinator) allow_network=1; shift ;;
      *) die "unknown submit argument: $1" ;;
    esac
  done
  [ -n "$model_dir" ] && [ -n "$queue" ] && [ -n "$memory" ] || \
    die "submit requires --model_dir, --queue, and --memory_mib_per_job"
  # start_workers.sh validates a per-host SPEC against the pool host table.
  is_int "$priority" || [[ "$priority" == *[,=]* ]] || \
    die "schedule priority must be an integer or a per-host SPEC"
  is_uint "$cpu" && [ "$cpu" -gt 0 ] || die "CPU units must be a positive integer"
  is_uint "$memory" && [ "$memory" -gt 0 ] || die "memory MiB must be a positive integer"
  validate_memory_calibration "$calibration_jobs" "$calibration_seconds" "$calibration_min" "$memory"
  if [ -z "$io" ]; then
    # Charge one unit by default only when the live pool enforces shared I/O.
    io="$(env_value "$POOL_FILE" shared_io_capacity)" || exit 1
    is_uint "$io" || die "pool shared_io_capacity is not a non-negative integer: $io"
    [ "$io" -eq 0 ] || io=1
  fi
  is_uint "$io" || die "shared I/O units must be a non-negative integer"
  is_uint "$maximum" || die "max workers must be a non-negative integer"
  reject_record_breaks "$model_dir" "$queue" "$run_name"
  [ -x "$START_WORKERS_SCRIPT" ] || die "start_workers.sh is not executable: $START_WORKERS_SCRIPT"
  set --
  [ "$recover" -eq 0 ] || set -- "$@" --recover_inactive
  [ "$allow_network" -eq 0 ] || set -- "$@" --allow_network_coordinator
  [ "$project_option_set" -eq 0 ] || set -- "$@" --project "$project"
  [ -z "$calibration_min" ] || set -- "$@" --memory_min_mib_per_job "$calibration_min"
  [ "$gpu" = 0 ] || set -- "$@" --gpu_units_per_job "$gpu"
  [ -z "$memory_limit" ] || set -- "$@" --memory_limit_mib "$memory_limit"
  ADAPTIVE_STATE_ROOT="$STATE_ROOT" ADAPTIVE_POOL_MARKER="$POOL_FILE" \
    MODE_TRANSITION_LOCK="$TRANSITION_LOCK" \
    bash "$START_WORKERS_SCRIPT" \
      --model_dir "$model_dir" --queue "$queue" --run_name "$run_name" \
      --execution_mode adaptive-shared-queue \
      --schedule_priority "$priority" --cpu_units_per_job "$cpu" \
      --memory_mib_per_job "$memory" --shared_io_units_per_job "$io" \
      --memory_calibration_jobs "$calibration_jobs" --memory_calibration_seconds "$calibration_seconds" \
      --max_workers "$maximum" --adaptive_admission "$admission" ${workflow_args[@]+"${workflow_args[@]}"} "$@"
}

adaptive_paths() {
  local batch_dir="$1" require_registration="${2:-1}" mode queue_id pool_id
  [ -d "$batch_dir" ] || die "batch directory not found: $batch_dir"
  mode="$(env_value "$batch_dir/run_meta.env" execution_mode)"
  [ "$mode" = adaptive-shared-queue ] || die "batch is not adaptive-shared-queue: $batch_dir"
  CONTROL_FILE="$batch_dir/adaptive/control.env"
  RESOURCE_FILE="$batch_dir/adaptive/resources.env"
  STATUS_FILE="$batch_dir/adaptive/status.env"
  [ "$require_registration" -eq 1 ] || return 0
  REGISTRATION_FILE="$(env_value "$batch_dir/run_meta.env" registration_file)"
  [ -f "$REGISTRATION_FILE" ] && [ ! -L "$REGISTRATION_FILE" ] || \
    die "adaptive registration is missing or ambiguous: $REGISTRATION_FILE"
  queue_id="$(env_value "$batch_dir/run_meta.env" run_token)"
  pool_id="$(env_value "$batch_dir/run_meta.env" pool_id)"
  [ "$(env_value "$REGISTRATION_FILE" queue_id)" = "$queue_id" ] && \
    [ "$(env_value "$REGISTRATION_FILE" pool_id)" = "$pool_id" ] || \
    die "adaptive registration identity does not match its batch"
  if [ -f "$POOL_FILE" ] && [ "$(env_value "$POOL_FILE" pool_id)" != "$pool_id" ]; then
    die "adaptive batch belongs to a different pool instance"
  fi
}

# An operator who has verified the outputs of jobs that ran across a share interruption
# confirms them; moving their records releases the queue's dependent stages.
confirm_interrupted_jobs() {
  local batch_dir="" reason="" records record count=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --batch_dir) batch_dir="$2"; shift 2 ;;
      --reason) reason="$2"; shift 2 ;;
      *) die "unknown confirm-interrupted-jobs argument: $1" ;;
    esac
  done
  [ -n "$batch_dir" ] && [ -n "$reason" ] || die "confirm-interrupted-jobs requires --batch_dir and --reason"
  case "$reason" in *$'\n'*|*$'\r'*) die "--reason must be one line" ;; esac
  # Retired queues keep holding dependents, so the registration is not required.
  adaptive_paths "$batch_dir" 0
  records="$batch_dir/share-interrupted"
  [ -d "$records" ] && [ ! -L "$records" ] || die "no share interruptions are recorded for $batch_dir"
  mkdir -p "$records/confirmed"
  for record in "$records"/job_*; do
    [ -f "$record" ] && [ ! -L "$record" ] || continue
    mv "$record" "$records/confirmed/"
    printf '%s\t%s\t%s\t%s\n' "$(date +%s)" "$(id -un)" "$(basename "$record")" "$reason" \
      >> "$records/confirmed/audit.tsv"
    echo "confirmed=$(basename "$record")"
    count=$((count + 1))
  done
  echo "confirmed_jobs=$count"
}

assert_memory_controls_supported() {
  local require_all="${1:-0}" ready="$STATE_ROOT/coordinator.ready"
  local feature="${2:-memory_control_version}" description="live memory controls"
  local pool pid lease now updated resources host target cpu memory enabled capability result
  [ "$feature" != memory_calibration_version ] || description="memory calibration"
  [ -f "$POOL_FILE" ] && [ "$(env_value "$POOL_FILE" state)" = ACTIVE ] || \
    die "set-memory requires an ACTIVE adaptive pool"
  pool="$(env_value "$POOL_FILE" pool_id)"
  POOL_ID="$pool"
  capability="$(awk -F= -v feature="$feature" '$1 == feature {n++; value=$2}
    END {if (n > 1) exit 1; print value}' "$POOL_FILE")" || die "invalid pool $feature capability"
  [ "$require_all" = 1 ] || [ "$capability" = 1 ] || \
    die "pool was not bootstrapped for $description; deploy the complete scheduler first"
  pid="$(env_value "$POOL_FILE" coordinator_pid)"
  [ -f "$ready" ] && [ "$(env_value "$ready" "$feature")" = 1 ] && \
    [ "$(env_value "$ready" pool_id)" = "$pool" ] && \
    [ "$(env_value "$ready" pid)" = "$pid" ] && \
    [ "$(env_value "$ready" state)" = ready ] && coordinator_is_live "$pid" || \
    die "running coordinator does not support $description; deploy the complete scheduler first"
  lease="$(env_value "$POOL_FILE" lease_seconds)"
  updated="$(env_value "$ready" updated_epoch)"
  now="$(date +%s)"
  is_uint "$lease" && is_uint "$updated" && [ "$now" -ge "$updated" ] && \
    [ $((now - updated)) -le "$lease" ] || die "coordinator memory-control capability is stale"
  resources="$(env_value "$POOL_FILE" resources_file)"
  [ "$(hash_file "$resources")" = "$(env_value "$POOL_FILE" resources_sha256)" ] || \
    die "reviewed resource table hash mismatch"
  while IFS=$'\t' read -r host target cpu memory enabled _; do
    [ "$enabled" = 1 ] || continue
    if ssh_no_input "$target" "set -e
state_dir=\"\$HOME/.parallel-workers/adaptive/$pool\"
lock=\"\$state_dir/daemon.lock\"
[ -f \"\$lock\" ] && [ ! -L \"\$lock\" ]
grep -Fqx '$feature=1' \"\$lock\"
grep -Fqx 'pool_id=$pool' \"\$lock\"
grep -Fqx 'host_id=$host' \"\$lock\"
grep -Fqx \"state_dir=\$state_dir\" \"\$lock\"
grep -Fqx \"script=\$state_dir/bin/mmhostdaemon.sh\" \"\$lock\"
pid=\$(awk -F= '\$1 == \"pid\" {n++; value=\$2} END {if (n != 1) exit 1; print value}' \"\$lock\")
case \$pid in ''|*[!0-9]*) exit 1;; esac
command=\$(ps -ww -p \"\$pid\" -o command=)
case \" \$command \" in *\" \$state_dir/bin/mmhostdaemon.sh \"*) ;; *) exit 1;; esac
case \" \$command \" in *\" --state-dir \$state_dir \"*) ;; *) exit 1;; esac
" >/dev/null; then
      continue
    else
      result=$?
    fi
    # A verified pool can outlive a temporary SSH outage; old grants stay charged.
    if [ "$require_all" = 0 ] && [ "$capability" = 1 ] && [ "$result" = 255 ]; then
      echo "set-memory: $host is unreachable; its existing memory charges remain reserved" >&2
      continue
    fi
    die "daemon on $target does not support $description; deploy every pool daemon first"
  done < <(sed -n '2,$p' "$resources")
}

assert_host_priorities_supported() {
  local ready="$STATE_ROOT/coordinator.ready"
  [ -f "$ready" ] && [ "$(env_value "$ready" pool_id)" = "$(env_value "$POOL_FILE" pool_id)" ] && \
    [ "$(env_value "$ready" pid)" = "$(env_value "$POOL_FILE" coordinator_pid)" ] && \
    [ "$(awk -F= '$1 == "host_priority_version" {print $2}' "$ready")" = 1 ] || \
    die "running coordinator does not support per-host priorities; deploy the complete scheduler and restart the pool"
}

update_control() {
  local batch_dir="$1" field="$2" value="$3" expected="${4:-}"
  local generation queue_id admission priority maximum memory="" tmp schema=1
  local host_priorities="" resources fields gpu limit
  local host_priorities_format='^[A-Za-z0-9._-]+=(-?[0-9]+|off)(,[A-Za-z0-9._-]+=(-?[0-9]+|off))*$'
  local calibration_jobs=0 calibration_seconds=0 calibration_min="" calibration_generation=""
  assert_local_state_root
  acquire_lock "$TRANSITION_LOCK"
  adaptive_paths "$batch_dir"
  [ -z "$expected" ] || [ "$(env_value "$POOL_FILE" pool_id)" = "$expected" ] || \
    die "adaptive pool identity changed"
  queue_id="$(env_value "$CONTROL_FILE" queue_id)"
  generation="$(env_value "$CONTROL_FILE" generation)"
  admission="$(env_value "$CONTROL_FILE" admission)"
  priority="$(env_value "$CONTROL_FILE" schedule_priority)"
  maximum="$(env_value "$CONTROL_FILE" max_workers)"
  if grep -q '^host_priorities=' "$CONTROL_FILE"; then
    host_priorities="$(env_value "$CONTROL_FILE" host_priorities)"
    [ -z "$host_priorities" ] || { [[ "$host_priorities" =~ $host_priorities_format ]] && \
      [ "$(env_value "$CONTROL_FILE" schema_version)" = 2 ]; } || \
      die "stored host_priorities require control schema2 and well-formed HOST=VALUE items"
  fi
  if grep -q '^memory_mib_per_job=' "$CONTROL_FILE"; then
    memory="$(env_value "$CONTROL_FILE" memory_mib_per_job)"
    is_uint "$memory" && [ "$memory" -gt 0 ] || die "memory per job must be a positive integer"
  fi
  if grep -Eq '^(memory_calibration_jobs|memory_calibration_seconds|memory_min_mib_per_job|memory_calibration_generation)=' "$CONTROL_FILE"; then
    calibration_jobs="$(env_value "$CONTROL_FILE" memory_calibration_jobs)"
    calibration_seconds="$(env_value "$CONTROL_FILE" memory_calibration_seconds)"
    calibration_min="$(env_value "$CONTROL_FILE" memory_min_mib_per_job)"
    calibration_generation="$(env_value "$CONTROL_FILE" memory_calibration_generation)"
    [ "$(env_value "$CONTROL_FILE" schema_version)" = 2 ] && [ -n "$memory" ] || \
      die "memory calibration requires schema2 with an explicit memory floor"
    validate_memory_calibration "$calibration_jobs" "$calibration_seconds" "$calibration_min" "$memory"
    is_uint "$calibration_generation" && [ "$calibration_generation" -gt 0 ] && \
      [ "$calibration_generation" -le "$generation" ] || die "invalid memory calibration generation"
    { [ "$calibration_jobs" -gt 0 ] || [ "$calibration_seconds" -gt 0 ]; } || \
      die "stored memory calibration must have an enabled trigger"
  fi
  is_uint "$generation" || die "invalid control generation in $CONTROL_FILE"
  if [ "$admission" = closed ] && [ "$field" = admission ] && [ "$value" != closed ]; then
    die "hard-stop admission closure cannot be reopened for this batch"
  fi
  case "$field" in
    admission) admission="$value" ;;
    schedule_priority)
      # A new SPEC replaces every earlier host override.
      priority="$value" host_priorities=""
      if ! is_int "$value"; then
        [ -f "$POOL_FILE" ] || die "per-host priorities require the batch's active pool"
        resources="$(env_value "$POOL_FILE" resources_file)"
        [ "$(hash_file "$resources")" = "$(env_value "$POOL_FILE" resources_sha256)" ] || \
          die "reviewed resource table hash mismatch"
        fields="$(python3 "$COORDINATOR_SCRIPT" priority-spec --resources-file "$resources" \
          "--spec=$value")" || die "invalid schedule priority SPEC: $value"
        IFS=$'\t' read -r priority host_priorities <<< "$fields"
        [ -z "$host_priorities" ] || assert_host_priorities_supported
      fi
      ;;
    max_workers) maximum="$value" ;;
    memory_mib_per_job)
      is_uint "$value" && [ "$value" -gt 0 ] || die "memory per job must be a positive integer"
      [ "$(hash_file "$RESOURCE_FILE")" = "$(env_value "$REGISTRATION_FILE" resources_sha256)" ] || \
        die "immutable queue resource hash mismatch"
      # Opt-in requests the queue was submitted with still bind the corrected floor.
      gpu="$(awk -F= '$1 == "gpu_units_per_job" {print $2}' "$RESOURCE_FILE")"
      limit="$(awk -F= '$1 == "memory_limit_mib" {print $2}' "$RESOURCE_FILE")"
      [ -z "$limit" ] || [ "$value" -le "$limit" ] || \
        die "memory per job must not exceed the queue's memory limit of $limit MiB"
      python3 "$COORDINATOR_SCRIPT" profile-fits --state-root "$STATE_ROOT" \
        --pool-id "$(env_value "$POOL_FILE" pool_id)" \
        --cpu-units "$(env_value "$RESOURCE_FILE" cpu_units_per_job)" --memory-mib "$value" \
        --gpu-units "${gpu:-0}" || \
        die "this job profile does not fit any enabled adaptive host"
      assert_memory_controls_supported
      memory="$value"
      calibration_jobs="${5:-0}"
      calibration_seconds="${6:-0}"
      calibration_min="${7:-}"
      calibration_generation=""
      validate_memory_calibration "$calibration_jobs" "$calibration_seconds" "$calibration_min" "$memory"
      if [ "$calibration_jobs" -gt 0 ] || [ "$calibration_seconds" -gt 0 ]; then
        assert_memory_controls_supported 0 memory_calibration_version
        calibration_min="${calibration_min:-64}"
        calibration_generation=$((generation + 1))
      fi
      ;;
    *) die "internal control field error: $field" ;;
  esac
  [[ "$admission" = enabled || "$admission" = paused || "$admission" = closed ]] || \
    die "invalid admission state"
  is_int "$priority" || die "schedule priority must be an integer"
  is_uint "$maximum" || die "max workers must be a non-negative integer"
  [ -z "$memory" ] && [ -z "$host_priorities" ] || schema=2
  tmp="${CONTROL_FILE}.tmp.$$"
  {
    echo "schema_version=$schema"
    echo "queue_id=$queue_id"
    echo "generation=$((generation + 1))"
    echo "admission=$admission"
    echo "schedule_priority=$priority"
    [ -z "$host_priorities" ] || echo "host_priorities=$host_priorities"
    echo "max_workers=$maximum"
    [ -z "$memory" ] || echo "memory_mib_per_job=$memory"
    if [ -n "$calibration_generation" ]; then
      echo "memory_calibration_jobs=$calibration_jobs"
      echo "memory_calibration_seconds=$calibration_seconds"
      echo "memory_min_mib_per_job=$calibration_min"
      echo "memory_calibration_generation=$calibration_generation"
    fi
    echo "updated_epoch=$(date +%s)"
  } > "$tmp"
  chmod 666 "$tmp"
  mv -f "$tmp" "$CONTROL_FILE"
  echo "queue_id=$queue_id"
  echo "generation=$((generation + 1))"
  echo "admission=$admission"
  echo "schedule_priority=$priority"
  [ -z "$host_priorities" ] || echo "host_priorities=$host_priorities"
  echo "max_workers=$maximum"
  [ -z "$memory" ] || echo "memory_mib_per_job=$memory"
  if [ -n "$calibration_generation" ]; then
    echo "memory_calibration_jobs=$calibration_jobs"
    echo "memory_calibration_seconds=$calibration_seconds"
    echo "memory_min_mib_per_job=$calibration_min"
    echo "memory_calibration_generation=$calibration_generation"
  fi
}

control_command() {
  local action="$1" batch_dir="" value="" expected=""
  local calibration_jobs=0 calibration_seconds=0 calibration_min=""
  shift
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --batch_dir|--schedule_priority|--max_workers|--memory_mib_per_job|--expected-pool-id|\
      --memory_calibration_jobs|--memory_calibration_seconds|--memory_min_mib_per_job)
        [ $# -ge 2 ] || die "$1 requires a value" ;;
    esac
    case "$1" in
      --batch_dir) batch_dir="$2"; shift 2 ;;
      --schedule_priority)
        [ "$action" = set-priority ] || die "unknown $action argument: $1"
        value="$2"; shift 2 ;;
      --max_workers)
        [ "$action" = set-max-workers ] || die "unknown $action argument: $1"
        value="$2"; shift 2 ;;
      --memory_mib_per_job)
        [ "$action" = set-memory ] || die "unknown $action argument: $1"
        value="$2"; shift 2 ;;
      --memory_calibration_jobs|--memory_calibration_seconds|--memory_min_mib_per_job)
        [ "$action" = set-memory ] || die "unknown $action argument: $1"
        case "$1" in
          --memory_calibration_jobs) calibration_jobs="$2" ;;
          --memory_calibration_seconds) calibration_seconds="$2" ;;
          --memory_min_mib_per_job) calibration_min="$2" ;;
        esac
        shift 2 ;;
      --expected-pool-id)
        [[ "$2" =~ ^[A-Za-z0-9._-]+$ && "$2" != . && "$2" != .. ]] || die "invalid expected pool ID"
        expected="$2"; shift 2 ;;
      *) die "unknown $action argument: $1" ;;
    esac
  done
  [ -n "$batch_dir" ] || die "$action requires --batch_dir"
  case "$action" in
    pause) update_control "$batch_dir" admission paused "$expected" ;;
    close-for-hard-stop) update_control "$batch_dir" admission closed "$expected" ;;
    resume)
      [ -f "$POOL_FILE" ] && [ "$(env_value "$POOL_FILE" state)" = ACTIVE ] || \
        die "cannot resume while the adaptive pool is not ACTIVE"
      update_control "$batch_dir" admission enabled "$expected"
      ;;
    set-priority)
      [ -n "$value" ] || die "set-priority requires --schedule_priority"
      update_control "$batch_dir" schedule_priority "$value" "$expected"
      ;;
    set-max-workers)
      [ -n "$value" ] || die "set-max-workers requires --max_workers"
      update_control "$batch_dir" max_workers "$value" "$expected"
      ;;
    set-memory)
      [ -n "$value" ] || die "set-memory requires --memory_mib_per_job"
      update_control "$batch_dir" memory_mib_per_job "$value" "$expected" \
        "$calibration_jobs" "$calibration_seconds" "$calibration_min"
      ;;
  esac
}

queue_status() {
  local batch_dir="" requested effective record status_queue status_generation status_requested status_effective
  local status_state status_reason status_samples status_candidate
  local calibration_state=off calibration_reason=disabled calibration_samples=0 calibration_candidate=""
  while [[ $# -gt 0 ]]; do
    case "$1" in --batch_dir) batch_dir="$2"; shift 2 ;; *) die "unknown queue-status argument: $1" ;; esac
  done
  [ -n "$batch_dir" ] || die "queue-status requires --batch_dir"
  adaptive_paths "$batch_dir" 0
  cat "$RESOURCE_FILE"
  # Report host overrides beside the default priority, even when there are none.
  awk -F= '
    {line[NR]=$0; if ($1 == "host_priorities") present=1}
    END {
      for (i=1; i<=NR; i++) {
        print line[i]
        if (!present && line[i] ~ /^schedule_priority=/) print "host_priorities="
      }
    }
  ' "$CONTROL_FILE"
  echo "initial_memory_mib_per_job=$(env_value "$RESOURCE_FILE" memory_mib_per_job)"
  if grep -q '^memory_mib_per_job=' "$CONTROL_FILE"; then
    requested="$(env_value "$CONTROL_FILE" memory_mib_per_job)"
  else
    requested="$(env_value "$RESOURCE_FILE" memory_mib_per_job)"
  fi
  effective="$requested"
  if grep -q '^memory_calibration_generation=' "$CONTROL_FILE"; then
    calibration_state=pending calibration_reason=awaiting_plan
    if record="$(state_record "$STATUS_FILE" 'queue_id control_generation requested_memory_mib_per_job effective_memory_mib_per_job memory_calibration_state memory_calibration_reason memory_calibration_sample_jobs memory_calibration_candidate_mib' 2>/dev/null)"; then
      IFS=$'\t' read -r status_queue status_generation status_requested status_effective \
        status_state status_reason status_samples status_candidate <<< "$record"
      if [ "$status_queue" = "$(env_value "$CONTROL_FILE" queue_id)" ] && \
          [ "$status_generation" = "$(env_value "$CONTROL_FILE" generation)" ] && [ "$status_requested" = "$requested" ]; then
        is_uint "$status_effective" && [ "$status_effective" -gt 0 ] && [ "$status_effective" -le "$requested" ] || \
          die "invalid effective calibrated memory in $STATUS_FILE"
        effective="$status_effective"
        calibration_state="$status_state" calibration_reason="$status_reason"
        calibration_samples="$status_samples" calibration_candidate="$status_candidate"
      fi
    fi
  fi
  echo "requested_memory_mib_per_job=$requested"
  echo "effective_memory_mib_per_job=$effective"
  echo "memory_calibration_state=$calibration_state"
  echo "memory_calibration_reason=$calibration_reason"
  echo "memory_calibration_sample_jobs=$calibration_samples"
  echo "memory_calibration_candidate_mib=$calibration_candidate"
  if [ -f "$STATUS_FILE" ]; then
    awk -F= '$1 !~ /^(initial_memory_mib_per_job|requested_memory_mib_per_job|effective_memory_mib_per_job|memory_calibration_state|memory_calibration_reason|memory_calibration_sample_jobs|memory_calibration_candidate_mib)$/ {print}' \
      "$STATUS_FILE"
  fi
}

wait_closed() {
  local batch_dir="" timeout=30 waited=0 run_token batch_pool registration control current_control status
  local schema queue_id generation admission status_schema status_queue lifecycle applied
  local status_admission admission_matches propagation running targets
  local pool_record pool_id pool_state closure_latched
  local terminal_latched
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --batch_dir) batch_dir="$2"; shift 2 ;;
      --timeout_sec) timeout="$2"; shift 2 ;;
      *) die "unknown wait-closed argument: $1" ;;
    esac
  done
  [ -n "$batch_dir" ] || die "wait-closed requires --batch_dir"
  is_uint "$timeout" || die "timeout must be a non-negative integer"
  adaptive_paths "$batch_dir" 0
  run_token="$(env_value "$batch_dir/run_meta.env" run_token)"
  batch_pool="$(env_value "$batch_dir/run_meta.env" pool_id)"
  registration="$(env_value "$batch_dir/run_meta.env" registration_file)"
  while [ "$waited" -le "$timeout" ]; do
    control="$(state_record "$CONTROL_FILE" 'schema_version queue_id generation admission')" || \
      die "invalid adaptive control record: $CONTROL_FILE"
    IFS=$'\t' read -r schema queue_id generation admission <<< "$control"
    [[ "$schema" = 1 || "$schema" = 2 ]] && [ "$queue_id" = "$run_token" ] && is_uint "$generation" && \
      [[ "$admission" = enabled || "$admission" = paused || "$admission" = closed ]] || \
      die "adaptive control identity or state is invalid"
    pool_state=""
    closure_latched=0
    [ "$admission" != closed ] || closure_latched=1
    if [ -f "$POOL_FILE" ]; then
      pool_record="$(state_record "$POOL_FILE" 'pool_id state')" || \
        die "invalid adaptive pool record: $POOL_FILE"
      IFS=$'\t' read -r pool_id pool_state <<< "$pool_record"
      [ "$pool_id" = "$batch_pool" ] && \
        [[ "$pool_state" = ACTIVE || "$pool_state" = DRAINING ]] || \
        die "adaptive pool identity or state does not match this batch"
      if [ "$pool_state" = DRAINING ]; then
        closure_latched=1
      fi
    fi
    if [ -f "$STATUS_FILE" ]; then
      status="$(state_record "$STATUS_FILE" \
        'schema_version queue_id lifecycle admission control_generation propagation running_workers target_workers')" || \
        die "invalid adaptive status record: $STATUS_FILE"
      IFS=$'\t' read -r status_schema status_queue lifecycle status_admission applied propagation running targets <<< "$status"
      [ "$status_schema" = 1 ] && [ "$status_queue" = "$run_token" ] && \
        [[ "$lifecycle" =~ ^(PENDING|RUNNING|DRAINING|COMPLETED|FINISHED_INCOMPLETE)$ ]] && \
        [[ "$status_admission" = enabled || "$status_admission" = paused || "$status_admission" = closed ]] && \
        is_uint "$applied" && is_uint "$running" && is_uint "$targets" && \
        [[ "$propagation" = pending || "$propagation" = complete ]] || \
        die "adaptive status identity or state is invalid"
      terminal_latched=0
      [[ "$lifecycle" != COMPLETED && "$lifecycle" != FINISHED_INCOMPLETE ]] || \
        { closure_latched=1; terminal_latched=1; }
      admission_matches=0
      if [ "$pool_state" = DRAINING ]; then
        [ "$status_admission" != paused ] || admission_matches=1
      elif [ "$status_admission" = "$admission" ] || \
          { [ "$terminal_latched" -eq 1 ] && [ -z "$pool_state" ]; }; then
        admission_matches=1
      fi
      if [ "$closure_latched" -eq 1 ] && is_uint "$applied" && \
          [ "$applied" = "$generation" ] && [ "$admission_matches" -eq 1 ] && \
          [ "$targets" = 0 ] && [ "$propagation" = complete ]; then
        current_control="$(state_record "$CONTROL_FILE" \
          'schema_version queue_id generation admission')" || \
          die "invalid adaptive control record: $CONTROL_FILE"
        if [ "$current_control" = "$control" ] && \
            { [ "$admission" = closed ] || [ "$pool_state" = DRAINING ] || \
              { [ "$terminal_latched" -eq 1 ] && [ "$running" = 0 ] && \
                [ ! -e "$registration" ] && [ ! -L "$registration" ]; }; }; then
          echo "admission_closed=1"
          return 0
        fi
      fi
    fi
    [ "$waited" -lt "$timeout" ] || break
    sleep 1
    waited=$((waited + 1))
  done
  die "zero-admission propagation was not confirmed within ${timeout}s"
}

recovery_command() {
  local batch="" host="" reservation="" reason="" expected_pool="" expected_seen=0 pool queue target resources helper_hash payload
  local verify_lost=0
  while [ $# -gt 0 ]; do
    if [ "$1" = --verify_lost_job ]; then verify_lost=1; shift; continue; fi
    [ $# -ge 2 ] || die "recovery option requires a value: $1"
    case "$1" in
      --batch_dir) batch="$2" ;;
      --host_id) host="$2" ;;
      --reservation_id) reservation="$2" ;;
      --reason) reason="$2" ;;
      --expected-pool-id) expected_pool="$2"; expected_seen=1 ;;
      *) die "unknown recovery option: $1" ;;
    esac
    shift 2
  done
  [ -n "$batch" ] && [ -n "$host" ] && [ -n "$reason" ] || \
    die "recovery requires --batch_dir, --host_id, and --reason"
  [ "$COMMAND" != resolve-uncertain ] || [ -n "$reservation" ] || \
    die "resolve-uncertain requires --reservation_id"
  reject_record_breaks "$batch" "$host" "$reservation" "$reason" "$expected_pool"
  assert_local_state_root
  acquire_lock "$TRANSITION_LOCK"
  adaptive_paths "$batch"
  pool="$(env_value "$POOL_FILE" pool_id)"
  [[ "$pool" =~ ^[A-Za-z0-9._-]+$ ]] && [[ "$pool" != . && "$pool" != .. ]] || die "invalid pool ID"
  [ "$expected_seen" -eq 0 ] || [ "$pool" = "$expected_pool" ] || die "adaptive pool identity changed"
  queue="$(env_value "$RESOURCE_FILE" queue_id)"
  resources="$(env_value "$POOL_FILE" resources_file)"
  [ "$(hash_file "$resources")" = "$(env_value "$POOL_FILE" resources_sha256)" ] || die "resources hash mismatch"
  target="$(awk -F '\t' -v host="$host" 'NR > 1 && $1 == host {print $2}' "$resources")"
  [ -n "$target" ] || die "unknown adaptive host: $host"
  helper_hash="$(env_value "$POOL_FILE" recovery_sha256)"
  [[ "$helper_hash" =~ ^[0-9a-f]{64}$ ]] || die "invalid recovery helper hash"
  if [ "$verify_lost" -eq 1 ]; then
    [ "$COMMAND" = resolve-uncertain ] || die "--verify_lost_job requires resolve-uncertain"
    [ "$(env_value "$POOL_FILE" workflow_version)" = 1 ] && \
      [ "$(hash_file "$RECOVERY_SCRIPT")" = "$helper_hash" ] || \
      die "verified loss requires a compatible fresh pool"
  fi
  payload="$(python3 -c 'import json,sys; print(json.dumps(dict(zip(
    ("action","pool_id","host_id","queue_id","batch_dir","reservation_id","reason","verify_lost_job"),sys.argv[1:]))))' \
    "$COMMAND" "$pool" "$host" "$queue" "$batch" "$reservation" "$reason" "$verify_lost")"
  recovery_helper "$target" "$pool" "$payload"
}

[ $# -gt 0 ] || { usage >&2; exit 2; }
COMMAND="$1"
shift
[ "$POOL_FILE" = "$STATE_ROOT/pool.env" ] || \
  die "ADAPTIVE_POOL_MARKER must be $STATE_ROOT/pool.env"
[ "$TRANSITION_LOCK" = "$(dirname "$STATE_ROOT")/mode-transition.lock" ] || \
  die "MODE_TRANSITION_LOCK must be $(dirname "$STATE_ROOT")/mode-transition.lock"
case "$COMMAND" in
  pool-start) pool_start "$@" ;;
  pool-status) [ $# -eq 0 ] || die "pool-status takes no arguments"; pool_status ;;
  set-host-budget) set_host_budget "$@" ;;
  set-host-state) set_host_state "$@" ;;
  pool-drain) pool_drain "$@" ;;
  verify-memory-calibration)
    [ "$#" -eq 0 ] || die "verify-memory-calibration takes no arguments"
    assert_local_state_root
    assert_memory_controls_supported 0 memory_calibration_version
    ;;
  submit) submit_queue "$@" ;;
  submit-chain) python3 "$WORKFLOW_SCRIPT" submit-chain --scheduler "$0" "$@" ;;
  set-priority|set-max-workers|set-memory|pause|resume|close-for-hard-stop) control_command "$COMMAND" "$@" ;;
  queue-status) queue_status "$@" ;;
  wait-closed) wait_closed "$@" ;;
  reset-failures|resolve-uncertain) recovery_command "$@" ;;
  restart-daemon) restart_daemon "$@" ;;
  confirm-interrupted-jobs) confirm_interrupted_jobs "$@" ;;
  finalize-drain) finalize_drain "$@" ;;
  -h|--help|help) usage ;;
  *) usage >&2; die "unknown command: $COMMAND" ;;
esac
