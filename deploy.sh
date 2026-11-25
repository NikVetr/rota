#!/usr/bin/env bash
export PARALLEL_WORKERS_ROOT="${PARALLEL_WORKERS_ROOT:?Source the inventory environment first}"

set -euo pipefail

MODE=all
CHECK_ONLY=0
COORDINATOR="${PARALLEL_WORKERS_COORDINATOR:-}"
HOSTS_FILE="${PARALLEL_WORKERS_HOSTS_FILE:-${PARALLEL_WORKERS_ROOT}/hosts/hosts-ethernet.txt}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=8)
DEPLOY_LOCK=${PARALLEL_WORKERS_ROOT}/.parallel-workers/mode-transition.lock
DEPLOY_LOCK_TOKEN="deploy-$$-$(date +%s)"
DEPLOY_LOCK_HELD=0
RETIRED_SCHEDULER_FILES=(mmadaptiveagent.sh)

# Matches cluster_config.py: the root reaches job tags and remote commands unquoted.
case "$PARALLEL_WORKERS_ROOT" in
  *[!A-Za-z0-9._+=@,:%~^#/-]*|*//*|*/|*/./*|*/.|*/../*|*/..|[!/]*|"")
    echo "PARALLEL_WORKERS_ROOT must be a normalized absolute path using only letters," \
      "digits and ._+=@,:%~^#- (no spaces, quotes or shell metacharacters)." >&2; exit 2 ;;
esac

usage() {
  cat <<'EOF'
Usage: deploy.sh [options]

Options:
  --check                 Compare source and deployed files without changing them
  --scheduler-only        Deploy scheduler and benchmark scripts only
  --monitoring-only       Synchronize monitoring scripts only
  --coordinator <host>    Override PARALLEL_WORKERS_COORDINATOR
  --hosts_file <path>     Hosts audited before scheduler deployment

Scheduler deployment is refused while any scheduler process or adaptive pool
is active. Files are staged beside their destination and then replaced
atomically on the coordinator.
Set PARALLEL_WORKERS_ROOT to the mounted shared drive.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) CHECK_ONLY=1; shift ;;
    --scheduler-only) MODE=scheduler; shift ;;
    --monitoring-only) MODE=monitoring; shift ;;
    --coordinator) COORDINATOR="$2"; shift 2 ;;
    --hosts_file) HOSTS_FILE="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
[ -n "$COORDINATOR" ] || { echo "Source the inventory environment or pass --coordinator." >&2; exit 2; }
SNAPSHOT_DIR="$REPO_DIR/scheduler"
MONITOR_SYNC="$REPO_DIR/sync_parallel_workers_monitoring.sh"

if [ ! -f "$HOSTS_FILE" ]; then
  echo "Hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

release_deploy_lock() {
  [ "$DEPLOY_LOCK_HELD" -eq 1 ] || return 0
  ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" "set -e
lock='$DEPLOY_LOCK'
grep -Fqx 'token=$DEPLOY_LOCK_TOKEN' \"\$lock/owner.env\"
rm -f \"\$lock/owner.env\"
rmdir \"\$lock\"" || return 1
  DEPLOY_LOCK_HELD=0
}

cleanup_deploy_lock() {
  release_deploy_lock || echo "Failed to release scheduler deployment lock: $DEPLOY_LOCK" >&2
}
trap cleanup_deploy_lock EXIT

acquire_deploy_lock() {
  local output
  if ! output=$(ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" "set -e
lock='$DEPLOY_LOCK'
mkdir -p \"\$(dirname \"\$lock\")\"
if ! mkdir \"\$lock\" 2>/dev/null; then
  [ ! -f \"\$lock/owner.env\" ] || sed -n '1,5p' \"\$lock/owner.env\" >&2
  exit 3
fi
trap 'rm -f \"\$lock/owner.env.tmp.\$\$\"; rmdir \"\$lock\" 2>/dev/null || true' EXIT
{
  echo 'token=$DEPLOY_LOCK_TOKEN'
  echo 'command=deploy.sh'
  echo created_epoch=\$(date +%s)
} > \"\$lock/owner.env.tmp.\$\$\"
mv -f \"\$lock/owner.env.tmp.\$\$\" \"\$lock/owner.env\"
trap - EXIT" 2>&1); then
    echo "Cannot acquire scheduler deployment lock: $DEPLOY_LOCK" >&2
    [ -z "$output" ] || printf '%s\n' "$output" >&2
    return 1
  fi
  DEPLOY_LOCK_HELD=1
}

audit_scheduler_idle() {
  local host slots output
  local active=0
  local -a audit_hosts=("$COORDINATOR")
  local audited_hosts=$'\n'
  local process_pattern='[m]mworker_|[m]mworker[.]sh|[m]mshardworker_|[m]mshardcoordinator_|[m]mhostdaemon|[m]madaptiveagent|[a]daptive_coordinator[.]py|[a]daptive_scheduler[.]sh|[s]tart_workers[.]sh|[k]ill_workers[.]sh|[r]econcile_queue_batch[.]sh|--[j]ob_tag'

  acquire_deploy_lock || return 1
  if ! output=$(ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" \
    "if [ -e '${PARALLEL_WORKERS_ROOT}/.parallel-workers/adaptive/pool.env' ] ||
        [ -L '${PARALLEL_WORKERS_ROOT}/.parallel-workers/adaptive/pool.env' ]; then
       cat '${PARALLEL_WORKERS_ROOT}/.parallel-workers/adaptive/pool.env' >&2
       exit 4
     fi" 2>&1); then
    echo "Scheduler deployment refused: an adaptive pool marker exists or could not be audited." >&2
    [ -z "$output" ] || printf '%s\n' "$output" >&2
    return 1
  fi

  while read -r host slots _; do
    [ -n "${host:-}" ] || continue
    [[ "$host" == \#* ]] && continue
    if ! [[ "${slots:-}" =~ ^[0-9]+$ ]]; then
      echo "Invalid slot count for $host in $HOSTS_FILE: ${slots:-<missing>}" >&2
      return 1
    fi
    audit_hosts+=("$host")
  done < "$HOSTS_FILE"

  for host in "${audit_hosts[@]}"; do
    [[ "$audited_hosts" != *$'\n'"$host"$'\n'* ]] || continue
    audited_hosts+="$host"$'\n'
    if ! output=$(ssh -n "${SSH_OPTS[@]}" "$host" \
      "ps -ww -axo pid=,command= | grep -E '$process_pattern' || true"); then
      echo "Cannot audit scheduler processes on $host" >&2
      return 1
    fi
    if [ -n "$output" ]; then
      echo "Active scheduler processes on $host:" >&2
      printf '%s\n' "$output" >&2
      active=1
    fi
  done

  if [ "$active" -ne 0 ]; then
    echo "Scheduler deployment refused until the cluster is idle." >&2
    return 1
  fi
}

check_one() {
  local source_file="$1"
  local destination_file="$2"
  local expected_mode="$3"
  local source_hash remote_status deployed_hash deployed_mode

  source_hash=$(shasum -a 256 "$source_file" | awk '{print $1}')
  remote_status=$(ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" \
    "if [ -f '$destination_file' ]; then
       shasum -a 256 '$destination_file'
       if mode=\$(/usr/bin/stat -f '%Lp' '$destination_file' 2>/dev/null); then
         printf '%s\\n' \"\$mode\"
       else
         /usr/bin/stat -c '%a' '$destination_file' 2>/dev/null
       fi
     fi" 2>/dev/null || true)
  deployed_hash=$(printf '%s\n' "$remote_status" | awk 'NR == 1 {print $1}')
  deployed_mode=$(printf '%s\n' "$remote_status" | awk 'NR == 2 {print $1}')
  if [ "$source_hash" = "$deployed_hash" ] && [ "$expected_mode" = "$deployed_mode" ]; then
    printf 'CURRENT  %s\n' "$destination_file"
    return 0
  fi
  printf 'OUTDATED %s\n' "$destination_file"
  return 1
}

deploy_one() {
  local source_file="$1"
  local destination_file="$2"
  local mode="$3"
  local incoming_file="${destination_file}.incoming.$$"
  local destination_dir

  destination_dir=$(dirname "$destination_file")
  ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" \
    "mkdir -p '$destination_dir'" || return 1
  scp -q "${SSH_OPTS[@]}" "$source_file" \
    "$COORDINATOR:$incoming_file" || return 1
  ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" \
    "chmod '$mode' '$incoming_file' && mv -f '$incoming_file' '$destination_file'" || return 1
  check_one "$source_file" "$destination_file" "$mode" >/dev/null || return 1
  printf 'UPDATED  %s\n' "$destination_file"
}

process_one() {
  local source_file="$1"
  local destination_file="$2"
  local mode="$3"

  if [ ! -f "$source_file" ]; then
    echo "Deployment source is missing: $source_file" >&2
    return 1
  fi
  if [ "$CHECK_ONLY" -eq 1 ]; then
    check_one "$source_file" "$destination_file" "$mode"
  else
    deploy_one "$source_file" "$destination_file" "$mode"
  fi
}

write_version_marker() {
  local value="$1"
  local incoming_file="${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION.incoming.$$"

  printf '%s\n' "$value" | ssh "${SSH_OPTS[@]}" "$COORDINATOR" \
    "cat > '$incoming_file' && chmod 644 '$incoming_file' && mv -f '$incoming_file' '${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION'"
}

process_scheduler() {
  local status=0
  local file version deployed_version_status deployed_version deployed_version_mode absent

  if [ "$CHECK_ONLY" -eq 0 ] && [ -n "$(git -C "$REPO_DIR" status --porcelain)" ]; then
    echo "Scheduler deployment requires a clean repository." >&2
    return 1
  fi

  version=$(git -C "$REPO_DIR" rev-parse HEAD) || return 1
  if [ "$CHECK_ONLY" -eq 0 ]; then
    write_version_marker INCOMPLETE || return 1
  fi

  for file in start_workers.sh mmworker.sh mmshardworker.sh mmshardcoordinator.sh \
    mmhostdaemon.sh adaptive_scheduler.sh kill_workers.sh reconcile_queue_batch.sh; do
    if ! bash -n "$SNAPSHOT_DIR/$file"; then
      status=1
      continue
    fi
    process_one "$SNAPSHOT_DIR/$file" "${PARALLEL_WORKERS_ROOT}/$file" 755 || status=1
  done

  for file in adaptive_coordinator.py adaptive_calibration.py adaptive_claim.py adaptive_memory.py adaptive_recovery.py adaptive_workflow.py; do
    if ! python3 -c 'import ast, pathlib, sys; ast.parse(pathlib.Path(sys.argv[1]).read_text())' "$SNAPSHOT_DIR/$file"; then
      status=1
      continue
    fi
    process_one "$SNAPSHOT_DIR/$file" "${PARALLEL_WORKERS_ROOT}/$file" 755 || status=1
  done

  process_one "$SNAPSHOT_DIR/benchmarks/bench_job.R" \
    "${PARALLEL_WORKERS_ROOT}/stan_projects/test/benchmarks/bench_job.R" 755 || status=1
  process_one "$SNAPSHOT_DIR/benchmarks/make_queue_stan_sv_box.sh" \
    "${PARALLEL_WORKERS_ROOT}/stan_projects/test/benchmarks/make_queue_stan_sv_box.sh" 755 || status=1
  process_one "$SNAPSHOT_DIR/benchmarks/monitor_cluster_cpu.sh" \
    "${PARALLEL_WORKERS_ROOT}/stan_projects/test/benchmarks/monitor_cluster_cpu.sh" 755 || status=1
  process_one "$SNAPSHOT_DIR/benchmarks/summarize_results.R" \
    "${PARALLEL_WORKERS_ROOT}/stan_projects/test/benchmarks/summarize_results.R" 755 || status=1
  process_one "$SNAPSHOT_DIR/benchmarks/README.md" \
    "${PARALLEL_WORKERS_ROOT}/stan_projects/test/benchmarks/README.md" 644 || status=1
  process_one "$SNAPSHOT_DIR/benchmarks/Stan/models/stochastic_volatility.stan" \
    "${PARALLEL_WORKERS_ROOT}/stan_projects/test/benchmarks/Stan/models/stochastic_volatility.stan" 644 || status=1

  # Scripts the scheduler no longer uses (mmadaptiveagent.sh became mmhostdaemon.sh). Deployment
  # runs only with no pool marker and no scheduler process, so nothing can still execute them.
  for file in "${RETIRED_SCHEDULER_FILES[@]}"; do
    if [ "$CHECK_ONLY" -eq 1 ]; then
      absent=0
      ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" "[ ! -e '${PARALLEL_WORKERS_ROOT}/$file' ]" || absent=$?
      if [ "$absent" -eq 1 ]; then
        echo "RETIRED  ${PARALLEL_WORKERS_ROOT}/$file is still present"
        status=1
      elif [ "$absent" -ne 0 ]; then
        echo "Cannot check for retired ${PARALLEL_WORKERS_ROOT}/$file" >&2
        status=1
      fi
    elif ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" "rm -f '${PARALLEL_WORKERS_ROOT}/$file'"; then
      echo "REMOVED  ${PARALLEL_WORKERS_ROOT}/$file"
    else
      status=1
    fi
  done

  if [ "$CHECK_ONLY" -eq 1 ]; then
    deployed_version_status=$(ssh -n "${SSH_OPTS[@]}" "$COORDINATOR" \
      "if [ -f '${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION' ]; then
         cat '${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION'
         if mode=\$(/usr/bin/stat -f '%Lp' '${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION' 2>/dev/null); then
           printf '%s\\n' \"\$mode\"
         else
           /usr/bin/stat -c '%a' '${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION' 2>/dev/null
         fi
       fi" || true)
    deployed_version=$(printf '%s\n' "$deployed_version_status" | awk 'NR == 1 {print $1}')
    deployed_version_mode=$(printf '%s\n' "$deployed_version_status" | awk 'NR == 2 {print $1}')
    # Later commits that leave the scheduler sources unchanged do not need a redeploy.
    if [ "$deployed_version_mode" = 644 ] && [[ "$deployed_version" =~ ^[0-9a-f]{40}$ ]] &&
        git -C "$REPO_DIR" cat-file -e "${deployed_version}^{commit}" 2>/dev/null &&
        git -C "$REPO_DIR" diff --quiet "$deployed_version" "$version" -- scheduler; then
      echo "CURRENT  ${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION (${deployed_version:0:7})"
    else
      echo "OUTDATED ${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION"
      status=1
    fi
  else
    if [ "$status" -ne 0 ]; then
      return "$status"
    fi
    write_version_marker "$version" || return 1
    echo "UPDATED  ${PARALLEL_WORKERS_ROOT}/PARALLEL_WORKERS_VERSION"
  fi

  return "$status"
}

status=0
if [ "$MODE" != monitoring ]; then
  if [ "$CHECK_ONLY" -eq 0 ]; then
    audit_scheduler_idle || exit 1
  fi
  process_scheduler || status=1
fi

if [ "$MODE" != scheduler ]; then
  monitor_args=(--hosts_file "$HOSTS_FILE" --coordinator "$COORDINATOR")
  [ "$CHECK_ONLY" -eq 0 ] || monitor_args=(--check "${monitor_args[@]}")
  "$MONITOR_SYNC" "${monitor_args[@]}" || status=1
fi

if [ "$DEPLOY_LOCK_HELD" -eq 1 ]; then
  release_deploy_lock || status=1
fi
trap - EXIT

exit "$status"
