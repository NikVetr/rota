#!/usr/bin/env bash
export ROTA_ROOT="${ROTA_ROOT:?Source the inventory environment first}"

set -euo pipefail

HOSTS_FILE="${ROTA_HOSTS_FILE:-${ROTA_ROOT}/hosts/hosts-ethernet.txt}"
COORDINATOR="${ROTA_COORDINATOR:-}"
CHECK_ONLY=0

usage() {
  cat <<'EOF'
Usage: sync_rota_monitoring.sh [--check] [--hosts_file <path>] [--coordinator <user@host>]

Atomically update monitoring scripts at ROTA_ROOT and in
~/rota-monitor on every selected host. --check is read-only.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) CHECK_ONLY=1; shift ;;
    --hosts_file) HOSTS_FILE="$2"; shift 2 ;;
    --coordinator) COORDINATOR="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ ! -f "$HOSTS_FILE" ]; then
  echo "Hosts file not found: $HOSTS_FILE" >&2
  exit 1
fi

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MONITOR_DIR="$REPO_DIR/monitor"
[ -n "$COORDINATOR" ] || { echo "Source the inventory environment or pass --coordinator." >&2; exit 2; }
MONITOR_FILES=(
  start_monitoring.sh
  start_monitoring_remote.sh
  start_tmux_monitoring.sh
  start_tmux_monitoring_remote.sh
  monitor_queue_eta.py
  monitor_pool_eta.py
  monitor_queue_progress.sh
  monitor_queue_failures.py
  monitor_queue_pane.sh
  monitor_queue_collect.sh
  monitor_queue_workers.sh
  render_queue_progress.sh
  monitor_dashboard_layout.sh
  monitor_mount_status.sh
  monitor_cluster_io.sh
  monitor_cluster_io_rates.py
  monitor_storage.py
  monitor_cpu.py
  monitor_linux.py
  monitor_mac.py
  monitor_sample.py
  monitor_snapshot.py
  scheduler_snapshot.py
  monitor_htoprc
)

for file in "${MONITOR_FILES[@]}"; do
  if [ ! -f "$MONITOR_DIR/$file" ]; then
    echo "Monitoring source is missing: $MONITOR_DIR/$file" >&2
    exit 1
  fi
done

sync_target() {
  local host="$1"
  local target_kind="$2"
  local label="$3"
  local remote_code remote_command

  remote_code=$(cat <<'REMOTE'
set -euo pipefail

if [ "$target_kind" = shared ]; then
  target_dir=${ROTA_ROOT}
else
  target_dir="$HOME/rota-monitor"
fi
if [ "$check_only" -eq 1 ]; then
  stage=$(mktemp -d "${TMPDIR:-/tmp}/rota-monitor.XXXXXX")
else
  mkdir -p "$target_dir"
  stage=$(mktemp -d "$target_dir/.rota-monitor.XXXXXX")
fi
trap 'rm -rf "$stage"' EXIT
tar -xf - -C "$stage"

shell_files=(
  start_monitoring.sh
  start_monitoring_remote.sh
  start_tmux_monitoring.sh
  start_tmux_monitoring_remote.sh
  monitor_queue_progress.sh
  monitor_queue_pane.sh
  monitor_queue_collect.sh
  monitor_queue_workers.sh
  render_queue_progress.sh
  monitor_dashboard_layout.sh
  monitor_mount_status.sh
  monitor_cluster_io.sh
)
files=(monitor_queue_eta.py monitor_pool_eta.py "${shell_files[@]}" monitor_queue_failures.py monitor_cluster_io_rates.py monitor_storage.py monitor_cpu.py monitor_linux.py monitor_mac.py monitor_sample.py monitor_snapshot.py scheduler_snapshot.py monitor_htoprc)

file_mode() {
  local mode
  if mode=$(/usr/bin/stat -f '%Lp' "$1" 2>/dev/null); then
    printf '%s\n' "$mode"
  else
    /usr/bin/stat -c '%a' "$1" 2>/dev/null
  fi
}

expected_mode() {
  if [ "$1" = monitor_htoprc ]; then
    printf '644\n'
  else
    printf '755\n'
  fi
}

if [ "$check_only" -eq 1 ]; then
  outdated=0
  for file in "${files[@]}"; do
    mode=$(expected_mode "$file")
    if [ ! -f "$target_dir/$file" ] || \
       ! cmp -s "$stage/$file" "$target_dir/$file" || \
       [ "$(file_mode "$target_dir/$file")" != "$mode" ]; then
      printf 'OUTDATED %s/%s\n' "$target_dir" "$file"
      outdated=1
    fi
  done
  exit "$outdated"
fi

for file in "${shell_files[@]}"; do
  bash -n "$stage/$file"
done
python3 -m py_compile "$stage/monitor_pool_eta.py" "$stage/monitor_queue_eta.py" "$stage/monitor_queue_failures.py" "$stage/monitor_cluster_io_rates.py" "$stage/monitor_storage.py" "$stage/monitor_cpu.py" "$stage/monitor_linux.py" "$stage/monitor_mac.py" "$stage/monitor_sample.py" "$stage/monitor_snapshot.py" "$stage/scheduler_snapshot.py"

for file in "${files[@]}"; do
  mode=$(expected_mode "$file")
  if [ -f "$target_dir/$file" ] && \
     cmp -s "$stage/$file" "$target_dir/$file" && \
     [ "$(file_mode "$target_dir/$file")" = "$mode" ]; then
    continue
  fi
  if [ -f "$target_dir/$file" ] && cmp -s "$stage/$file" "$target_dir/$file"; then
    chmod "$mode" "$target_dir/$file"
  else
    chmod "$mode" "$stage/$file"
    mv -f "$stage/$file" "$target_dir/$file"
  fi
  printf 'UPDATED  %s/%s\n' "$target_dir" "$file"
done
REMOTE
  )
  remote_command="ROTA_ROOT=$(printf '%q' "$ROTA_ROOT") target_kind=$(printf '%q' "$target_kind") check_only=$CHECK_ONLY bash -c $(printf '%q' "$remote_code")"

  if [ "$CHECK_ONLY" -eq 1 ]; then
    if tar -cf - -C "$MONITOR_DIR" "${MONITOR_FILES[@]}" |
      ssh -o BatchMode=yes -o ConnectTimeout=8 "$host" "$remote_command"; then
      echo "CURRENT  $label"
      return 0
    fi
    return 1
  fi

  if ! tar -cf - -C "$MONITOR_DIR" "${MONITOR_FILES[@]}" |
    ssh -o BatchMode=yes -o ConnectTimeout=8 "$host" "$remote_command"; then
    echo "Monitoring sync failed for $label" >&2
    return 1
  fi
  echo "SYNCED   $label"
}

status=0
sync_target "$COORDINATOR" shared "shared root on $COORDINATOR" || status=1

while read -r host slots _; do
  [ -n "${host:-}" ] || continue
  [[ "$host" == \#* ]] && continue
  if ! [[ "${slots:-}" =~ ^[0-9]+$ ]]; then
    echo "Invalid slot count for $host in $HOSTS_FILE: ${slots:-<missing>}" >&2
    status=1
    continue
  fi
  [ "$slots" -gt 0 ] || [ -n "${ROTA_HOSTS_FILE:-}" ] || continue
  sync_target "$host" mirror "monitor mirror on $host" || status=1
done < "$HOSTS_FILE"

exit "$status"
