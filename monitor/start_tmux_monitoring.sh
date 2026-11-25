#!/usr/bin/env bash
export ROTA_ROOT="${ROTA_ROOT:?Source the inventory environment first}"
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SESSION="monitoring"
HOSTS_FILE="${ROTA_HOSTS_FILE:-${ROTA_ROOT}/hosts/hosts-ethernet.txt}"
HOSTS_OVERRIDE=""
LAYOUT="even-vertical"
SHOW_MEMORY_PANEL=1
MEM_PANE_WIDTH_PCT=8
MEM_PANE_WIDTH_COLS=26
MEM_WIDTH_MODE="cols"
MEM_REFRESH_SEC=1
RESIZE_TERMINAL=1
TERMINAL_COLS=180
TERMINAL_COLS_SET=0
TERMINAL_ROWS=""
SHOW_QUEUE_PROGRESS=1
PROGRESS_SCRIPT="${SCRIPT_DIR}/monitor_queue_progress.sh"
QUEUE_PANE_SCRIPT="${SCRIPT_DIR}/monitor_queue_pane.sh"
QUEUE_RENDER_SCRIPT="${SCRIPT_DIR}/render_queue_progress.sh"
QUEUE_WORKER_SCRIPT="${SCRIPT_DIR}/monitor_queue_workers.sh"
QUEUE_COLLECT_SCRIPT="${SCRIPT_DIR}/monitor_queue_collect.sh"
DASHBOARD_LAYOUT_SCRIPT="${SCRIPT_DIR}/monitor_dashboard_layout.sh"
MOUNT_STATUS_SCRIPT="${SCRIPT_DIR}/monitor_mount_status.sh"
CPU_SCRIPT="${SCRIPT_DIR}/monitor_cpu.py"
SNAPSHOT_SCRIPT="${SCRIPT_DIR}/monitor_snapshot.py"
# On Linux every pane renders the cluster status collector's snapshot and opens
# no SSH session; --direct samples each host from this launcher instead. The
# collector service exists only for Linux, so Mac launchers default to --direct.
DIRECT=0
[ "$(uname -s)" != Darwin ] || DIRECT=1
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/rota"
CLUSTER_SNAPSHOT="${ROTA_CLUSTER_SNAPSHOT:-$STATE_DIR/cluster-status.json}"
QUEUE_SNAPSHOT="${ROTA_QUEUE_SNAPSHOT:-$STATE_DIR/queue-progress.tsv}"
SNAPSHOT_MOUNT_REFRESH_SEC=5
SHOW_MOUNT_STATUS=1
MOUNT_PANE_WIDTH_COLS=8
MOUNT_REFRESH_SEC=60
PROGRESS_REFRESH_SEC=30
PROGRESS_LIMIT=0
PROGRESS_HEIGHT_PCT=18
PROGRESS_HEIGHT_SET=0
SHOW_CLUSTER_IO=1
CLUSTER_IO_SCRIPT="${SCRIPT_DIR}/monitor_cluster_io.sh"
CLUSTER_IO_WIDTH_COLS=32
CLUSTER_IO_REFRESH_SEC=3
LAUNCHER_HOST="$(hostname -s 2>/dev/null || hostname 2>/dev/null || echo unknown)"
MIN_PROGRESS_LINES=15
CPU_LINES_PER_HOST=3
TMUX_STATUS_LINES=1
DEFAULT_USER="$(id -un)"
QUEUE_FALLBACK_HOST="${ROTA_COORDINATOR:-}"
SSH_OPTS=(
  -tt
  -o BatchMode=yes
  -o ConnectTimeout=5
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=2
  -o StrictHostKeyChecking=accept-new
  -o ControlMaster=auto
  -o ControlPersist=600
  -o ControlPath=/tmp/rota-ssh-%C
)

# Reading long-lived UI helpers from an SMB-mounted script directory can stall
# a Mac dashboard during server pressure. Prefer synchronized local copies when
# this launcher itself came from the shared drive.
LOCAL_MONITOR_DIR="${HOME}/rota-monitor"
if [[ "$SCRIPT_DIR" == "$ROTA_ROOT" || "$SCRIPT_DIR" == "$ROTA_ROOT/"* ]] && [ -d "$LOCAL_MONITOR_DIR" ]; then
  [ -x "$LOCAL_MONITOR_DIR/monitor_queue_progress.sh" ] && PROGRESS_SCRIPT="$LOCAL_MONITOR_DIR/monitor_queue_progress.sh"
  [ -x "$LOCAL_MONITOR_DIR/monitor_queue_pane.sh" ] && QUEUE_PANE_SCRIPT="$LOCAL_MONITOR_DIR/monitor_queue_pane.sh"
  [ -x "$LOCAL_MONITOR_DIR/render_queue_progress.sh" ] && QUEUE_RENDER_SCRIPT="$LOCAL_MONITOR_DIR/render_queue_progress.sh"
  [ -x "$LOCAL_MONITOR_DIR/monitor_queue_workers.sh" ] && QUEUE_WORKER_SCRIPT="$LOCAL_MONITOR_DIR/monitor_queue_workers.sh"
  [ -x "$LOCAL_MONITOR_DIR/monitor_queue_collect.sh" ] && QUEUE_COLLECT_SCRIPT="$LOCAL_MONITOR_DIR/monitor_queue_collect.sh"
  [ -x "$LOCAL_MONITOR_DIR/monitor_dashboard_layout.sh" ] && DASHBOARD_LAYOUT_SCRIPT="$LOCAL_MONITOR_DIR/monitor_dashboard_layout.sh"
  [ -x "$LOCAL_MONITOR_DIR/monitor_mount_status.sh" ] && MOUNT_STATUS_SCRIPT="$LOCAL_MONITOR_DIR/monitor_mount_status.sh"
  [ -x "$LOCAL_MONITOR_DIR/monitor_cluster_io.sh" ] && CLUSTER_IO_SCRIPT="$LOCAL_MONITOR_DIR/monitor_cluster_io.sh"
  [ -x "$LOCAL_MONITOR_DIR/monitor_cpu.py" ] && CPU_SCRIPT="$LOCAL_MONITOR_DIR/monitor_cpu.py"
  [ -x "$LOCAL_MONITOR_DIR/monitor_snapshot.py" ] && SNAPSHOT_SCRIPT="$LOCAL_MONITOR_DIR/monitor_snapshot.py"
fi

usage() {
  cat <<EOF
CPU monitor (tmux) for the cluster (local/ethernet).

Usage:
  $0 [--direct|--snapshot] [--hosts_file <path>] [--hosts "<h1> <h2> ..."] [--session <name>] [--layout <tmux_layout>] [--cpu_only] [--mem_width_cols <n>] [--mem_width_pct <n>] [--mem_refresh_sec <n>] [--no_cluster_io] [--cluster_io_width_cols <n>] [--cluster_io_refresh_sec <n>] [--cluster_io_script <path>] [--terminal_cols <n>] [--terminal_rows <n>] [--no_resize_terminal] [--no_queue_progress] [--progress_refresh_sec <n>] [--progress_limit <n>] [--progress_height_pct <n>] [--progress_script <path>] [--queue_fallback_host <user@host>]

Defaults:
  --hosts_file $HOSTS_FILE
  --layout $LAYOUT
  --mem_width_cols $MEM_PANE_WIDTH_COLS
  --mem_width_pct $MEM_PANE_WIDTH_PCT (legacy override)
  --mem_refresh_sec $MEM_REFRESH_SEC
  --cluster_io_width_cols $CLUSTER_IO_WIDTH_COLS
  --cluster_io_refresh_sec $CLUSTER_IO_REFRESH_SEC
  --terminal_cols $TERMINAL_COLS
  --terminal_rows ${TERMINAL_ROWS:-compact auto-size}
  --progress_refresh_sec $PROGRESS_REFRESH_SEC
  --progress_limit $PROGRESS_LIMIT
  --progress_height_pct $PROGRESS_HEIGHT_PCT

Notes:
- Panes render $CLUSTER_SNAPSHOT, written by the
  rota-collector user service, and open no SSH sessions. Without
  a fresh snapshot they say so; start the service with
  systemctl --user start rota-collector.service
- --direct samples every host over SSH from this launcher instead (the
  default on a Mac, which has no collector); --hosts requires it.
  --snapshot selects snapshot mode explicitly.
- Hosts can be raw names/IPs or user@host.
- Default view is CPU pane (left) + memory pane (right) per host.
- A shared-mount status column occupies the left and coordinator I/O occupies the right.
- I/O width is at least --cluster_io_width_cols and grows to fit node disk space.
- Use --no_mount_status to omit the mount column.
- Pane titles show CPU and MEM labels at the top border.
- Default resize behavior uses a compact height and a queue-safe width.
- By default, a bottom pane shows queue progress snapshots refreshed every 30s.
- Queue rows open in tmux copy mode; use the mouse wheel or arrow/PageUp/PageDown
  keys to scroll, and press q to leave copy mode until the next refresh.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --direct) DIRECT=1; shift ;;
    --snapshot) DIRECT=0; shift ;;
    --hosts_file) HOSTS_FILE="$2"; shift 2 ;;
    --hosts) HOSTS_OVERRIDE="$2"; shift 2 ;;
    --session) SESSION="$2"; shift 2 ;;
    --layout) LAYOUT="$2"; shift 2 ;;
    --cpu_only) SHOW_MEMORY_PANEL=0; shift ;;
    --mem_width_cols) MEM_PANE_WIDTH_COLS="$2"; MEM_WIDTH_MODE="cols"; shift 2 ;;
    --mem_width_pct) MEM_PANE_WIDTH_PCT="$2"; MEM_WIDTH_MODE="pct"; shift 2 ;;
    --mem_refresh_sec) MEM_REFRESH_SEC="$2"; shift 2 ;;
    --no_cluster_io) SHOW_CLUSTER_IO=0; shift ;;
    --cluster_io_width_cols) CLUSTER_IO_WIDTH_COLS="$2"; shift 2 ;;
    --cluster_io_refresh_sec) CLUSTER_IO_REFRESH_SEC="$2"; shift 2 ;;
    --cluster_io_script) CLUSTER_IO_SCRIPT="$2"; shift 2 ;;
    --terminal_cols) TERMINAL_COLS="$2"; TERMINAL_COLS_SET=1; shift 2 ;;
    --terminal_rows) TERMINAL_ROWS="$2"; shift 2 ;;
    --no_resize_terminal) RESIZE_TERMINAL=0; shift ;;
    --no_queue_progress) SHOW_QUEUE_PROGRESS=0; shift ;;
    --no_mount_status) SHOW_MOUNT_STATUS=0; shift ;;
    --progress_refresh_sec) PROGRESS_REFRESH_SEC="$2"; shift 2 ;;
    --progress_limit) PROGRESS_LIMIT="$2"; shift 2 ;;
    --progress_height_pct) PROGRESS_HEIGHT_PCT="$2"; PROGRESS_HEIGHT_SET=1; shift 2 ;;
    --progress_script) PROGRESS_SCRIPT="$2"; shift 2 ;;
    --queue_fallback_host) QUEUE_FALLBACK_HOST="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if ! [[ "$MEM_PANE_WIDTH_PCT" =~ ^[0-9]+$ ]] || [ "$MEM_PANE_WIDTH_PCT" -lt 8 ] || [ "$MEM_PANE_WIDTH_PCT" -gt 40 ]; then
  echo "--mem_width_pct must be an integer between 8 and 40" >&2
  exit 2
fi

if ! [[ "$MEM_PANE_WIDTH_COLS" =~ ^[0-9]+$ ]] || [ "$MEM_PANE_WIDTH_COLS" -lt 24 ] || [ "$MEM_PANE_WIDTH_COLS" -gt 120 ]; then
  echo "--mem_width_cols must be an integer between 24 and 120" >&2
  exit 2
fi

if ! [[ "$MEM_REFRESH_SEC" =~ ^[0-9]+$ ]] || [ "$MEM_REFRESH_SEC" -lt 1 ] || [ "$MEM_REFRESH_SEC" -gt 30 ]; then
  echo "--mem_refresh_sec must be an integer between 1 and 30" >&2
  exit 2
fi

if ! [[ "$CLUSTER_IO_WIDTH_COLS" =~ ^[0-9]+$ ]] || [ "$CLUSTER_IO_WIDTH_COLS" -lt 30 ] || [ "$CLUSTER_IO_WIDTH_COLS" -gt 100 ]; then
  echo "--cluster_io_width_cols must be an integer between 30 and 100" >&2
  exit 2
fi

if ! [[ "$CLUSTER_IO_REFRESH_SEC" =~ ^[0-9]+$ ]] || [ "$CLUSTER_IO_REFRESH_SEC" -lt 2 ] || [ "$CLUSTER_IO_REFRESH_SEC" -gt 30 ]; then
  echo "--cluster_io_refresh_sec must be an integer between 2 and 30" >&2
  exit 2
fi

if ! [[ "$TERMINAL_COLS" =~ ^[0-9]+$ ]] || [ "$TERMINAL_COLS" -lt 120 ] || [ "$TERMINAL_COLS" -gt 400 ]; then
  echo "--terminal_cols must be an integer between 120 and 400" >&2
  exit 2
fi

if [ -n "$TERMINAL_ROWS" ]; then
  if ! [[ "$TERMINAL_ROWS" =~ ^[0-9]+$ ]] || [ "$TERMINAL_ROWS" -lt 24 ] || [ "$TERMINAL_ROWS" -gt 200 ]; then
    echo "--terminal_rows must be an integer between 24 and 200" >&2
    exit 2
  fi
fi

if ! [[ "$PROGRESS_REFRESH_SEC" =~ ^[0-9]+$ ]] || [ "$PROGRESS_REFRESH_SEC" -lt 10 ] || [ "$PROGRESS_REFRESH_SEC" -gt 600 ]; then
  echo "--progress_refresh_sec must be an integer between 10 and 600" >&2
  exit 2
fi

if ! [[ "$PROGRESS_LIMIT" =~ ^[0-9]+$ ]] || [ "$PROGRESS_LIMIT" -gt 1000 ]; then
  echo "--progress_limit must be an integer between 0 and 1000 (0=all selected rows)" >&2
  exit 2
fi

if ! [[ "$PROGRESS_HEIGHT_PCT" =~ ^[0-9]+$ ]] || [ "$PROGRESS_HEIGHT_PCT" -lt 10 ] || [ "$PROGRESS_HEIGHT_PCT" -gt 50 ]; then
  echo "--progress_height_pct must be an integer between 10 and 50" >&2
  exit 2
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux not found. Install tmux first." >&2
  exit 1
fi

if [ "$DIRECT" -eq 0 ] && [ -n "$HOSTS_OVERRIDE" ]; then
  echo "--hosts requires --direct: the collector samples the hosts in its own hosts file" >&2
  exit 2
fi
if [ "$DIRECT" -eq 0 ] && [ ! -x "$SNAPSHOT_SCRIPT" ]; then
  echo "Snapshot renderer not executable: $SNAPSHOT_SCRIPT" >&2
  exit 1
fi
if [ "$SHOW_QUEUE_PROGRESS" -eq 1 ] && [ "$DIRECT" -eq 1 ] && [ ! -x "$PROGRESS_SCRIPT" ]; then
  echo "Queue progress script not executable: $PROGRESS_SCRIPT" >&2
  echo "Run: chmod +x $PROGRESS_SCRIPT" >&2
  exit 1
fi
if [ "$SHOW_QUEUE_PROGRESS" -eq 1 ]; then
  queue_helpers=("$QUEUE_PANE_SCRIPT" "$QUEUE_RENDER_SCRIPT" "$DASHBOARD_LAYOUT_SCRIPT")
  [ "$DIRECT" -eq 0 ] || queue_helpers+=("$QUEUE_WORKER_SCRIPT" "$QUEUE_COLLECT_SCRIPT")
  for helper in "${queue_helpers[@]}"; do
    if [ ! -x "$helper" ]; then
      echo "Queue progress helper not executable: $helper" >&2
      echo "Run: chmod +x $helper" >&2
      exit 1
    fi
  done
fi

if [ "$SHOW_CLUSTER_IO" -eq 1 ] && [ ! -x "$CLUSTER_IO_SCRIPT" ]; then
  echo "Cluster I/O script not executable: $CLUSTER_IO_SCRIPT" >&2
  echo "Run: chmod +x $CLUSTER_IO_SCRIPT" >&2
  exit 1
fi
if [ "$SHOW_MOUNT_STATUS" -eq 1 ] && [ ! -x "$MOUNT_STATUS_SCRIPT" ]; then
  echo "Mount status helper not executable: $MOUNT_STATUS_SCRIPT" >&2
  exit 1
fi

hosts=()
if [[ -n "$HOSTS_OVERRIDE" ]]; then
  # shellcheck disable=SC2206
  hosts=($HOSTS_OVERRIDE)
elif [[ -f "$HOSTS_FILE" ]]; then
  while read -r host _; do
    [[ -z "${host:-}" ]] && continue
    [[ "$host" == \#* ]] && continue
    hosts+=("$host")
  done < "$HOSTS_FILE"
elif [[ -n "${ROTA_HOSTS_FILE:-}" ]]; then
  echo "Configured hosts file is unavailable: $HOSTS_FILE" >&2
  exit 1
else
  echo "Source the inventory environment or supply --hosts_file." >&2
  exit 1
fi

if [[ "${#hosts[@]}" -lt 1 ]]; then
  echo "No hosts found." >&2
  exit 1
fi

if [ "$SHOW_QUEUE_PROGRESS" -eq 1 ] && [ "$PROGRESS_HEIGHT_SET" -eq 0 ]; then
  if [ "${#hosts[@]}" -ge 4 ]; then
    PROGRESS_HEIGHT_PCT=28
  elif [ "${#hosts[@]}" -eq 3 ]; then
    PROGRESS_HEIGHT_PCT=24
  else
    PROGRESS_HEIGHT_PCT=20
  fi
fi

# Three lines give all ten cores comfortably wide meters by default.
# Size the default window to those rows exactly so integer layout remainders do
# not leave a different amount of blank space beneath individual hosts.
# Include the top pane-border row as well as the separators between hosts.
cpu_region_lines=$(( ${#hosts[@]} * CPU_LINES_PER_HOST + ${#hosts[@]} ))
AUTO_WINDOW_ROWS="$cpu_region_lines"
if [ "$SHOW_QUEUE_PROGRESS" -eq 1 ]; then
  AUTO_WINDOW_ROWS=$((cpu_region_lines + 1 + MIN_PROGRESS_LINES))
  pct_rows=$(( (cpu_region_lines + 1) * 100 + (100 - PROGRESS_HEIGHT_PCT) - 1 ))
  pct_rows=$((pct_rows / (100 - PROGRESS_HEIGHT_PCT)))
  [ "$AUTO_WINDOW_ROWS" -lt "$pct_rows" ] && AUTO_WINDOW_ROWS="$pct_rows"
fi
# tmux reports window height without its status row, while the terminal resize
# escape addresses the full client grid. Reserve that row explicitly.
AUTO_TERMINAL_ROWS=$((AUTO_WINDOW_ROWS + TMUX_STATUS_LINES))

mk_label() {
  local h="$1"
  local hh="${h##*@}"
  local configured=""
  if [ -f "$HOSTS_FILE" ]; then
    configured=$(awk -v host="$h" '$1 == host && NF >= 3 {print $3; exit}' "$HOSTS_FILE")
    if [ -n "$configured" ]; then printf '%s\n' "$configured"; return; fi
  fi
  printf '%s\n' "${configured:-$hh}"
}

if [ "$SHOW_CLUSTER_IO" -eq 1 ]; then
  storage_width_args=(python3 "$(dirname "$CLUSTER_IO_SCRIPT")/monitor_storage.py" width)
  for storage_host in "${hosts[@]}"; do
    storage_label="$(mk_label "$storage_host")"
    storage_width_args+=(--node-label "$storage_label")
  done
  storage_width="$("${storage_width_args[@]}")"
  [ "$CLUSTER_IO_WIDTH_COLS" -ge "$storage_width" ] || CLUSTER_IO_WIDTH_COLS="$storage_width"
fi

mk_ssh_cmd() {
  local h="$1"
  local remote_cmd="$2"
  local reset_title="${3:-}"
  local label cmd opt

  label="$(mk_label "$h")"
  cmd="while true; do ssh"
  for opt in "${SSH_OPTS[@]}"; do
    cmd+=" $(printf "%q" "$opt")"
  done
  cmd+=" $(printf "%q" "$h") $(printf "%q" "$remote_cmd")"
  cmd+="; rc=\$?"
  # A disconnected stream must not leave its last CPU total in the pane title.
  [ -z "$reset_title" ] || cmd+="; printf '\\033]2;%s\\033\\\\' $(printf "%q" "$reset_title")"
  cmd+="; printf '\\n[monitor] %s ssh exited %s; retrying in 5s...\\n' $(printf "%q" "$label") \"\$rc\"; sleep 5; done"

  printf "bash -lc %q" "$cmd"
}

# Snapshot-mode panes run local renderers; one that exits restarts after 5 s.
mk_local_cmd() {
  local label="$1" reset_title="$2" cmd
  shift 2
  printf -v cmd '%q ' "$@"
  cmd="while true; do ${cmd}; rc=\$?"
  [ -z "$reset_title" ] || cmd+="; printf '\\033]2;%s\\033\\\\' $(printf "%q" "$reset_title")"
  cmd+="; printf '\\n[monitor] %s renderer exited %s; retrying in 5s...\\n' $(printf "%q" "$label") \"\$rc\"; sleep 5; done"
  printf "bash -c %q" "$cmd"
}

cpu_title_suffix() {
  [ "$1" -ne 0 ] || printf '%s' ' ─── * = Sys, # = User ────'
}

# Each CPU stream titles its own pane with the node total after every sample.
mk_cpu_pane_cmd() {
  local index="$1" label suffix remote
  label="$(mk_label "${hosts[$index]}")"
  suffix="$(cpu_title_suffix "$index")"
  if [ "$DIRECT" -eq 0 ]; then
    mk_local_cmd "$label" "CPU $label$suffix" python3 "$CPU_SCRIPT" --snapshot "$CLUSTER_SNAPSHOT" \
      --host "$label" --title-prefix "CPU $label" "--title-suffix=$suffix"
    return
  fi
  remote="export PATH=\"/opt/homebrew/bin:/usr/local/bin:\$PATH\"; exec python3 \"\$HOME/rota-monitor/monitor_cpu.py\""
  remote+=" --title-prefix $(printf '%q' "CPU $label") --title-suffix=$(printf '%q' "$suffix")"
  mk_ssh_cmd "${hosts[$index]}" "$(printf 'exec /bin/bash -lc %q' "$remote")" "CPU $label$suffix"
}

# A snapshot-only dashboard also accepts just a hosts file. The collector records
# the coordinator label, even when its host stream is temporarily unavailable.
if [ -z "$QUEUE_FALLBACK_HOST" ] && [ "$DIRECT" -eq 0 ]; then
  QUEUE_FALLBACK_HOST="$(python3 - "$CLUSTER_SNAPSHOT" <<'PYTHON'
import json
import sys
try:
    with open(sys.argv[1]) as stream:
        print(json.load(stream)["shared"]["host"] or "coordinator")
except (OSError, ValueError, KeyError, TypeError):
    print("coordinator")
PYTHON
  )"
fi
if [ -z "$QUEUE_FALLBACK_HOST" ] && { [ "$SHOW_CLUSTER_IO" -eq 1 ] || [ "$SHOW_QUEUE_PROGRESS" -eq 1 ]; }; then
  echo "Source the inventory environment or supply --queue_fallback_host for the coordinator." >&2
  exit 1
fi

REMOTE_MEM_CMD_TEMPLATE=$(cat <<'EOF'
if [ "$(uname -s)" = Linux ]; then
  exec python3 "$HOME/rota-monitor/monitor_linux.py" memory --interval __MEM_REFRESH_SEC__
fi
exec python3 "$HOME/rota-monitor/monitor_mac.py" memory --interval __MEM_REFRESH_SEC__
EOF
)
REMOTE_MEM_CMD="${REMOTE_MEM_CMD_TEMPLATE//__MEM_REFRESH_SEC__/$MEM_REFRESH_SEC}"

mk_mem_pane_cmd() {
  local label
  label="$(mk_label "${hosts[$1]}")"
  if [ "$DIRECT" -eq 0 ]; then
    mk_local_cmd "$label" "" python3 "$SNAPSHOT_SCRIPT" memory --snapshot "$CLUSTER_SNAPSHOT" --host "$label"
  else
    mk_ssh_cmd "${hosts[$1]}" "$REMOTE_MEM_CMD"
  fi
}

session_rows=""
session_cols=""
if [ -t 1 ]; then
  read -r session_rows session_cols < <(stty size 2>/dev/null || true)
fi

if [ "$RESIZE_TERMINAL" -eq 1 ] && [ -t 1 ]; then
  # Request terminal window resize (supported by most modern terminal emulators).
  rows="${TERMINAL_ROWS:-$AUTO_TERMINAL_ROWS}"
  cols="$TERMINAL_COLS"
  if [ "$TERMINAL_COLS_SET" -eq 0 ]; then
    current_cols="$(stty size 2>/dev/null | awk '{print $2}')"
    if [[ "$current_cols" =~ ^[0-9]+$ ]] && [ "$current_cols" -gt "$cols" ]; then
      cols="$current_cols"
    fi
  fi
  if [[ "$rows" =~ ^[0-9]+$ ]]; then
    target_min_rows="$AUTO_TERMINAL_ROWS"
    if [ "$rows" -lt "$target_min_rows" ]; then
      rows="$target_min_rows"
    fi
  fi
  if [[ "$rows" =~ ^[0-9]+$ ]] && [[ "$cols" =~ ^[0-9]+$ ]]; then
    session_rows="$rows"
    session_cols="$cols"
    printf '\033[8;%s;%st' "$rows" "$cols" || true
  fi
fi

if ! [[ "$session_cols" =~ ^[0-9]+$ ]]; then
  session_cols="$TERMINAL_COLS"
fi
if ! [[ "$session_rows" =~ ^[0-9]+$ ]]; then
  session_rows="$AUTO_TERMINAL_ROWS"
fi

replace_monitor_session() {
  local staged="${SESSION}-starting-$$" pane
  # Keep a session alive throughout replacement: killing the last one can
  # race tmux's exit-empty shutdown against the next new-session request.
  pane=$(tmux new-session -d -P -F "#{pane_id}" -x "$session_cols" -y "$session_rows" \
    -s "$staged" -n mac-minis "$(mk_cpu_pane_cmd 0)") || return 1
  if tmux has-session -t "=$SESSION" 2>/dev/null; then
    if ! tmux kill-session -t "=$SESSION"; then
      tmux kill-session -t "=$staged"
      return 1
    fi
  fi
  if ! tmux rename-session -t "=$staged" "$SESSION"; then
    tmux kill-session -t "=$staged"
    return 1
  fi
  printf '%s\n' "$pane"
}

first_cpu_pane=$(replace_monitor_session)
MONITOR_INSTANCE="${SESSION}-$$-$(date +%s)-${RANDOM}"
tmux set-option -t "$SESSION" @monitor_instance "$MONITOR_INSTANCE"
tmux set-option -t "$SESSION" status on
tmux set-option -t "$SESSION" mouse on
tmux set-window-option -t "$SESSION":0 history-limit 50000

cpu_panes=()
cpu_panes[0]="$first_cpu_pane"
for ((i = ${#hosts[@]} - 1; i >= 1; i--)); do
  # Allocate one row initially; repeated halving runs out before six nodes exist.
  new_cpu_pane=$(tmux split-window -v -l 1 -t "$first_cpu_pane" -P -F "#{pane_id}" "$(mk_cpu_pane_cmd "$i")")
  cpu_panes[$i]="$new_cpu_pane"
done

tmux select-layout -t "$SESSION":0 "$LAYOUT" >/dev/null 2>&1 || tmux select-layout -t "$SESSION":0 even-vertical

for ((i = 0; i < ${#hosts[@]}; i++)); do
  tmux select-pane -t "${cpu_panes[$i]}" -T "CPU $(mk_label "${hosts[$i]}")$(cpu_title_suffix "$i")" || true
done

cluster_io_pane=""
if [ "$SHOW_CLUSTER_IO" -eq 1 ] && [ "$DIRECT" -eq 0 ]; then
  # The panel and its storage lines come from the collector's coordinator stream.
  cluster_io_pane=$(tmux split-window -f -h -t "${cpu_panes[0]}" -l "$CLUSTER_IO_WIDTH_COLS" -P -F "#{pane_id}" \
    "$(mk_local_cmd "CLUSTER I/O" "" bash "$CLUSTER_IO_SCRIPT" --snapshot "$CLUSTER_SNAPSHOT" \
      --root "$ROTA_ROOT" --refresh_sec "$CLUSTER_IO_REFRESH_SEC")")
  tmux select-pane -t "$cluster_io_pane" -T "CLUSTER I/O $(mk_label "$QUEUE_FALLBACK_HOST")" || true
elif [ "$SHOW_CLUSTER_IO" -eq 1 ]; then
  io_script_name="$(basename "$CLUSTER_IO_SCRIPT")"
  io_rates_name="monitor_cluster_io_rates.py"
  storage_file="/tmp/rota-storage-${MONITOR_INSTANCE}.json"
  printf -v root_q '%q' "$ROTA_ROOT"
  printf -v io_script_q '%q' "$ROTA_ROOT/$io_script_name"
  printf -v io_rates_q '%q' "$ROTA_ROOT/$io_rates_name"
  io_options=" --rates_state_file $(printf '%q' "/tmp/rota-io-rates-${MONITOR_INSTANCE}.json")"
  [ -z "${ROTA_NETWORK_INTERFACE:-}" ] || io_options+=" --interface $(printf '%q' "$ROTA_NETWORK_INTERFACE")"
  [ -z "${ROTA_DISK:-}" ] || io_options+=" --disk $(printf '%q' "$ROTA_DISK")"
  remote_io_cmd="if [ -f $io_script_q ] && [ -f $io_rates_q ]; then exec bash $io_script_q --root $root_q$io_options --refresh_sec $CLUSTER_IO_REFRESH_SEC --storage_file $(printf '%q' "$storage_file") --rates_script $io_rates_q; elif [ -f \"\$HOME/rota-monitor/$io_script_name\" ] && [ -f \"\$HOME/rota-monitor/$io_rates_name\" ]; then exec bash \"\$HOME/rota-monitor/$io_script_name\" --root $root_q$io_options --refresh_sec $CLUSTER_IO_REFRESH_SEC --storage_file $(printf '%q' "$storage_file") --rates_script \"\$HOME/rota-monitor/$io_rates_name\"; else echo 'Missing cluster I/O monitor scripts'; sleep 30; fi"


  io_cols="$CLUSTER_IO_WIDTH_COLS"
  cluster_io_pane=$(tmux split-window -f -h -t "${cpu_panes[0]}" -l "$io_cols" -P -F "#{pane_id}" "$(mk_ssh_cmd "$QUEUE_FALLBACK_HOST" "$remote_io_cmd")")
  tmux select-pane -t "$cluster_io_pane" -T "CLUSTER I/O $(mk_label "$QUEUE_FALLBACK_HOST")" || true
  storage_args=(python3 "$(dirname "$CLUSTER_IO_SCRIPT")/monitor_storage.py" watch
    --host "$QUEUE_FALLBACK_HOST" --file "$storage_file" --session "$SESSION" --instance "$MONITOR_INSTANCE"
    --root "$ROTA_ROOT")
  for storage_host in "${hosts[@]}"; do
    storage_label="$(mk_label "$storage_host")"
    storage_args+=(--mini-host "$storage_label=$storage_host")
  done
  printf -v storage_cmd '%q ' "${storage_args[@]}"
  tmux run-shell -b "$storage_cmd"
fi

mem_panes=()
if [ "$SHOW_MEMORY_PANEL" -eq 1 ]; then
  for ((i = 0; i < ${#hosts[@]}; i++)); do
    if [ "$MEM_WIDTH_MODE" = "cols" ]; then
      pane_cols="$(tmux display-message -p -t "${cpu_panes[$i]}" "#{pane_width}" 2>/dev/null || echo 0)"
      mem_cols="$MEM_PANE_WIDTH_COLS"
      if [[ "$pane_cols" =~ ^[0-9]+$ ]] && [ "$pane_cols" -gt 0 ]; then
        max_cols=$((pane_cols - 22))
        if [ "$max_cols" -lt 24 ]; then
          max_cols=24
        fi
        if [ "$mem_cols" -gt "$max_cols" ]; then
          mem_cols="$max_cols"
        fi
      fi
      mem_pane=$(tmux split-window -h -t "${cpu_panes[$i]}" -l "$mem_cols" -P -F "#{pane_id}" "$(mk_mem_pane_cmd "$i")")
    else
      mem_pane=$(tmux split-window -h -t "${cpu_panes[$i]}" -p "$MEM_PANE_WIDTH_PCT" -P -F "#{pane_id}" "$(mk_mem_pane_cmd "$i")")
    fi
    mem_panes[$i]="$mem_pane"
    if [ "$i" -eq 0 ]; then
      tmux select-pane -t "$mem_pane" -T "MEM $(mk_label "${hosts[$i]}") ─── # = Used ────" || true
    else
      tmux select-pane -t "$mem_pane" -T "MEM $(mk_label "${hosts[$i]}")" || true
    fi
  done
fi

mount_pane=""
if [ "$SHOW_MOUNT_STATUS" -eq 1 ]; then
  mount_args=(bash "$MOUNT_STATUS_SCRIPT" --root "$ROTA_ROOT" --hosts "${hosts[*]}" --cpu_panes "${cpu_panes[*]}")
  if [ "$DIRECT" -eq 0 ]; then
    mount_labels=""
    for h in "${hosts[@]}"; do mount_labels+="${mount_labels:+ }$(mk_label "$h")"; done
    # Rereading the snapshot is cheap, so state changes show within seconds.
    mount_args+=(--snapshot "$CLUSTER_SNAPSHOT" --snapshot_script "$SNAPSHOT_SCRIPT"
      --host_labels "$mount_labels" --refresh_sec "$SNAPSHOT_MOUNT_REFRESH_SEC")
  else
    mount_args+=(--refresh_sec "$MOUNT_REFRESH_SEC")
  fi
  mount_cmd=""
  for mount_arg in "${mount_args[@]}"; do
    mount_cmd+="$(printf '%q ' "$mount_arg")"
  done
  mount_pane=$(tmux split-window -b -f -h -t "${cpu_panes[0]}" \
    -l "$MOUNT_PANE_WIDTH_COLS" -P -F "#{pane_id}" "$mount_cmd")
  mount_title=Share
  tmux select-pane -t "$mount_pane" -T "$mount_title"
  tmux set-option -p -t "$mount_pane" remain-on-exit on
fi

progress_pane=""
if [ "$SHOW_QUEUE_PROGRESS" -eq 1 ]; then
  window_rows="$(tmux display-message -p -t "$SESSION":0 "#{window_height}" 2>/dev/null || echo 0)"
  if ! [[ "$window_rows" =~ ^[0-9]+$ ]] || [ "$window_rows" -le 0 ]; then
    window_rows=30
  fi
  progress_lines=$(( window_rows * PROGRESS_HEIGHT_PCT / 100 ))
  if [ "$progress_lines" -lt "$MIN_PROGRESS_LINES" ]; then
    progress_lines="$MIN_PROGRESS_LINES"
  fi
  # Keep enough top-pane height so CPU/MEM rows remain visible.
  cpu_top_min_lines=$(( ${#hosts[@]} * CPU_LINES_PER_HOST + ${#hosts[@]} ))
  if [ "$cpu_top_min_lines" -lt 10 ]; then
    cpu_top_min_lines=10
  fi
  max_progress_lines=$(( window_rows - cpu_top_min_lines - 1 ))
  if [ "$max_progress_lines" -lt 3 ]; then
    max_progress_lines=3
  fi
  if [ "$progress_lines" -gt "$max_progress_lines" ]; then
    progress_lines="$max_progress_lines"
  fi

  # The coordinator reads queue metadata from its locally attached drive.
  queue_progress_mode="remote"
  coordinator_name="${QUEUE_FALLBACK_HOST#*@}"
  if [[ "$LAUNCHER_HOST" == "$coordinator_name" || "$LAUNCHER_HOST.local" == "$coordinator_name" ||
        ( -z "${ROTA_HOSTS_FILE:-}" && "${LAUNCHER_HOST%%.*}" == "$(mk_label "$QUEUE_FALLBACK_HOST")" ) ]]; then
    queue_progress_mode="local"
  fi

  progress_hosts="${hosts[*]}"
  progress_host_labels=""
  for progress_host in "${hosts[@]}"; do
    progress_label="$(mk_label "$progress_host")"
    progress_host_labels+="${progress_host_labels:+ }${progress_label}"
  done
  progress_args=(
    bash "$QUEUE_PANE_SCRIPT"
    --renderer_script "$QUEUE_RENDER_SCRIPT"
    --host_labels "$progress_host_labels"
    --instance "$MONITOR_INSTANCE"
  )
  if [ "$DIRECT" -eq 0 ]; then
    # The collector is the only queue collector; this pane renders its TSV.
    progress_args+=(
      --root "$ROTA_ROOT"
      --snapshot_tsv "$QUEUE_SNAPSHOT"
      --snapshot_json "$CLUSTER_SNAPSHOT"
      --snapshot_script "$SNAPSHOT_SCRIPT"
    )
  else
    progress_args+=(
      --root "$ROTA_ROOT"
      --collector_script "$PROGRESS_SCRIPT"
      --collect_script "$QUEUE_COLLECT_SCRIPT"
      --worker_script "$QUEUE_WORKER_SCRIPT"
      --hosts "$progress_hosts"
      --fallback_host "$QUEUE_FALLBACK_HOST"
      --mode "$queue_progress_mode"
      --refresh_sec "$PROGRESS_REFRESH_SEC"
      --limit "$PROGRESS_LIMIT"
    )
  fi
  progress_pane=$(tmux split-window -f -v -t "$SESSION":0 -l "$progress_lines" -P -F "#{pane_id}" "while true; do sleep 3600; done")
  tmux select-pane -t "$progress_pane" -T "QUEUE PROGRESS" || true
  tmux set-option -w -t "$progress_pane" @queue_progress_pane "$progress_pane"
  progress_supervisor_args=("${progress_args[@]}" --pane "$progress_pane")
  progress_supervisor_cmd=""
  for progress_arg in "${progress_supervisor_args[@]}"; do
    progress_supervisor_cmd+="$(printf '%q ' "$progress_arg")"
  done
  tmux run-shell -b "$progress_supervisor_cmd"
fi

tmux set-option -t "$SESSION":0 pane-border-status top
tmux set-option -t "$SESSION":0 pane-border-format " #{pane_title}#{?pane_in_mode, | scroll #{scroll_position}/#{history_size},}"
tmux set-option -t "$SESSION":0 pane-border-style "fg=default"
tmux set-option -t "$SESSION":0 pane-active-border-style "fg=default"

if [ -n "$cluster_io_pane" ] && [ -n "$progress_pane" ] && [ "$SHOW_MEMORY_PANEL" -eq 1 ]; then
  layout_cpu_panes="${cpu_panes[*]}"
  layout_mem_panes="${mem_panes[*]}"
  layout_args=(
    bash "$DASHBOARD_LAYOUT_SCRIPT"
    --session "$SESSION"
    --cpu_panes "$layout_cpu_panes"
    --mem_panes "$layout_mem_panes"
    --io_pane "$cluster_io_pane"
    --progress_pane "$progress_pane"
    --io_cols "$CLUSTER_IO_WIDTH_COLS"
    --mem_cols "$MEM_PANE_WIDTH_COLS"
    --progress_lines "$progress_lines"
    --instance "$MONITOR_INSTANCE"
  )
  if [ -n "$mount_pane" ]; then
    layout_args+=(--mount_pane "$mount_pane" --mount_cols "$MOUNT_PANE_WIDTH_COLS")
  fi
  layout_cmd=""
  for layout_arg in "${layout_args[@]}"; do
    layout_cmd+="$(printf '%q ' "$layout_arg")"
  done
  "${layout_args[@]}" --once
  tmux run-shell -b "$layout_cmd"
fi

if [ -n "$progress_pane" ]; then
  tmux select-pane -t "$progress_pane"
else
  tmux select-pane -t "${cpu_panes[0]}"
fi
tmux attach-session -t "$SESSION"
