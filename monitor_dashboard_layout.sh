#!/usr/bin/env bash
set -euo pipefail

SESSION=""
CPU_PANES=""
MEM_PANES=""
MOUNT_PANE=""
MOUNT_COLS=8
IO_PANE=""
PROGRESS_PANE=""
IO_COLS=32
MEM_COLS=26
PROGRESS_LINES=15
MAX_NODE_ROWS=4
POLL_SEC=0.25
INSTANCE=""
ONCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --session) SESSION="$2"; shift 2 ;;
    --cpu_panes) CPU_PANES="$2"; shift 2 ;;
    --mem_panes) MEM_PANES="$2"; shift 2 ;;
    --mount_pane) MOUNT_PANE="$2"; shift 2 ;;
    --mount_cols) MOUNT_COLS="$2"; shift 2 ;;
    --io_pane) IO_PANE="$2"; shift 2 ;;
    --progress_pane) PROGRESS_PANE="$2"; shift 2 ;;
    --io_cols) IO_COLS="$2"; shift 2 ;;
    --mem_cols) MEM_COLS="$2"; shift 2 ;;
    --progress_lines) PROGRESS_LINES="$2"; shift 2 ;;
    --max_node_rows) MAX_NODE_ROWS="$2"; shift 2 ;;
    --poll_sec) POLL_SEC="$2"; shift 2 ;;
    --instance) INSTANCE="$2"; shift 2 ;;
    --once) ONCE=1; shift ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$SESSION" ] || [ -z "$CPU_PANES" ] || [ -z "$MEM_PANES" ] || [ -z "$IO_PANE" ] || [ -z "$PROGRESS_PANE" ]; then
  echo "Dashboard layout monitor requires session and pane IDs" >&2
  exit 1
fi

# shellcheck disable=SC2206
cpu_panes=($CPU_PANES)
# shellcheck disable=SC2206
mem_panes=($MEM_PANES)
if [ "${#cpu_panes[@]}" -ne "${#mem_panes[@]}" ] || [ "${#cpu_panes[@]}" -eq 0 ]; then
  echo "CPU and memory pane counts must match" >&2
  exit 1
fi
if ! [[ "$MOUNT_COLS" =~ ^[0-9]+$ ]] || [ "$MOUNT_COLS" -lt 7 ]; then
  echo "Mount column width must be at least 7" >&2
  exit 2
fi
if ! [[ "$MAX_NODE_ROWS" =~ ^[0-9]+$ ]] || [ "$MAX_NODE_ROWS" -lt 3 ]; then
  echo "Maximum node rows must be at least 3" >&2
  exit 2
fi

apply_layout() {
  local window_cols="$1"
  local window_rows="$2"
  local top_rows left_cols host_cols host_x=0 cpu_cols row_content base_rows extra_rows
  local queue_lines="$PROGRESS_LINES" active_lines="$3" top_border="$4" preferred_top maximum_top minimum_rows
  local rows_layout="" row_y=0 row_lines cpu_id mem_id row_layout
  local io_id progress_id mount_id mount_pid left_layout body checksum=0 code checksum_hex i
  local expected_panes=() actual_order expected_order

  # tmux assigns cells in traversal order, not by the IDs in a layout string.
  # A missing/reordered pane must not shift every other monitor into its slot.
  [ -z "$MOUNT_PANE" ] || expected_panes+=("$MOUNT_PANE")
  for i in "${!cpu_panes[@]}"; do
    expected_panes+=("${cpu_panes[$i]}" "${mem_panes[$i]}")
  done
  expected_panes+=("$IO_PANE" "$PROGRESS_PANE")
  expected_order="$(printf '%s\n' "${expected_panes[@]}")"
  actual_order="$(tmux list-panes -t "$SESSION":0 -F '#{pane_id}')" || return 1
  if [ "$actual_order" != "$expected_order" ]; then
    echo "Dashboard pane order changed; refusing to assign monitors to incorrect cells. Reload the dashboard if a pane was removed." >&2
    return 1
  fi

  # Reclaim history first, but keep the complete ACTIVE section and its summary.
  if [ "$active_lines" -gt "$queue_lines" ]; then queue_lines="$active_lines"; fi
  preferred_top=$((4 * ${#cpu_panes[@]} - 1 + top_border))
  if [ $((window_rows - queue_lines - 1)) -lt "$preferred_top" ]; then
    queue_lines=$((window_rows - preferred_top - 1))
    [ "$queue_lines" -ge "$active_lines" ] || queue_lines="$active_lines"
  fi
  # Past MAX_NODE_ROWS per node, extra height shows more queue history instead.
  maximum_top=$(((MAX_NODE_ROWS + 1) * ${#cpu_panes[@]} - 1 + top_border))
  if [ $((window_rows - queue_lines - 1)) -gt "$maximum_top" ]; then
    queue_lines=$((window_rows - maximum_top - 1))
  fi
  minimum_rows=$((2 * ${#cpu_panes[@]} + top_border + active_lines))
  if [ "$window_rows" -lt "$minimum_rows" ]; then
    echo "Dashboard needs at least $minimum_rows rows for all nodes and the ACTIVE section (has $window_rows)." >&2
    return 1
  fi
  top_rows=$((window_rows - queue_lines - 1))
  left_cols=$((window_cols - IO_COLS - 1))
  host_cols="$left_cols"
  if [ -n "$MOUNT_PANE" ]; then
    host_x=$((MOUNT_COLS + 1))
    host_cols=$((left_cols - host_x))
  fi
  cpu_cols=$((host_cols - MEM_COLS - 1))
  row_content=$((top_rows - ${#cpu_panes[@]} + 1 - top_border))
  base_rows=$((row_content / ${#cpu_panes[@]}))
  extra_rows=$((row_content % ${#cpu_panes[@]}))
  if [ "$cpu_cols" -lt 22 ]; then
    echo "Dashboard is too narrow for CPU meters and fixed side columns." >&2
    return 1
  fi

  for ((i = 0; i < ${#cpu_panes[@]}; i++)); do
    row_lines="$base_rows"
    [ "$i" -ge $((${#cpu_panes[@]} - extra_rows)) ] && row_lines=$((row_lines + 1))
    # tmux takes the outer top border from the first cell, but uses the existing
    # separator for later cells. Balance content heights, not layout cells.
    [ "$i" -eq 0 ] && row_lines=$((row_lines + top_border))
    cpu_id="${cpu_panes[$i]#%}"
    mem_id="${mem_panes[$i]#%}"
    row_layout="${host_cols}x${row_lines},${host_x},${row_y}{${cpu_cols}x${row_lines},${host_x},${row_y},${cpu_id},${MEM_COLS}x${row_lines},$((host_x + cpu_cols + 1)),${row_y},${mem_id}}"
    if [ -n "$rows_layout" ]; then
      rows_layout="${rows_layout},${row_layout}"
    else
      rows_layout="$row_layout"
    fi
    row_y=$((row_y + row_lines + 1))
  done

  io_id="${IO_PANE#%}"
  progress_id="${PROGRESS_PANE#%}"
  left_layout="${host_cols}x${top_rows},${host_x},0[${rows_layout}]"
  if [ -n "$MOUNT_PANE" ]; then
    mount_id="${MOUNT_PANE#%}"
    left_layout="${left_cols}x${top_rows},0,0{${MOUNT_COLS}x${top_rows},0,0,${mount_id},${left_layout}}"
  fi
  body="${window_cols}x${window_rows},0,0[${window_cols}x${top_rows},0,0{${left_layout},${IO_COLS}x${top_rows},$((left_cols + 1)),0,${io_id}},${window_cols}x${queue_lines},0,$((top_rows + 1)),${progress_id}]"
  for ((i = 0; i < ${#body}; i++)); do
    printf -v code '%d' "'${body:i:1}"
    checksum=$(( (((checksum >> 1) | ((checksum & 1) << 15)) + code) & 65535 ))
  done
  printf -v checksum_hex '%04x' "$checksum"
  tmux select-layout -t "$SESSION":0 "${checksum_hex},${body}" >/dev/null
  # Rebalancing can move CPU boundaries without resizing the mount pane again.
  if [ -n "$MOUNT_PANE" ]; then
    mount_pid="$(tmux display-message -p -t "$MOUNT_PANE" '#{pane_pid}')"
    if [[ "$mount_pid" =~ ^[1-9][0-9]*$ ]]; then
      kill -WINCH "$mount_pid" 2>/dev/null || true
    fi
  fi
}

last_size=""
while tmux has-session -t "$SESSION" 2>/dev/null; do
  if [ -n "$INSTANCE" ] && [ "$(tmux show-options -qv -t "$SESSION" @monitor_instance 2>/dev/null || true)" != "$INSTANCE" ]; then
    exit 0
  fi
  dynamic_progress_pane="$(tmux show-options -wqv -t "$SESSION":0 @queue_progress_pane 2>/dev/null || true)"
  if [[ "$dynamic_progress_pane" == %* ]]; then
    PROGRESS_PANE="$dynamic_progress_pane"
  fi
  if [ "$ONCE" -eq 1 ]; then
    IO_COLS="$(tmux display-message -p -t "$IO_PANE" '#{pane_width}')"
    MEM_COLS="$(tmux display-message -p -t "${mem_panes[0]}" '#{pane_width}')"
    PROGRESS_LINES="$(tmux display-message -p -t "$PROGRESS_PANE" '#{pane_height}')"
    if [ -n "$MOUNT_PANE" ]; then
      MOUNT_COLS="$(tmux display-message -p -t "$MOUNT_PANE" '#{pane_width}')"
    fi
    if ! [[ "$IO_COLS" =~ ^[0-9]+$ ]] || ! [[ "$MEM_COLS" =~ ^[0-9]+$ ]] \
        || ! [[ "$PROGRESS_LINES" =~ ^[0-9]+$ ]] \
        || ! [[ "$MOUNT_COLS" =~ ^[0-9]+$ ]]; then
      echo "Unable to read initial dashboard pane dimensions" >&2
      exit 1
    fi
  fi
  # Active-row changes also require reallocation, even without a window resize.
  size="$(tmux display-message -p -t "$SESSION":0 '#{window_width} #{window_height} #{?@queue_active_lines,#{@queue_active_lines},0} #{pane-border-status} #{?@storage_width,#{@storage_width},0}' 2>/dev/null || true)"
  layout_state="$size $PROGRESS_PANE"
  if [ -n "$size" ] && [ "$layout_state" != "$last_size" ]; then
    read -r window_cols window_rows active_lines border_status storage_width <<< "$size"
    if [[ "$storage_width" =~ ^[0-9]+$ ]] && [ "$storage_width" -gt "$IO_COLS" ]; then
      IO_COLS="$storage_width"
    fi
    # Before the first successful queue snapshot, retain its whole current area.
    if ! [[ "$active_lines" =~ ^[1-9][0-9]*$ ]]; then active_lines="$PROGRESS_LINES"; fi
    top_border=0
    [ "$border_status" = top ] && top_border=1
    if [[ "$window_cols" =~ ^[0-9]+$ ]] && [[ "$window_rows" =~ ^[0-9]+$ ]]; then
      if [ "$ONCE" -eq 1 ]; then
        apply_layout "$window_cols" "$window_rows" "$active_lines" "$top_border"
      else
        apply_layout "$window_cols" "$window_rows" "$active_lines" "$top_border" || true
      fi
      last_size="$layout_state"
    fi
  fi
  [ "$ONCE" -eq 0 ] || exit 0
  sleep "$POLL_SEC"
done

if [ "$ONCE" -eq 1 ]; then
  echo "Dashboard session is unavailable: $SESSION" >&2
  exit 1
fi
