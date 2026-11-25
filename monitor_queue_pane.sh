#!/usr/bin/env bash
export ROTA_ROOT="${ROTA_ROOT:-}"
set -euo pipefail

ROOT="${ROTA_ROOT}"
COLLECTOR_SCRIPT=""
COLLECT_SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/monitor_queue_collect.sh"
RENDERER_SCRIPT=""
WORKER_SCRIPT=""
SNAPSHOT_TSV=""
SNAPSHOT_JSON=""
SNAPSHOT_SCRIPT=""
HOSTS=""
HOST_LABELS=""
FALLBACK_HOST=""
MODE="remote"
REFRESH_SEC=30
COLLECTOR_TIMEOUT_SEC=120
LIMIT=0
TARGET_PANE="${TMUX_PANE:-}"
INSTANCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root) ROOT="$2"; shift 2 ;;
    --collector_script) COLLECTOR_SCRIPT="$2"; shift 2 ;;
    --collect_script) COLLECT_SCRIPT="$2"; shift 2 ;;
    --renderer_script) RENDERER_SCRIPT="$2"; shift 2 ;;
    --worker_script) WORKER_SCRIPT="$2"; shift 2 ;;
    # Snapshot mode renders the cluster status collector's TSV and collects nothing.
    --snapshot_tsv) SNAPSHOT_TSV="$2"; shift 2 ;;
    --snapshot_json) SNAPSHOT_JSON="$2"; shift 2 ;;
    --snapshot_script) SNAPSHOT_SCRIPT="$2"; shift 2 ;;
    --hosts) HOSTS="$2"; shift 2 ;;
    --host_labels) HOST_LABELS="$2"; shift 2 ;;
    --fallback_host) FALLBACK_HOST="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --refresh_sec) REFRESH_SEC="$2"; shift 2 ;;
    --collector_timeout_sec) COLLECTOR_TIMEOUT_SEC="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    --pane) TARGET_PANE="$2"; shift 2 ;;
    --instance) INSTANCE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

if ! [[ "$COLLECTOR_TIMEOUT_SEC" =~ ^[0-9]+$ ]] || [ "$COLLECTOR_TIMEOUT_SEC" -lt 10 ] || [ "$COLLECTOR_TIMEOUT_SEC" -gt 600 ]; then
  echo "--collector_timeout_sec must be an integer between 10 and 600" >&2
  exit 2
fi

if [ -n "$SNAPSHOT_TSV" ]; then
  [ -n "$SNAPSHOT_JSON" ] || { echo "--snapshot_tsv requires --snapshot_json" >&2; exit 2; }
  required_helpers=("$RENDERER_SCRIPT" "$SNAPSHOT_SCRIPT")
else
  : "${ROOT:?Source the inventory environment or pass --root}"
  required_helpers=("$COLLECT_SCRIPT" "$COLLECTOR_SCRIPT" "$RENDERER_SCRIPT" "$WORKER_SCRIPT")
fi
for required in "${required_helpers[@]}"; do
  if [ ! -f "$required" ]; then
    echo "Missing queue monitor helper: ${required:-<unset>}" >&2
    exit 1
  fi
done
if [ -z "$TARGET_PANE" ]; then
  echo "monitor_queue_pane.sh requires --pane or TMUX_PANE" >&2
  exit 1
fi
TMUX_PANE="$TARGET_PANE"

data_file="$(mktemp "${TMPDIR:-/tmp}/queue-progress-data.XXXXXX")"
data_next="${data_file}.next"
snapshot_file="${data_file}.rendered"
snapshot_next="${snapshot_file}.next"
stage_ack="${data_file}.stage-ack"
error_file="${data_file}.error"
render_error="${data_file}.render-error"
collector_pid=""
collector_started_epoch=0
stage_session=""
redraw_generation=0
last_cols=0
last_lines=0
next_refresh=0

process_tree_pids() {
  local parent="$1"
  local child
  for child in $(pgrep -P "$parent" 2>/dev/null || true); do
    process_tree_pids "$child"
  done
  printf '%s\n' "$parent"
}

stop_collector() {
  local deadline pid parent_state targets
  [ -n "$collector_pid" ] || return
  targets="$(process_tree_pids "$collector_pid")"
  for pid in $targets; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  deadline=$(( $(date +%s) + 2 ))
  while kill -0 "$collector_pid" 2>/dev/null && [ "$(date +%s)" -lt "$deadline" ]; do
    sleep 0.1
  done
  for pid in $targets; do
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid" 2>/dev/null || true
  done
  parent_state="$(ps -o state= -p "$collector_pid" 2>/dev/null || true)"
  case "$parent_state" in
    ""|Z*)
    wait "$collector_pid" 2>/dev/null || true
      ;;
  esac
  collector_pid=""
  collector_started_epoch=0
}

cleanup() {
  stop_collector
  if [ -n "$stage_session" ]; then
    tmux kill-session -t "$stage_session" 2>/dev/null || true
  fi
  rm -f "$data_file" "$data_next" "$snapshot_file" "$snapshot_next" \
    "$stage_ack" "$error_file" "$render_error"
}
trap cleanup EXIT
trap 'exit 0' HUP INT TERM

pane_size() {
  pane_cols="$(tmux display-message -p -t "$TMUX_PANE" '#{pane_width}' 2>/dev/null || echo 0)"
  pane_lines="$(tmux display-message -p -t "$TMUX_PANE" '#{pane_height}' 2>/dev/null || echo 0)"
}

set_title() {
  tmux select-pane -t "$TMUX_PANE" -T "$1" >/dev/null 2>&1 || true
}

# Local readers such as a desktop status indicator get one stable, atomically
# replaced copy instead of globbing this pane's private temporary files.
SNAPSHOT_EXPORT="${ROTA_QUEUE_SNAPSHOT:-${XDG_STATE_HOME:-$HOME/.local/state}/rota/queue-progress.tsv}"
publish_snapshot() {
  local export_dir export_tmp=""
  export_dir="$(dirname "$SNAPSHOT_EXPORT")"
  if mkdir -p "$export_dir" && export_tmp="$(mktemp "$export_dir/.queue-progress.XXXXXX")" \
      && cp "$data_file" "$export_tmp" && mv -f "$export_tmp" "${SNAPSHOT_EXPORT}.details" \
      && export_tmp="$(mktemp "$export_dir/.queue-progress.XXXXXX")" \
      && cut -f1-21 "$data_file" > "$export_tmp" && mv -f "$export_tmp" "$SNAPSHOT_EXPORT"; then
    return 0
  fi
  [ -z "$export_tmp" ] || rm -f "$export_tmp"
  echo "Could not publish queue snapshot to $SNAPSHOT_EXPORT" >&2
}

wait_for_generation() {
  local file="$1"
  local generation="$2"
  local deadline value=""

  deadline=$(( $(date +%s) + 3 ))
  while [ "$(date +%s)" -le "$deadline" ]; do
    value="$(command cat "$file" 2>/dev/null || true)"
    [ "$value" = "$generation" ] && return 0
    sleep 0.02
  done
  return 1
}

render_and_redraw() {
  local title="$1"
  local viewer_body viewer_cmd target_session stage_pane old_pane
  pane_size
  # Header, separator, ACTIVE heading and total must remain visible together.
  tmux set-option -w -t "$TMUX_PANE" @queue_active_lines \
    "$(awk -F'\t' '$1 == 0 {n++} END {print n ? n + 4 : 3}' "$data_file")"
  if [ "$pane_cols" -lt 120 ]; then
    set_title 'QUEUE PROGRESS | Needs 120 columns; retaining snapshot'
    last_cols="$pane_cols"
    last_lines="$pane_lines"
    return 0
  fi
  if ! "$RENDERER_SCRIPT" --data_file "$data_file" --term_cols "$pane_cols" --term_lines "$pane_lines" \
      --host_labels "$HOST_LABELS" --color > "$snapshot_next" 2> "$render_error"; then
    set_title "QUEUE PROGRESS | Render failed: $(tr '\n' ' ' < "$render_error"); retaining snapshot"
    cat "$render_error" >&2
    rm -f "$snapshot_next"
    last_cols="$pane_cols"
    last_lines="$pane_lines"
    return 0
  fi
  mv -f "$snapshot_next" "$snapshot_file"

  redraw_generation=$((redraw_generation + 1))
  rm -f "$stage_ack"
  target_session="$(tmux display-message -p -t "$TMUX_PANE" '#{session_name}')"
  stage_session="${target_session}-queue-stage-$$-${redraw_generation}"

  # Build a complete, correctly sized pane off-screen. Its scrollback is
  # populated before one atomic swap replaces the visible queue pane.
  stage_pane="$(tmux new-session -d -P -F '#{pane_id}' -s "$stage_session" 'while true; do sleep 3600; done')"
  tmux resize-window -t "$stage_session":0 -x "$pane_cols" -y "$pane_lines"
  viewer_body="command cat $(printf '%q' "$snapshot_file"); printf '\\033[J'; printf '%s\\n' $(printf '%q' "$redraw_generation") > $(printf '%q' "$stage_ack"); while true; do sleep 3600; done"
  viewer_cmd="$(printf 'bash -lc %q' "$viewer_body")"
  tmux respawn-pane -k -t "$stage_pane" "$viewer_cmd"

  if ! wait_for_generation "$stage_ack" "$redraw_generation"; then
    echo "Queue staging pane did not acknowledge redraw $redraw_generation" >&2
    return 1
  fi
  tmux select-pane -t "$stage_pane" -T "$title" \; \
    copy-mode -t "$stage_pane" \; \
    send-keys -X -t "$stage_pane" history-top >/dev/null 2>&1

  old_pane="$TMUX_PANE"
  tmux swap-pane -d -s "$stage_pane" -t "$old_pane"
  TARGET_PANE="$stage_pane"
  TMUX_PANE="$stage_pane"
  tmux set-option -w -t "$TMUX_PANE" @queue_progress_pane "$TMUX_PANE" >/dev/null
  tmux kill-session -t "$stage_session"
  stage_session=""
  last_cols="$pane_cols"
  last_lines="$pane_lines"
}

collect_snapshot() {
  : > "$error_file"
  "$COLLECT_SCRIPT" --root "$ROOT" --collector_script "$COLLECTOR_SCRIPT" --worker_script "$WORKER_SCRIPT" \
    --hosts "$HOSTS" --fallback_host "$FALLBACK_HOST" --mode "$MODE" --limit "$LIMIT" \
    --timeout_sec "$COLLECTOR_TIMEOUT_SEC" --eta_warnings > "$data_next" 2>> "$error_file"
  [ -s "$data_next" ]
}

pane_retired() {
  ! tmux display-message -p -t "$TMUX_PANE" '#{pane_id}' >/dev/null 2>&1 || \
    { [ -n "$INSTANCE" ] && [ "$(tmux show-options -qv -t "$(tmux display-message -p -t "$TMUX_PANE" '#{session_name}')" @monitor_instance 2>/dev/null || true)" != "$INSTANCE" ]; }
}

reflow_if_resized() {
  pane_size
  if [ -s "$data_file" ] && { [ "$pane_cols" != "$last_cols" ] || [ "$pane_lines" != "$last_lines" ]; }; then
    if [ -n "$collector_pid" ]; then
      render_and_redraw 'QUEUE PROGRESS | Refreshing in background...'
    else
      render_and_redraw "QUEUE PROGRESS | Reflowed $(date '+%Y/%m/%d %H:%M:%S')"
    fi
  fi
}

# The collector replaces its TSV atomically after every successful refresh, so
# a newer file than our copy means new data; its health comes from the JSON.
watch_collector_snapshot() {
  local status_due=0 status
  set_title 'QUEUE PROGRESS | Waiting for the cluster status collector...'
  while ! pane_retired; do
    if [ -s "$SNAPSHOT_TSV" ] && { [ ! -s "$data_file" ] || [ "$SNAPSHOT_TSV" -nt "$data_file" ]; }; then
      cp "$SNAPSHOT_TSV" "$data_file"
      if [ -r "${SNAPSHOT_TSV}.details" ] && cp "${SNAPSHOT_TSV}.details" "$data_next" \
          && cut -f1-21 "$data_next" | cmp -s - "$data_file"; then
        mv -f "$data_next" "$data_file"
      fi
      render_and_redraw "QUEUE PROGRESS | Updated $(date -r "$SNAPSHOT_TSV" '+%Y/%m/%d %H:%M:%S')"
      status_due=0
    fi
    if [ "$SECONDS" -ge "$status_due" ]; then
      status="$(python3 "$SNAPSHOT_SCRIPT" queue-status --snapshot "$SNAPSHOT_JSON")" || \
        status="Collector status unreadable"
      [ -z "$status" ] || set_title "QUEUE PROGRESS | $status"
      status_due=$((SECONDS + 10))
    fi
    reflow_if_resized
    sleep 0.25
  done
}

if [ -n "$SNAPSHOT_TSV" ]; then
  watch_collector_snapshot
  exit 0
fi

set_title 'QUEUE PROGRESS | Building first snapshot...'
while true; do
  if pane_retired; then
    exit 0
  fi
  now_epoch="$(date +%s)"
  if [ -z "$collector_pid" ] && [ "$now_epoch" -ge "$next_refresh" ]; then
    set_title 'QUEUE PROGRESS | Refreshing in background...'
    collect_snapshot &
    collector_pid=$!
    collector_started_epoch="$now_epoch"
    next_refresh=$((now_epoch + REFRESH_SEC))
  fi

  if [ -n "$collector_pid" ] && kill -0 "$collector_pid" 2>/dev/null \
      && [ $((now_epoch - collector_started_epoch)) -ge "$COLLECTOR_TIMEOUT_SEC" ]; then
    printf 'Queue collector exceeded %ss; terminating PID %s\n' "$COLLECTOR_TIMEOUT_SEC" "$collector_pid" >> "$error_file"
    stop_collector
    rm -f "$data_next"
    failed="$(date '+%Y/%m/%d %H:%M:%S')"
    set_title "QUEUE PROGRESS | Timed out $failed; retaining snapshot"
  fi

  if [ -n "$collector_pid" ] && ! kill -0 "$collector_pid" 2>/dev/null; then
    if wait "$collector_pid" && [ -s "$data_next" ]; then
      mv -f "$data_next" "$data_file"
      publish_snapshot
      updated="$(date '+%Y/%m/%d %H:%M:%S')"
      render_and_redraw "QUEUE PROGRESS | Updated $updated"
    else
      rm -f "$data_next"
      failed="$(date '+%Y/%m/%d %H:%M:%S')"
      set_title "QUEUE PROGRESS | Refresh failed $failed; retaining snapshot"
    fi
    collector_pid=""
    collector_started_epoch=0
  fi

  reflow_if_resized
  sleep 0.25
done
