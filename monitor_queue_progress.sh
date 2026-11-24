#!/usr/bin/env bash
export PARALLEL_WORKERS_ROOT="${PARALLEL_WORKERS_ROOT:-}"
set -euo pipefail

ROOT="${PARALLEL_WORKERS_ROOT}"
LIMIT=0
RUNNING_WINDOW_SEC=86400
ADAPTIVE_STATUS_STALE_SEC="${ADAPTIVE_STATUS_STALE_SEC:-60}"
HISTORY_WINDOW_SEC=86400
OLDER_LIMIT=5
BAR_WIDTH=12
MAX_LABEL_LEN=160
MAX_BATCH_SCAN=30
CACHE_FILE="${TMPDIR:-/tmp}/parallel_workers_queue_progress_cache.tsv"
TERM_COLS=0
TERM_LINES=0
SCROLLABLE=0
HOSTS_FILE="${PARALLEL_WORKERS_HOSTS_FILE:-}"
HOSTS_OVERRIDE=""
SSH_CONNECT_TIMEOUT_SEC=3
REGISTRY_FILE=""
WRITE_REGISTRY=1
DATA_ONLY=0
PROCESS_SCAN_FILE=""
ALLOW_NETWORK_ROOT=0
SELF_TIMEOUT_SEC=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_SCANNER_SCRIPT="$SCRIPT_DIR/monitor_queue_workers.sh"
RENDERER_SCRIPT="$SCRIPT_DIR/render_queue_progress.sh"
FAILURE_SCRIPT="$SCRIPT_DIR/monitor_queue_failures.py"
if [ "$(uname -s)" = Darwin ]; then
  STAT_STYLE=bsd
else
  STAT_STYLE=gnu
fi

usage() {
  cat <<EOF
Queue progress monitor (snapshot view).

Usage:
  $0 [--root <path>] [--limit <n>] [--running_window_sec <n>] [--history_window_sec <n>] [--older_limit <n>] [--bar_width <n>] [--max_label_len <n>] [--max_batch_scan <n>] [--cache_file <path>] [--term_cols <n>] [--term_lines <n>] [--scrollable] [--hosts_file <path>] [--hosts "<h1> <h2> ..."] [--registry_file <path>] [--process_scan_file <path>] [--self_timeout_sec <n>] [--data_only] [--no_registry] [--allow_network_root]

Defaults:
  --root $ROOT
  --limit $LIMIT (0=all selected rows)
  --running_window_sec $RUNNING_WINDOW_SEC
  --history_window_sec $HISTORY_WINDOW_SEC
  --older_limit $OLDER_LIMIT
  --bar_width $BAR_WIDTH
  --max_label_len $MAX_LABEL_LEN
  --max_batch_scan $MAX_BATCH_SCAN
  --cache_file $CACHE_FILE
  --term_cols auto
  --term_lines auto
  --scrollable disabled (enable for a tmux copy-mode pane)
  --hosts_file ${HOSTS_FILE:-$ROOT/hosts/hosts-ethernet.txt}
  --registry_file ${REGISTRY_FILE:-$ROOT/QUEUE_PROGRESS.csv}
  --self_timeout_sec $SELF_TIMEOUT_SEC (0=disabled)
  --allow_network_root disabled (queue discovery should run on the storage host)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root) ROOT="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    --running_window_sec) RUNNING_WINDOW_SEC="$2"; shift 2 ;;
    --history_window_sec) HISTORY_WINDOW_SEC="$2"; shift 2 ;;
    --older_limit) OLDER_LIMIT="$2"; shift 2 ;;
    --bar_width) BAR_WIDTH="$2"; shift 2 ;;
    --max_label_len) MAX_LABEL_LEN="$2"; shift 2 ;;
    --max_batch_scan) MAX_BATCH_SCAN="$2"; shift 2 ;;
    --cache_file) CACHE_FILE="$2"; shift 2 ;;
    --term_cols) TERM_COLS="$2"; shift 2 ;;
    --term_lines) TERM_LINES="$2"; shift 2 ;;
    --scrollable) SCROLLABLE=1; shift ;;
    --hosts_file) HOSTS_FILE="$2"; shift 2 ;;
    --hosts) HOSTS_OVERRIDE="$2"; shift 2 ;;
    --registry_file) REGISTRY_FILE="$2"; shift 2 ;;
    --process_scan_file) PROCESS_SCAN_FILE="$2"; shift 2 ;;
    --self_timeout_sec) SELF_TIMEOUT_SEC="$2"; shift 2 ;;
    --data_only) DATA_ONLY=1; WRITE_REGISTRY=1; shift ;;
    --no_registry) WRITE_REGISTRY=0; shift ;;
    --allow_network_root) ALLOW_NETWORK_ROOT=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

: "${ROOT:?Source the inventory environment or pass --root}"

REGISTRY_FILE="${REGISTRY_FILE:-$ROOT/QUEUE_PROGRESS.csv}"

if ! [[ "$LIMIT" =~ ^[0-9]+$ ]] || [ "$LIMIT" -gt 1000 ]; then
  echo "--limit must be an integer between 0 and 1000 (0=all selected rows)" >&2
  exit 2
fi

if ! [[ "$RUNNING_WINDOW_SEC" =~ ^[0-9]+$ ]] || [ "$RUNNING_WINDOW_SEC" -lt 30 ] || [ "$RUNNING_WINDOW_SEC" -gt 604800 ]; then
  echo "--running_window_sec must be an integer between 30 and 604800" >&2
  exit 2
fi

if ! [[ "$ADAPTIVE_STATUS_STALE_SEC" =~ ^[0-9]+$ ]] || [ "$ADAPTIVE_STATUS_STALE_SEC" -lt 10 ]; then
  echo "ADAPTIVE_STATUS_STALE_SEC must be an integer of at least 10 seconds" >&2
  exit 2
fi

if ! [[ "$HISTORY_WINDOW_SEC" =~ ^[0-9]+$ ]] || [ "$HISTORY_WINDOW_SEC" -lt 60 ] || [ "$HISTORY_WINDOW_SEC" -gt 604800 ]; then
  echo "--history_window_sec must be an integer between 60 and 604800" >&2
  exit 2
fi
if ! [[ "$SELF_TIMEOUT_SEC" =~ ^[0-9]+$ ]] \
    || { [ "$SELF_TIMEOUT_SEC" -ne 0 ] && [ "$SELF_TIMEOUT_SEC" -lt 10 ]; } \
    || [ "$SELF_TIMEOUT_SEC" -gt 600 ]; then
  echo "--self_timeout_sec must be 0 or an integer between 10 and 600" >&2
  exit 2
fi

if ! [[ "$OLDER_LIMIT" =~ ^[0-9]+$ ]] || [ "$OLDER_LIMIT" -gt 100 ]; then
  echo "--older_limit must be an integer between 0 and 100" >&2
  exit 2
fi

if ! [[ "$BAR_WIDTH" =~ ^[0-9]+$ ]] || [ "$BAR_WIDTH" -lt 8 ] || [ "$BAR_WIDTH" -gt 60 ]; then
  echo "--bar_width must be an integer between 8 and 60" >&2
  exit 2
fi

if ! [[ "$MAX_LABEL_LEN" =~ ^[0-9]+$ ]] || [ "$MAX_LABEL_LEN" -lt 20 ] || [ "$MAX_LABEL_LEN" -gt 300 ]; then
  echo "--max_label_len must be an integer between 20 and 300" >&2
  exit 2
fi

if ! [[ "$MAX_BATCH_SCAN" =~ ^[0-9]+$ ]] || [ "$MAX_BATCH_SCAN" -lt 5 ] || [ "$MAX_BATCH_SCAN" -gt 2000 ]; then
  echo "--max_batch_scan must be an integer between 5 and 2000" >&2
  exit 2
fi

if ! [[ "$TERM_COLS" =~ ^[0-9]+$ ]] || [ "$TERM_COLS" -lt 0 ] || [ "$TERM_COLS" -gt 400 ]; then
  echo "--term_cols must be an integer between 0 and 400 (0=auto)" >&2
  exit 2
fi

if ! [[ "$TERM_LINES" =~ ^[0-9]+$ ]] || [ "$TERM_LINES" -lt 0 ] || [ "$TERM_LINES" -gt 400 ]; then
  echo "--term_lines must be an integer between 0 and 400 (0=auto)" >&2
  exit 2
fi

if [ ! -d "$ROOT/stan_projects" ]; then
  echo "Missing $ROOT/stan_projects"
  exit 0
fi

root_mount_type() {
  local mount_line rest
  while IFS= read -r mount_line; do
    case "$mount_line" in
      *" on $ROOT ("*)
        rest="${mount_line#*" on $ROOT ("}"
        printf '%s\n' "${rest%%,*}"
        return
        ;;
      *" on $ROOT type "*)
        rest="${mount_line#*" on $ROOT type "}"
        printf '%s\n' "${rest%% *}"
        return
        ;;
    esac
  done < <(mount)
  printf '%s\n' unknown
}

file_mtime() {
  if [ "$STAT_STYLE" = bsd ]; then
    stat -f %m "$1"
  else
    stat -c %Y "$1"
  fi
}

file_size() {
  if [ "$STAT_STYLE" = bsd ]; then
    stat -f %z "$1"
  else
    stat -c %s "$1"
  fi
}

format_epoch() {
  local epoch="$1" format="$2"
  if [ "$STAT_STYLE" = bsd ]; then
    date -r "$epoch" "$format"
  else
    date -d "@$epoch" "$format"
  fi
}

root_fs_type="$(root_mount_type)"
case "$root_fs_type" in
  smbfs|cifs|nfs|nfs4)
    if [ "$ALLOW_NETWORK_ROOT" -ne 1 ]; then
      echo "Refusing queue discovery on $root_fs_type root $ROOT; run this collector on the storage host or pass --allow_network_root explicitly." >&2
      exit 3
    fi
    ;;
esac

if [ "$TERM_COLS" -eq 0 ] && [ -n "${TMUX_PANE:-}" ] && command -v tmux >/dev/null 2>&1; then
  maybe_cols="$(tmux display-message -p -t "$TMUX_PANE" "#{pane_width}" 2>/dev/null || echo 0)"
  if [[ "$maybe_cols" =~ ^[0-9]+$ ]]; then
    TERM_COLS="$maybe_cols"
  fi
fi
if [ "$TERM_LINES" -eq 0 ] && [ -n "${TMUX_PANE:-}" ] && command -v tmux >/dev/null 2>&1; then
  maybe_lines="$(tmux display-message -p -t "$TMUX_PANE" "#{pane_height}" 2>/dev/null || echo 0)"
  if [[ "$maybe_lines" =~ ^[0-9]+$ ]]; then
    TERM_LINES="$maybe_lines"
  fi
fi
if [ "$TERM_COLS" -eq 0 ] && command -v tput >/dev/null 2>&1; then
  maybe_cols="$(tput cols 2>/dev/null || echo 0)"
  if [[ "$maybe_cols" =~ ^[0-9]+$ ]]; then
    TERM_COLS="$maybe_cols"
  fi
fi
if [ "$TERM_LINES" -eq 0 ] && command -v tput >/dev/null 2>&1; then
  maybe_lines="$(tput lines 2>/dev/null || echo 0)"
  if [[ "$maybe_lines" =~ ^[0-9]+$ ]]; then
    TERM_LINES="$maybe_lines"
  fi
fi

hosts=()
if [ -n "$HOSTS_OVERRIDE" ]; then
  # shellcheck disable=SC2206
  hosts=($HOSTS_OVERRIDE)
else
  if [ ! -f "$HOSTS_FILE" ]; then
    if [ -f "$ROOT/hosts/hosts-ethernet.txt" ]; then
      HOSTS_FILE="$ROOT/hosts/hosts-ethernet.txt"
    elif [ -f "$ROOT/hosts/hosts-r.txt" ]; then
      HOSTS_FILE="$ROOT/hosts/hosts-r.txt"
    elif [ -f "$ROOT/hosts-ethernet.txt" ]; then
      HOSTS_FILE="$ROOT/hosts-ethernet.txt"
    elif [ -f "$ROOT/hosts-r.txt" ]; then
      HOSTS_FILE="$ROOT/hosts-r.txt"
    fi
  fi
fi

if [ -f "$HOSTS_FILE" ] && [ "${#hosts[@]}" -eq 0 ]; then
  while read -r host _; do
    [[ -z "${host:-}" ]] && continue
    [[ "$host" == \#* ]] && continue
    hosts+=("$host")
  done < "$HOSTS_FILE"
fi

# Adaptive status names hosts by pool host_id; map them onto the hosts-file
# order so adaptive rows print per-host counts exactly like fixed rows.
ADAPTIVE_RESOURCES_FILE="${ADAPTIVE_STATE_ROOT:-$ROOT/.parallel-workers/adaptive}/resources.tsv"
host_ids=()
for host in "${hosts[@]}"; do
  host_id=""
  # A completed drain archives this table; fixed-only clusters also lack it.
  if [ -f "$ADAPTIVE_RESOURCES_FILE" ]; then
    host_id="$(awk -F'\t' -v target="$host" 'NR > 1 && $2 == target {print $1; exit}' \
      "$ADAPTIVE_RESOURCES_FILE")"
  fi
  host_ids+=("$host_id")
done

# Prints running jobs per host ("." = host not in the pool); fails when no host maps.
adaptive_host_counts() {
  local running_by_host="$1" host_i id count text="" mapped=0
  [ "$running_by_host" != none ] || return 1
  for host_i in "${!hosts[@]}"; do
    id="${host_ids[$host_i]}"
    count=""
    [ -z "$id" ] || count="$(printf '%s\n' "$running_by_host" | tr ',' '\n' | \
      awk -F: -v id="$id" '$1 == id {print $2; exit}')"
    if [ -n "$count" ]; then mapped=1; else count="."; fi
    text="${text:+$text-}$count"
  done
  [ "$mapped" -eq 1 ] || return 1
  printf '%s\n' "$text"
}

count_done_markers() {
  local done_dir="$1"
  if [ ! -d "$done_dir" ]; then
    echo 0
    return
  fi
  # Avoid one stat call per marker: APFS directories with thousands of completed
  # jobs are much faster to count from their unsorted directory entries.
  LC_ALL=C ls -1f "$done_dir" 2>/dev/null | awk '/^job_.*\.done$/ { count++ } END { print count + 0 }'
}

count_done_markers_cached() {
  local done_dir="$1"
  local done_mtime=0
  local cached=""
  local counted=0

  if [ -d "$done_dir" ]; then
    done_mtime="$(file_mtime "$done_dir" 2>/dev/null || echo 0)"
  fi

  if [ -f "$CACHE_FILE" ]; then
    cached="$(awk -F'\t' -v d="$done_dir" -v m="$done_mtime" '$1 == d && $2 == m {print $3; exit}' "$CACHE_FILE")"
  fi

  if [ -n "$cached" ]; then
    counted="$cached"
  else
    counted="$(count_done_markers "$done_dir")"
  fi

  printf "%s\t%s\t%s\n" "$done_dir" "$done_mtime" "$counted" >> "$tmp_cache"
  echo "$counted"
}

count_worker_all_done_markers() {
  local batch_dir="$1"
  local workers_dir="$batch_dir/workers"
  if [ ! -d "$workers_dir" ]; then
    echo 0
    return
  fi
  find "$workers_dir" -maxdepth 1 -type f -name '*.all_done' -print 2>/dev/null | wc -l | awk '{print $1}'
}

csv_escape() {
  local s="$1"
  s="${s//\"/\"\"}"
  printf '"%s"' "$s"
}

queue_has_content() {
  local queue_file="$1"
  local size_bytes=0
  if [ ! -f "$queue_file" ]; then
    echo -1
    return
  fi
  size_bytes="$(file_size "$queue_file" 2>/dev/null || echo 0)"
  if [ "$size_bytes" -gt 0 ]; then
    echo 1
  else
    echo 0
  fi
}

adaptive_status_summary() {
  local status_file="$1" expected_queue_id="$2"
  [ -f "$status_file" ] && [ ! -L "$status_file" ] && [ -r "$status_file" ] || return 1
  awk -F= -v expected_queue_id="$expected_queue_id" '
    BEGIN {
      required="schema_version queue_id lifecycle admission schedule_priority running_workers target_workers max_workers propagation"
      split(required, keys, " ")
      for (i in keys) wanted[keys[i]]=1
      wanted["effective_admission"]=wanted["blocking_reason"]=wanted["failure_holds"]=1
      wanted["running_by_host"]=wanted["host_priorities"]=1
    }
    $1 in wanted {
      if (seen[$1]++) exit 1
      value[$1]=substr($0, length($1) + 2)
    }
    END {
      for (i in keys) if (seen[keys[i]] != 1) exit 1
      if (value["schema_version"] != "1" ||
          value["queue_id"] != expected_queue_id ||
          value["lifecycle"] !~ /^(PENDING|RUNNING|DRAINING|COMPLETED|FINISHED_INCOMPLETE)$/ ||
          value["admission"] !~ /^(enabled|paused|closed)$/ ||
          value["schedule_priority"] !~ /^-?[0-9]+$/ ||
          value["running_workers"] !~ /^[0-9]+$/ ||
          value["target_workers"] !~ /^[0-9]+$/ ||
          value["max_workers"] !~ /^[0-9]+$/ ||
          value["propagation"] !~ /^(complete|pending)$/) exit 1
      if (seen["effective_admission"] != seen["blocking_reason"]) exit 1
      if (seen["effective_admission"]) {
        if (value["effective_admission"] !~ /^(enabled|blocked|paused|closed)$/ ||
            value["blocking_reason"] !~ /^[a-z_]+([+][a-z_]+)*$/) exit 1
      } else {
        value["effective_admission"]=value["admission"]
        value["blocking_reason"]="none"
      }
      if (value["running_by_host"] !~ /^([A-Za-z0-9._-]+:[0-9]+(,[A-Za-z0-9._-]+:[0-9]+)*)?$/) exit 1
      if (value["host_priorities"] !~ /^([A-Za-z0-9._-]+=(-?[0-9]+|off)(,[A-Za-z0-9._-]+=(-?[0-9]+|off))*)?$/) exit 1
      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n", \
        value["lifecycle"], value["admission"], value["schedule_priority"], \
        value["running_workers"], value["target_workers"], value["max_workers"], \
        value["propagation"], value["effective_admission"], value["blocking_reason"], \
        (value["failure_holds"] != ""), \
        (value["running_by_host"] == "" ? "none" : value["running_by_host"]), \
        (value["host_priorities"] != "" ? "*" : "")
    }
  ' "$status_file"
}

get_latest_worker_epoch() {
  local batch_dir="$1"
  local newest_log=""
  local newest_epoch=0 shard_epoch=0 shard_file
  newest_log="$(ls -t "$batch_dir"/worker_*.log 2>/dev/null | awk 'NR==1 {print $0; exit}')"
  if [ -n "$newest_log" ] && [ -f "$newest_log" ]; then
    newest_epoch="$(file_mtime "$newest_log" 2>/dev/null || echo 0)"
  fi
  for shard_file in "$batch_dir"/shards/host_*.env; do
    [ -f "$shard_file" ] || continue
    shard_epoch="$(awk -F= '$1 == "updated_epoch" {print $2; exit}' "$shard_file")"
    if [[ "$shard_epoch" =~ ^[0-9]+$ ]] && [ "$shard_epoch" -gt "$newest_epoch" ]; then
      newest_epoch="$shard_epoch"
    fi
  done
  echo "$newest_epoch"
}

# A finished adaptive queue's status.env is rewritten every tick until it retires, so its end is
# its last job event (worker logs, done markers) or, when no job ran, its last control change.
adaptive_end_epoch() {
  local batch_dir="$1" path epoch latest
  latest="$(get_latest_worker_epoch "$batch_dir")"
  for path in "$batch_dir/done" "$batch_dir/adaptive/control.env"; do
    [ -e "$path" ] || continue
    epoch="$(file_mtime "$path" 2>/dev/null || echo 0)"
    [ "$epoch" -le "$latest" ] || latest="$epoch"
  done
  echo "$latest"
}

shard_progress_summary() {
  local batch_dir="$1"
  local shard_file values
  local succeeded=0 finished=0 failed=0 terminal=0 observed=0
  local shard_succeeded shard_finished shard_failed shard_terminal

  for shard_file in "$batch_dir"/shards/host_*.env; do
    [ -f "$shard_file" ] || continue
    values="$(awk -F= '
      $1 == "state" { state=$2 }
      $1 == "succeeded_jobs" { succeeded=$2 }
      $1 == "finished_jobs" { finished=$2 }
      $1 == "failed_jobs" { failed=$2 }
      END {
        terminal=(state == "COMPLETED" || state == "FAILED" || state == "INTERRUPTED") ? 1 : 0
        printf "%d\t%d\t%d\t%d", succeeded+0, finished+0, failed+0, terminal
      }
    ' "$shard_file")"
    IFS=$'\t' read -r shard_succeeded shard_finished shard_failed shard_terminal <<< "$values"
    succeeded=$((succeeded + shard_succeeded))
    finished=$((finished + shard_finished))
    failed=$((failed + shard_failed))
    terminal=$((terminal + shard_terminal))
    observed=$((observed + 1))
  done
  printf '%s\t%s\t%s\t%s\t%s\n' "$succeeded" "$finished" "$failed" "$terminal" "$observed"
}

infer_queue_from_logs() {
  local batch_dir="$1"
  local wlog
  local q
  for wlog in "$batch_dir"/worker_*.log; do
    [ -f "$wlog" ] || continue
    q=$(awk '/Currently reading from queue file at:/ {sub(/^.*: /, ""); print; exit}' "$wlog")
    if [ -n "$q" ]; then
      echo "$q"
      return
    fi
  done
  echo ""
}

collect_running_worker_tags() {
  local scan_file
  : > "$tmp_running_tags"
  : > "$tmp_worker_slots"
  : > "$tmp_host_scan_status"
  HOST_SCAN_TOTAL="${#hosts[@]}"
  HOST_SCAN_OK=0
  HOST_SCAN_FAIL=0
  PROCESS_SCAN_MODE="none"

  if [ "${#hosts[@]}" -eq 0 ]; then
    return
  fi

  if [ -n "$PROCESS_SCAN_FILE" ]; then
    if [ ! -r "$PROCESS_SCAN_FILE" ]; then
      echo "Unreadable process scan file: $PROCESS_SCAN_FILE" >&2
      return 1
    fi
    scan_file="$PROCESS_SCAN_FILE"
  else
    if [ ! -x "$PROCESS_SCANNER_SCRIPT" ]; then
      echo "Missing executable process scanner: $PROCESS_SCANNER_SCRIPT" >&2
      return 1
    fi
    "$PROCESS_SCANNER_SCRIPT" --hosts "${hosts[*]}" \
      --ssh_connect_timeout_sec "$SSH_CONNECT_TIMEOUT_SEC" > "$tmp_process_scan"
    scan_file="$tmp_process_scan"
  fi

  while IFS=$'\t' read -r rec_type rec_a rec_b rec_c; do
    case "${rec_type:-}" in
      STATUS)
        if [[ "${rec_a:-}" =~ ^[0-9]+$ ]] && [[ "${rec_b:-}" =~ ^[01]$ ]]; then
          printf '%s\t%s\n' "$rec_a" "$rec_b" >> "$tmp_host_scan_status"
          if [ "$rec_b" -eq 1 ]; then
            HOST_SCAN_OK=$((HOST_SCAN_OK + 1))
          else
            HOST_SCAN_FAIL=$((HOST_SCAN_FAIL + 1))
          fi
        fi
        ;;
      TAG)
        [ -n "${rec_a:-}" ] && printf '%s\n' "$rec_a" >> "$tmp_running_tags"
        ;;
      SLOT)
        if [ -n "${rec_a:-}" ] && [[ "${rec_b:-}" =~ ^[0-9]+$ ]] && [[ "${rec_c:-}" =~ ^[0-9]+$ ]]; then
          printf '%s\t%s\t%s\n' "$rec_a" "$rec_b" "$rec_c" >> "$tmp_worker_slots"
        fi
        ;;
    esac
  done < "$scan_file"

  if [ "$HOST_SCAN_OK" -gt 0 ] && [ "$HOST_SCAN_FAIL" -eq 0 ] \
      && [ "$HOST_SCAN_OK" -eq "$HOST_SCAN_TOTAL" ]; then
    PROCESS_SCAN_MODE="full"
  elif [ "$HOST_SCAN_OK" -gt 0 ]; then
    PROCESS_SCAN_MODE="partial"
  else
    PROCESS_SCAN_MODE="none"
  fi

  if [ -s "$tmp_running_tags" ]; then
    sort -u "$tmp_running_tags" -o "$tmp_running_tags" 2>/dev/null || true
  fi
}

worker_slots_for_tag() {
  local tag="$1"
  local slot_txt=""
  local host_i slot_sum scan_ok

  if [ -z "$tag" ] || [ "${#hosts[@]}" -eq 0 ]; then
    echo "n/a"
    return
  fi

  if [ "$PROCESS_SCAN_MODE" = "none" ]; then
    echo "n/a"
    return
  fi

  for host_i in "${!hosts[@]}"; do
    scan_ok="$(
      awk -F'\t' -v host_i="$host_i" '
        $1 == host_i { ok = $2 }
        END { if (ok == "") print 0; else print ok + 0 }
      ' "$tmp_host_scan_status"
    )"
    if [ "$scan_ok" -ne 1 ]; then
      slot_sum="?"
    else
      slot_sum="$(
        awk -F'\t' -v tag="$tag" -v host_i="$host_i" '
          $1 == tag && $2 == host_i { s += $3 + 0 }
          END { print s + 0 }
        ' "$tmp_worker_slots"
      )"
    fi
    if [ -z "$slot_txt" ]; then
      slot_txt="$slot_sum"
    else
      slot_txt="${slot_txt}-${slot_sum}"
    fi
  done

  echo "$slot_txt"
}

tmp_rows="$(mktemp)"
tmp_cache="$(mktemp)"
tmp_legacy_candidates="$(mktemp)"
tmp_recent_candidates="$(mktemp)"
tmp_project_latest="$(mktemp)"
tmp_registry_candidates="$(mktemp)"
tmp_registry_ranked="$(mktemp)"
tmp_registry_selected="$(mktemp)"
tmp_all_candidates="$(mktemp)"
tmp_discovered_candidates="$(mktemp)"
tmp_sorted_candidates="$(mktemp)"
tmp_running_tags="$(mktemp)"
tmp_running_assigned="$(mktemp)"
tmp_worker_slots="$(mktemp)"
tmp_host_scan_status="$(mktemp)"
tmp_process_scan="$(mktemp)"
tmp_registry="$(mktemp)"
tmp_selected="$(mktemp)"
tmp_older="$(mktemp)"
tmp_display="$(mktemp)"
tmp_failed="$(mktemp)"
REGISTRY_LOCK_DIR="${REGISTRY_FILE}.writer.lock"
registry_lock_held=0
registry_stage=""
self_timeout_pid=""

cleanup() {
  local timeout_child
  rm -f "$tmp_rows" "$tmp_cache" "$tmp_legacy_candidates" "$tmp_recent_candidates" \
    "$tmp_project_latest" "$tmp_registry_candidates" "$tmp_all_candidates" \
    "$tmp_registry_ranked" "$tmp_registry_selected" \
    "$tmp_discovered_candidates" "$tmp_sorted_candidates" "$tmp_running_tags" \
    "$tmp_running_assigned" "$tmp_worker_slots" "$tmp_host_scan_status" "$tmp_process_scan" \
    "$tmp_registry" "$tmp_selected" "$tmp_older" "$tmp_display" "$tmp_failed"
  [ -z "$registry_stage" ] || rm -f "$registry_stage"
  if [ "$registry_lock_held" -eq 1 ]; then
    rm -f "$REGISTRY_LOCK_DIR/owner"
    rmdir "$REGISTRY_LOCK_DIR" 2>/dev/null || true
  fi
  if [ -n "$self_timeout_pid" ]; then
    for timeout_child in $(pgrep -P "$self_timeout_pid" 2>/dev/null || true); do
      kill "$timeout_child" 2>/dev/null || true
    done
    kill "$self_timeout_pid" 2>/dev/null || true
    wait "$self_timeout_pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 124' TERM
trap 'exit 130' INT

if [ "$SELF_TIMEOUT_SEC" -gt 0 ]; then
  collector_main_pid="$$"
  (
    sleep "$SELF_TIMEOUT_SEC" || exit 0
    kill -TERM "$collector_main_pid" 2>/dev/null || true
  ) &
  self_timeout_pid=$!
fi

discover_batch_candidates() (
  local search_root batch_dir
  shopt -s nullglob dotglob
  for search_root in "$ROOT/stan_projects" "$ROOT/tmp"; do
    [ -d "$search_root" ] || continue
    for batch_dir in \
      "$search_root"/*/logs/batch_* \
      "$search_root"/*/*/logs/batch_* \
      "$search_root"/*/*/*/logs/batch_*; do
      [ -d "$batch_dir" ] && [ ! -L "$batch_dir" ] && printf '%s\n' "$batch_dir"
    done
  done
)

acquire_registry_lock() {
  local current_host owner_host="" owner_pid="" lock_mtime=0 now
  if mkdir "$REGISTRY_LOCK_DIR" 2>/dev/null; then
    registry_lock_held=1
    return 0
  fi

  current_host="$(hostname -s 2>/dev/null || hostname)"
  if [ -r "$REGISTRY_LOCK_DIR/owner" ]; then
    IFS=$'\t' read -r owner_host owner_pid < "$REGISTRY_LOCK_DIR/owner" || true
  fi
  if [ "$owner_host" = "$current_host" ] && [[ "$owner_pid" =~ ^[0-9]+$ ]] \
      && ! kill -0 "$owner_pid" 2>/dev/null; then
    rm -f "$REGISTRY_LOCK_DIR/owner"
    rmdir "$REGISTRY_LOCK_DIR" 2>/dev/null || true
  elif [ -z "$owner_host" ]; then
    lock_mtime="$(file_mtime "$REGISTRY_LOCK_DIR" 2>/dev/null || echo 0)"
    now="$(date +%s)"
    if [[ "$lock_mtime" =~ ^[0-9]+$ ]] && [ $((now - lock_mtime)) -gt 30 ]; then
      rm -f "$REGISTRY_LOCK_DIR/owner"
      rmdir "$REGISTRY_LOCK_DIR" 2>/dev/null || true
    fi
  fi

  if mkdir "$REGISTRY_LOCK_DIR" 2>/dev/null; then
    registry_lock_held=1
    return 0
  fi
  return 1
}

now_epoch="$(date +%s)"
CANDIDATE_CACHE_FILE="${TMPDIR:-/tmp}/parallel_workers_queue_candidates_v8.txt"
CANDIDATE_CACHE_TTL=60
use_cached_candidates=0

if [ -f "$CANDIDATE_CACHE_FILE" ]; then
  cache_epoch="$(awk 'NR == 1 {print $1; exit}' "$CANDIDATE_CACHE_FILE" 2>/dev/null || echo 0)"
  if [[ "$cache_epoch" =~ ^[0-9]+$ ]] && [ $((now_epoch - cache_epoch)) -le "$CANDIDATE_CACHE_TTL" ]; then
    awk 'NR > 1 {print $0}' "$CANDIDATE_CACHE_FILE" > "$tmp_all_candidates" || true
    if [ -s "$tmp_all_candidates" ]; then
      use_cached_candidates=1
    fi
  fi
fi

if [ "$use_cached_candidates" -eq 0 ]; then
  today="$(date '+%Y-%m-%d')"
  yesterday="$(date -v-1d '+%Y-%m-%d' 2>/dev/null || date -d yesterday '+%Y-%m-%d' 2>/dev/null || echo '')"
  discover_batch_candidates > "$tmp_discovered_candidates"
  if [ ! -s "$tmp_discovered_candidates" ]; then
    echo "Queue Progress: no batch logs found."
    exit 0
  fi

  batch_candidates=()
  while IFS= read -r batch_candidate; do
    batch_candidates+=("$batch_candidate")
  done < "$tmp_discovered_candidates"
  ls -dt "${batch_candidates[@]}" > "$tmp_sorted_candidates"
  awk -v max="$MAX_BATCH_SCAN" 'NR <= max {print $0}' "$tmp_sorted_candidates" > "$tmp_legacy_candidates"
  awk -v today="_$today" -v yesterday="${yesterday:+_$yesterday}" -v max="$((MAX_BATCH_SCAN * 2))" '
    selected >= max { next }
    substr($0, length($0) - length(today) + 1) == today { print; selected++; next }
    yesterday != "" && substr($0, length($0) - length(yesterday) + 1) == yesterday { print; selected++ }
  ' "$tmp_sorted_candidates" > "$tmp_recent_candidates"
  awk -v max=20 '{
    path = $0
    project = path
    sub(/\/logs\/batch_[^\/]+$/, "", project)
    if (!seen[project]++ && selected++ < max) print path
  }' "$tmp_sorted_candidates" > "$tmp_project_latest"
  if [ -f "$REGISTRY_FILE" ]; then
    candidate_history_cutoff=$((now_epoch - HISTORY_WINDOW_SEC))
    awk -F',' -v cutoff="$candidate_history_cutoff" 'NR > 1 {
      path = $3
      sub(/^"/, "", path)
      sub(/"$/, "", path)
      state = $9
      gsub(/^"|"$/, "", state)
      priority = $10 + 0
      start_epoch = $11 + 0
      last_activity = $13 + 0
      end_epoch = $14 + 0
      history_epoch = end_epoch > 0 ? end_epoch : (last_activity > 0 ? last_activity : start_epoch)
      if (path == "") next
      if (state == "RUNNING" || state == "PENDING" || state == "DRAINING" || (state == "UNKNOWN" && priority == 0)) print "0\t" history_epoch "\t" path
      else if (history_epoch >= cutoff) print "1\t" history_epoch "\t" path
    }' "$REGISTRY_FILE" > "$tmp_registry_ranked"
    LC_ALL=C sort -t $'\t' -k1,1n -k2,2nr "$tmp_registry_ranked" > "$tmp_registry_selected"
    awk -F'\t' -v max="$MAX_BATCH_SCAN" '
      $1 == 0 { print $3; next }
      terminal < max { print $3; terminal++ }
    ' "$tmp_registry_selected" > "$tmp_registry_candidates"
  fi
  awk '!seen[$0]++' "$tmp_recent_candidates" "$tmp_legacy_candidates" "$tmp_project_latest" "$tmp_registry_candidates" > "$tmp_all_candidates"
  {
    echo "$now_epoch"
    cat "$tmp_all_candidates"
  } > "$CANDIDATE_CACHE_FILE" 2>/dev/null || true
fi

collect_running_worker_tags

if [ ! -f "$FAILURE_SCRIPT" ]; then
  echo "Missing $FAILURE_SCRIPT; failed adaptive jobs are not reported" >&2
elif ! python3 "$FAILURE_SCRIPT" --state_root "${ADAPTIVE_STATE_ROOT:-$ROOT/.parallel-workers/adaptive}" \
    < "$tmp_all_candidates" > "$tmp_failed"; then
  : > "$tmp_failed"
fi
today_ymd="$(format_epoch "$now_epoch" '+%Y%m%d' 2>/dev/null || echo '')"

if [ "$WRITE_REGISTRY" -eq 1 ]; then
  printf '%s\n' \
    'update_epoch,update_time,batch_dir,project,batch,label,run_name,worker_slots,state,priority,start_epoch,start_time,last_activity_epoch,end_epoch,done_jobs,total_jobs,pct,eta_sec,queue_dir,queue_file,tag_clean,project_name' > "$tmp_registry"
fi

while IFS= read -r batch_dir; do
  [ -d "$batch_dir" ] || continue

  project_dir="$(dirname "$(dirname "$batch_dir")")"
  project_name="$(basename "$project_dir")"
  batch_name="$(basename "$batch_dir")"
  start_epoch="$(file_mtime "$batch_dir" 2>/dev/null || echo "$now_epoch")"

  run_name=""
  project_name_meta="?"
  project_name_fields=0
  execution_mode="shared-queue"
  queue_file=""
  original_queue_file=""
  editable_queue_file=""
  tag_clean_meta=""
  run_token_meta=""
  total_jobs_meta=0
  expected_hosts_meta=0
  has_meta=0
  adaptive_status_valid=0
  adaptive_status_stale=0
  adaptive_lifecycle=""
  adaptive_admission=""
  adaptive_schedule_priority=0
  adaptive_running_workers=0
  adaptive_target_workers=0
  adaptive_max_workers=0
  adaptive_propagation=""
  adaptive_effective_admission=""
  adaptive_blocking_reason=""
  adaptive_failure_held=0
  adaptive_running_by_host=none
  adaptive_priority_mark=""

  if [ -f "$batch_dir/run_meta.env" ]; then
    has_meta=1
    while IFS='=' read -r k v; do
      case "$k" in
        run_name) run_name="$v" ;;
        project_name) project_name_meta="$v"; project_name_fields=$((project_name_fields + 1)) ;;
        execution_mode) execution_mode="$v" ;;
        queue_file) queue_file="$v" ;;
        original_queue_file) original_queue_file="$v" ;;
        editable_queue_file) editable_queue_file="$v" ;;
        tag_clean) tag_clean_meta="$v" ;;
        run_token) run_token_meta="$v" ;;
        total_jobs)
          if [[ "$v" =~ ^[0-9]+$ ]]; then
            total_jobs_meta="$v"
          fi
          ;;
        expected_hosts)
          if [[ "$v" =~ ^[0-9]+$ ]]; then
            expected_hosts_meta="$v"
          fi
          ;;
        start_epoch)
          if [[ "$v" =~ ^[0-9]+$ ]]; then
            start_epoch="$v"
          fi
          ;;
      esac
    done < <(grep -E '^(run_name|project_name|execution_mode|queue_file|original_queue_file|editable_queue_file|tag_clean|run_token|total_jobs|expected_hosts|start_epoch)=' "$batch_dir/run_meta.env" || true)
    if [ "$project_name_fields" -gt 0 ] &&
        { [ "$project_name_fields" -ne 1 ] || [ "${#project_name_meta}" -gt 64 ] ||
          ! [[ "$project_name_meta" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; }; then
      echo "Invalid project_name metadata: $batch_dir/run_meta.env" >&2
      project_name_meta="?"
    fi
  fi

  # For legacy batches without run metadata, only inspect very recent dirs.
  if [ "$has_meta" -eq 0 ] && [ $((now_epoch - start_epoch)) -gt "$RUNNING_WINDOW_SEC" ]; then
    continue
  fi

  if [ -z "$queue_file" ] && [ -n "$original_queue_file" ]; then
    queue_file="$(basename "$original_queue_file")"
  fi

  if [ "$execution_mode" != "host-local-shards" ] && [ -z "$editable_queue_file" ] && [ -n "$original_queue_file" ]; then
    editable_queue_file="${original_queue_file%.*}-edit.${original_queue_file##*.}"
  fi

  if [ "$execution_mode" = "adaptive-shared-queue" ]; then
    adaptive_status_file="$batch_dir/adaptive/status.env"
    if [ -n "$run_token_meta" ] && \
       adaptive_values="$(adaptive_status_summary "$adaptive_status_file" "$run_token_meta" 2>/dev/null)"; then
      IFS=$'\t' read -r adaptive_lifecycle adaptive_admission adaptive_schedule_priority \
        adaptive_running_workers adaptive_target_workers adaptive_max_workers \
        adaptive_propagation adaptive_effective_admission adaptive_blocking_reason \
        adaptive_failure_held adaptive_running_by_host adaptive_priority_mark <<< "$adaptive_values"
      adaptive_status_valid=1
    else
      echo "Invalid or missing adaptive status: $adaptive_status_file" >&2
    fi
  fi

  tag_clean="$tag_clean_meta"
  if [ -z "$tag_clean" ] && [ "$has_meta" -eq 1 ] && [ -n "$queue_file" ]; then
    model_dir="$project_dir/"
    if [ -n "$original_queue_file" ]; then
      model_dir="$(dirname "$(dirname "$original_queue_file")")/"
    fi
    tag_clean="$(printf "%s" "${model_dir}${queue_file}" | tr '/' '_')"
  fi

  running_by_process=-1
  if [ "$execution_mode" != "adaptive-shared-queue" ] && [ -n "$tag_clean" ] && [ "${#hosts[@]}" -gt 0 ]; then
    if grep -Fxq "$tag_clean" "$tmp_running_tags"; then
      running_by_process=1
    elif [ "$PROCESS_SCAN_MODE" = "full" ]; then
      running_by_process=0
    else
      running_by_process=-1
    fi
  fi

  latest_worker_epoch=0
  is_recently_active=0
  if [ "$execution_mode" = "adaptive-shared-queue" ] && [ -f "$batch_dir/adaptive/status.env" ]; then
    latest_worker_epoch="$(file_mtime "$batch_dir/adaptive/status.env" 2>/dev/null || echo 0)"
  else
    latest_worker_epoch="$(get_latest_worker_epoch "$batch_dir")"
  fi
  if [ "$latest_worker_epoch" -gt 0 ] && [ $((now_epoch - latest_worker_epoch)) -le "$RUNNING_WINDOW_SEC" ]; then
    is_recently_active=1
  fi
  if [ "$execution_mode" = "adaptive-shared-queue" ] && [ "$adaptive_status_valid" -eq 1 ]; then
    case "$adaptive_lifecycle" in
      PENDING|RUNNING|DRAINING)
        if [ "$latest_worker_epoch" -le 0 ] \
            || [ $((now_epoch - latest_worker_epoch)) -gt "$ADAPTIVE_STATUS_STALE_SEC" ]; then
          adaptive_status_stale=1
        fi
        ;;
    esac
  fi

  if [ -z "$original_queue_file" ] && [ "$is_recently_active" -eq 1 ]; then
    original_queue_file="$(infer_queue_from_logs "$batch_dir")"
    if [ -z "$queue_file" ] && [ -n "$original_queue_file" ]; then
      queue_file="$(basename "$original_queue_file")"
    fi
    if [ "$execution_mode" != "host-local-shards" ] && [ -z "$editable_queue_file" ] && [ -n "$original_queue_file" ]; then
      editable_queue_file="${original_queue_file%.*}-edit.${original_queue_file##*.}"
    fi
  fi

  done_jobs=0
  shard_finished_jobs=0
  shard_failed_jobs=0
  shard_terminal_hosts=0
  shard_observed_hosts=0
  queue_nonempty=-1
  if [ "$execution_mode" != "host-local-shards" ] && [ "$execution_mode" != "adaptive-shared-queue" ] \
      && [ "$has_meta" -eq 1 ] && [ "$running_by_process" -ne 1 ] && [ -n "$editable_queue_file" ]; then
    queue_nonempty="$(queue_has_content "$editable_queue_file")"
  fi

  if [ "$execution_mode" = "host-local-shards" ]; then
    IFS=$'\t' read -r done_jobs shard_finished_jobs shard_failed_jobs shard_terminal_hosts shard_observed_hosts \
      <<< "$(shard_progress_summary "$batch_dir")"
    total_jobs="$total_jobs_meta"
  elif [ "$total_jobs_meta" -gt 0 ]; then
    total_jobs="$total_jobs_meta"
    done_jobs="$(count_done_markers_cached "$batch_dir/done")"
    if [ "$done_jobs" -gt "$total_jobs" ]; then
      done_jobs="$total_jobs"
    fi
  else
    done_jobs="$(count_done_markers_cached "$batch_dir/done")"
    total_jobs=0
  fi

  workers_done_hosts=0
  all_workers_done=0
  if [ "$execution_mode" != "host-local-shards" ] && [ "$execution_mode" != "adaptive-shared-queue" ] \
      && [ "$expected_hosts_meta" -gt 0 ]; then
    workers_done_hosts="$(count_worker_all_done_markers "$batch_dir")"
    if [ "$workers_done_hosts" -ge "$expected_hosts_meta" ]; then
      all_workers_done=1
    fi
  fi

  effective_running_by_process="$running_by_process"
  if [ "$running_by_process" -eq 1 ] && [ -n "$tag_clean" ]; then
    if [ "$total_jobs" -eq 0 ] || [ "$done_jobs" -lt "$total_jobs" ]; then
      if grep -Fxq "$tag_clean" "$tmp_running_assigned"; then
        effective_running_by_process=0
      else
        printf "%s\n" "$tag_clean" >> "$tmp_running_assigned"
      fi
    fi
  fi

  if [ "$execution_mode" = "adaptive-shared-queue" ]; then
    if [ "$adaptive_status_valid" -eq 1 ] && [ "$adaptive_status_stale" -eq 0 ]; then
      state="$adaptive_lifecycle"
      case "$state" in
        PENDING|RUNNING|DRAINING) priority=0 ;;
        COMPLETED) priority=2 ;;
        FINISHED_INCOMPLETE) priority=1 ;;
      esac
    else
      state="UNKNOWN"
      if [ "$adaptive_status_valid" -eq 1 ] && [ "$is_recently_active" -eq 1 ]; then
        priority=0
      else
        priority=1
      fi
    fi
  elif [ "$has_meta" -eq 1 ]; then
    if [ "$total_jobs" -gt 0 ] && [ "$done_jobs" -ge "$total_jobs" ]; then
      state="COMPLETED"
      priority=2
    elif [ "$execution_mode" = "host-local-shards" ] && [ "$shard_failed_jobs" -gt 0 ] \
        && [ "$shard_finished_jobs" -ge "$total_jobs" ]; then
      state="FAILED"
      priority=1
    elif [ "$effective_running_by_process" -eq 1 ]; then
      state="RUNNING"
      priority=0
    elif [ "$execution_mode" = "host-local-shards" ] && [ "$expected_hosts_meta" -gt 0 ] \
        && [ "$shard_terminal_hosts" -ge "$expected_hosts_meta" ]; then
      state="FAILED"
      priority=1
    elif [ "$effective_running_by_process" -eq 0 ] && [ "$total_jobs" -gt 0 ] && [ "$done_jobs" -lt "$total_jobs" ]; then
      state="KILLED"
      priority=1
    elif [ "$all_workers_done" -eq 1 ] && [ "$total_jobs" -gt 0 ] && [ "$done_jobs" -lt "$total_jobs" ]; then
      state="KILLED"
      priority=1
    elif [ "$workers_done_hosts" -gt 0 ] && [ "$total_jobs" -gt 0 ] && [ "$done_jobs" -lt "$total_jobs" ]; then
      state="RUNNING"
      priority=0
    elif [ "$effective_running_by_process" -eq 0 ] && [ "$queue_nonempty" -eq 1 ]; then
      state="KILLED"
      priority=1
    elif [ "$effective_running_by_process" -lt 0 ] && [ "$is_recently_active" -eq 1 ] && [ "$total_jobs" -gt 0 ] && [ "$done_jobs" -lt "$total_jobs" ]; then
      state="UNKNOWN"
      priority=1
    elif [ "$effective_running_by_process" -lt 0 ] && [ "$total_jobs" -gt 0 ] && [ "$done_jobs" -lt "$total_jobs" ]; then
      state="KILLED"
      priority=1
    elif [ "$done_jobs" -gt 0 ]; then
      state="COMPLETED"
      priority=2
    else
      state="KILLED"
      priority=1
    fi
  elif [ "$is_recently_active" -eq 1 ]; then
    state="RUNNING"
    priority=0
  elif [ "$total_jobs" -eq 0 ] && [ "$done_jobs" -gt 0 ]; then
    state="COMPLETED"
    priority=2
  else
    state="KILLED"
    priority=1
  fi

  # Skip fully empty historical batches with no metadata/progress; these add noise.
  if [ "$state" = "KILLED" ] && [ "$done_jobs" -eq 0 ] && [ "$total_jobs" -eq 0 ]; then
    continue
  fi
  quality=0
  if [ "$total_jobs" -eq 0 ] && [ "$done_jobs" -eq 0 ]; then
    quality=1
  fi

  elapsed_sec=$((now_epoch - start_epoch))
  if [ "$elapsed_sec" -lt 0 ]; then
    elapsed_sec=0
  fi
  end_epoch=0
  display_elapsed_sec="$elapsed_sec"
  end_source_epoch="$latest_worker_epoch"
  case "$execution_mode:$state" in
    adaptive-shared-queue:COMPLETED|adaptive-shared-queue:FINISHED_INCOMPLETE)
      end_source_epoch="$(adaptive_end_epoch "$batch_dir")" ;;
  esac
  if [ "$state" = "COMPLETED" ]; then
    if [ -d "$batch_dir/done" ]; then
      end_epoch="$(file_mtime "$batch_dir/done" 2>/dev/null || echo 0)"
    fi
    if [ "$end_epoch" -le 0 ] && [ "$end_source_epoch" -gt 0 ]; then
      end_epoch="$end_source_epoch"
    fi
    if [ "$end_epoch" -gt "$start_epoch" ]; then
      display_elapsed_sec=$((end_epoch - start_epoch))
    fi
  elif [ "$state" = "KILLED" ] || [ "$state" = "FAILED" ] \
      || [ "$state" = "FINISHED_INCOMPLETE" ]; then
    if [ "$end_source_epoch" -gt "$start_epoch" ]; then
      end_epoch="$end_source_epoch"
      display_elapsed_sec=$((end_source_epoch - start_epoch))
    fi
  fi

  if [ "$total_jobs" -gt 0 ]; then
    pct="$(awk -v d="$done_jobs" -v t="$total_jobs" 'BEGIN {printf "%.1f", (100*d)/t}')"
  else
    pct="0.0"
  fi

  batch_short="$batch_name"
  if [[ "$batch_name" =~ ^batch_([0-9]+)_ ]]; then
    batch_short="b${BASH_REMATCH[1]}"
  fi

  if [ -n "$run_name" ]; then
    label="$run_name"
  elif [ -n "$queue_file" ]; then
    label="${project_name}/${queue_file}"
  else
    label="${project_name}/${batch_name}"
  fi
  label="${label} [${batch_short}]"
  start_txt="$(format_epoch "$start_epoch" '+%Y/%m/%d | %H:%M' 2>/dev/null || echo 'n/a')"
  if [ "$execution_mode" = "adaptive-shared-queue" ]; then
    if [ "$adaptive_status_valid" -eq 1 ]; then
      # The renderer separates these labels from the per-host worker counts.
      adaptive_hold=""
      [ -n "$adaptive_effective_admission" ] || adaptive_effective_admission="$adaptive_admission"
      if [ "$adaptive_status_stale" -ne 0 ]; then
        adaptive_hold=STALE
      else
        case "$adaptive_effective_admission:$adaptive_blocking_reason" in
          closed:*) adaptive_hold=CLOSED ;;
          paused:pool_draining) adaptive_hold=DRAIN ;;
          paused:*) adaptive_hold=PAUSED ;;
          blocked:smb_stuck) adaptive_hold=SMB! ;;
          blocked:smb_*) adaptive_hold=SMB ;;
          blocked:*failure_hold*) adaptive_hold=FAIL ;;
          blocked:*host_unavailable*) adaptive_hold=HOST ;;
          blocked:*worker_cap*) adaptive_hold=CAP ;;
          blocked:*shared_io_capacity*) adaptive_hold=IO ;;
          blocked:*memory*) adaptive_hold=RAM ;;
          blocked:*cpu*) adaptive_hold=CPU ;;
          blocked:queue_empty) adaptive_hold=EMPTY ;;
          blocked:dependency_wait) adaptive_hold=DEP ;;
          blocked:dependency_failed) adaptive_hold=DEPFAIL ;;
          blocked:dependency_unverified) adaptive_hold=VERIFY ;;
          blocked:dependency_unavailable) adaptive_hold=DEPMISSING ;;
          blocked:*) adaptive_hold=WAIT ;;
        esac
        # A host holding the queue after failures needs attention even while others run.
        case "$adaptive_hold" in ''|CAP|IO|RAM|CPU|WAIT|EMPTY)
          [ "$adaptive_failure_held" -eq 0 ] || adaptive_hold=FAIL ;;
        esac
      fi
      adaptive_counts="$(adaptive_host_counts "$adaptive_running_by_host")" || \
        adaptive_counts="${adaptive_running_workers} run"
      # A trailing * marks per-host overrides of the default priority.
      worker_slots="$adaptive_counts P${adaptive_schedule_priority}${adaptive_priority_mark}${adaptive_hold:+ $adaptive_hold}"
    else
      worker_slots="adaptive:n/a"
    fi
  else
    worker_slots="$(worker_slots_for_tag "$tag_clean")"
  fi

  queue_dir="$project_dir"
  if [ -n "$original_queue_file" ]; then
    queue_dir="$(dirname "$original_queue_file")"
  fi
  queue_dir_relative="${queue_dir#"${ROOT%/}"/}"
  queue_path="$queue_dir"
  if [ -n "$original_queue_file" ]; then
    queue_path="$original_queue_file"
  elif [ -n "$queue_file" ]; then
    queue_path="${queue_dir%/}/$queue_file"
  fi
  queue_path_relative="${queue_path#"${ROOT%/}"/}"

  failed_jobs=0
  failed_txt=""
  rerun_jobs=0
  if [ "$execution_mode" = "adaptive-shared-queue" ]; then
    IFS=$'\t' read -r failed_jobs failed_epoch rerun_jobs <<< \
      "$(awk -F'\t' -v batch="$batch_dir" '$1 == batch {print $2 "\t" $3 "\t" $4; exit}' "$tmp_failed")" || true
    [[ "$failed_jobs" =~ ^[0-9]+$ ]] || failed_jobs=0
    [[ "$rerun_jobs" =~ ^[0-9]+$ ]] || rerun_jobs=0
    if [ "$failed_jobs" -gt 0 ]; then
      if [ "$(format_epoch "$failed_epoch" '+%Y%m%d' 2>/dev/null || true)" = "$today_ymd" ]; then
        failed_txt="$(format_epoch "$failed_epoch" '+%H:%M' 2>/dev/null || echo '?')"
      else
        failed_txt="$(format_epoch "$failed_epoch" '+%m-%d' 2>/dev/null || echo '?')"
      fi
    fi
  fi

  eta_sec=-1
  if { [ "$state" = "RUNNING" ] || [ "$state" = "DRAINING" ]; } \
      && [ "$done_jobs" -gt 0 ] && [ "$done_jobs" -lt "$total_jobs" ]; then
    eta_workers="$worker_slots"
    finished_jobs="$done_jobs"
    if [ "$execution_mode" = "adaptive-shared-queue" ]; then
      eta_workers="$adaptive_running_workers"
      finished_jobs=$((done_jobs + failed_jobs + rerun_jobs))
      case "$adaptive_hold" in
        SMB|SMB!|HOST) eta_workers=0 ;;
        PAUSED|CLOSED|DRAIN|DEP|DEPFAIL|VERIFY|DEPMISSING)
          [ $((total_jobs - finished_jobs)) -le "$eta_workers" ] || eta_workers=0 ;;
      esac
    fi
    if [ "$eta_workers" != 0 ]; then
      eta_sec="$(python3 "$SCRIPT_DIR/monitor_queue_eta.py" "$batch_dir" "$execution_mode" \
        "$total_jobs" "$finished_jobs" "$eta_workers" "$now_epoch")" || eta_sec=-1
    fi
  fi

  if [ "$state" = "RUNNING" ] || [ "$state" = "PENDING" ] || [ "$state" = "DRAINING" ] \
      || { [ "$state" = "UNKNOWN" ] && [ "$priority" -eq 0 ]; }; then
    history_epoch="$start_epoch"
    end_txt=""
  else
    history_epoch="$end_epoch"
    if [ "$history_epoch" -le 0 ] && [ "$latest_worker_epoch" -gt 0 ]; then
      history_epoch="$latest_worker_epoch"
    fi
    if [ "$history_epoch" -le 0 ]; then
      history_epoch="$start_epoch"
    fi
    end_txt="$(format_epoch "$history_epoch" '+%Y/%m/%d | %H:%M' 2>/dev/null || echo 'n/a')"
  fi

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$priority" "$quality" "$start_epoch" "$state" "$label" "$worker_slots" "$done_jobs" "$total_jobs" "$display_elapsed_sec" "$eta_sec" "$pct" "$start_txt" "$history_epoch" "$end_txt" "$queue_path_relative" "$tag_clean" "$project_name_meta" "$failed_jobs" "$failed_txt" "$rerun_jobs" >> "$tmp_rows"

  if [ "$WRITE_REGISTRY" -eq 1 ]; then
    update_time_txt="$(format_epoch "$now_epoch" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo '')"
    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$now_epoch" \
      "$(csv_escape "$update_time_txt")" \
      "$(csv_escape "$batch_dir")" \
      "$(csv_escape "$project_name")" \
      "$(csv_escape "$batch_name")" \
      "$(csv_escape "$label")" \
      "$(csv_escape "$run_name")" \
      "$(csv_escape "$worker_slots")" \
      "$(csv_escape "$state")" \
      "$priority" \
      "$start_epoch" \
      "$(csv_escape "$start_txt")" \
      "$latest_worker_epoch" \
      "$end_epoch" \
      "$done_jobs" \
      "$total_jobs" \
      "$pct" \
      "$eta_sec" \
      "$(csv_escape "$queue_dir_relative")" \
      "$(csv_escape "$queue_file")" \
      "$(csv_escape "$tag_clean")" \
      "$(csv_escape "$project_name_meta")" >> "$tmp_registry"
  fi
done < "$tmp_all_candidates"

if [ -s "$tmp_cache" ]; then
  mv "$tmp_cache" "$CACHE_FILE" 2>/dev/null || cp "$tmp_cache" "$CACHE_FILE" 2>/dev/null || true
fi

if [ "$WRITE_REGISTRY" -eq 1 ] && [ -s "$tmp_registry" ]; then
  registry_dir="$(dirname "$REGISTRY_FILE")"
  mkdir -p "$registry_dir"
  if acquire_registry_lock; then
    printf '%s\t%s\n' "$(hostname -s 2>/dev/null || hostname)" "$$" > "$REGISTRY_LOCK_DIR/owner"
    registry_stage="$(mktemp "$registry_dir/.QUEUE_PROGRESS.csv.XXXXXX")"
    cp "$tmp_registry" "$registry_stage"
    mv -f "$registry_stage" "$REGISTRY_FILE"
    registry_stage=""
    rm -f "$REGISTRY_LOCK_DIR/owner"
    rmdir "$REGISTRY_LOCK_DIR"
    registry_lock_held=0
  else
    echo "Skipping registry update because another writer holds $REGISTRY_LOCK_DIR" >&2
  fi
fi

if [ ! -s "$tmp_rows" ]; then
  echo "Queue Progress: no batch logs found."
  exit 0
fi

history_cutoff=$((now_epoch - HISTORY_WINDOW_SEC))

# Preserve every active queue and every terminal queue observed within the
# history window. Older terminal queues are selected separately by activity time.
awk -F'\t' -v cutoff="$history_cutoff" '
  $4 == "RUNNING" || $4 == "PENDING" || $4 == "DRAINING" { print "0\t" $0; next }
  $4 == "UNKNOWN" && $1 == 0 { print "0\t" $0; next }
  $13 >= cutoff { print "1\t" $0 }
' "$tmp_rows" > "$tmp_selected"

if [ "$OLDER_LIMIT" -gt 0 ]; then
  awk -F'\t' -v cutoff="$history_cutoff" '
    $4 != "RUNNING" && $4 != "PENDING" && $4 != "DRAINING" && !($4 == "UNKNOWN" && $1 == 0) && $13 < cutoff { print }
  ' "$tmp_rows" > "$tmp_older"
  sort -t $'\t' -k13,13nr -k3,3nr "$tmp_older" | awk -F'\t' -v max="$OLDER_LIMIT" 'NR <= max { print "2\t" $0 }' >> "$tmp_selected"
fi

# Group terminal rows by end state, then show the newest terminal event first
# within each state. Alphabetical state order places completed work before
# failures, killed work, and uncertain history.
sort -t $'\t' -k1,1n -k5,5 -k14,14nr -k4,4nr "$tmp_selected" > "$tmp_display"

if [ "$LIMIT" -gt 0 ]; then
  awk -F'\t' -v max="$LIMIT" 'NR <= max { print }' "$tmp_display" > "${tmp_display}.limited"
  mv "${tmp_display}.limited" "$tmp_display"
fi

if [ ! -s "$tmp_display" ]; then
  echo "Queue Progress: no queues match the history selection."
  exit 0
fi

if [ "$DATA_ONLY" -eq 1 ]; then
  cat "$tmp_display"
  exit 0
fi

if [ ! -x "$RENDERER_SCRIPT" ]; then
  echo "Missing executable queue renderer: $RENDERER_SCRIPT" >&2
  exit 1
fi

if [ "$SCROLLABLE" -eq 0 ] && [ "$TERM_LINES" -gt 0 ]; then
  visible_rows=$((TERM_LINES - 6))
  [ "$visible_rows" -ge 1 ] || visible_rows=1
  awk -v max="$visible_rows" 'NR <= max' "$tmp_display" > "${tmp_display}.limited"
  mv "${tmp_display}.limited" "$tmp_display"
fi
[ "$TERM_COLS" -ne 0 ] || TERM_COLS=150
render_args=(--data_file "$tmp_display" --term_cols "$TERM_COLS" --term_lines "$TERM_LINES"
  --bar_width "$BAR_WIDTH" --max_label_len "$MAX_LABEL_LEN"
  --history_window_sec "$HISTORY_WINDOW_SEC" --older_limit "$OLDER_LIMIT"
  --host_labels "${hosts[*]:-}")
if [ -t 1 ]; then
  printf '\033[H'
  render_args+=(--color)
fi
"$RENDERER_SCRIPT" "${render_args[@]}"
if [ -t 1 ]; then
  printf '\033[J'
fi
