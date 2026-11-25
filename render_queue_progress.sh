#!/usr/bin/env bash
set -euo pipefail

DATA_FILE=""
TERM_COLS=150
TERM_LINES=0
BAR_WIDTH=12
MAX_LABEL_LEN=160
HISTORY_WINDOW_SEC=86400
OLDER_LIMIT=5
ENABLE_COLOR=0
HOST_LABELS=""

usage() {
  cat <<EOF
Render a collected queue-progress TSV snapshot.

Usage:
  $0 --data_file <path> [--term_cols <n>] [--term_lines <n>] [--bar_width <n>] [--max_label_len <n>] [--history_window_sec <n>] [--older_limit <n>] [--host_labels "<h1> <h2> ..."] [--color]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --data_file) DATA_FILE="$2"; shift 2 ;;
    --term_cols) TERM_COLS="$2"; shift 2 ;;
    --term_lines) TERM_LINES="$2"; shift 2 ;;
    --bar_width) BAR_WIDTH="$2"; shift 2 ;;
    --max_label_len) MAX_LABEL_LEN="$2"; shift 2 ;;
    --history_window_sec) HISTORY_WINDOW_SEC="$2"; shift 2 ;;
    --older_limit) OLDER_LIMIT="$2"; shift 2 ;;
    --host_labels) HOST_LABELS="$2"; shift 2 ;;
    --color) ENABLE_COLOR=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [ -z "$DATA_FILE" ] || [ ! -s "$DATA_FILE" ]; then
  echo "Queue Progress: no queues match the history selection."
  exit 0
fi
if ! [[ "$TERM_COLS" =~ ^[0-9]+$ ]] || [ "$TERM_COLS" -lt 120 ] || [ "$TERM_COLS" -gt 1000 ]; then
  echo "--term_cols must be an integer between 120 and 1000" >&2
  exit 2
fi

run_color=$'\033[38;5;153m'
done_color=$'\033[38;5;187m'
killed_color=$'\033[38;5;217m'
unknown_color=$'\033[38;5;180m'
summary_color=$'\033[38;5;120m'
pct_fade_color=$'\033[38;5;244m'
failed_color=$'\033[1;38;5;203m'
reset_color=$'\033[0m'
clear_eol=$'\033[K'
if [ "$ENABLE_COLOR" -eq 0 ]; then
  run_color=""
  done_color=""
  killed_color=""
  unknown_color=""
  summary_color=""
  pct_fade_color=""
  failed_color=""
  reset_color=""
  clear_eol=""
fi

awk -F'\t' \
  -v term_cols="$TERM_COLS" \
  -v base_bar_cells="$BAR_WIDTH" \
  -v max_label_len="$MAX_LABEL_LEN" \
  -v history_window_sec="$HISTORY_WINDOW_SEC" \
  -v older_limit="$OLDER_LIMIT" \
  -v enable_color="$ENABLE_COLOR" \
  -v run_color="$run_color" \
  -v done_color="$done_color" \
  -v killed_color="$killed_color" \
  -v unknown_color="$unknown_color" \
  -v summary_color="$summary_color" \
  -v pct_fade_color="$pct_fade_color" \
  -v failed_color="$failed_color" \
  -v reset_color="$reset_color" \
  -v clear_eol="$clear_eol" \
  -v host_labels="$HOST_LABELS" '
function min(a, b) { return a < b ? a : b }
function trunc(s, m) {
  if (length(s) <= m) return s
  return substr(s, 1, m - 1) "~"
}
function project_label(s) {
  return length(s) <= 64 && s ~ /^[A-Za-z0-9][A-Za-z0-9._-]*$/ ? s : "?"
}
function dur(sec) {
  if (sec < 0) return "n/a"
  if (sec >= 86400) return sprintf("%.2fd", sec / 86400.0)
  if (sec >= 3600) return sprintf("%.2fh", sec / 3600.0)
  if (sec >= 60) return sprintf("%dm", int(sec / 60))
  return sprintf("%ds", sec)
}
function pad(s, width) {
  if (length(s) > width) s = trunc(s, width)
  return sprintf("%-" width "s", s)
}
function dynamic_info(s) {
  if (s == "adaptive:n/a") return "n/a"
  if (match(s, / P-?[0-9]+[*]?( |$)/)) return substr(s, RSTART + 1)
  return ""
}
function worker_counts(s) {
  if (s == "adaptive:n/a") return "?"
  if (match(s, / P-?[0-9]+[*]?( |$)/)) return substr(s, 1, RSTART - 1)
  return s
}
function priority_info(s) {
  if (s == "n/a") return "?"
  if (match(s, /^P-?[0-9]+[*]?/)) return substr(s, 2, RLENGTH - 1)
  return "-"
}
function ranked_priority(r) {
  return value[r, 1] + 0 == 0 && priority_value[r] ~ /^-?[0-9]+[*]?$/ && \
         value[r, 5] ~ /^(RUNNING|PENDING|DRAINING|UNKNOWN)$/
}
function admission_info(s,   hold) {
  if (s == "") return "-"
  if (s == "n/a") return "Unknown"
  hold = s
  sub(/^P-?[0-9]+[*]?[[:space:]]*/, "", hold)
  if (hold == "") return "Ready"
  if (hold == "PAUSED") return "Paused"
  if (hold == "DRAIN") return "Draining"
  if (hold == "CLOSED") return "Closed"
  if (hold == "WAIT") return "Wait"
  if (hold == "EMPTY") return "No pending"
  if (hold == "DEP") return "Dependency"
  if (hold == "DEPFAIL") return "Dep failed"
  if (hold == "VERIFY") return "Verify deps"
  if (hold == "DEPMISSING") return "Dep missing"
  if (hold == "IO") return "I/O wait"
  if (hold == "CPU") return "CPU wait"
  if (hold == "RAM") return "RAM wait"
  if (hold == "FAIL") return "Failure hold"
  if (hold == "HOST") return "Host wait"
  if (hold == "CAP") return "Worker cap"
  if (hold == "SMB!") return "SMB stuck"
  if (hold == "SMB") return "SMB wait"
  if (hold == "STALE") return "Stale"
  return hold
}
function running_count(s, state,   n, part, i, count, known) {
  if (state == "UNKNOWN") return -1
  if (s ~ /^[0-9]+ run( |$)/) return s + 0
  n = split(s, part, "-")
  count = 0
  known = 0
  for (i = 1; i <= n; i++) {
    if (part[i] ~ /^[0-9]+$/) {
      count += part[i] + 0
      known = 1
    }
  }
  return known ? count : -1
}
function make_progress(done, total, pct, cells,   filled, empty, a, b, pct_txt) {
  if (total > 0) {
    filled = int((pct / 100.0) * cells + 0.5)
    if (filled < 0) filled = 0
    if (filled > cells) filled = cells
    empty = cells - filled
    a = sprintf("%" filled "s", "")
    b = sprintf("%" empty "s", "")
    gsub(/ /, "#", a)
    gsub(/ /, "-", b)
    pct_txt = sprintf("%.1f%%", pct)
  } else {
    a = ""
    b = sprintf("%" cells "s", "")
    gsub(/ /, ".", b)
    pct_txt = (done > 0 ? "n/a" : "0.0%")
  }
  return "[" a b pct_txt "]"
}
function row_text(label, project, workers, priority, admission, start, progress, complete, elapsed, eta_end, state) {
  return pad(label, label_w) sep pad(project, project_w) sep \
         pad(workers, workers_w) sep pad(priority, priority_w) sep pad(admission, admission_w) sep pad(start, start_w) sep \
         pad(progress, progress_w) sep pad(complete, count_w) sep \
         pad(elapsed, elapsed_w) sep pad(eta_end, time_w) sep pad(state, state_w)
}
function completion_count(done, total) {
  return total > 0 ? sprintf("(%d / %d)", done, total) : sprintf("(%d / ?)", done)
}
# Claimed jobs that ended without a done marker and were not yet rerun by a later batch,
# with the newest last activity; otherwise how many were rerun. Narrow panes drop
# the time (style 1), then shorten to a terse failure count (style 0).
function failed_text(failed, when, rerun, style) {
  if (style == "") style = failed_style
  if (failed > 0) return style == 0 ? "!" failed : sprintf("%d failed%s", failed, style == 2 && when != "" ? " " when : "")
  return rerun > 0 && style > 0 ? rerun " rerun" : ""
}
function complete_text(done, total, failed, when, rerun, style,   note) {
  note = failed_text(failed, when, rerun, style)
  return completion_count(done, total) (note == "" ? "" : " " note)
}
function active_worker_vector(   worker_vector, host_total, i) {
  worker_vector = ""
  for (i = 1; i <= active_host_count; i++) {
    if (adaptive_distribution_unknown) active_host_unknown[i] = 1
    if (active_host_unknown[i]) {
      host_total = (active_host_workers[i] > 0 ? active_host_workers[i] "+?" : "?")
    } else {
      host_total = active_host_workers[i] + 0
    }
    worker_vector = worker_vector (worker_vector == "" ? "" : "-") host_total
  }
  return worker_vector
}
function print_active_summary(   worker_txt, state_txt, failed_txt, row) {
  if (active_queues == 0) return
  if (active_unknown && active_workers == 0) worker_txt = "? workers"
  else worker_txt = sprintf("%d%s workers", active_workers, active_unknown ? "+" : "")
  state_txt = sprintf("[%d queues]", active_queues)
  failed_txt = active_failed > 0 ? sprintf("%d job%s failed", active_failed, active_failed == 1 ? "" : "s") : ""
  row = row_text(label_w < 12 ? "TOTAL" : "TOTAL ACTIVE", "", active_worker_vector(), "", "", "", failed_txt, worker_txt, "", "", state_txt)
  failed_txt = trunc(failed_txt, progress_w)
  if (enable_color && failed_txt != "") sub(failed_txt, failed_color failed_txt reset_color summary_color, row)
  printf "%s%s%s%s\n", summary_color, row, reset_color, clear_eol
}
function accumulate_workers(s, state, dynamic,   n, part, i, vector, stale) {
  # Older status without a host distribution supplies only "<total> run".
  if (s ~ /^[0-9]+ run( |$)/) {
    if (state == "UNKNOWN") active_unknown = 1
    else active_workers += s + 0
    adaptive_distribution_unknown = 1
    if (host_label_count > active_host_count) active_host_count = host_label_count
    return
  }
  # Stale adaptive counts are reported as unknown rather than added.
  stale = (state == "UNKNOWN" && dynamic != "")
  vector = s
  sub(/ .*/, "", vector)
  n = split(vector, part, "-")
  if (n > active_host_count) active_host_count = n
  for (i = 1; i <= n; i++) {
    if (part[i] == ".") continue
    if (part[i] ~ /^[0-9]+$/ && !stale) {
      active_workers += part[i] + 0
      active_host_workers[i] += part[i] + 0
    } else {
      active_unknown = 1
      active_host_unknown[i] = 1
    }
  }
}
{
  rows++
  for (i = 1; i <= NF; i++) value[rows, i] = $i
  # Keep the collected snapshot/registry format stable; split only for display.
  sub(/ \[b[0-9]+\]$/, "", value[rows, 6])
  value[rows, 7] = worker_counts($7)
  dynamic_value[rows] = dynamic_info($7)
  priority_value[rows] = priority_info(dynamic_value[rows])
  admission_value[rows] = admission_info(dynamic_value[rows])
  if (length(value[rows, 7]) > max_workers_seen) max_workers_seen = length(value[rows, 7])
  if (length(priority_value[rows]) > max_priority_seen) max_priority_seen = length(priority_value[rows])
  if (length(admission_value[rows]) > max_admission_seen) max_admission_seen = length(admission_value[rows])
  if (length(value[rows, 6]) > max_label_seen) max_label_seen = length(value[rows, 6])
  project_value[rows] = project_label($18)
  if (length(project_value[rows]) > max_project_seen) max_project_seen = length(project_value[rows])
  for (style = 0; style <= 2; style++) {
    count_length = length(complete_text($8 + 0, $9 + 0, $19 + 0, $20, $21 + 0, style))
    if (count_length > max_count_by_style[style]) max_count_by_style[style] = count_length
  }
}
END {
  host_label_count = split(host_labels, host_label, /[[:space:]]+/)
  if (host_labels == "") host_label_count = 0
  for (r = 1; r <= rows; r++) {
    display_order[r] = r
    if (value[r, 1] + 0 == 0) {
      active_rows[++active_queues] = r
      active_rank[r] = running_count(value[r, 7], value[r, 5])
      accumulate_workers(value[r, 7], value[r, 5], dynamic_value[r])
      active_failed += value[r, 19]
      if (ranked_priority(r)) active_priority[priority_value[r] + 0] = 1
    }
  }
  # Dense score ranks describe current standing, including paused/held queues.
  for (r = 1; r <= rows; r++) {
    if (ranked_priority(r)) {
      rank = 1
      for (score in active_priority) if (score + 0 > priority_value[r] + 0) rank++
      priority_value[r] = priority_value[r] " (#" rank ")"
      if (length(priority_value[r]) > max_priority_seen) max_priority_seen = length(priority_value[r])
    }
  }
  # Stable insertion sort changes only active rows; collected history order stays intact.
  for (i = 2; i <= active_queues; i++) {
    candidate = active_rows[i]
    j = i - 1
    while (j > 0 && active_rank[active_rows[j]] < active_rank[candidate]) {
      active_rows[j + 1] = active_rows[j]
      j--
    }
    active_rows[j + 1] = candidate
  }
  j = 0
  for (r = 1; r <= rows; r++) {
    if (value[r, 1] + 0 == 0) display_order[r] = active_rows[++j]
  }
  summary_width = length(active_worker_vector())
  if (summary_width > max_workers_seen) max_workers_seen = summary_width
  # Leave the final terminal column unused. Some emulators immediately wrap
  # after writing there, which made otherwise exact-width rows appear doubled.
  layout_cols = term_cols - 1
  if (term_cols >= 180) sep = "  |  "
  else if (term_cols >= 157) sep = " | "
  else sep = "|"
  workers_w = max_workers_seen
  if (workers_w < 7) workers_w = 7
  priority_w = max_priority_seen
  if (priority_w < 8) priority_w = 8
  admission_w = max_admission_seen
  if (admission_w < 9) admission_w = 9
  start_w = 11
  failed_style = 2
  max_count_seen = max_count_by_style[failed_style]
  count_w = (max_count_seen > 14 ? max_count_seen : 14)
  elapsed_w = 8
  time_w = 11
  state_w = 12
  fixed_columns = workers_w + priority_w + admission_w + start_w + count_w + elapsed_w + time_w + state_w
  # Spend separator padding before shortening useful data, especially worker vectors.
  while (length(sep) > 1 && layout_cols - fixed_columns - 10 * length(sep) < 50) {
    sep = (length(sep) == 5 ? " | " : "|")
  }
  sep_total = 10 * length(sep)
  flexible = layout_cols - fixed_columns - sep_total
  if (flexible < 36) {
    compact_times = 1
    start_w = 5
    time_w = 7
    count_w = (max_count_seen > 11 ? max_count_seen : 11)
    elapsed_w = 6
    fixed_columns = workers_w + priority_w + admission_w + start_w + count_w + elapsed_w + time_w + state_w
    flexible = layout_cols - fixed_columns - sep_total
  }
  # Full admission labels take precedence over spare compact-time padding.
  if (compact_times && flexible == 26) {
    time_w--
    flexible++
  }
  while (flexible < 24 && failed_style > 0) {
    max_count_seen = max_count_by_style[--failed_style]
    count_min = compact_times ? 11 : 14
    flexible += count_w
    count_w = (max_count_seen > count_min ? max_count_seen : count_min)
    flexible -= count_w
  }
  if (flexible < 24) {
    printf "Terminal too narrow for complete worker counts; use --term_cols %d or more.\n", term_cols + 24 - flexible > "/dev/stderr"
    exit 2
  }
  label_w = 14
  project_goal = min(max_project_seen, 30)
  if (project_goal < 7) project_goal = 7
  project_w = min(project_goal, 20)
  progress_w = 16
  if (flexible < label_w + project_w + progress_w) {
    # Compact queue text before shortening numeric priorities or worker vectors.
    label_w = min(12, flexible - 15)
    project_w = 7
    progress_w = min(16, flexible - label_w - project_w)
  }
  extra = flexible - label_w - project_w - progress_w
  label_goal = min(max_label_len, max_label_seen)
  if (label_goal < label_w) label_goal = label_w
  progress_goal = 40
  while (extra > 0) {
    changed = 0
    if (project_w < project_goal && extra > 0) { project_w++; extra--; changed = 1 }
    if (label_w < label_goal && extra > 0) { label_w++; extra--; changed = 1 }
    if (project_w < project_goal && extra > 0) { project_w++; extra--; changed = 1 }
    if (progress_w < progress_goal && extra > 0) { progress_w++; extra--; changed = 1 }
    if (!changed) break
  }
  progress_cells = progress_w - 8
  if (progress_cells < 0) progress_cells = 0

  header = row_text("Queue", "Project", "Workers", "Priority", "Admission", "Start", "Progress", "Complete", "Elapsed", "ETA/End", "State")
  printf "%s%s\n", header, clear_eol
  line = sprintf("%" length(header) "s", "")
  gsub(/ /, "-", line)
  printf "%s%s\n", line, clear_eol

  previous_group = -1
  for (display_index = 1; display_index <= rows; display_index++) {
    r = display_order[display_index]
    group = value[r, 1] + 0
    if (group != previous_group) {
      if (previous_group == 0) print_active_summary()
      if (display_index > 1) printf "%s\n", clear_eol
      if (group == 0) section = "ACTIVE"
      else if (group == 1) section = sprintf("LAST %.0f HOURS", history_window_sec / 3600.0)
      else section = sprintf("EARLIER (%d MOST RECENT)", older_limit)
      printf "%s-- %s --%s%s\n", pct_fade_color, section, reset_color, clear_eol
      previous_group = group
    }

    state = value[r, 5]
    workers = value[r, 7]
    done = value[r, 8] + 0
    total = value[r, 9] + 0
    pct = value[r, 12] + 0.0
    start_txt = value[r, 13]
    start_short = substr(start_txt, 6, 5) " " substr(start_txt, 14, 5)
    end_txt = value[r, 15]
    end_short = (end_txt == "" ? "" : substr(end_txt, 6, 5) " " substr(end_txt, 14, 5))
    if (compact_times) {
      start_short = substr(start_txt, 14, 5)
      end_short = substr(end_txt, 14, 5)
    }
    progress = make_progress(done, total, pct, progress_cells)
    failed_txt = failed_text(value[r, 19] + 0, value[r, 20], value[r, 21] + 0)
    complete = complete_text(done, total, value[r, 19] + 0, value[r, 20], value[r, 21] + 0)
    elapsed = dur(value[r, 10] + 0)
    eta_end = end_short
    if (group == 0) eta_end = ((state == "RUNNING" || state == "DRAINING" || state == "PENDING") && value[r, 11] + 0 >= 0 ? dur(value[r, 11] + 0) : "")
    if (group == 0 && eta_end != "" && value[r, 22] ~ /^(recent_slower|recent_faster|long_running)$/) eta_end = eta_end "?"
    state_txt = "[" (state == "FINISHED_INCOMPLETE" ? "INCOMPLETE" : state) "]"
    row = row_text(value[r, 6], project_value[r], workers, priority_value[r], admission_value[r], start_short, progress, complete, elapsed, eta_end, state_txt)

    if (state == "RUNNING" || state == "PENDING" || state == "DRAINING") color = run_color
    else if (state == "KILLED" || state == "FAILED" || state == "FINISHED_INCOMPLETE") color = killed_color
    else if (state == "UNKNOWN") color = unknown_color
    else color = done_color
    pct_txt = (total > 0 ? sprintf("%.1f%%", pct) : (done > 0 ? "n/a" : "0.0%"))
    colored = row
    if (enable_color) sub(pct_txt, pct_fade_color pct_txt color, colored)
    if (enable_color && failed_txt != "")
      sub(failed_txt, (value[r, 19] + 0 > 0 ? failed_color : pct_fade_color) failed_txt reset_color color, colored)
    printf "%s%s%s%s\n", color, colored, reset_color, clear_eol

  }
  if (previous_group == 0) print_active_summary()
}
' "$DATA_FILE"
