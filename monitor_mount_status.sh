#!/usr/bin/env bash
set -euo pipefail

HOSTS=""
REFRESH_SEC=60
HEIGHT=""
WIDTH=""
ONCE=0
CPU_PANES=""
ROOT="${PARALLEL_WORKERS_ROOT:-}"
HOST_LABELS=""
SNAPSHOT=""
SNAPSHOT_SCRIPT=""

usage() {
  echo "Usage: $0 --hosts \"user@host ...\" [--cpu_panes \"%id ...\"] [--refresh_sec 60] [--height N] [--width N] [--once]"
  echo "       [--snapshot <cluster-status.json> --snapshot_script <monitor_snapshot.py> --host_labels \"<label> ...\"]"
  echo "Shows the mount-table entry for --root PATH (default: $ROOT), or the collector's view of it."
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root|--hosts|--host_labels|--refresh_sec|--height|--width|--cpu_panes|--snapshot|--snapshot_script)
      [ "$#" -ge 2 ] || { echo "$1 requires a value" >&2; exit 2; } ;;
  esac
  case "$1" in
    --root) ROOT="$2"; shift 2 ;;
    --hosts) HOSTS="$2"; shift 2 ;;
    --cpu_panes) CPU_PANES="$2"; shift 2 ;;
    --host_labels) HOST_LABELS="$2"; shift 2 ;; # Used only with --snapshot.
    --snapshot) SNAPSHOT="$2"; shift 2 ;;
    --snapshot_script) SNAPSHOT_SCRIPT="$2"; shift 2 ;;
    --refresh_sec) REFRESH_SEC="$2"; shift 2 ;;
    --height) HEIGHT="$2"; shift 2 ;;
    --width)
      [ -n "$2" ] || { echo "--width must be an integer of at least 3" >&2; exit 2; }
      WIDTH="$2"; shift 2 ;;
    --once) ONCE=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

: "${ROOT:?Source the inventory environment or pass --root}"

if ! [[ "$REFRESH_SEC" =~ ^[1-9][0-9]*$ ]] || [ "$REFRESH_SEC" -gt 600 ]; then
  echo "--refresh_sec must be an integer between 1 and 600" >&2
  exit 2
fi
if [ -n "$HEIGHT" ] && ! [[ "$HEIGHT" =~ ^[1-9][0-9]*$ ]]; then
  echo "--height must be a positive integer" >&2
  exit 2
fi
if [ -n "$WIDTH" ] && { ! [[ "$WIDTH" =~ ^[1-9][0-9]*$ ]] || [ "$WIDTH" -lt 3 ]; }; then
  echo "--width must be an integer of at least 3" >&2
  exit 2
fi
read -r -a hosts <<< "$HOSTS"
if [ "${#hosts[@]}" -eq 0 ]; then
  echo "--hosts is required" >&2
  exit 2
fi
for host in "${hosts[@]}"; do
  if [[ "$host" == -* ]]; then
    echo "Invalid SSH host: $host" >&2
    exit 2
  fi
done
read -r -a cpu_panes <<< "$CPU_PANES"
if [ -n "$CPU_PANES" ] && [ "${#cpu_panes[@]}" -ne "${#hosts[@]}" ]; then
  echo "--cpu_panes must match the host count" >&2
  exit 2
fi
if [ -n "$SNAPSHOT" ]; then
  read -r -a host_labels <<< "$HOST_LABELS"
  if [ "${#host_labels[@]}" -ne "${#hosts[@]}" ] || [ ! -f "$SNAPSHOT_SCRIPT" ]; then
    echo "--snapshot requires --snapshot_script and one --host_labels entry per host" >&2
    exit 2
  fi
fi

REMOTE_BODY=$(cat <<'REMOTE'
if [ "$(uname -s)" = Linux ]; then
  exec python3 "$HOME/parallel-workers-monitor/monitor_linux.py" mount --root "$1"
fi
mount_table=$(/sbin/mount 2>/dev/null) || { printf 'UNKNOWN\n'; exit 2; }
awk -v root="$1" '
  NF {
    count++
    if ($0 !~ /^.+ on \/.* \([^()]+\)$/) invalid=1
    path=$0
    sub(/^.* on /, "", path)
    sub(/ \([^()]+\)$/, "", path)
    if (path == root) mounted=1
  }
  END {
    if (!count || invalid) { print "UNKNOWN"; exit 2 }
    if (mounted) { print "MOUNTED"; exit 0 }
    print "MISSING"; exit 1
  }
' <<< "$mount_table"
REMOTE
)
REMOTE_COMMAND="/bin/bash -lc $(printf '%q' "$REMOTE_BODY") mount-check $(printf '%q' "$ROOT")"
states=()
down_since=()
for host in "${hosts[@]}"; do states+=("?"); down_since+=(0); done

pane_size() {
  local dimensions detected_height="" detected_width=""
  if [ -z "$HEIGHT" ] || [ -z "$WIDTH" ]; then
    if dimensions="$(stty size 2>/dev/null)" && \
        [[ "$dimensions" =~ ^([1-9][0-9]*)[[:space:]]+([1-9][0-9]*)$ ]]; then
      detected_height="${BASH_REMATCH[1]}"
      detected_width="${BASH_REMATCH[2]}"
    fi
  fi
  pane_lines="${HEIGHT:-$detected_height}"
  pane_cols="${WIDTH:-$detected_width}"
  if [ -z "$pane_lines" ]; then
    echo "Cannot read pane height; supply --height" >&2
    exit 1
  fi
  if [ -z "$pane_cols" ]; then
    echo "Cannot read pane width; supply --width" >&2
    exit 1
  fi
}

poll_host() {
  local reply status
  if reply="$(ssh -n -o BatchMode=yes -o ConnectTimeout=4 -o ConnectionAttempts=1 \
      -o ServerAliveInterval=4 -o ServerAliveCountMax=1 "$1" "$REMOTE_COMMAND" 2>/dev/null)"; then
    status=0
  else
    status=$?
  fi
  case "$status:$reply" in
    0:MOUNTED) printf '✔' ;;
    1:MISSING) printf '✗' ;;
    *) printf '?' ;;
  esac
}

draw() {
  local i blank row=1 block extra base available color separator padding offset age age_text now
  local geometry pane_id pane_top pane_height mount_top="" cpu_top=() cpu_height=()
  now="$(date +%s)"
  pane_size
  if [ -n "$CPU_PANES" ]; then
    geometry="$(tmux list-panes -t "$TMUX_PANE" -F '#{pane_id} #{pane_top} #{pane_height}')" || return
    while read -r pane_id pane_top pane_height; do
      if [ "$pane_id" = "$TMUX_PANE" ]; then mount_top="$pane_top"; fi
      for i in "${!cpu_panes[@]}"; do
        if [ "$pane_id" = "${cpu_panes[$i]}" ]; then
          cpu_top[$i]="$pane_top"; cpu_height[$i]="$pane_height"
        fi
      done
    done <<< "$geometry"
    [ -n "$mount_top" ] && [ "${#cpu_top[@]}" -eq "${#hosts[@]}" ] || return 1
  fi
  if [ "$ONCE" -eq 0 ]; then printf '\033[0m\033[H\033[J'; fi
  if [ "$pane_cols" -lt 3 ]; then
    if [ "$ONCE" -eq 1 ]; then
      echo "Pane width must allow a three-character checkbox" >&2
      exit 2
    fi
    printf '%.*s' "$pane_cols" resize
    return
  fi
  if [ "$pane_lines" -lt $(( ${#hosts[@]} * 2 - 1 )) ]; then
    if [ "$ONCE" -eq 1 ]; then
      echo "Pane height must allow one row per host and separators" >&2
      exit 2
    fi
    printf '%.*s' "$pane_cols" resize
    return
  fi
  printf -v separator '%*s' "$pane_cols" ''
  separator="${separator// /─}"
  padding=$(((pane_cols - 3) / 2))
  # The CPU grid shares its top border; our first host lacks that one row.
  available=$((pane_lines - ${#hosts[@]} + 2))
  base=$((available / ${#hosts[@]}))
  extra=$((available % ${#hosts[@]}))
  for i in "${!hosts[@]}"; do
    block="$base"
    if [ "$i" -lt "$extra" ]; then block=$((block + 1)); fi
    if [ "$i" -eq 0 ]; then block=$((block - 1)); fi
    if [ -n "$CPU_PANES" ]; then
      row=$((cpu_top[i] - mount_top + 1))
      block="${cpu_height[$i]}"
    fi
    offset=0
    if [ "$block" -ge 2 ]; then offset=1; fi
    case "${states[$i]}" in
      ✔) color=32 ;;
      ✗) color=31 ;;
      *) color=33 ;;
    esac
    if [ "$ONCE" -eq 0 ]; then
      if [ "$i" -gt 0 ]; then printf '\033[%d;1H\033[0m%s' "$((row - 1))" "$separator"; fi
      printf '\033[%d;1H' "$((row + offset))"
    elif [ "$offset" -eq 1 ]; then
      printf '\n'
    fi
    printf '\033[0m%*s\033[%sm[%s]\033[0m' "$padding" '' "$color" "${states[$i]}"
    blank=$offset
    if [ "${states[$i]}" = ✗ ] && [ "$block" -gt $((offset + 1)) ]; then
      age=$((now - down_since[i]))
      [ "$age" -lt 0 ] && age=0
      if [ "$age" -ge 86400 ]; then age_text="$((age / 86400))d"
      elif [ "$age" -ge 3600 ]; then age_text="$((age / 3600))h"
      elif [ "$age" -ge 60 ]; then age_text="$((age / 60))m"
      else age_text="${age}s"; fi
      age_text="${age_text:0:pane_cols}"
      if [ "$ONCE" -eq 1 ]; then printf '\n'
      else printf '\033[%d;1H' "$((row + offset + 1))"; fi
      printf '%*s%s' "$(((pane_cols - ${#age_text}) / 2))" '' "$age_text"
      blank=$((blank + 1))
    fi
    if [ "$ONCE" -eq 1 ]; then
      for ((; blank<block; blank++)); do printf '\n'; done
      if [ "$i" -lt $((${#hosts[@]} - 1)) ]; then printf '\033[0m%s\n' "$separator"; fi
    fi
    row=$((row + block + 1))
  done
}

# The collector tracks when each host's mount went missing; an offline host or
# a stale snapshot reads as unknown.
refresh_from_snapshot() {
  local i=0 state since
  while read -r state since; do
    states[$i]="$state"
    down_since[$i]="$since"
    i=$((i + 1))
  done < <(python3 "$SNAPSHOT_SCRIPT" mount --snapshot "$SNAPSHOT" --hosts "$HOST_LABELS")
  if [ "$i" -ne "${#hosts[@]}" ]; then
    for i in "${!hosts[@]}"; do states[$i]="?"; done
  fi
  if [ "$ONCE" -eq 0 ]; then draw; fi
}

refresh() {
  local i
  if [ -n "$SNAPSHOT" ]; then
    refresh_from_snapshot
    return
  fi
  # Retain the last observation while the next SSH check is in flight.
  if [ "$ONCE" -eq 0 ]; then draw; fi
  for i in "${!hosts[@]}"; do
    states[$i]="$(poll_host "${hosts[$i]}")"
    case "${states[$i]}" in
      ✔) down_since[$i]=0 ;;
      ✗) if [ "${down_since[$i]}" -eq 0 ]; then down_since[$i]="$(date +%s)"; fi ;;
    esac
    if [ "$ONCE" -eq 0 ]; then draw; fi
  done
}

if [ "$ONCE" -eq 1 ]; then
  refresh
  draw
  exit 0
fi
sleep_pid=""
redraw_needed=0
trap 'if [ -n "$sleep_pid" ]; then kill "$sleep_pid" 2>/dev/null || true; fi; printf "\033[0m\033[?25h"' EXIT
trap 'exit 0' HUP INT TERM
# A trap can interrupt a draw while stdin is redirected to pane geometry.
# Defer drawing to the main loop instead of reentering it from the trap.
trap 'redraw_needed=1' WINCH
printf '\033[?25l'
while true; do
  refresh
  # Bash defers traps during a foreground sleep. Waiting on a child lets WINCH
  # redraw immediately without triggering another network poll.
  sleep "$REFRESH_SEC" &
  sleep_pid=$!
  while kill -0 "$sleep_pid" 2>/dev/null; do
    if [ "$redraw_needed" -eq 1 ]; then
      redraw_needed=0
      draw
    else
      wait "$sleep_pid" || true
    fi
  done
  sleep_pid=""
done
