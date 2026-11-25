#!/usr/bin/env bash
export ROTA_ROOT="${ROTA_ROOT:-}"
set -euo pipefail

HOSTS_FILE="${ROTA_HOSTS_FILE:-${ROTA_ROOT}/hosts/hosts-ethernet.txt}"
HOSTS_OVERRIDE=""
SSH_CONNECT_TIMEOUT_SEC=3
SSH_SCAN_TIMEOUT_SEC=15

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hosts_file) HOSTS_FILE="$2"; shift 2 ;;
    --hosts) HOSTS_OVERRIDE="$2"; shift 2 ;;
    --ssh_connect_timeout_sec) SSH_CONNECT_TIMEOUT_SEC="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--hosts_file <path>] [--hosts \"<h1> <h2> ...\"] [--ssh_connect_timeout_sec <n>]"
      exit 0
      ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

hosts=()
if [ -n "$HOSTS_OVERRIDE" ]; then
  # shellcheck disable=SC2206
  hosts=($HOSTS_OVERRIDE)
elif [ -f "$HOSTS_FILE" ]; then
  while read -r host _; do
    [[ -z "${host:-}" ]] && continue
    [[ "$host" == \#* ]] && continue
    hosts+=("$host")
  done < "$HOSTS_FILE"
fi

if [ "${#hosts[@]}" -eq 0 ]; then
  echo "No worker hosts configured" >&2
  exit 1
fi

REMOTE_SCAN_CMD=$(cat <<'EOF'
ps -ww -axo ppid=,command= | awk '{
  parent_ppid = $1
  line_tag = ""
  slots = ""
  for (i = 2; i <= NF; i++) {
    if ($i ~ /^_Volumes_/) {
      print "TAG\t" $i
      line_tag = $i
    } else if ($i ~ /^mmworker_/) {
      t=$i
      sub(/^mmworker_/, "", t)
      print "TAG\t" t
      if (line_tag == "") line_tag = t
    } else if ($i == "--job_tag" && i < NF) {
      print "TAG\t" $(i + 1)
    } else if ($i ~ /^mmshardworker_/) {
      t=$i
      sub(/^mmshardworker_/, "", t)
      print "TAG\t" t
      if (line_tag == "") line_tag = t
    }
    if ($i ~ /(^|\/)mmworker\.sh$/) slots = $(i + 1)
    if ($i ~ /(^|\/)mmshardworker\.sh$/) slots = $(i + 2)
  }
  if (parent_ppid == 1 && line_tag != "" && slots ~ /^[0-9]+$/) {
    print "SLOT\t" line_tag "\t" slots
  }
}' | LC_ALL=C sort -u
EOF
)

scan_host() {
  local host_i="$1" host scan_ok scan_out
  local rec_type rec_tag rec_slots
  host="${hosts[$host_i]}"
  scan_ok=0
  scan_out=""
  # ConnectTimeout does not bound a multiplexed session: a master whose peer
  # vanished keeps clients waiting, so keepalives retire masters this scan
  # starts and a hard alarm bounds masters started by anything else.
  if scan_out="$(perl -e 'alarm shift; exec @ARGV' "$SSH_SCAN_TIMEOUT_SEC" \
    ssh -o BatchMode=yes -o ConnectTimeout="$SSH_CONNECT_TIMEOUT_SEC" -o ConnectionAttempts=2 \
    -o ServerAliveInterval=5 -o ServerAliveCountMax=2 \
    -o ControlMaster=auto -o ControlPersist=600 -o ControlPath=/tmp/rota-ssh-%C \
    "$host" "$REMOTE_SCAN_CMD" 2>/dev/null)"; then
    scan_ok=1
  fi
  if [ "$scan_ok" -eq 1 ]; then
    printf 'STATUS\t%s\t1\n' "$host_i"
    if [ -n "$scan_out" ]; then
      while IFS=$'\t' read -r rec_type rec_tag rec_slots; do
        case "${rec_type:-}" in
          TAG) [ -n "${rec_tag:-}" ] && printf 'TAG\t%s\n' "$rec_tag" ;;
          SLOT)
            if [ -n "${rec_tag:-}" ] && [[ "${rec_slots:-}" =~ ^[0-9]+$ ]]; then
              printf 'SLOT\t%s\t%s\t%s\n' "$rec_tag" "$host_i" "$rec_slots"
            fi
            ;;
        esac
      done <<< "$scan_out"
    fi
  else
    printf 'STATUS\t%s\t0\n' "$host_i"
  fi
}

# Scan hosts concurrently so an unreachable node costs one timeout, not one per
# remaining host; print in hosts-file order as before.
scan_dir="$(mktemp -d "${TMPDIR:-/tmp}/queue-worker-scan.XXXXXX")"
trap 'rm -f "$scan_dir"/*.out; rmdir "$scan_dir"' EXIT
for host_i in "${!hosts[@]}"; do
  scan_host "$host_i" > "$scan_dir/$host_i.out" &
done
wait
for host_i in "${!hosts[@]}"; do
  cat "$scan_dir/$host_i.out"
done
