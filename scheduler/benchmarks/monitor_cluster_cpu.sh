#!/usr/bin/env bash
export ROTA_ROOT="${ROTA_ROOT:?Source the inventory environment first}"
set -euo pipefail

HOSTS_FILE="${ROTA_HOSTS_FILE:?Source the inventory environment first}"
INTERVAL_SEC=2

usage() {
  cat <<EOF
Polls per-host CPU usage summary via SSH (macOS top header).

Usage:
  $0 [--hosts_file <path>] [--interval_sec <n>]

Tip: run this in a separate terminal while workers run.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hosts_file) HOSTS_FILE="$2"; shift 2 ;;
    --interval_sec) INTERVAL_SEC="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ ! -f "$HOSTS_FILE" ]]; then
  echo "Hosts file not found: $HOSTS_FILE" >&2
  exit 2
fi

while true; do
  ts=$(date +"%F %T")
  echo "=== $ts ==="
  while read -r host _ || [[ -n "${host:-}" ]]; do
    [[ -z "$host" || "$host" == \#* ]] && continue
    echo "-- $host"
    ssh -n -o BatchMode=yes -o ConnectTimeout=2 "$host" \
      "top -l 1 -n 0 | awk '/CPU usage|Processes|Load Avg/ { print; if (++n == 3) exit }'" || echo "ssh_failed"
  done < "$HOSTS_FILE"
  sleep "$INTERVAL_SEC"
done
