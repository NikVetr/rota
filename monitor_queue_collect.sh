#!/usr/bin/env bash
export PARALLEL_WORKERS_ROOT="${PARALLEL_WORKERS_ROOT:-}"
set -euo pipefail

# One queue-progress collection, printed as the collector's TSV: scan worker
# processes on every host, then collect queue metadata where the share is
# directly attached (on the coordinator over SSH, or here in local mode).
ROOT="${PARALLEL_WORKERS_ROOT}"
COLLECTOR_SCRIPT=""
WORKER_SCRIPT=""
HOSTS=""
FALLBACK_HOST=""
MODE="remote"
LIMIT=0
TIMEOUT_SEC=120
NO_REGISTRY=""
ETA_WARNINGS=""

usage() {
  echo "Usage: $0 --collector_script <path> --worker_script <path> [--hosts \"<h1> ...\"] [--root <path>] [--mode remote|local] [--fallback_host <user@host>] [--limit <n>] [--timeout_sec <n>] [--no_registry] [--eta_warnings]"
  echo "Without --hosts, the worker scan reads its hosts file."
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root) ROOT="$2"; shift 2 ;;
    --collector_script) COLLECTOR_SCRIPT="$2"; shift 2 ;;
    --worker_script) WORKER_SCRIPT="$2"; shift 2 ;;
    --hosts) HOSTS="$2"; shift 2 ;;
    --fallback_host) FALLBACK_HOST="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --limit) LIMIT="$2"; shift 2 ;;
    --timeout_sec) TIMEOUT_SEC="$2"; shift 2 ;;
    --eta_warnings) ETA_WARNINGS=" --eta_warnings"; shift ;;
    --no_registry) NO_REGISTRY=" --no_registry"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

: "${ROOT:?Source the inventory environment or pass --root}"

for required in "$COLLECTOR_SCRIPT" "$WORKER_SCRIPT"; do
  if [ ! -f "$required" ]; then
    echo "Missing queue monitor helper: ${required:-<unset>}" >&2
    exit 1
  fi
done
case "$MODE" in
  local) ;;
  remote) [ -n "$FALLBACK_HOST" ] || { echo "--mode remote requires --fallback_host" >&2; exit 2; } ;;
  *) echo "--mode must be remote or local" >&2; exit 2 ;;
esac
if ! [[ "$TIMEOUT_SEC" =~ ^[0-9]+$ ]] || [ "$TIMEOUT_SEC" -lt 10 ] || [ "$TIMEOUT_SEC" -gt 600 ]; then
  echo "--timeout_sec must be an integer between 10 and 600" >&2
  exit 2
fi

worker_file="$(mktemp "${TMPDIR:-/tmp}/queue-progress-workers.XXXXXX")"
trap 'rm -f "$worker_file"' EXIT
"$WORKER_SCRIPT" --hosts "$HOSTS" > "$worker_file"
if [ "$MODE" = "local" ]; then
  # shellcheck disable=SC2086
  "$COLLECTOR_SCRIPT" --root "$ROOT" --limit "$LIMIT" --hosts "$HOSTS" \
    --process_scan_file "$worker_file" --data_only $NO_REGISTRY $ETA_WARNINGS
  exit
fi
# The remote collector stops itself before the caller's deadline.
remote_timeout_sec=$((TIMEOUT_SEC - 10))
[ "$remote_timeout_sec" -ge 10 ] || remote_timeout_sec=10
printf -v root_q '%q' "$ROOT"
printf -v collector_q '%q' "$ROOT/$(basename "$COLLECTOR_SCRIPT")"
printf -v hosts_q '%q' "$HOSTS"
collector_name="$(basename "$COLLECTOR_SCRIPT")"
collector_args="--root $root_q --limit $LIMIT --hosts $hosts_q --process_scan_file /dev/stdin --self_timeout_sec $remote_timeout_sec --data_only$NO_REGISTRY$ETA_WARNINGS"
ssh -o BatchMode=yes -o ConnectTimeout=6 -o ConnectionAttempts=1 \
  -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR "$FALLBACK_HOST" \
  "if [ -f $collector_q ]; then exec bash $collector_q $collector_args; elif [ -f \"\$HOME/parallel-workers-monitor/$collector_name\" ]; then exec bash \"\$HOME/parallel-workers-monitor/$collector_name\" $collector_args; else exit 127; fi" \
  < "$worker_file"
