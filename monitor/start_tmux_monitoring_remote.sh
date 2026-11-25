#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -n "${HOSTS_OVERRIDE:-}" ]; then
  exec "$SCRIPT_DIR/start_tmux_monitoring.sh" --direct --hosts "$HOSTS_OVERRIDE" "$@"
fi
exec "$SCRIPT_DIR/start_tmux_monitoring.sh" --direct "$@"
