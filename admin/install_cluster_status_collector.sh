#!/usr/bin/env bash
set -euo pipefail

# Install the cluster status collector as a systemd --user service on the
# Linux dashboard host. Remote hosts must already have the monitor
# helpers: run ./sync_parallel_workers_monitoring.sh first.
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT=parallel-workers-collector.service
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
SETTINGS_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/parallel-workers"
SETTINGS="$SETTINGS_DIR/monitor-environment.json"
SYSTEMCTL="${SYSTEMCTL:-systemctl}"
ACTION=install

usage() {
  cat <<EOF
Usage: $0 [--print | --uninstall]

Installs $UNIT into $UNIT_DIR (ExecStart uses $REPO_DIR),
then enables and starts it, and links pw-control (scheduler controls built on
the collector snapshot) into ~/.local/bin. --print shows the unit without
installing; --uninstall stops, disables and removes both.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --print) ACTION=print; shift ;;
    --uninstall) ACTION=uninstall; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

# systemd unit paths cannot be quoted; refuse paths the substitution would break.
if ! [[ "$REPO_DIR" =~ ^/[A-Za-z0-9._/-]+$ ]]; then
  echo "Repository path is unsuitable for a unit file: $REPO_DIR" >&2
  exit 1
fi
unit_text="$(sed "s|@REPO_DIR@|$REPO_DIR|g" "$REPO_DIR/systemd/$UNIT")"

case "$ACTION" in
  print)
    printf '%s\n' "$unit_text"
    ;;
  uninstall)
    "$SYSTEMCTL" --user disable --now "$UNIT"
    rm -f "$UNIT_DIR/$UNIT"
    rm -f "$SETTINGS"
    [ ! -L "$HOME/.local/bin/pw-control" ] || rm -f "$HOME/.local/bin/pw-control"
    "$SYSTEMCTL" --user daemon-reload
    echo "Removed $UNIT; the last snapshot stays in ${XDG_STATE_HOME:-$HOME/.local/state}/parallel-workers"
    ;;
  install)
    : "${PARALLEL_WORKERS_CONFIG_DIR:?Source the rendered inventory before installing the collector}"
    python3 -c 'import sys
from pathlib import Path
for filename in sys.argv[1:]:
    compile(Path(filename).read_bytes(), filename, "exec")' \
      "$REPO_DIR/cluster_status_collector.py" "$REPO_DIR/cluster_config.py" "$REPO_DIR/pw_control.py"
    if [ -n "${PARALLEL_WORKERS_CONFIG_DIR:-}" ]; then
      mkdir -p "$SETTINGS_DIR"
      settings_tmp="$(mktemp "$SETTINGS_DIR/.monitor-environment.XXXXXX")"
      trap 'rm -f "$settings_tmp"' EXIT
      install -m 600 "$PARALLEL_WORKERS_CONFIG_DIR/monitor-environment.json" "$settings_tmp"
      mv -f "$settings_tmp" "$SETTINGS"
    fi
    mkdir -p "$UNIT_DIR"
    staged="$(mktemp "$UNIT_DIR/.$UNIT.XXXXXX")"
    printf '%s\n' "$unit_text" > "$staged"
    chmod 644 "$staged"
    mv -f "$staged" "$UNIT_DIR/$UNIT"
    "$SYSTEMCTL" --user daemon-reload
    "$SYSTEMCTL" --user enable "$UNIT"
    # restart also starts it, and makes a reinstall pick up changed collector code.
    "$SYSTEMCTL" --user restart "$UNIT"
    mkdir -p "$HOME/.local/bin"
    ln -sfn "$REPO_DIR/pw_control.py" "$HOME/.local/bin/pw-control"
    echo "Installed $UNIT_DIR/$UNIT and $HOME/.local/bin/pw-control"
    echo "Check: systemctl --user status $UNIT; journalctl --user -u $UNIT -f"
    ;;
esac
