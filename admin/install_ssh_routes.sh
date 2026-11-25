#!/usr/bin/env bash
# Install the inventory's SSH fallback routes for this user: copy the rendered ssh_config and
# the route script into ~/.ssh (local disk, so a hung share cannot stall ssh) and include them
# first from ~/.ssh/config. --check reports drift without changing anything.
set -euo pipefail

usage() {
  echo "Usage: install_ssh_routes.sh [--check] <rendered config directory or its ssh_config>" >&2
  exit 2
}
check=0
[ "${1:-}" != --check ] || { check=1; shift; }
[ $# -eq 1 ] || usage
source_config=$1
[ ! -d "$source_config" ] || source_config=$source_config/ssh_config
[ -f "$source_config" ] || { echo "install_ssh_routes: no ssh_config at $source_config" >&2; exit 1; }

ssh_dir=$HOME/.ssh
config=$ssh_dir/rota.conf
route=$ssh_dir/rota-route.sh
user_config=$ssh_dir/config
include='Include rota.conf'
route_source="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/ssh_route.sh"

if [ "$check" -eq 1 ]; then
  status=0
  cmp -s "$source_config" "$config" || { echo "OUTDATED $config"; status=1; }
  cmp -s "$route_source" "$route" || { echo "OUTDATED $route"; status=1; }
  [ "$(head -n 1 "$user_config" 2>/dev/null)" = "$include" ] || { echo "MISSING '$include' on the first line of $user_config"; status=1; }
  [ "$status" -ne 0 ] || echo "CURRENT  $config"
  exit "$status"
fi

mkdir -p "$ssh_dir"
chmod 700 "$ssh_dir"
install -m 600 "$source_config" "$config"
install -m 755 "$route_source" "$route"
if [ "$(head -n 1 "$user_config" 2>/dev/null)" != "$include" ]; then
  # Options take their first value, so the routes must come before any other Host block.
  if [ -e "$user_config" ]; then
    cp -p "$user_config" "$user_config.bak-$(date +%Y%m%d%H%M%S)"
  fi
  { printf '%s\n' "$include"; cat "$user_config" 2>/dev/null || true; } > "$user_config.tmp"
  chmod 600 "$user_config.tmp"
  mv "$user_config.tmp" "$user_config"
fi
echo "Installed $config and $route; $user_config includes them first"
