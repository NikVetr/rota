#!/usr/bin/env bash
# ProxyCommand for scheduler SSH: connect to the host's primary address, or to its fallback
# (such as a Tailscale address) when the primary does not answer within two seconds.
# ssh still verifies the host key under the primary name, so a fallback cannot impersonate it.
set -u
host=${1:?usage: ssh_route.sh HOST PORT FALLBACK...}
port=${2:?usage: ssh_route.sh HOST PORT FALLBACK...}
shift 2

# macOS nc bounds the connect with -G; elsewhere -w does.
if [ "$(uname -s)" = Darwin ]; then probe=(nc -z -G 2 -w 2); else probe=(nc -z -w 2); fi
for address in "$host" "$@"; do
  if "${probe[@]}" "$address" "$port" >/dev/null 2>&1; then
    # Quiet by default: callers may fold ssh's stderr into output they parse.
    [ "${SSH_ROUTE_DEBUG:-0}" != 1 ] || printf 'ssh_route: %s:%s via %s\n' "$host" "$port" "$address" >&2
    exec nc "$address" "$port"
  fi
done
printf 'ssh_route: no route to %s:%s (tried %s)\n' "$host" "$port" "$host $*" >&2
exit 255
