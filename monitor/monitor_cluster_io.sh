#!/usr/bin/env bash
export ROTA_ROOT="${ROTA_ROOT:-}"
set -euo pipefail

ROOT="${ROTA_ROOT}"
INTERFACE="${ROTA_NETWORK_INTERFACE:-}"
DISK="${ROTA_DISK:-}"
DISK_EXPLICIT=0
[ -z "$DISK" ] || DISK_EXPLICIT=1
REFRESH_SEC=3
REGISTRY_FILE=""
RATES_SCRIPT=""
STORAGE_FILE=""
STATE_FILE=""
JSON=0
SNAPSHOT=""

usage() {
  cat <<EOF
Coordinator network/SMB/storage/scheduler monitor.

Usage:
  $0 [--root <path>] [--interface <name>] [--disk <name>] [--refresh_sec <n>] [--registry_file <path>] [--rates_script <path>] [--storage_file <path>] [--rates_state_file <path>] [--json | --snapshot <path>]

--rates_state_file keeps queue-rate deltas private to one dashboard; concurrent
dashboards sharing the default state would split each other's deltas.
--json prints one JSON sample per refresh instead of drawing (the cluster
status collector reads this stream); --snapshot draws the panel from the
collector's cluster-status.json without sampling anything.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root) ROOT="$2"; shift 2 ;;
    --interface) INTERFACE="$2"; shift 2 ;;
    --disk) DISK="$2"; DISK_EXPLICIT=1; shift 2 ;;
    --refresh_sec) REFRESH_SEC="$2"; shift 2 ;;
    --registry_file) REGISTRY_FILE="$2"; shift 2 ;;
    --rates_script) RATES_SCRIPT="$2"; shift 2 ;;
    --storage_file) STORAGE_FILE="$2"; shift 2 ;;
    --rates_state_file) STATE_FILE="$2"; shift 2 ;;
    --json) JSON=1; shift ;;
    --snapshot) SNAPSHOT="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

: "${ROOT:?Source the inventory environment or pass --root}"

resolve_darwin_disk() {
  local disk_info store_count store

  if ! disk_info="$(diskutil info -plist "$ROOT" 2>/dev/null)"; then
    echo "Cannot resolve the physical disk backing $ROOT." >&2
    return 1
  fi
  if ! store_count="$(printf '%s' "$disk_info" | plutil -extract APFSPhysicalStores raw - 2>/dev/null)" \
      || ! [[ "$store_count" =~ ^[0-9]+$ ]] || [ "$store_count" -ne 1 ]; then
    echo "Cannot resolve one APFS physical store backing $ROOT." >&2
    return 1
  fi
  if ! store="$(printf '%s' "$disk_info" | plutil -extract APFSPhysicalStores.0.APFSPhysicalStore raw - 2>/dev/null)" \
      || ! [[ "$store" =~ ^(disk[0-9]+)(s[0-9]+)+$ ]]; then
    echo "Cannot resolve a physical disk from APFS store ${store:-<missing>} for $ROOT." >&2
    return 1
  fi
  printf '%s\n' "${BASH_REMATCH[1]}"
}

if [ "$JSON" -eq 1 ] && [ -n "$SNAPSHOT" ]; then
  echo "--json and --snapshot are mutually exclusive" >&2
  exit 2
fi
PLATFORM="$(uname -s)"
if [ -n "$SNAPSHOT" ]; then
  : # A snapshot renderer samples nothing on this host.
elif [ "$PLATFORM" = Darwin ]; then
  INTERFACE="${INTERFACE:-en0}"
  if [ "$DISK_EXPLICIT" -eq 0 ]; then DISK="$(resolve_darwin_disk)" || exit 1; fi
elif [ "$PLATFORM" != Linux ]; then
  echo "Unsupported monitor platform: $PLATFORM" >&2; exit 2
fi

if ! [[ "$REFRESH_SEC" =~ ^[0-9]+$ ]] || [ "$REFRESH_SEC" -lt 2 ] || [ "$REFRESH_SEC" -gt 30 ]; then
  echo "--refresh_sec must be an integer between 2 and 30" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTRY_FILE="${REGISTRY_FILE:-$ROOT/QUEUE_PROGRESS.csv}"
RATES_SCRIPT="${RATES_SCRIPT:-$SCRIPT_DIR/monitor_cluster_io_rates.py}"
ADAPTIVE_STATE_ROOT="${ADAPTIVE_STATE_ROOT:-$ROOT/.rota/adaptive}"
outage_dir="${ROTA_OUTAGE_LOG_DIR:-$HOME/Library/Logs/rota}"
outage_pending=0
outage_logged=unknown
outage_retry_at=0
outage_capture_after=0
outage_log_error=""
if [ -n "$STATE_FILE" ]; then
  LOCK_PATHS_FILE="${STATE_FILE%.json}.locks.txt"
else
  STATE_FILE="${TMPDIR:-/tmp}/rota_cluster_io_rates.json"
  LOCK_PATHS_FILE="${TMPDIR:-/tmp}/rota_cluster_io_locks.txt"
fi

mtime() {
  if [ "$PLATFORM" = Darwin ]; then stat -f %m "$1" 2>/dev/null || echo 0
  else stat -c %Y "$1" 2>/dev/null || echo 0; fi
}

network_bytes() {
  netstat -bI "$INTERFACE" 2>/dev/null | awk -v iface="$INTERFACE" '
    $1 == iface && $3 ~ /^<Link#/ { print $7 + 0, $10 + 0; exit }
  '
}

human_bytes() {
  awk -v value="${1:-0}" 'BEGIN {
    if (value >= 1073741824) printf "%.1fG", value / 1073741824
    else if (value >= 1048576) printf "%.1fM", value / 1048576
    else if (value >= 1024) printf "%.1fK", value / 1024
    else printf "%.0fB", value
  }'
}

human_mib() {
  awk -v value="${1:-0}" 'BEGIN {
    if (value >= 1024) printf "%.1fG", value / 1024
    else printf "%.0fM", value
  }'
}

state_record() {
  local file="$1" keys="$2"
  [ -f "$file" ] && [ ! -L "$file" ] || return 1
  awk -F= -v keys="$keys" '
    BEGIN {
      count=split(keys, ordered, " ")
      for (i=1; i<=count; i++) wanted[ordered[i]]=1
    }
    $1 in wanted {
      if (seen[$1]++) bad=1
      value[$1]=substr($0, length($1) + 2)
    }
    END {
      for (i=1; i<=count; i++) if (seen[ordered[i]] != 1) bad=1
      if (bad) exit 1
      for (i=1; i<=count; i++)
        printf "%s%s", value[ordered[i]], (i == count ? "\n" : "\t")
    }
  ' "$file"
}

# Each host's job limit as the coordinator applies it: the pool's reviewed resource table with
# set-host-budget overrides, and none for a disabled host or one declared away with set-host-state.
# Prints host=slots pairs joined by commas.
host_slots() {
  python3 - "$ADAPTIVE_STATE_ROOT" "$1" <<'PY'
import csv, json, sys, time
from pathlib import Path

root, pool_id = Path(sys.argv[1]), sys.argv[2]
pool = dict(line.split("=", 1) for line in (root / "pool.env").read_text().splitlines() if "=" in line)
with open(pool["resources_file"], newline="") as handle:
    rows = list(csv.DictReader(handle, delimiter="\t"))
controls = []
for name in ("host-budgets.json", "host-states.json"):
    path = root / name
    control = json.loads(path.read_text()) if path.exists() else {"pool_id": pool_id, "hosts": {}}
    if control["pool_id"] != pool_id:
        raise SystemExit(f"{name} belongs to another pool")
    controls.append(control["hosts"])
budgets, states = controls
now = time.time()
away = {host for host, entry in states.items()
        if entry["away_from"] <= now and (entry["away_until"] is None or now < entry["away_until"])}
print(",".join(f"{row['host_id']}={int(budgets.get(row['host_id'], row)['cpu_units'])}"
               if row["enabled"] == "1" and row["host_id"] not in away else f"{row['host_id']}=0"
               for row in rows))
PY
}

adaptive_snapshot() {
  local now="$1" pool_file="$ADAPTIVE_STATE_ROOT/pool.env"
  local ready_file="$ADAPTIVE_STATE_ROOT/coordinator.ready" pool_record ready_record
  local pool_schema pool_id pool_state pool_pid lease
  local ready_schema ready_pool ready_pid ready_state resource_state live_cpu live_memory
  local io_pressure_state smbd_cpu_tenths updated_epoch age quarantined slots=""

  pool_record="$(state_record "$pool_file" \
    'schema_version pool_id state coordinator_pid lease_seconds')" || return 1
  ready_record="$(state_record "$ready_file" \
    'schema_version pool_id pid state resource_state live_cpu_units live_memory_mib io_pressure_state smbd_cpu_tenths updated_epoch')" || return 1
  IFS=$'\t' read -r pool_schema pool_id pool_state pool_pid lease <<< "$pool_record"
  IFS=$'\t' read -r ready_schema ready_pool ready_pid ready_state resource_state \
    live_cpu live_memory io_pressure_state smbd_cpu_tenths updated_epoch <<< "$ready_record"

  [ "$pool_schema" = 1 ] && [ "$ready_schema" = 1 ] && [ -n "$pool_id" ] || return 1
  case "$pool_state" in ACTIVE|DRAINING) ;; *) return 1 ;; esac
  [ "$ready_pool" = "$pool_id" ] && [ "$ready_pid" = "$pool_pid" ] && \
    [ "$ready_state" = ready ] || return 1
  case "$resource_state" in ok|degraded|unavailable) ;; *) return 1 ;; esac
  case "$io_pressure_state" in disabled|waiting|ok|high|stuck|unavailable) ;; *) return 1 ;; esac
  [[ "$pool_pid" =~ ^[1-9][0-9]*$ ]] && [[ "$lease" =~ ^[1-9][0-9]*$ ]] && \
    [[ "$live_cpu" =~ ^[0-9]+$ ]] && [[ "$live_memory" =~ ^[0-9]+$ ]] && \
    [[ "$smbd_cpu_tenths" =~ ^[0-9]+$ ]] && [[ "$updated_epoch" =~ ^[0-9]+$ ]] || return 1
  age=$((now - updated_epoch))
  [ "$age" -ge 0 ] && [ "$age" -le "$lease" ] || return 1

  [ "$(state_record "$pool_file" \
    'schema_version pool_id state coordinator_pid lease_seconds')" = "$pool_record" ] && \
    [ "$(state_record "$ready_file" \
      'schema_version pool_id pid state resource_state live_cpu_units live_memory_mib io_pressure_state smbd_cpu_tenths updated_epoch')" = "$ready_record" ] || return 1
  quarantined="$(state_record "$ready_file" quarantined_queues 2>/dev/null)" || quarantined=0
  # Only the collector's stream publishes job limits; unreadable budgets publish none.
  if [ "$JSON" -eq 1 ]; then slots="$(host_slots "$pool_id")" || slots=""; fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$resource_state" "$live_cpu" "$live_memory" "$io_pressure_state" "$smbd_cpu_tenths" "$quarantined" \
    "$pool_state" "$slots"
}

lock_snapshot() {
  local held=0 oldest=0 now_epoch lock_path lock_epoch lock_age
  now_epoch="$(date +%s)"
  if [ -f "$LOCK_PATHS_FILE" ]; then
    while IFS= read -r lock_path; do
      [ -n "$lock_path" ] || continue
      if [ -d "$lock_path" ]; then
        held=$((held + 1))
        lock_epoch="$(mtime "$lock_path")"
        if [[ "$lock_epoch" =~ ^[0-9]+$ ]] && [ "$lock_epoch" -gt 0 ]; then
          lock_age=$((now_epoch - lock_epoch))
          [ "$lock_age" -gt "$oldest" ] && oldest="$lock_age"
        fi
      fi
    done < "$LOCK_PATHS_FILE"
  fi
  printf '%s %s\n' "$held" "$oldest"
}

add_line() {
  local priority="$1" line
  shift
  printf -v line "$@"
  panel_lines+=("$line")
  panel_priorities+=("$priority")
}

# Drop the least important lines (bottom-most first within a priority) until
# the panel fits; without a known height, print everything.
print_panel() {
  local rows="$1" count="${#panel_lines[@]}" excess priority i printed=0
  local keep=()
  for ((i = 0; i < count; i++)); do keep[i]=1; done
  [[ "$rows" =~ ^[1-9][0-9]*$ ]] || rows="$count"
  excess=$((count - rows))
  for ((priority = 9; priority >= 1 && excess > 0; priority--)); do
    for ((i = count - 1; i >= 0 && excess > 0; i--)); do
      if [ "${panel_priorities[i]}" -eq "$priority" ]; then
        keep[i]=0
        excess=$((excess - 1))
      fi
    done
  done
  for ((i = 0; i < count; i++)); do
    [ "${keep[i]}" -eq 1 ] || continue
    [ "$printed" -eq 0 ] || printf '\n'
    printf '%s\033[K' "${panel_lines[i]}"
    printed=$((printed + 1))
  done
}

first=1
registry_mtime=-1
active_queues="n/a"
claimed_per_min="n/a"
completed_per_min="n/a"
rewrite_retries_per_min="n/a"
duplicate_retries_per_min="n/a"
stale_locks_per_min="n/a"
registry_age_sec="n/a"
share_due=0
storage_width=0

dim=$'\033[38;5;244m'
label=$'\033[97m'
good=$'\033[38;5;120m'
warn=$'\033[38;5;228m'
bad=$'\033[38;5;203m'
value_color=$'\033[38;5;214m'
reset=$'\033[0m'
if [ "$JSON" -eq 0 ]; then
  printf '\033[?25l'
  trap 'printf "\033[?25h"' EXIT
fi
trap 'exit 0' HUP INT TERM

if [ -z "$SNAPSHOT" ] && [ "$PLATFORM" = Darwin ]; then
  read -r rx_before tx_before <<< "$(network_bytes || echo '0 0')"
fi
sample_before="$(date +%s)"

# Blocks for one refresh interval while the disk counters accumulate.
sample_cluster_io() {
  if [ "$PLATFORM" = Linux ]; then
    linux_sample="$(python3 "$SCRIPT_DIR/monitor_linux.py" io --root "$ROOT" \
      --interface "$INTERFACE" --disk "$DISK" --interval "$REFRESH_SEC")" || exit 1
    IFS=$'\t' read -r sampled_interface sampled_disk rx_mbps tx_mbps disk_kbt disk_tps disk_mbps \
      smb_connections smb_recv_q smb_send_q <<< "$linux_sample"
    sample_after="$(date +%s)"
  else
    sampled_interface="$INTERFACE"
    sampled_disk="$DISK"
    disk_sample="$(iostat -d -w "$REFRESH_SEC" -c 2 "$DISK" 2>/dev/null | awk '
      NF == 3 && $1 ~ /^[0-9.]+$/ { kb=$1; tps=$2; mb=$3 }
      END { if (kb == "") print "0 0 0"; else print kb, tps, mb }
    ')"
    read -r disk_kbt disk_tps disk_mbps <<< "$disk_sample"

    read -r rx_after tx_after <<< "$(network_bytes || echo "$rx_before $tx_before")"
    sample_after="$(date +%s)"
    elapsed=$((sample_after - sample_before))
    [ "$elapsed" -lt 1 ] && elapsed=1
    rx_delta=$((rx_after - rx_before))
    tx_delta=$((tx_after - tx_before))
    [ "$rx_delta" -lt 0 ] && rx_delta=0
    [ "$tx_delta" -lt 0 ] && tx_delta=0
    rx_mbps="$(awk -v bytes="$rx_delta" -v sec="$elapsed" 'BEGIN {printf "%.1f", bytes * 8 / sec / 1000000}')"
    tx_mbps="$(awk -v bytes="$tx_delta" -v sec="$elapsed" 'BEGIN {printf "%.1f", bytes * 8 / sec / 1000000}')"

    read -r smb_connections smb_recv_q smb_send_q <<< "$(netstat -anv -p tcp 2>/dev/null | awk '
      $4 ~ /[.:]445$/ && $6 == "ESTABLISHED" { conns++; recv += $2; send += $3 }
      END { print conns + 0, recv + 0, send + 0 }
    ')"
    rx_before="$rx_after"
    tx_before="$tx_after"
  fi
  sample_before="$sample_after"
  smbd_cpu="$(ps -Ao %cpu=,command= 2>/dev/null | awk '$0 ~ /[\/]smbd([[:space:]]|$)/ {sum += $1} END {printf "%.1f", sum + 0}')"

  current_registry_mtime="$(mtime "$REGISTRY_FILE")"
  if [ "$current_registry_mtime" != "$registry_mtime" ] && [ -f "$RATES_SCRIPT" ] && command -v python3 >/dev/null 2>&1; then
    metrics_output="$(python3 "$RATES_SCRIPT" --registry "$REGISTRY_FILE" --state "$STATE_FILE" --lock-paths "$LOCK_PATHS_FILE" 2>/dev/null || true)"
    if [ -n "$metrics_output" ]; then
      while IFS=$'\t' read -r metric_name metric_value; do
        case "$metric_name" in
          active_queues) active_queues="$metric_value" ;;
          claimed_per_min) claimed_per_min="$metric_value" ;;
          completed_per_min) completed_per_min="$metric_value" ;;
          rewrite_retries_per_min) rewrite_retries_per_min="$metric_value" ;;
          duplicate_retries_per_min) duplicate_retries_per_min="$metric_value" ;;
          stale_locks_per_min) stale_locks_per_min="$metric_value" ;;
          registry_age_sec) registry_age_sec="$metric_value" ;;
        esac
      done <<< "$metrics_output"
      registry_mtime="$current_registry_mtime"
    fi
  elif [[ "$current_registry_mtime" =~ ^[0-9]+$ ]] && [ "$current_registry_mtime" -gt 0 ]; then
    registry_age_sec=$((sample_after - current_registry_mtime))
    [ "$registry_age_sec" -lt 0 ] && registry_age_sec=0
  fi

  read -r locks_held oldest_lock_sec <<< "$(lock_snapshot)"
  adaptive_record="$(adaptive_snapshot "$sample_after" 2>/dev/null || true)"
  pool_status=inactive
  if [ -n "$adaptive_record" ]; then
    pool_status=record
    IFS=$'\t' read -r adaptive_resource_state adaptive_live_cpu adaptive_live_memory \
      adaptive_io_state adaptive_smbd_tenths adaptive_quarantined adaptive_pool_state adaptive_slots \
      <<< "$adaptive_record"
  elif [ -e "$ADAPTIVE_STATE_ROOT/pool.env" ]; then
    pool_status=unavailable
  fi
}

# Preserve the onset, including across collector reconnects. The two published
# files stay on the coordinator's internal disk. No account lookup is needed.
capture_resource_outage() (
  umask 077
  mkdir -p "$outage_dir" || exit 1
  [ ! -f "$outage_dir/resource-outage.open" ] || exit 0
  local tmp path limit
  tmp="$(mktemp "$outage_dir/.resource-outage.XXXXXX")" || exit 1
  trap 'rm -f "$tmp"' EXIT
  {
    printf 'observed_epoch=%s\nresource_state=%s\nlive_cpu_units=%s\nio_pressure_state=%s\n' \
      "$sample_after" "$adaptive_resource_state" "$adaptive_live_cpu" "$adaptive_io_state" || exit 1
    for path in "$ADAPTIVE_STATE_ROOT/coordinator.ready" "$ADAPTIVE_STATE_ROOT/io-pressure.json" \
        "$ADAPTIVE_STATE_ROOT/coordinator.log" /var/log/com.apple.xpc.launchd/launchd.log.1 \
        /var/log/com.apple.xpc.launchd/launchd.log; do
      case "${path##*/}" in
        launchd.log) limit=49152 ;;
        launchd.log.1|coordinator.log) limit=16384 ;;
        *) limit=2048 ;;
      esac
      printf '\n--- %s (last %s bytes) ---\n' "${path##*/}" "$limit" || exit 1
      if [ -r "$path" ] && [ -f "$path" ]; then
        tail -c "$limit" "$path" || exit 1
      else
        printf '[missing or unreadable]\n' || exit 1
      fi
    done
  } > "$tmp" || exit 1
  ln "$tmp" "$outage_dir/resource-outage.open" || [ -f "$outage_dir/resource-outage.open" ]
)

record_resource_outage() {
  if [ "$pool_status" != record ]; then
    outage_pending=0
    return 0
  fi
  if [ "$SECONDS" -lt "$outage_retry_at" ]; then return 0; fi
  if [ "$adaptive_resource_state" != unavailable ]; then
    outage_pending=0
    if [ "$outage_logged" != no ] && [ -f "$outage_dir/resource-outage.open" ]; then
      mv -f "$outage_dir/resource-outage.open" "$outage_dir/resource-outage.last" || return 1
    fi
    outage_logged=no
  elif [ "$outage_logged" != yes ]; then
    if [ "$outage_pending" -eq 0 ]; then outage_pending=1; return 0; fi
    if [ "$SECONDS" -lt "$outage_capture_after" ]; then return 0; fi
    capture_resource_outage || return 1
    outage_logged=yes
    outage_capture_after=$((SECONDS + 300))
  fi
  outage_log_error=""
}

# Here the share is this host's own disk, so df reads its usage directly.
share_usage() {
  local report
  share_total="" share_used="" share_free="" share_error=""
  if ! report="$(df -Pk "$ROOT" 2>/dev/null)"; then
    share_error="df failed for $ROOT"
    return
  fi
  report="$(awk -v root="$ROOT" '
    NR == 2 && match($0, / [0-9]+ +[0-9]+ +[0-9]+ +[0-9]+% +/) {
      split(substr($0, RSTART, RLENGTH), size, " ")
      if (substr($0, RSTART + RLENGTH) == root) printf "%.0f %.0f %.0f\n", size[1] * 1024, size[2] * 1024, size[3] * 1024
    }' <<< "$report")"
  if [ -z "$report" ]; then
    share_error="$ROOT is not a mount point"
    return
  fi
  read -r share_total share_used share_free <<< "$report"
}

emit_json() {
  local name disk_unit=1000000
  for name in "$sampled_interface" "$sampled_disk"; do
    if ! [[ "$name" =~ ^[A-Za-z0-9_.:-]+$ ]]; then
      echo "Refusing to emit an unexpected device name: $name" >&2
      exit 1
    fi
  done
  # macOS iostat reports MB/s in units of 2^20 bytes.
  [ "$PLATFORM" != Darwin ] || disk_unit=1048576
  if [ "$sample_after" -ge "$share_due" ]; then
    share_usage
    share_due=$((sample_after + 180))
  fi
  awk -v epoch="$sample_after" -v interface="$sampled_interface" -v disk="$sampled_disk" -v outage_error="$outage_log_error" \
    -v pool_status="$pool_status" -v pool_state="${adaptive_pool_state:-}" \
    -v live_cpu="${adaptive_live_cpu:-}" -v live_memory="${adaptive_live_memory:-}" \
    -v resource_state="${adaptive_resource_state:-}" -v io_state="${adaptive_io_state:-}" \
    -v smbd_tenths="${adaptive_smbd_tenths:-}" -v quarantined="${adaptive_quarantined:-}" \
    -v slots="${adaptive_slots:-}" -v rx="$rx_mbps" -v tx="$tx_mbps" -v smbd_cpu="$smbd_cpu" -v sessions="$smb_connections" \
    -v recv_q="$smb_recv_q" -v send_q="$smb_send_q" -v disk_mbps="$disk_mbps" -v disk_unit="$disk_unit" \
    -v disk_tps="$disk_tps" -v disk_kbt="$disk_kbt" -v share_total="$share_total" \
    -v share_used="$share_used" -v share_free="$share_free" -v share_error="$share_error" \
    -v active="$active_queues" -v claimed="$claimed_per_min" -v completed="$completed_per_min" \
    -v rewrites="$rewrite_retries_per_min" -v duplicates="$duplicate_retries_per_min" \
    -v stale="$stale_locks_per_min" -v age="$registry_age_sec" -v locks="$locks_held" \
    -v oldest_lock="$oldest_lock_sec" '
    function num(value, scale, format) {
      return value ~ /^-?[0-9]+([.][0-9]+)?$/ ? sprintf(format, value * scale) : "null"
    }
    function text(value) {
      if (value == "") return "null"
      gsub(/[\\"]/, "\\\\&", value)
      return "\"" value "\""
    }
    function slot_map(value,   count, pairs, pair, i, out) {
      if (value == "") return "null"
      count = split(value, pairs, ",")
      for (i = 1; i <= count; i++) {
        split(pairs[i], pair, "=")
        out = out (i > 1 ? "," : "") text(pair[1]) ":" num(pair[2], 1, "%d")
      }
      return "{" out "}"
    }
    BEGIN {
      if (pool_status == "record") {
        pool = sprintf("{\"state\":%s,\"capacity\":%s,\"capacity_memory_mib\":%s,\"resource_state\":%s,\"io_state\":%s,\"smbd_cpu_pct\":%s,\"quarantined\":%s,\"slots\":%s}",
          text(tolower(pool_state)), num(live_cpu, 1, "%d"), num(live_memory, 1, "%d"), text(resource_state),
          text(io_state), num(smbd_tenths, 0.1, "%.1f"), num(quarantined, 1, "%d"), slot_map(slots))
      } else {
        pool = "{\"state\":" text(pool_status) "}"
      }
      printf "{\"epoch\":%s,\"interface\":%s,\"pool\":%s", num(epoch, 1, "%d"), text(interface), pool
      if (outage_error != "") printf ",\"outage_log_error\":%s", text(outage_error)
      printf ",\"network\":{\"rx_bytes_per_s\":%s,\"tx_bytes_per_s\":%s}", num(rx, 125000, "%.0f"), num(tx, 125000, "%.0f")
      printf ",\"smbd\":{\"cpu_pct\":%s,\"sessions\":%s,\"recv_queue_bytes\":%s,\"send_queue_bytes\":%s}",
        num(smbd_cpu, 1, "%.1f"), num(sessions, 1, "%d"), num(recv_q, 1, "%d"), num(send_q, 1, "%d")
      printf ",\"disk\":{\"name\":%s,\"bytes_per_s\":%s,\"read_bytes_per_s\":null,\"write_bytes_per_s\":null,\"ops_per_s\":%s,\"kb_per_op\":%s",
        text(disk), num(disk_mbps, disk_unit, "%.0f"), num(disk_tps, 1, "%.1f"), num(disk_kbt, 1, "%.2f")
      printf ",\"total_bytes\":%s,\"used_bytes\":%s,\"free_bytes\":%s,\"error\":%s}",
        num(share_total, 1, "%.0f"), num(share_used, 1, "%.0f"), num(share_free, 1, "%.0f"), text(share_error)
      printf ",\"registry\":{\"active_queues\":%s,\"claimed_per_min\":%s,\"completed_per_min\":%s,\"rewrite_retries_per_min\":%s,\"duplicate_retries_per_min\":%s,\"stale_locks_per_min\":%s,\"age_sec\":%s,\"locks_held\":%s,\"oldest_lock_sec\":%s}}\n",
        num(active, 1, "%d"), num(claimed, 1, "%.1f"), num(completed, 1, "%.1f"), num(rewrites, 1, "%.1f"),
        num(duplicates, 1, "%.1f"), num(stale, 1, "%.1f"), num(age, 1, "%d"), num(locks, 1, "%d"), num(oldest_lock, 1, "%d")
    }'
}

# The collector already sampled the coordinator; read its values into the variables the panel draws.
read_snapshot() {
  local fields line name value
  storage_rows=()
  fields="$(python3 "$SCRIPT_DIR/monitor_snapshot.py" io --snapshot "$SNAPSHOT" --root "$ROOT" --width "$1")" || return 1
  while IFS= read -r line; do
    name="${line%%=*}"
    value="${line#*=}"
    case "$name" in
      pool_status|adaptive_resource_state|adaptive_live_cpu|adaptive_live_memory|\
      adaptive_io_state|adaptive_smbd_tenths|adaptive_quarantined|adaptive_pool_state|rx_mbps|tx_mbps|\
      smbd_cpu|smb_connections|smb_recv_q|smb_send_q|sampled_disk|disk_mbps|disk_tps|disk_kbt|\
      active_queues|registry_age_sec|claimed_per_min|completed_per_min|rewrite_retries_per_min|\
      duplicate_retries_per_min|stale_locks_per_min|locks_held|oldest_lock_sec|storage_width)
        printf -v "$name" '%s' "$value" ;;
      storage) storage_rows+=("$value") ;;
      problem) problem_rows+=("$value") ;;
      *) echo "Unexpected cluster snapshot field: $name" >&2; return 1 ;;
    esac
  done <<< "$fields"
}

render_panel() {
  local storage_row problem_row
  panel_lines=()
  panel_priorities=()
  if [ "${#problem_rows[@]}" -gt 0 ]; then
    add_line 1 '%bCLUSTER I/O unavailable%b' "$warn" "$reset"
    for problem_row in "${problem_rows[@]}"; do
      add_line 1 '%s' "$problem_row"
    done
    return
  fi
  recv_q_text="$(human_bytes "$smb_recv_q")"
  send_q_text="$(human_bytes "$smb_send_q")"

  smbd_color="$good"
  awk -v cpu="$smbd_cpu" 'BEGIN {exit !(cpu >= 400)}' && smbd_color="$bad"
  if [ "$smbd_color" = "$good" ]; then
    awk -v cpu="$smbd_cpu" 'BEGIN {exit !(cpu >= 100)}' && smbd_color="$warn"
  fi
  queue_color="$good"
  if [ "$smb_recv_q" -ge 1048576 ] || [ "$smb_send_q" -ge 1048576 ]; then
    queue_color="$bad"
  elif [ "$smb_recv_q" -ge 65536 ] || [ "$smb_send_q" -ge 65536 ]; then
    queue_color="$warn"
  fi
  lock_color="$good"
  [ "$oldest_lock_sec" -ge 5 ] && lock_color="$warn"
  [ "$oldest_lock_sec" -ge 30 ] && lock_color="$bad"
  if [ "$pool_status" = record ]; then
    adaptive_memory="$(human_mib "$adaptive_live_memory")"
    adaptive_smbd_cpu="$(awk -v value="$adaptive_smbd_tenths" 'BEGIN {printf "%.1f", value / 10}')"
    adaptive_resource_color="$good"
    [ "$adaptive_resource_state" = degraded ] && adaptive_resource_color="$warn"
    [ "$adaptive_resource_state" = unavailable ] && adaptive_resource_color="$bad"
    adaptive_io_color="$good"
    [ "$adaptive_io_state" = disabled ] && adaptive_io_color="$dim"
    case "$adaptive_io_state" in high|stuck|unavailable) adaptive_io_color="$bad" ;; esac
    adaptive_quarantine_text=""
    [[ "$adaptive_quarantined" =~ ^[1-9][0-9]*$ ]] && adaptive_quarantine_text=" ${bad}Q${adaptive_quarantined}"
  fi

  # Lines carry a priority (1 = keep longest). Printing more lines than the pane
  # holds scrolls it, so every redraw would flash between two partial views.
  if [ "$pool_status" = record ]; then
    add_line 1 '%bPOOL %s%b' "$label" "$adaptive_pool_state" "$reset"
    if [ "$adaptive_pool_state" = DRAINING ]; then
      add_line 1 ' %bNew starts OFF (draining)%b' "$warn" "$reset"
    else
      add_line 9 ' New starts per queue admission'
    fi
    add_line 7 ' Capacity %b%s CPU / %s%b' "$value_color" "$adaptive_live_cpu" "$adaptive_memory" "$reset"
    add_line 7 ' Resources %b%s%b%b' "$adaptive_resource_color" "$adaptive_resource_state" "$adaptive_quarantine_text" "$reset"
    add_line 2 ' I/O gate %b%s SMB%s%%%b' "$adaptive_io_color" "$adaptive_io_state" "$adaptive_smbd_cpu" "$reset"
  elif [ "$pool_status" = unavailable ]; then
    add_line 1 '%bPOOL telemetry unavailable%b' "$warn" "$reset"
  else
    add_line 1 '%bPOOL inactive%b' "$label" "$reset"
  fi
  add_line 6 '%bNET Mb/s%b RX %b%s%b TX %b%s%b' "$label" "$reset" "$value_color" "$rx_mbps" "$reset" "$value_color" "$tx_mbps" "$reset"
  add_line 3 '%bSMB%b (100%% CPU = 1 core)' "$label" "$reset"
  add_line 3 ' CPU %b%s%%%b  Sessions %b%s%b' "$smbd_color" "$smbd_cpu" "$reset" "$value_color" "$smb_connections" "$reset"
  add_line 8 ' TCP Q R:%b%s%b S:%b%s%b' "$queue_color" "$recv_q_text" "$reset" "$queue_color" "$send_q_text" "$reset"
  shared_label=Shared
  add_line 5 '%b%s%b %s' "$label" "$shared_label" "$reset" "${sampled_disk:-$DISK}"
  add_line 5 ' MB/s %b%s%b  Ops/s %b%s%b' "$value_color" "$disk_mbps" "$reset" "$value_color" "$disk_tps" "$reset"
  if [ -n "$SNAPSHOT" ]; then
    for storage_row in "${storage_rows[@]}"; do
      add_line 4 '%s' "$storage_row"
    done
  elif [ -n "$STORAGE_FILE" ]; then
    storage_line="$(python3 "$SCRIPT_DIR/monitor_storage.py" render --file "$STORAGE_FILE" --width "$((term_cols - 1))")"
    while IFS= read -r storage_row; do
      add_line 4 '%s' "$storage_row"
    done <<< "$storage_line"
  else
    add_line 5 ' Mean I/O %b%s KB%b' "$dim" "$disk_kbt" "$reset"
  fi
  add_line 6 '%bQUEUES %s%b | %bdata %ss old%b' "$label" "$active_queues" "$reset" "$dim" "$registry_age_sec" "$reset"
  add_line 6 ' Claim/min %b%s%b Done/min %b%s%b' "$value_color" "$claimed_per_min" "$reset" "$value_color" "$completed_per_min" "$reset"
  add_line 8 ' Locks now   %b%s%b oldest %b%ss%b' "$lock_color" "$locks_held" "$reset" "$lock_color" "$oldest_lock_sec" "$reset"
  add_line 9 ' Rewrite/min %b%s%b Dup/min %b%s%b' "$value_color" "$rewrite_retries_per_min" "$reset" "$value_color" "$duplicate_retries_per_min" "$reset"
  add_line 9 ' Stale locks/min   %b%s%b' "$value_color" "$stale_locks_per_min" "$reset"
}

shown_storage_width=0
while true; do
  read -r term_rows term_cols <<< "$(stty size 2>/dev/null || true)"
  [[ "${term_cols:-}" =~ ^[1-9][0-9]*$ ]] || term_cols=32
  problem_rows=()
  if [ -z "$SNAPSHOT" ]; then
    sample_cluster_io
    if [ "$JSON" -eq 1 ]; then
      if [ "$PLATFORM" = Darwin ] && ! record_resource_outage; then
        outage_log_error="Could not update the coordinator resource outage log"
        outage_retry_at=$((SECONDS + 60))
      fi
      emit_json
      continue
    fi
  else
    read_snapshot "$((term_cols - 1))" || problem_rows=("Snapshot reader failed; see its error above")
    # The layout watcher widens this pane to fit the storage line.
    if [ -n "${TMUX_PANE:-}" ] && [ "$storage_width" != "$shown_storage_width" ]; then
      tmux set-option -w -t "$TMUX_PANE" @storage_width "$storage_width" >/dev/null 2>&1 || true
      shown_storage_width="$storage_width"
    fi
  fi
  render_panel

  if [ "$first" -eq 1 ]; then
    printf '\033[2J'
    first=0
  fi
  printf '\033[H'
  print_panel "${term_rows:-}"
  printf '\033[J'
  [ -z "$SNAPSHOT" ] || sleep "$REFRESH_SEC"
done
