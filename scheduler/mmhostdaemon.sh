#!/usr/bin/env bash

set -u

die() {
  echo "mmhostdaemon: $*" >&2
  exit 1
}

usage() {
  echo "Usage: $0 --state-dir <dir> [--poll-seconds <seconds>] [--sample-seconds <integer>] [--once]" >&2
}

is_uint() {
  case "$1" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac
}

is_positive_int() {
  is_uint "$1" && [ "$1" -gt 0 ]
}

env_value() {
  local file="$1" key="$2"
  awk -v key="$key" '
    index($0, key "=") == 1 {count++; value=substr($0, length(key) + 2)}
    END {if (count != 1) exit 1; print value}
  ' "$file"
}

# Absent keys take the default; duplicates still fail.
optional_env_value() {
  awk -v key="$2" -v fallback="$3" '
    index($0, key "=") == 1 {count++; value=substr($0, length(key) + 2)}
    END {if (count > 1) exit 1; print (count ? value : fallback)}
  ' "$1"
}

# A set of PIDs and all their descendants, from one process listing.
tree_pids() {
  [ "$#" -gt 0 ] || return 0
  ps -axo pid=,ppid= 2>/dev/null | awk -v roots="$*" '
    BEGIN {count = split(roots, root, " "); for (i = 1; i <= count; i++) keep[root[i]] = 1}
    {parent[$1] = $2; pids[++n] = $1}
    END {
      for (changed = 1; changed;) {
        changed = 0
        for (i = 1; i <= n; i++)
          if (!(pids[i] in keep) && (parent[pids[i]] in keep)) {keep[pids[i]] = 1; changed = 1}
      }
      for (pid in keep) if (pid in parent) print pid
    }
  '
}

# Seconds since the console user last used the keyboard or mouse. macOS reports HIDIdleTime;
# Linux reports the graphical session's idle hint (set by the desktop's own idle timer).
console_idle_seconds() {
  local value session hint since
  if command -v ioreg >/dev/null 2>&1; then
    value="$(ioreg -c IOHIDSystem 2>/dev/null | awk '/"HIDIdleTime" = / {print int($NF / 1000000000); exit}')"
    is_uint "$value" || return 1
    printf '%s\n' "$value"
    return 0
  fi
  command -v loginctl >/dev/null 2>&1 || return 1
  # A failed query is unreadable activity; only a successful one without a session is an empty console.
  value="$(loginctl show-seat seat0 -p ActiveSession 2>/dev/null)" || return 1
  session="$(awk -F= '$1 == "ActiveSession" {print $2}' <<< "$value")"
  if [ -z "$session" ]; then
    printf '%s\n' 999999  # nobody is at the console
    return 0
  fi
  value="$(loginctl show-session "$session" -p IdleHint -p IdleSinceHint 2>/dev/null)" || return 1
  hint="$(awk -F= '$1 == "IdleHint" {print $2}' <<< "$value")"
  since="$(awk -F= '$1 == "IdleSinceHint" {print $2}' <<< "$value")"
  case "$hint" in
    no) printf '%s\n' 0 ;;
    yes) is_uint "$since" && [ "$since" -gt 0 ] || return 1
      awk -v now="$(date +%s)" -v since="$since" 'BEGIN {v = int(now - since / 1000000); print (v < 0 ? 0 : v)}' ;;
    *) return 1 ;;
  esac
}

hash_file() {
  local output
  output="$(shasum -a 256 "$1")" || return 1
  printf '%s\n' "${output%% *}"
}

atomic_write() {
  local destination="$1" temporary
  shift
  temporary="${destination}.tmp.$$.$RANDOM"
  printf '%s\n' "$@" > "$temporary" && mv -f "$temporary" "$destination"
}

inspect_process() {
  local pid="$1" reservation_id="$2" snapshot state
  snapshot="$(ps -ww -o stat=,command= -p "$pid" 2>/dev/null)" || return 1
  snapshot="${snapshot#"${snapshot%%[![:space:]]*}"}"
  state="${snapshot%%[[:space:]]*}"
  case "$state" in *Z*) return 1 ;; esac
  case " $snapshot " in *" $reservation_id "*) return 0 ;; *) return 2 ;; esac
}

write_reservation() {
  local path="$1" reservation_id="$2" queue_id="$3" generation="$4"
  local cpu="$5" memory="$6" io="$7" pid="$8" state="$9"
  local started_epoch
  if [ -f "$path" ]; then
    started_epoch="$(awk -F= '$1 == "started_epoch" {print $2}' "$path")"
  else
    started_epoch="$(date +%s)"
  fi
  atomic_write "$path" \
    "schema_version=1" "pool_id=$pool_id" "host_id=$host_id" \
    "reservation_id=$reservation_id" "queue_id=$queue_id" "generation=$generation" \
    "cpu_units=$cpu" "memory_mib=$memory" "shared_io_units=$io" \
    "pid=$pid" "state=$state" "started_epoch=$started_epoch"
}

failure_record() {
  local file="$failures_dir/$1.env" record
  failure_count=0 failure_reason=none failure_last_id="" failure_reset_token=""
  [ -f "$file" ] || return 0
  # One parse per record: status publication reads every record each poll.
  record="$(awk -F= -v pool="$pool_id" -v host="$host_id" -v queue="$1" '
    {if (seen[$1]++) bad=1; value[$1]=substr($0, length($1) + 2)}
    END {
      if (bad || value["schema_version"] != 1 || value["pool_id"] != pool ||
          value["host_id"] != host || value["queue_id"] != queue ||
          value["rapid_failures"] !~ /^[0-9]+$/ ||
          value["reason"] !~ /^(none|rapid_job_failures|infrastructure_failed)$/ ||
          !("last_reservation_id" in value) || !("reset_token" in value)) exit 1
      printf "%s|%s|%s|%s\n", value["rapid_failures"], value["reason"],
        value["last_reservation_id"], value["reset_token"]
    }
  ' "$file")" || die "invalid failure record: $file"
  IFS='|' read -r failure_count failure_reason failure_last_id failure_reset_token <<< "$record"
}

save_failure() {
  atomic_write "$failures_dir/$1.env" "schema_version=1" "pool_id=$pool_id" \
    "host_id=$host_id" "queue_id=$1" "rapid_failures=$failure_count" \
    "reason=$failure_reason" "last_reservation_id=$failure_last_id" \
    "reset_token=$failure_reset_token" || die "cannot save failure feedback"
}

record_outcome() {
  local queue="$1" id="$2" outcome="$3" reservation="$4" started now
  # An audited reboot is not evidence against the queue, so it never starts a hold.
  [ "$outcome" != empty ] && [ "$outcome" != host_rebooted ] || return 0
  failure_record "$queue"
  [ "$failure_last_id" != "$id" ] || return 0
  started="$(awk -F= '$1 == "started_epoch" {print $2}' "$reservation")"
  now="$(date +%s)"
  case "$outcome" in
    infrastructure_failed) failure_reason=infrastructure_failed ;;
    job_failed)
      if is_uint "$started" && [ "$now" -ge "$started" ] && [ $((now - started)) -le 30 ]; then
        failure_count=$((failure_count + 1))
        [ "$failure_count" -lt 3 ] || [ "$failure_reason" != none ] || failure_reason=rapid_job_failures
      else
        failure_count=0
      fi
      ;;
    succeeded) failure_count=0 ;;
  esac
  failure_last_id="$id"
  save_failure "$queue"
  if [ "$failure_reason" != none ]; then
    echo "mmhostdaemon: holding queue $queue on $host_id: $failure_reason; explicit reset required" >&2
  fi
}

apply_failure_resets() {
  local request queue token archive
  for request in "$failure_resets_dir"/*.env; do
    [ -f "$request" ] || continue
    queue="$(env_value "$request" queue_id)" || die "invalid failure reset queue"
    token="$(env_value "$request" token)" || die "invalid failure reset token"
    case "$queue:$token" in *[!A-Za-z0-9._:-]*|:*|*:) die "invalid failure reset identity" ;; esac
    [ "$(env_value "$request" schema_version)" = 1 ] &&
      [ -n "$(env_value "$request" reason)" ] &&
      [ "$(env_value "$request" pool_id)" = "$pool_id" ] &&
      [ "$(env_value "$request" host_id)" = "$host_id" ] || die "failure reset identity mismatch"
    failure_record "$queue"
    if [ "$failure_reset_token" != "$token" ]; then
      failure_count=0 failure_reason=none failure_reset_token="$token"
      save_failure "$queue"
    fi
    archive="$state_dir/audit/failure-resets/$token.env"
    mkdir -p "${archive%/*}" || die "cannot create failure reset audit directory"
    mv "$request" "$archive" || die "cannot archive failure reset"
  done
}

valid_allocations() {
  awk -F '\t' '
    BEGIN {
      header="queue_id\ttarget_running\tstart_grants\tcpu_units_per_job\tmemory_mib_per_job\tshared_io_units_per_job\toriginal_queue_file\teditable_queue_file\tmodel_dir\tlog_dir\tlock_dir\ttag_clean\tqueue_sha256\tgpu_units_per_job\tmemory_limit_mib"
    }
    NR == 1 {if ($0 != header) exit 1; next}
    NF != 15 || $1 !~ /^[A-Za-z0-9._-]+$/ || seen[$1]++ {exit 1}
    $2 !~ /^[0-9]+$/ || $3 !~ /^[0-9]+$/ || $3 > $2 || $4 !~ /^[1-9][0-9]*$/ ||
      $5 !~ /^[1-9][0-9]*$/ || $6 !~ /^[0-9]+$/ ||
      length($13) != 64 || $13 ~ /[^0-9a-f]/ || $14 !~ /^[0-9]+$/ || $15 !~ /^[0-9]+$/ {exit 1}
    {for (i=7; i<=12; i++) if ($i == "") exit 1}
    END {if (NR == 0) exit 1}
  ' "$1"
}

lease_record() {
  awk -F= '
    BEGIN {
      required="schema_version pool_id host_id generation ttl_seconds allocations_file allocations_sha256 cpu_units memory_mib cpu_tolerance_pct"
      split(required, keys, " ")
      for (i in keys) wanted[keys[i]]=1
    }
    $1 in wanted {
      if (seen[$1]++) exit 1
      value[$1]=substr($0, length($1) + 2)
    }
    END {
      for (i in keys) if (seen[keys[i]] != 1) exit 1
      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", value["schema_version"], \
        value["pool_id"], value["host_id"], value["generation"], \
        value["ttl_seconds"], value["allocations_file"], value["allocations_sha256"], \
        value["cpu_units"], value["memory_mib"], value["cpu_tolerance_pct"]
    }
  ' "$1"
}

file_mtime() {
  if [ "$(uname -s)" = Darwin ]; then
    stat -f %m "$1"
  else
    stat -c %Y "$1"
  fi
}

accept_plan() {
  local lease="$state_dir/lease.env" snapshot record schema lease_pool lease_host generation ttl
  local name expected extra mtime now remaining hash accepted_copy old
  local lease_cpu lease_memory lease_tolerance

  [ -f "$lease" ] || { plan_message=no_lease; return; }
  snapshot="$state_dir/.lease.observed.$$.$RANDOM"
  ln "$lease" "$snapshot" 2>/dev/null || { plan_message=changing_lease; return; }
  record="$(lease_record "$snapshot" 2>/dev/null)" || {
    rm -f "$snapshot"
    plan_message=invalid_lease
    return
  }
  mtime="$(file_mtime "$snapshot" 2>/dev/null)" || mtime=""
  if ! [ "$lease" -ef "$snapshot" ]; then
    rm -f "$snapshot"
    plan_message=changing_lease
    return
  fi
  rm -f "$snapshot"
  IFS=$'\t' read -r schema lease_pool lease_host generation ttl name expected \
    lease_cpu lease_memory lease_tolerance extra <<< "$record"
  [ -z "$extra" ] && [ "$schema" = 1 ] || { plan_message=invalid_lease; return; }
  [ "$lease_pool" = "$pool_id" ] && [ "$lease_host" = "$host_id" ] || {
    plan_message=identity_mismatch
    return
  }
  is_positive_int "$generation" && is_positive_int "$ttl" || { plan_message=invalid_lease; return; }
  is_positive_int "$lease_cpu" && is_positive_int "$lease_memory" && \
    is_uint "$lease_tolerance" && [ "$lease_tolerance" -le 100 ] || {
    plan_message=invalid_lease
    return
  }
  now="$(date +%s)"
  if ! is_uint "$mtime" || ! is_uint "$now" || [ "$now" -lt "$mtime" ] || \
      [ $((now - mtime)) -ge "$ttl" ]; then
    plan_message=expired_lease
    return
  fi
  remaining=$((ttl - (now - mtime)))
  if [ "$generation" -le "$saved_generation" ]; then
    [ "$generation" -eq "$accepted_generation" ] || plan_message=stale_lease
    return
  fi
  [ "$name" = "allocations.${generation}.tsv" ] && [ -r "$state_dir/$name" ] || {
    plan_message=invalid_allocations
    return
  }
  hash="$(hash_file "$state_dir/$name")" || { plan_message=invalid_allocations; return; }
  [ "$hash" = "$expected" ] && valid_allocations "$state_dir/$name" || {
    plan_message=invalid_allocations
    return
  }
  accepted_copy="$state_dir/accepted.allocations.tsv"
  cp "$state_dir/$name" "${accepted_copy}.tmp.$$" || die "cannot stage accepted allocations"
  [ "$(hash_file "${accepted_copy}.tmp.$$")" = "$expected" ] || die "allocations changed during acceptance"
  mv -f "${accepted_copy}.tmp.$$" "$accepted_copy" || die "cannot accept allocations"
  accepted_generation="$generation"
  accepted_allocations="$accepted_copy"
  accepted_paths_ready=1
  lease_deadline=$((SECONDS + remaining))
  saved_generation="$generation"
  atomic_write "$generation_file" "generation=$generation" || die "cannot save generation"
  cpu_budget="$lease_cpu"
  memory_budget="$lease_memory"
  cpu_tolerance_pct="$lease_tolerance"
  for old in "$state_dir"/allocations.*.tsv; do
    [ ! -e "$old" ] || [ "$old" = "$state_dir/$name" ] || rm -f "$old"
  done
  for old in "$empty_dir"/*; do
    [ ! -e "$old" ] || rm -f "$old"
  done
  plan_message=accepted
}

reservation_record() {
  awk -F= '
    BEGIN {
      required="schema_version pool_id host_id reservation_id queue_id generation cpu_units memory_mib shared_io_units pid state"
      split(required, keys, " ")
      for (i in keys) wanted[keys[i]]=1
    }
    $1 in wanted {
      if (seen[$1]++) exit 1
      value[$1]=substr($0, length($1) + 2)
    }
    END {
      for (i in keys) if (seen[keys[i]] != 1) exit 1
      printf "%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n", \
        value["schema_version"], value["pool_id"], value["host_id"], \
        value["reservation_id"], value["queue_id"], value["generation"], \
        value["cpu_units"], value["memory_mib"], value["shared_io_units"], \
        value["pid"], value["state"]
    }
  ' "$1"
}

result_record() {
  awk -F= '
    $1 == "reservation_id" || $1 == "outcome" {
      if (seen[$1]++) exit 1
      value[$1]=substr($0, length($1) + 2)
    }
    END {
      if (seen["reservation_id"] != 1 || seen["outcome"] != 1) exit 1
      printf "%s\t%s\n", value["reservation_id"], value["outcome"]
    }
  ' "$1"
}

reconcile_reservations() {
  local file record schema record_pool record_host id queue_id generation cpu memory io pid state extra
  local result result_data result_id outcome temporary process_state processes planned_memory

  uncertain=0
  [ -e "$usage_file" ] || : > "$usage_file" || die "cannot initialize resource measurements"
  temporary="${usage_file}.tmp.$$.$RANDOM"
  : > "$temporary" || die "cannot initialize reservation accounting"
  for file in "$reservations_dir"/*.env; do
    [ -e "$file" ] || continue
    record="$(reservation_record "$file" 2>/dev/null)" || { uncertain=1; continue; }
    IFS='|' read -r schema record_pool record_host id queue_id generation cpu memory io pid state extra <<< "$record"
    if [ "$schema" != 1 ] || [ "$record_pool" != "$pool_id" ] || [ "$record_host" != "$host_id" ] ||
        ! is_positive_int "$generation" || ! is_positive_int "$cpu" ||
        ! is_positive_int "$memory" || ! is_uint "$io"; then
      uncertain=1
      continue
    fi
    case "$id" in ''|*[!A-Za-z0-9._-]*) uncertain=1; continue ;; esac
    case "$queue_id" in ''|*[!A-Za-z0-9._-]*) uncertain=1; continue ;; esac
    [ -z "$pid" ] || is_positive_int "$pid" || { uncertain=1; continue; }
    case "$state" in starting|running|uncertain) ;; *) uncertain=1; continue ;; esac
    [ "$state" != uncertain ] || uncertain=1
    process_state=1
    if is_positive_int "$pid"; then
      inspect_process "$pid" "$id"
      process_state=$?
    fi
    planned_memory=""
    if [ "$accepted_generation" -gt 0 ] && [ -r "$accepted_allocations" ]; then
      planned_memory="$(awk -F '\t' -v queue="$queue_id" 'NR > 1 && $1 == queue {print $5}' \
        "$accepted_allocations")"
    fi
    # Reprice owned work before acknowledging its lease. An ambiguous worker
    # keeps its previous floor when a correction would release capacity.
    if is_positive_int "$planned_memory" && [ "$planned_memory" -ne "$memory" ] &&
        { [ "$planned_memory" -gt "$memory" ] ||
          { [ "$process_state" -eq 0 ] && [ "$state" != uncertain ]; }; }; then
      write_reservation "$file" "$id" "$queue_id" "$generation" "$cpu" \
        "$planned_memory" "$io" "$pid" "$state" || die "cannot reprice $id"
      memory="$planned_memory"
    fi
    if is_positive_int "$pid"; then
      if [ "$process_state" -eq 0 ] || [ "$process_state" -eq 2 ]; then
        [ "$process_state" -eq 0 ] || uncertain=1
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
          "$id" "$queue_id" "$state" "$pid" "$cpu" "$memory" 0 "$memory" >> "$temporary"
        continue
      fi
    fi
    [ -n "$pid" ] && wait "$pid" 2>/dev/null || true
    result="$results_dir/${id}.env"
    if [ ! -f "$result" ]; then
      uncertain=1
      if [ "$state" != uncertain ]; then
        write_reservation "$file" "$id" "$queue_id" "$generation" "$cpu" "$memory" "$io" "$pid" uncertain ||
          die "cannot mark $id uncertain"
      fi
      printf '%s\t%s\tuncertain\t%s\t%s\t%s\t%s\t%s\n' \
        "$id" "$queue_id" "${pid:-0}" "$cpu" "$memory" 0 "$memory" >> "$temporary"
      continue
    fi
    result_data="$(result_record "$result" 2>/dev/null)" || { uncertain=1; continue; }
    IFS=$'\t' read -r result_id outcome extra <<< "$result_data"
    [ "$result_id" = "$id" ] || { uncertain=1; continue; }
    case "$outcome" in
      empty)
        : > "$empty_dir/${generation}.${queue_id}" || die "cannot mark empty queue"
        ;;
      succeeded|job_failed|infrastructure_failed|host_rebooted) ;;
      *) uncertain=1; continue ;;
    esac
    python3 "$memory_script" --complete-reservation "$file" --result-file "$result" \
      --history-file "$state_dir/memory-history.json" --pool-id "$pool_id" --host-id "$host_id" \
      --max-gap-seconds "$((sample_seconds * 3 + 15))" || die "cannot retain memory measurement for $id"
    record_outcome "$queue_id" "$id" "$outcome" "$file"
    rm -f "$file" "$result" "${file%.env}.options" "${file%.env}.jobpid" "${file%.env}.limit-kill"
    case "$outcome" in
      infrastructure_failed|host_rebooted) ;;
      *) rm -f "$worker_output_dir/${id}.log" ;;
    esac
  done
  processes="$(ps -ww -axo pid=,ppid=,command= 2>/dev/null)" || \
    die "cannot audit adaptive worker processes"
  # Here-strings, not printf | awk: a forked printf writing a table larger than the pipe
  # buffer once blocked forever on a pipe nobody drained, hanging the daemon's shutdown.
  if ! awk -v prefix="$pool_id.$host_id." \
      -v usage="$temporary" '
        BEGIN {
          while ((getline row < usage) > 0) {
            split(row, field, "\t")
            # PID 0 marks a PID-less reservation; launchd has PPID 0.
            if (field[4] != "0") owner[field[4]]=field[1]
          }
          close(usage)
        }
        {
          parent[$1]=$2
          command[$1]=$0
          rows++
        }
        END {
          for (pass=0; pass<=rows; pass++) {
            changed=0
            for (pid in parent) if (!(pid in owner) && (parent[pid] in owner)) {
              owner[pid]=owner[parent[pid]]
              changed=1
            }
            if (!changed) break
          }
          for (pid in command) {
            n=split(command[pid], field, " ")
            for (i=3; i<=n; i++) {
              if (index(field[i], prefix) != 1) continue
              suffix=substr(field[i], length(prefix) + 1)
              if (suffix ~ /^[0-9]+$/ && owner[pid] != field[i]) bad=1
            }
          }
          exit bad
        }
      ' <<< "$processes"; then
    uncertain=1
  fi
  # Lease corrections change the declaration, not measured memory history.
  awk -F '\t' -v previous="$usage_file" '
    BEGIN {
      OFS="\t"
      while ((read_result=(getline row < previous)) > 0) {
        count=split(row, old, "\t")
        if ((count != 8 && count != 10) || old[7] !~ /^[0-9]+$/ ||
            old[8] !~ /^[0-9]+$/ || (count == 10 &&
            (old[9] !~ /^[0-9]+$/ || old[10] !~ /^[0-9]+$/))) exit 1
        key=old[1] SUBSEP old[2] SUBSEP old[4] SUBSEP old[5]
        observed[key]=old[7]
        current[key]=(count == 10 ? old[9] : 0)
        sustained[key]=(count == 10 ? old[10] : 0)
      }
      if (read_result < 0) exit 1
      close(previous)
    }
    {
      key=$1 SUBSEP $2 SUBSEP $4 SUBSEP $5
      if (key in observed) {
        $7=observed[key]
        $8=($7 > $6 ? $7 : $6)
      }
      $9=current[key]+0
      $10=sustained[key]+0
      print
    }
  ' "$temporary" > "${temporary}.charged" || die "invalid previous reservation accounting"
  mv -f "${temporary}.charged" "$usage_file" || die "cannot publish reservation accounting"
  rm -f "$temporary"
}

smb_server_bytes() {
  local snapshot
  snapshot="$(netstat -anvb -p tcp 2>/dev/null)" || return 1
  awk '
    $9 == "rxbytes" && $10 == "txbytes" {header=1; next}
    header && $4 ~ /[.:]445$/ && $6 == "ESTABLISHED" {
      if ($7 !~ /^[0-9]+$/ || $8 !~ /^[0-9]+$/) bad=1
      total += $7 + $8
    }
    END {if (bad || !header) exit 1; printf "%.0f\n", total}
  ' <<< "$snapshot"
}

sample_live_resources() {
  local physical_cpu memory_bytes memory_kib physical_memory sample extra observed_usage

  live_cpu_units=0
  live_memory_mib=0
  smbd_cpu_tenths=0
  smb_bytes=-1
  telemetry_state=unavailable
  physical_cpu="$(sysctl -n hw.logicalcpu 2>/dev/null)" || physical_cpu=""
  if ! is_positive_int "$physical_cpu"; then
    physical_cpu="$(getconf _NPROCESSORS_ONLN 2>/dev/null)" || return 1
  fi
  is_positive_int "$physical_cpu" || return 1
  memory_bytes="$(sysctl -n hw.memsize 2>/dev/null)" || memory_bytes=""
  if is_positive_int "$memory_bytes"; then
    physical_memory=$((memory_bytes / 1048576))
  else
    memory_kib="$(awk '$1 == "MemTotal:" {print $2; found=1} END {exit !found}' \
      /proc/meminfo 2>/dev/null)" || return 1
    is_positive_int "$memory_kib" || return 1
    physical_memory=$((memory_kib / 1024))
  fi
  is_positive_int "$physical_memory" || return 1

  telemetry_snapshot="$state_dir/.resource-snapshot.$$.$RANDOM"
  telemetry_original_snapshot="${telemetry_snapshot}.rss"
  if ! LC_ALL=C ps -ww -axo pid=,ppid=,%cpu=,rss=,command= > "$telemetry_snapshot" 2>/dev/null; then
    rm -f "$telemetry_snapshot" "$telemetry_original_snapshot"
    telemetry_snapshot=""
    telemetry_original_snapshot=""
    return 1
  fi
  if ! cp "$telemetry_snapshot" "$telemetry_original_snapshot" ||
      ! python3 "$memory_script" --augment-ps-snapshot "$telemetry_snapshot"; then
    rm -f "$telemetry_snapshot" "$telemetry_original_snapshot"
    telemetry_snapshot=""
    telemetry_original_snapshot=""
    return 1
  fi
  smb_bytes="$(smb_server_bytes)" || smb_bytes=-1
  observed_usage="${usage_file}.observed.$$.$RANDOM"
  sample="$(LC_ALL=C awk -v usage="$usage_file" -v observed_usage="$observed_usage" \
      -v physical_cpu="$physical_cpu" -v physical_memory="$physical_memory" \
      -v cpu_tolerance_pct="$cpu_tolerance_pct" -v cpu_budget="$cpu_budget" \
      -v memory_budget="$memory_budget" '
    function ceiling(value, integer) {
      integer=int(value)
      return integer + (value > integer)
    }
    BEGIN {
      printf "%s", "" > observed_usage
      close(observed_usage)
      while ((getline row < usage) > 0) {
        count=split(row, field, "\t")
        # PID 0 is a PID-less uncertain reservation: charged, but owns no processes.
        if (count != 10 || field[1] == "" || (field[1] in reservation_seen) ||
            field[4] !~ /^[0-9]+$/ || (field[4] != "0" && field[4] in root) ||
            field[5] !~ /^[1-9][0-9]*$/ || field[6] !~ /^[1-9][0-9]*$/ ||
            field[7] !~ /^[0-9]+$/) {
          bad=1
        } else {
          reservation_seen[field[1]]=1
          if (field[4] != "0") root[field[4]]=field[1]
          reservation_id[++reservation_count]=field[1]
          reservation_queue[field[1]]=field[2]
          reservation_state[field[1]]=field[3]
          reservation_pid[field[1]]=field[4]
          reservation_cpu[field[1]]=field[5]
          reservation_memory[field[1]]=field[6]
          reservation_observed[field[1]]=field[7] + 0
          reservation_charged[field[1]]=field[8] + 0
          reservation_current[field[1]]=field[9] + 0
          reservation_sustained[field[1]]=field[10] + 0
        }
      }
      close(usage)
    }
    {
      if (NF < 5 || $1 !~ /^[1-9][0-9]*$/ || $2 !~ /^[0-9]+$/ ||
          $3 !~ /^[0-9]+([.][0-9]+)?$/ || $4 !~ /^[0-9]+$/) {
        bad=1
        next
      }
      pid=$1
      record_command=$5
      for (i=6; i<=NF; i++) record_command=record_command " " $i
      if (FILENAME == ARGV[1]) {
        if (pid in original_rss) bad=1
        original_rows++
        original_rss[pid]=$4 + 0
        original_parent[pid]=$2
        original_cpu[pid]=$3 + 0
        original_command[pid]=record_command
        next
      }
      rows++
      if (pid in parent) bad=1
      if (!(pid in original_rss) || $2 != original_parent[pid] ||
          $3 + 0 != original_cpu[pid] || record_command != original_command[pid]) bad=1
      parent[pid]=$2
      cpu[pid]=$3 + 0
      # On macOS the second snapshot holds the private footprint of same-UID
      # processes. Compute trees are charged the larger value, everything else the smaller.
      private_rss=$4 + 0
      rss[pid]=(private_rss > original_rss[pid] ? private_rss : original_rss[pid])
      external_charge_rss[pid]=(private_rss < original_rss[pid] ? private_rss : original_rss[pid])
      command[pid]=record_command
      if (pid in root) {
        for (i=5; i<=NF; i++) if ($i == root[pid]) root_identity[pid]=1
      }
      executable=$5
      sub(/^.*\//, "", executable)
      if (executable == "smbd") smbd += cpu[pid]
      script=5
      if (executable ~ /^(bash|sh|zsh)$/ || executable ~ /^(mmworker_|mmshardworker_)/) script=6
      runner=$script
      sub(/^.*\//, "", runner)
      # Fixed controllers reserve their slots even during I/O or startup. Bash
      # forks retain the same argv; collapse those copies into one tree below.
      if (runner == "mmworker.sh" && NF >= script + 7 &&
          $(script+1) ~ /^[1-9][0-9]*$/) {
        fixed_candidate[pid]=$(script+1) + 0
      } else if (runner == "mmshardworker.sh") {
        if (NF >= script + 3 && $(script+1) == "--run-one") {
          fixed_candidate[pid]=1
          shard_state[pid]=$(script+2)
          shard_child[pid]=1
        } else if (NF >= script + 2 && $(script+2) ~ /^[1-9][0-9]*$/) {
          fixed_candidate[pid]=$(script+2) + 0
          shard_state[pid]=$(script+1)
        }
      }
    }
    END {
      if (bad || rows == 0 || rows != original_rows) exit 1
      for (pid in root) {
        if (root_identity[pid]) owner[pid]=root[pid]
        else if (reservation_state[root[pid]] != "uncertain") exit 1
      }
      for (pass=0; pass<=rows; pass++) {
        changed=0
        for (pid in parent) {
          if (!(pid in owner) && (parent[pid] in owner)) {
            owner[pid]=owner[parent[pid]]
            changed=1
          }
        }
        if (!changed) break
      }
      for (pid in fixed_candidate) {
        if (pid in owner) continue
        ancestor=parent[pid]
        covered=0
        for (pass=0; ancestor in parent && pass<=rows; pass++) {
          if ((ancestor in fixed_candidate) &&
              (command[ancestor] == command[pid] ||
               (shard_child[pid] && !shard_child[ancestor] &&
                shard_state[pid] == shard_state[ancestor]))) {
            covered=1
            break
          }
          ancestor=parent[ancestor]
        }
        if (!covered) {
          fixed_owner[pid]=pid
          fixed_slots += fixed_candidate[pid]
        }
      }
      for (pass=0; pass<=rows; pass++) {
        changed=0
        for (pid in parent) {
          if (!(pid in owner) && !(pid in fixed_owner) &&
              (parent[pid] in fixed_owner)) {
            fixed_owner[pid]=fixed_owner[parent[pid]]
            changed=1
          }
        }
        if (!changed) break
      }
      for (pid in parent) {
        if (pid in owner) {
          adaptive_rss[owner[pid]] += rss[pid]
          adaptive_cpu[owner[pid]] += cpu[pid]
        } else {
          if (pid in fixed_owner) {
            fixed_tree_cpu[fixed_owner[pid]] += cpu[pid]
            fixed_rss += rss[pid]
            external_rss += rss[pid]
          } else {
            external_cpu += cpu[pid]
            # Summed RSS counts shared pages once per process, and cold desktop
            # swap can stay cold, so unrelated processes are charged the smaller value.
            external_rss += external_charge_rss[pid]
          }
        }
      }
      for (pid in fixed_tree_cpu) {
        floor_cpu=fixed_candidate[pid] * 100
        fixed_cpu += (fixed_tree_cpu[pid] > floor_cpu ? fixed_tree_cpu[pid] : floor_cpu)
      }
      for (i=1; i<=reservation_count; i++) {
        id=reservation_id[i]
        current=reservation_current[id]
        if (reservation_state[id] != "uncertain" && reservation_pid[id] != "0") {
          if (!(reservation_pid[id] in parent)) exit 1
          current=ceiling(adaptive_rss[id] / 1024)
        }
        # Whole cores used beyond the declaration are charged like external load.
        overuse=int(adaptive_cpu[id] / 100 + 0.5) - reservation_cpu[id]
        if (overuse > 0) cpu_overuse += overuse
        printf "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\n", id,
          reservation_queue[id], reservation_state[id], reservation_pid[id],
          reservation_cpu[id], reservation_memory[id], reservation_observed[id],
          reservation_charged[id], current, reservation_sustained[id] > observed_usage
      }
      close(observed_usage)
      # Reviewed budgets already reserve OS headroom. Known fixed work shares
      # that budget; all other processes still constrain physical capacity.
      fixed_charge=ceiling((fixed_cpu - cpu_tolerance_pct) / 100)
      if (fixed_charge < fixed_slots) fixed_charge=fixed_slots
      external_cpu += fixed_cpu - cpu_tolerance_pct
      if (external_cpu < 0) external_cpu=0
      external_charge=ceiling(external_cpu / 100)
      if (external_charge < fixed_slots) external_charge=fixed_slots
      cpu_capacity=physical_cpu - external_charge
      if (cpu_capacity > cpu_budget - fixed_charge) cpu_capacity=cpu_budget - fixed_charge
      cpu_capacity -= cpu_overuse
      memory_capacity=physical_memory - ceiling(external_rss / 1024)
      if (memory_capacity > memory_budget - ceiling(fixed_rss / 1024))
        memory_capacity=memory_budget - ceiling(fixed_rss / 1024)
      if (cpu_capacity < 0) cpu_capacity=0
      if (memory_capacity < 0) memory_capacity=0
      printf "%d\t%d\t%d\n", cpu_capacity, memory_capacity, ceiling(smbd * 10)
    }
  ' "$telemetry_original_snapshot" "$telemetry_snapshot")" || sample=""
  rm -f "$telemetry_snapshot" "$telemetry_original_snapshot"
  telemetry_snapshot=""
  telemetry_original_snapshot=""
  if [ -z "$sample" ] || [ ! -f "$observed_usage" ]; then
    rm -f "$observed_usage"
    return 1
  fi
  IFS=$'\t' read -r live_cpu_units live_memory_mib smbd_cpu_tenths extra <<< "$sample"
  [ -z "${extra:-}" ] && is_uint "$live_cpu_units" && is_uint "$live_memory_mib" && \
    is_uint "$smbd_cpu_tenths" || { rm -f "$observed_usage"; return 1; }
  if ! python3 "$memory_script" --usage-file "$observed_usage" \
      --history-file "$state_dir/memory-history.json" --pool-id "$pool_id" \
      --host-id "$host_id" --max-gap-seconds "$((sample_seconds * 3 + 15))"; then
    rm -f "$observed_usage"
    return 1
  fi
  mv -f "$observed_usage" "$usage_file" || return 1
  [ "$live_cpu_units" -le "$cpu_budget" ] || live_cpu_units="$cpu_budget"
  [ "$live_memory_mib" -le "$memory_budget" ] || live_memory_mib="$memory_budget"
  telemetry_state=ok
  resource_sample_epoch="$(date +%s)"
  apply_host_policies
}

file_signature() {
  if [ "$(uname -s)" = Darwin ]; then
    stat -f '%d:%i:%z:%Fm:%Fc' "$1"
  else
    stat -c '%d:%i:%s:%.9Y:%.9Z' "$1"
  fi
}

# Hash a shared queue only when its identity or timestamps change.
queue_hash_matches() {
  local queue="$1" editable="$2" expected="$3" signature cached=""
  signature="$(file_signature "$editable" 2>/dev/null)" || return 1
  [ ! -f "$hash_cache_dir/$queue" ] || read -r cached < "$hash_cache_dir/$queue"
  [ "$cached" != "$signature $expected" ] || return 0
  [ "$(hash_file "$editable" 2>/dev/null)" = "$expected" ] || return 1
  atomic_write "$hash_cache_dir/$queue" "$signature $expected" || die "cannot cache queue hash"
}

# Check visibility of assigned work. Without assigned work, one visible registered
# queue shows the share is mounted, so a host that lost it stops being offered
# work; the other dormant queues need no SMB probes. Every grant still gets the
# full path/hash check immediately before launch.
allocation_paths_ready() {
  local mode="${1:-launch}" queue target grants cpu memory io original editable model log lock tag
  local queue_hash extra lock_parent assigned=0 dormant_logs=()

  while IFS=$'\t' read -r queue target grants cpu memory io original editable model log lock tag \
      queue_hash extra; do
    [ "$queue" != queue_id ] || continue
    [ -n "$queue" ] || continue
    if [ "$target" -le 0 ] && [ "$grants" -le 0 ]; then
      dormant_logs+=("$log")
      continue
    fi
    assigned=1
    [ -d "$log" ] || return 1
    [ "$mode" = launch ] && [ "$grants" -gt 0 ] || continue
    lock_parent="${lock%/*}"
    [ -f "$editable" ] && [ ! -L "$editable" ] && [ -r "$editable" ] && \
      queue_hash_matches "$queue" "$editable" "$queue_hash" && \
      [ -d "$model" ] && [ -x "$model" ] && \
      [ -d "$log" ] && [ -w "$log" ] && [ -x "$log" ] && \
      [ -d "$log/claims" ] && [ ! -L "$log/claims" ] && \
      [ -w "$log/claims" ] && [ -x "$log/claims" ] && \
      [ -d "$log/done" ] && [ ! -L "$log/done" ] && \
      [ -w "$log/done" ] && [ -x "$log/done" ] && \
      [ "$lock_parent" != "$lock" ] && [ -d "$lock_parent" ] && \
      [ -w "$lock_parent" ] && [ -x "$lock_parent" ] || \
      return 1
  done < "$accepted_allocations"
  [ "$assigned" -eq 0 ] && [ "${#dormant_logs[@]}" -gt 0 ] || return 0
  for log in "${dormant_logs[@]}"; do
    [ ! -d "$log" ] || return 0
  done
  return 1
}

next_reservation_id() {
  reservation_counter=$((reservation_counter + 1))
  atomic_write "$counter_file" "counter=$reservation_counter" || die "cannot save reservation counter"
  next_id="$pool_id.$host_id.$reservation_counter"
}

# Live reservations as "<reservation ID><TAB><worker PID>" lines. Each PID is checked against its
# reservation ID, so a PID the system has reused for another process is never signaled.
live_reservations() {
  local id queue state pid extra
  [ -f "$usage_file" ] || return 0
  while IFS=$'\t' read -r id queue state pid extra; do
    is_positive_int "$pid" && inspect_process "$pid" "$id" && printf '%s\t%s\n' "$id" "$pid"
  done < "$usage_file"
}

signal_trees() {
  local signal="$1" pids
  shift
  pids="$(tree_pids "$@")"
  [ -z "$pids" ] || kill "-$signal" $pids 2>/dev/null || true
}

# Resume the reservations the suspension record names and every live one. The record outlives a
# crash, so each of its PIDs is checked against its reservation ID again before it is signaled.
resume_suspended() {
  local id pid roots=""
  [ -f "$suspended_file" ] || return 0
  while IFS=$'\t' read -r id pid; do
    is_positive_int "$pid" && inspect_process "$pid" "$id" && roots="$roots $pid"
  done < "$suspended_file"
  signal_trees CONT $roots $(live_reservations | cut -f2)
  rm -f "$suspended_file"
}

# Opt-in host policies, applied after each resource sample.
apply_host_policies() {
  local id queue state pid cpu memory observed charged current sustained extra limit job note roots
  owner_state=off
  owner_idle=""
  if [ "$owner_idle_threshold" -gt 0 ]; then
    if owner_idle="$(console_idle_seconds)"; then
      if [ "$owner_idle" -lt "$owner_idle_threshold" ]; then owner_state=active; else owner_state=idle; fi
    else
      owner_idle=-1
      owner_state=unknown  # starts nothing until activity can be read
    fi
  fi
  if [ "$owner_action" = suspend ] && [ "$owner_state" = active ]; then
    roots="$(live_reservations)"
    if [ -n "$roots" ]; then
      # Record what is about to stop before stopping it, so a crash cannot strand a job; keep
      # earlier entries, which resume checks again. Repeated each sample: a process forked just
      # before the stop is caught next time.
      if atomic_write "$suspended_file" "$({ [ ! -f "$suspended_file" ] || cat "$suspended_file"
          printf '%s\n' "$roots"; } | awk 'NF && !seen[$0]++')"; then
        signal_trees STOP $(cut -f2 <<< "$roots")
        owner_state=suspended
      else
        echo "mmhostdaemon: cannot record suspended reservations on $host_id; leaving them running" >&2
      fi
    fi
  elif [ "$owner_state" = idle ] || [ "$owner_state" = off ]; then
    resume_suspended
  elif [ -f "$suspended_file" ]; then
    owner_state=suspended
  fi
  # Memory limits: end only the job's own process tree, so its worker records the failure.
  while IFS=$'\t' read -r id queue state pid cpu memory observed charged current sustained extra; do
    [ -f "$reservations_dir/$id.options" ] && [ -f "$reservations_dir/$id.jobpid" ] || continue
    limit="$(optional_env_value "$reservations_dir/$id.options" memory_limit_mib 0)" || continue
    is_positive_int "$limit" || continue
    note="$reservations_dir/$id.limit-kill"
    # After the first breach a surviving job is killed, even once its memory has fallen.
    [ -f "$note" ] || { is_uint "$current" && [ "$current" -gt "$limit" ]; } || continue
    job="$(cat "$reservations_dir/$id.jobpid")"
    # The job must still belong to this reservation's live worker.
    is_positive_int "$job" && is_positive_int "$pid" && inspect_process "$pid" "$id" &&
      tree_pids "$pid" | grep -qx "$job" || continue
    if [ -f "$note" ]; then
      signal_trees KILL "$job"
    else
      printf 'Ended by the scheduler: the job used %s MiB, over its memory limit of %s MiB.\n' \
        "$current" "$limit" > "$note"
      echo "mmhostdaemon: ending reservation $id on $host_id: $current MiB exceeds its $limit MiB limit" >&2
      signal_trees TERM "$job"
    fi
  done < "$usage_file"
}

# Device indices held by live reservations, one per line.
devices_in_use() {
  local file
  for file in "$reservations_dir"/*.options; do
    [ -f "$file" ] && [ -f "${file%.options}.env" ] || continue
    optional_env_value "$file" gpu_devices "" | tr ',' '\n'
  done | awk 'NF'
}

launch_one() {
  local queue_id="$1" cpu="$2" memory="$3" io="$4" original="$5" editable="$6"
  local model="$7" log="$8" lock="$9" tag="${10}" queue_hash="${11}" gpu="${12:-0}" limit="${13:-0}"
  local id reservation result output pid devices=""
  next_reservation_id
  id="$next_id"
  reservation="$reservations_dir/${id}.env"
  result="$results_dir/${id}.env"
  output="$worker_output_dir/${id}.log"
  if [ "$gpu" -gt 0 ]; then
    # The lowest free indices; the caller has already checked the device budget.
    devices="$(devices_in_use | awk -v budget="$gpu_budget" -v want="$gpu" '
      {used[$1] = 1}
      END {for (i = 0; i < budget && n < want; i++) if (!(i in used)) {list = list (n++ ? "," : "") i}
           if (n < want) exit 1; print list}')" || die "no free devices for reservation $id"
  fi
  if [ "$gpu" -gt 0 ] || [ "$limit" -gt 0 ]; then
    atomic_write "$reservations_dir/${id}.options" "gpu_devices=$devices" "memory_limit_mib=$limit" ||
      die "cannot record options for reservation $id"
  fi
  write_reservation "$reservation" "$id" "$queue_id" "$accepted_generation" \
    "$cpu" "$memory" "$io" "" starting || die "cannot create reservation $id"
  if command -v setsid >/dev/null 2>&1; then
    ROTA_GPU_DEVICES="$devices" setsid bash "$runner_path" 1 "$original" "$editable" "$model" "$log" "$lock" "$tag" \
      "$id" "$result" "$reservation" "$queue_hash" </dev/null >"$output" 2>&1 &
  else
    ROTA_GPU_DEVICES="$devices" perl -MPOSIX -e 'POSIX::setsid() >= 0 or die "setsid failed\n"; exec @ARGV' \
      bash "$runner_path" 1 "$original" "$editable" "$model" "$log" "$lock" "$tag" \
      "$id" "$result" "$reservation" "$queue_hash" </dev/null >"$output" 2>&1 &
  fi
  pid=$!
  write_reservation "$reservation" "$id" "$queue_id" "$accepted_generation" \
    "$cpu" "$memory" "$io" "$pid" running || die "cannot record reservation PID"
  printf '%s\t%s\trunning\t%s\t%s\t%s\t%s\t%s\t0\t0\n' \
    "$id" "$queue_id" "$pid" "$cpu" "$memory" 0 "$memory" >> "$usage_file" ||
    die "cannot update reservation accounting"
}

enforce_plan() {
  local queue target grants cpu memory io original editable model log lock tag queue_hash
  local gpu limit queue_running grants_started extra totals used_gpu
  # The owner is at this host (or its activity is unknown): start nothing new.
  case "$owner_state" in active|unknown|suspended) return 0 ;; esac
  used_gpu="$(devices_in_use | awk 'END {print NR}')"
  totals="$(awk -F '\t' '{cpu += $5; memory += $8} END {print cpu+0, memory+0}' "$usage_file")"
  used_cpu="${totals%% *}"
  used_memory="${totals#* }"
  # The rows go through a file, not a here-string: bash 5.1 leaks both ends of a pipe for each
  # here-string larger than the pipe's capacity, which shrinks under per-user pipe limits.
  awk -F '\t' -v usage="$usage_file" '
    BEGIN {
      while ((getline row < usage) > 0) {
        split(row, fields, "\t")
        running[fields[2]]++
      }
      close(usage)
    }
    FNR > 1 {print $0 FS (running[$1] + 0)}
  ' "$accepted_allocations" > "$plan_rows_file" || die "cannot account accepted start grants"
  while IFS=$'\t' read -r queue target grants cpu memory io original editable model log lock tag queue_hash \
      gpu limit queue_running extra; do
    [ -n "$queue" ] || continue
    [ ! -e "$empty_dir/${accepted_generation}.${queue}" ] || continue
    failure_record "$queue"
    [ "$failure_reason" = none ] || continue
    grants_started=0
    while [ "$grants_started" -lt "$grants" ] && [ "$queue_running" -lt "$target" ] &&
          [ "$stop_daemon" -eq 0 ] && [ "$SECONDS" -lt "$lease_deadline" ] &&
          [ $((used_cpu + cpu)) -le "$live_cpu_units" ] &&
          [ $((used_memory + memory)) -le "$live_memory_mib" ] &&
          [ $((used_gpu + gpu)) -le "$gpu_budget" ]; do
      launch_one "$queue" "$cpu" "$memory" "$io" "$original" "$editable" "$model" "$log" "$lock" "$tag" \
        "$queue_hash" "$gpu" "$limit"
      used_gpu=$((used_gpu + gpu))
      used_cpu=$((used_cpu + cpu))
      used_memory=$((used_memory + memory))
      queue_running=$((queue_running + 1))
      grants_started=$((grants_started + 1))
    done
  done < "$plan_rows_file"
}

publish_status() {
  local admission="$1" temporary id queue state pid cpu memory observed charged current sustained extra
  local file calibration_report
  temporary="${status_file}.tmp.$$.$RANDOM"
  printf 'record_type\tpool_id\thost_id\tgeneration\tadmission\treservation_id\tqueue_id\tstate\tpid\tcpu_units\tmemory_mib\tobserved_memory_mib\tcharged_memory_mib\tmessage\tlive_cpu_units\tlive_memory_mib\tsmbd_cpu_tenths\ttelemetry_state\tresource_sample_epoch\tsmb_bytes\tcurrent_memory_mib\tsustained_memory_mib\towner_state\towner_idle_seconds\n' > "$temporary" || return 1
  printf 'daemon\t%s\t%s\t%s\t%s\t\t\t\t\t\t\t\t\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\t\t%s\t%s\n' \
    "$pool_id" "$host_id" "$accepted_generation" "$admission" "$plan_message" \
    "$live_cpu_units" "$live_memory_mib" "$smbd_cpu_tenths" "$telemetry_state" "$resource_sample_epoch" \
    "$smb_bytes" "$owner_state" "${owner_idle:--1}" >> "$temporary" || return 1
  while IFS=$'\t' read -r id queue state pid cpu memory observed charged current sustained extra; do
    [ -n "$id" ] || continue
    printf 'reservation\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\t\t\t\t\t\t\t%s\t%s\t\t\n' \
      "$pool_id" "$host_id" "$accepted_generation" "$admission" "$id" "$queue" \
      "$state" "$pid" "$cpu" "$memory" "$observed" "$charged" "$current" "$sustained" >> "$temporary" || return 1
  done < "$usage_file"
  if [ -n "$accepted_allocations" ] && [ "$telemetry_state" = ok ]; then
    calibration_report="$(python3 "$memory_script" --calibration-report \
      --history-file "$state_dir/memory-history.json" --pool-id "$pool_id" --host-id "$host_id" \
      --calibration-queues-file "$accepted_allocations")" || return 1
    if [ -n "$calibration_report" ]; then
      awk -F '\t' -v OFS='\t' \
        -v pool="$pool_id" -v host="$host_id" '
        NF != 2 {exit 1}
        {for (i=1; i<=24; i++) value[i]="";
         value[1]="calibration"; value[2]=pool; value[3]=host; value[7]=$1; value[14]=$2;
         for (i=1; i<=24; i++) printf "%s%s", value[i], (i==24 ? "\n" : OFS)}
      ' <<< "$calibration_report" >> "$temporary" || return 1
    fi
  fi
  for file in "$failures_dir"/*.env; do
    [ -f "$file" ] || continue
    queue="${file##*/}"; queue="${queue%.env}"
    failure_record "$queue"
    [ "$failure_reason" != none ] || continue
    printf 'blocked_queue\t%s\t%s\t\t\t\t%s\t\t\t\t\t\t\t%s\t\t\t\t\t\t\t\t\t\t\n' \
      "$pool_id" "$host_id" "$queue" "$failure_reason" >> "$temporary" || return 1
  done
  mv -f "$temporary" "$status_file"
}

state_dir=""
poll_seconds=1
sample_seconds=5
run_once=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --state-dir) [ "$#" -ge 2 ] || { usage; exit 2; }; state_dir="$2"; shift 2 ;;
    --poll-seconds) [ "$#" -ge 2 ] || { usage; exit 2; }; poll_seconds="$2"; shift 2 ;;
    --sample-seconds) [ "$#" -ge 2 ] || { usage; exit 2; }; sample_seconds="$2"; shift 2 ;;
    --once) run_once=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done
[ -n "$state_dir" ] || { usage; exit 2; }
awk -v n="$poll_seconds" 'BEGIN {exit !(n ~ /^[0-9]+([.][0-9]+)?$/ && n > 0)}' || die "invalid poll interval"
is_positive_int "$sample_seconds" || die "invalid sample interval"
[ -d "$state_dir" ] || die "state directory does not exist: $state_dir"
daemon_file="$state_dir/daemon.env"
[ -r "$daemon_file" ] || die "missing daemon.env"
[ "$(env_value "$daemon_file" schema_version)" = 1 ] || die "unsupported daemon schema"
pool_id="$(env_value "$daemon_file" pool_id)" || die "missing pool_id"
host_id="$(env_value "$daemon_file" host_id)" || die "missing host_id"
cpu_budget="$(env_value "$daemon_file" cpu_units)" || die "missing cpu_units"
memory_budget="$(env_value "$daemon_file" memory_mib)" || die "missing memory_mib"
# Opt-in host settings; absent keys keep today's behavior.
gpu_budget="$(optional_env_value "$daemon_file" gpu_units 0)" || die "duplicate gpu_units"
owner_idle_threshold="$(optional_env_value "$daemon_file" owner_idle_seconds 0)" || die "duplicate owner_idle_seconds"
owner_action="$(optional_env_value "$daemon_file" owner_active hold)" || die "duplicate owner_active"
is_uint "$gpu_budget" && is_uint "$owner_idle_threshold" || die "invalid host options"
case "$owner_action" in hold|suspend) ;; *) die "invalid owner_active: $owner_action" ;; esac
cpu_tolerance_pct=10
runner_path="$(env_value "$daemon_file" runner_path)" || die "missing runner_path"
runner_hash="$(env_value "$daemon_file" runner_sha256)" || die "missing runner_sha256"
claim_hash="$(env_value "$daemon_file" claim_sha256)" || die "missing claim_sha256"
claim_script="${runner_path%/*}/adaptive_claim.py"
memory_script="${runner_path%/*}/adaptive_memory.py"
memory_hash="$(env_value "$daemon_file" memory_sha256)" || die "missing memory_sha256"
case "$pool_id" in ''|.|..|*[!A-Za-z0-9._-]*) die "invalid pool ID" ;; esac
case "$host_id" in ''|.|..|*[!A-Za-z0-9._-]*) die "invalid host ID" ;; esac
is_positive_int "$cpu_budget" && is_positive_int "$memory_budget" || die "invalid host budget"
[ -x "$runner_path" ] && [ "$(hash_file "$runner_path")" = "$runner_hash" ] || die "runner is missing or has the wrong hash"
[ -r "$claim_script" ] && [ "$(hash_file "$claim_script")" = "$claim_hash" ] || die "claim helper is missing or has the wrong hash"
[ -r "$memory_script" ] && [ "$(hash_file "$memory_script")" = "$memory_hash" ] || die "memory helper is missing or has the wrong hash"
python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 9))' || die "Python 3.9 or newer is required"
command -v setsid >/dev/null 2>&1 || command -v perl >/dev/null 2>&1 ||
  die "setsid or perl is required to launch independent workers"

reservations_dir="$state_dir/reservations"
results_dir="$state_dir/results"
empty_dir="$state_dir/empty"
worker_output_dir="$state_dir/worker-output"
failures_dir="$state_dir/failures"
failure_resets_dir="$state_dir/failure-reset-requests"
hash_cache_dir="$state_dir/queue-hash-cache"
mkdir -p "$reservations_dir" "$results_dir" "$empty_dir" "$worker_output_dir" \
  "$failures_dir" "$failure_resets_dir" "$hash_cache_dir" || die "cannot initialize state"
generation_file="$state_dir/last_generation.env"
counter_file="$state_dir/reservation_counter.env"
status_file="$state_dir/status.tsv"
usage_file="$state_dir/reservation_usage.tsv"
plan_rows_file="$state_dir/plan_rows.tsv"
suspended_file="$state_dir/suspended.tsv"
if [ ! -f "$generation_file" ]; then
  [ ! -e "$state_dir/lease.env" ] && [ ! -e "$state_dir/accepted.allocations.tsv" ] ||
    die "saved generation is missing while accepted plan state exists"
  for state_file in "$reservations_dir"/*.env; do
    [ ! -e "$state_file" ] || die "saved generation is missing while reservation state exists"
  done
  atomic_write "$generation_file" "generation=0" || die "cannot initialize saved generation"
fi
if [ ! -f "$counter_file" ]; then
  for state_file in "$reservations_dir"/*.env "$results_dir"/*.env; do
    [ ! -e "$state_file" ] || die "reservation counter is missing while durable reservation state exists"
  done
  atomic_write "$counter_file" "counter=0" || die "cannot initialize reservation counter"
fi
saved_generation="$(env_value "$generation_file" generation)" || die "invalid saved generation"
reservation_counter="$(env_value "$counter_file" counter)" || die "invalid reservation counter"
is_uint "$saved_generation" && is_uint "$reservation_counter" || die "invalid saved daemon state"

daemon_lock="$state_dir/daemon.lock"
lock_candidate="$state_dir/.daemon.lock.$$.$RANDOM"
lock_snapshot="$state_dir/.daemon.lock.observed.$$.$RANDOM"
lock_failure() {
  rm -f "$lock_candidate" "$lock_snapshot"
  die "$*"
}
atomic_write "$lock_candidate" "pid=$$" "script=$0" "pool_id=$pool_id" \
  "host_id=$host_id" "state_dir=$state_dir" "memory_control_version=1" \
  "memory_calibration_version=1" || die "cannot prepare daemon lock"
if ! ln "$lock_candidate" "$daemon_lock" 2>/dev/null; then
  [ -f "$daemon_lock" ] && ln "$daemon_lock" "$lock_snapshot" 2>/dev/null ||
    lock_failure "daemon lock is incomplete or changing: $daemon_lock"
  owner_pid="$(env_value "$lock_snapshot" pid 2>/dev/null)" || lock_failure "daemon lock has no unique PID"
  owner_script="$(env_value "$lock_snapshot" script 2>/dev/null)" || lock_failure "daemon lock has no unique script"
  owner_pool="$(env_value "$lock_snapshot" pool_id 2>/dev/null)" || lock_failure "daemon lock has no unique pool ID"
  owner_host="$(env_value "$lock_snapshot" host_id 2>/dev/null)" || lock_failure "daemon lock has no unique host ID"
  owner_state="$(env_value "$lock_snapshot" state_dir 2>/dev/null)" || lock_failure "daemon lock has no unique state directory"
  [ "$owner_script" = "$0" ] && [ "$owner_pool" = "$pool_id" ] && \
    [ "$owner_host" = "$host_id" ] && [ "$owner_state" = "$state_dir" ] || \
    lock_failure "daemon lock identity does not match this state directory"
  is_positive_int "$owner_pid" || lock_failure "daemon lock PID is invalid"
  owner_command=""
  owner_command="$(ps -ww -o command= -p "$owner_pid" 2>/dev/null || true)"
  if [[ " $owner_command " == *" $owner_script "* &&
        " $owner_command " == *" --state-dir $owner_state "* ]]; then
    lock_failure "another daemon owns this state directory (pid $owner_pid)"
  fi
  [ "$daemon_lock" -ef "$lock_snapshot" ] || lock_failure "daemon lock changed during owner audit"
  rm -f "$daemon_lock"
  ln "$lock_candidate" "$daemon_lock" 2>/dev/null || lock_failure "another daemon acquired this state directory"
fi
rm -f "$lock_candidate" "$lock_snapshot"
stop_daemon=0
trap 'stop_daemon=1' TERM INT
telemetry_snapshot=""
telemetry_original_snapshot=""
trap 'resume_suspended; lock_pid="$(env_value "$daemon_lock" pid 2>/dev/null || true)"; [ "$lock_pid" != "$$" ] || rm -f "$daemon_lock"; [ -z "$telemetry_snapshot" ] || rm -f "$telemetry_snapshot"; [ -z "$telemetry_original_snapshot" ] || rm -f "$telemetry_original_snapshot"' EXIT

SECONDS=0
accepted_generation=0
enforced_generation=0
accepted_allocations=""
accepted_paths_ready=1
lease_deadline=0
plan_message=no_lease
uncertain=0
live_cpu_units=0
live_memory_mib=0
smbd_cpu_tenths=0
smb_bytes=-1
telemetry_state=unavailable
resource_sample_epoch=0
next_telemetry_sample=0
owner_state=off
owner_idle=""
# A previous daemon may have stopped with jobs suspended for the owner.
resume_suspended
while [ "$stop_daemon" -eq 0 ]; do
  accept_plan
  apply_failure_resets
  reconcile_reservations
  if [ "$uncertain" -eq 0 ] && [ "$accepted_generation" -gt 0 ] && \
      [ "$SECONDS" -lt "$lease_deadline" ] && \
      [ "$enforced_generation" -ne "$accepted_generation" ]; then
    if sample_live_resources; then
      if allocation_paths_ready; then
        enforce_plan
      else
        accepted_paths_ready=0
        plan_message=queue_paths_unavailable
        telemetry_state=unavailable
      fi
    else
      plan_message=telemetry_unavailable
    fi
    enforced_generation="$accepted_generation"
    next_telemetry_sample=$((SECONDS + sample_seconds))
  elif [ "$SECONDS" -ge "$next_telemetry_sample" ]; then
    if sample_live_resources; then
      if [ -n "$accepted_allocations" ] && ! allocation_paths_ready visible; then
        telemetry_state=unavailable
        plan_message=queue_paths_unavailable
      elif [ "$plan_message" = telemetry_unavailable ] || [ "$plan_message" = queue_paths_unavailable ]; then
        plan_message=telemetry_ready
      fi
    fi
    next_telemetry_sample=$((SECONDS + sample_seconds))
  fi
  admission=drain-only
  if [ "$uncertain" -eq 0 ] && [ "$accepted_generation" -gt 0 ] && [ "$SECONDS" -lt "$lease_deadline" ]; then
    if [ "$telemetry_state" = ok ] && [ "$accepted_paths_ready" -eq 1 ]; then
      admission=enabled
    else
      [ "$accepted_paths_ready" -eq 0 ] && plan_message=queue_paths_unavailable
      [ "$plan_message" = queue_paths_unavailable ] || plan_message=telemetry_unavailable
    fi
  elif [ "$uncertain" -gt 0 ]; then
    plan_message=uncertain_reservation
  elif [ "$accepted_generation" -gt 0 ]; then
    plan_message=expired
  fi
  publish_status "$admission" || die "cannot publish status"
  [ "$run_once" -eq 0 ] || break
  sleep "$poll_seconds"
done
