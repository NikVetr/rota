#!/usr/bin/env bash

set -u

if [ "$#" -lt 5 ] || [ "$#" -gt 7 ]; then
  echo "Usage: $0 <batch_dir> <manifest_file> <run_token> <tag_clean> <poll_sec> [ssh_connect_timeout_sec] [pull_timeout_sec]" >&2
  exit 2
fi

BATCH_DIR="$1"
MANIFEST_FILE="$2"
RUN_TOKEN="$3"
TAG_CLEAN="$4"
POLL_SEC="$5"
SSH_CONNECT_TIMEOUT_SEC="${6:-5}"
PULL_TIMEOUT_SEC="${7:-15}"
SHARD_DIR="$BATCH_DIR/shards"
LOCAL_ROOT="${TMPDIR:-/tmp}/rota-shard-coordinator/$RUN_TOKEN"
COORDINATOR_STATE="$SHARD_DIR/coordinator.env"

case "$POLL_SEC:$SSH_CONNECT_TIMEOUT_SEC:$PULL_TIMEOUT_SEC" in
  *[!0-9:]*|:*|*::*|*:)
    echo "poll and timeout values must be positive integers." >&2
    exit 2
    ;;
esac
if [ "$POLL_SEC" -lt 1 ] || [ "$SSH_CONNECT_TIMEOUT_SEC" -lt 1 ] || [ "$PULL_TIMEOUT_SEC" -lt 1 ]; then
  echo "poll and timeout values must be at least 1." >&2
  exit 2
fi
case "$RUN_TOKEN:$TAG_CLEAN" in
  *[!A-Za-z0-9_.:-]*)
    echo "run_token and tag_clean contain unsupported characters." >&2
    exit 2
    ;;
esac
if [ ! -d "$BATCH_DIR" ] || [ ! -f "$MANIFEST_FILE" ]; then
  echo "Missing shard batch or manifest: $BATCH_DIR / $MANIFEST_FILE" >&2
  exit 1
fi

mkdir -p "$SHARD_DIR" "$LOCAL_ROOT"

publish_atomic() {
  local source_file="$1"
  local destination_file="$2"
  local stage_file="$SHARD_DIR/.$(basename "$destination_file").$$.$RANDOM.tmp"

  if cp "$source_file" "$stage_file" && chmod 666 "$stage_file" && mv -f "$stage_file" "$destination_file"; then
    return 0
  fi
  rm -f "$stage_file"
  return 1
}

write_coordinator_state() {
  local state="$1"
  local reachable="$2"
  local terminal="$3"
  local total="$4"
  local local_state="$LOCAL_ROOT/coordinator.env"

  {
    echo "state=$state"
    echo "run_token=$RUN_TOKEN"
    echo "tag_clean=$TAG_CLEAN"
    echo "reachable_hosts=$reachable"
    echo "terminal_hosts=$terminal"
    echo "total_hosts=$total"
    echo "updated_epoch=$(date +%s)"
    echo "pid=$$"
  } > "$local_state"
  publish_atomic "$local_state" "$COORDINATOR_STATE"
}

run_with_timeout() {
  local timeout_sec="$1"
  shift
  "$@" &
  local command_pid=$!
  local waited=0

  while kill -0 "$command_pid" 2>/dev/null; do
    if [ "$waited" -ge "$timeout_sec" ]; then
      kill -TERM "$command_pid" 2>/dev/null || true
      sleep 1
      kill -KILL "$command_pid" 2>/dev/null || true
      wait "$command_pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
    waited=$((waited + 1))
  done
  wait "$command_pid"
}

pull_progress_bundle() {
  local host="$1"
  local pull_dir="$2"
  local archive_file="$pull_dir/progress.tar"

  mkdir -p "$pull_dir"
  ssh -n -o BatchMode=yes -o ConnectTimeout="$SSH_CONNECT_TIMEOUT_SEC" -o ConnectionAttempts=1 "$host" \
    "state_dir=\"\$HOME/.rota/runs/$RUN_TOKEN\"; \
     [ -f \"\$state_dir/progress/progress.env\" ] || exit 3; \
     cd \"\$state_dir\"; \
     tar -cf - progress/progress.env progress/succeeded_ids.txt progress/failed_ids.txt" \
    > "$archive_file" 2> "$pull_dir/ssh.err" || return $?
  tar -xf "$archive_file" -C "$pull_dir" || return $?
}

publish_host_progress() {
  local host_key="$1"
  local pull_dir="$2"
  local progress_file="$pull_dir/progress/progress.env"
  local succeeded_file="$pull_dir/progress/succeeded_ids.txt"
  local failed_file="$pull_dir/progress/failed_ids.txt"
  local pulled_tag pulled_key

  [ -f "$progress_file" ] && [ -f "$succeeded_file" ] && [ -f "$failed_file" ] || return 1
  pulled_tag="$(awk -F= '$1 == "tag_clean" {print $2; exit}' "$progress_file")"
  pulled_key="$(awk -F= '$1 == "host_key" {print $2; exit}' "$progress_file")"
  [ "$pulled_tag" = "$TAG_CLEAN" ] && [ "$pulled_key" = "$host_key" ] || return 1

  publish_atomic "$succeeded_file" "$SHARD_DIR/${host_key}.succeeded_ids.txt" || return 1
  publish_atomic "$failed_file" "$SHARD_DIR/${host_key}.failed_ids.txt" || return 1
  publish_atomic "$progress_file" "$SHARD_DIR/${host_key}.env" || return 1
}

pull_final_joblog() {
  local host_key="$1"
  local host="$2"
  local local_joblog="$LOCAL_ROOT/${host_key}.parallel-joblog.tsv"

  if ssh -n -o BatchMode=yes -o ConnectTimeout="$SSH_CONNECT_TIMEOUT_SEC" -o ConnectionAttempts=1 "$host" \
    "joblog=\"\$HOME/.rota/runs/$RUN_TOKEN/parallel-joblog.tsv\"; [ -f \"\$joblog\" ] && cat \"\$joblog\"" \
    > "$local_joblog" 2>/dev/null; then
    publish_atomic "$local_joblog" "$SHARD_DIR/${host_key}.parallel-joblog.tsv" || true
  fi
}

stop_coordinator() {
  write_coordinator_state INTERRUPTED 0 0 "$TOTAL_HOSTS" || true
  exit 143
}

TOTAL_HOSTS="$(awk -F'\t' '$4 + 0 > 0 {n++} END {print n+0}' "$MANIFEST_FILE")"
if [ "$TOTAL_HOSTS" -lt 1 ]; then
  echo "Shard manifest contains no active hosts." >&2
  exit 1
fi
trap stop_coordinator TERM INT

write_coordinator_state RUNNING 0 0 "$TOTAL_HOSTS" || {
  echo "Unable to publish initial shard coordinator state." >&2
  exit 1
}

while true; do
  reachable_hosts=0
  terminal_hosts=0
  while IFS=$'\t' read -r host_key host cores shard_jobs; do
    [ "${shard_jobs:-0}" -gt 0 ] || continue
    case "$host_key" in
      host_[0-9][0-9]) ;;
      *)
        echo "Invalid host key in shard manifest: $host_key" >&2
        continue
        ;;
    esac
    pull_dir="$LOCAL_ROOT/${host_key}.pull.$$"
    rm -rf "$pull_dir"
    if run_with_timeout "$PULL_TIMEOUT_SEC" pull_progress_bundle "$host" "$pull_dir" \
        && publish_host_progress "$host_key" "$pull_dir"; then
      reachable_hosts=$((reachable_hosts + 1))
      shard_state="$(awk -F= '$1 == "state" {print $2; exit}' "$pull_dir/progress/progress.env")"
      case "$shard_state" in
        COMPLETED|FAILED|INTERRUPTED)
          terminal_hosts=$((terminal_hosts + 1))
          if [ ! -f "$SHARD_DIR/${host_key}.parallel-joblog.tsv" ]; then
            pull_final_joblog "$host_key" "$host"
          fi
          ;;
      esac
    else
      echo "$(date): unable to refresh shard progress from $host" >&2
    fi
    rm -rf "$pull_dir"
  done < "$MANIFEST_FILE"

  if [ "$terminal_hosts" -ge "$TOTAL_HOSTS" ]; then
    write_coordinator_state COMPLETED "$reachable_hosts" "$terminal_hosts" "$TOTAL_HOSTS" || true
    exit 0
  fi
  write_coordinator_state RUNNING "$reachable_hosts" "$terminal_hosts" "$TOTAL_HOSTS" || true
  sleep "$POLL_SEC"
done
