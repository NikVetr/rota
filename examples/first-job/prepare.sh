#!/usr/bin/env bash
set -euo pipefail

# A separate project per invocation keeps previous results and queues intact.
projects="${PARALLEL_WORKERS_ROOT:?Source the generated environment.sh first}/stan_projects"
mkdir -p "$projects"
project="$(mktemp -d "$projects/first-job.XXXXXX")"
mkdir "$project/queues" "$project/results"
cp "$(dirname "$0")/square.py" "$project/square.py"
for number in 3 7; do
  printf 'square%s python3 "%s/square.py" %s "%s/results/%s.txt"\n' \
    "$number" "$project" "$number" "$project" "$number"
done > "$project/queues/jobs.txt"
chmod g+rx "$project" "$project/queues"
chmod g+rwx "$project/results"
chmod g+r "$project/square.py" "$project/queues/jobs.txt"
printf '%s\n' "$project"
