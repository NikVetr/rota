# Run your first two jobs

Start with the [cluster setup](../../README.md#set-up-another-cluster).
Use a Bash shell on the coordinator, in the repository checkout, with the
generated environment sourced and the adaptive pool active. This example uses
Python’s standard library. It squares two numbers and writes a separate
result for each.

```bash
project=$(bash examples/first-job/prepare.sh)
cat "$project/queues/jobs.txt"
scheduler="$PARALLEL_WORKERS_ROOT/adaptive_scheduler.sh"
"$scheduler" submit --model_dir "$project" --queue jobs.txt \
  --project first-job --run_name squares --cpu_units_per_job 1 --memory_mib_per_job 128
```

Each queue line starts with a unique job ID, followed by the command a worker
will execute. The preparer uses the configured shared root for all paths. Each
worker’s SSH account needs access to the project and write access to
`results/`. The preparer grants these permissions to the project’s group.
The sample I/O reservation follows the pool’s configured default.

Copy the `batch_dir` value printed by `submit` into `batch`. Check status
until `lifecycle=COMPLETED`, then verify both completion markers and results.

```bash
batch=/path/printed/by/submit
"$scheduler" queue-status --batch_dir "$batch"
ls "$batch/done/job_square3.done" "$batch/done/job_square7.done"
cat "$project/results/3.txt" "$project/results/7.txt"
```

Expected output from `cat`:

```text
9
49
```

To run it again, start with the prepare command. It creates a fresh project.
For your own workload, replace the command and its inputs, keep output paths
unique, and accept the worker’s appended `--job_tag VALUE` argument.
