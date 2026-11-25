# Run your first two jobs

The two jobs in this example square 3 and 7 and write one file apiece. They
need only Python’s standard library. Complete the [cluster setup](../../README.md#set-up-another-cluster)
first. With the adaptive pool active, open a Bash shell in the coordinator’s
checkout and source the generated environment.

```bash
project=$(bash examples/first-job/prepare.sh)
cat "$project/queues/jobs.txt"
scheduler="$ROTA_ROOT/adaptive_scheduler.sh"
"$scheduler" submit --model_dir "$project" --queue jobs.txt \
  --project first-job --run_name squares --cpu_units_per_job 1 --memory_mib_per_job 128
```

In the printed queue, each line starts with a unique job ID. The rest of the
line is the command the worker runs. The preparation script puts everything
under the configured shared root and grants access to the project’s group.
Each worker’s SSH account needs access to the project and permission to write
to `results/`. These jobs use the pool’s default I/O reservation.

Set `batch` to the `batch_dir` printed by `submit`. When the status reaches
`lifecycle=COMPLETED`, look for a completion marker for each job, then read the
two results to check the arithmetic.

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
