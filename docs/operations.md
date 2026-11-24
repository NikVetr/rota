# Operating a small cluster

## Inventory

Copy `examples/cluster.json` to `cluster.local.json`. Set `shared_root` to the
same absolute directory on all machines. Mount it before rendering. Set
`coordinator` to the ID of the machine with locally attached storage. SSH from
the coordinator must reach all inventory targets, including the coordinator.
A host can coordinate even if its worker entry is disabled.

Each host has an ID, an SSH target, a positive CPU allowance, a positive memory
allowance in MiB, and an enabled flag. An SSH target can be an alias from
`~/.ssh/config`. Check the inventory without contacting any machines:

```bash
python3 cluster_config.py cluster.local.json
```

Rendering writes a versioned environment, a hosts file and a resource table
under the shared root. Source the environment in each admin shell. After an
inventory change, render again and source the new path. A running adaptive
pool retains its current budgets. Drain it before adopting an edited inventory,
or use the scheduler’s live budget controls.

For a second SSH route, add `fallback` to the host and run
`admin/install_ssh_routes.sh "$PARALLEL_WORKERS_CONFIG_DIR"` on each machine
that initiates those connections. The primary and fallback must reach the same
SSH server. A fallback route carries control traffic. Jobs that need shared
storage still require a working mount.

The example sets `shared_io_capacity` to zero. For the optional SMB admission
guard, set a positive capacity and name the sampling host in `io_pressure_host`.
A job’s default reservation follows the pool setting. Specify `network_interface`
or `disk` when automatic coordinator device selection is unsuitable.

## Submit your own queue

Use the [first-job walkthrough](../examples/first-job/README.md) as a template.
Create `queues/jobs.txt` in a project directory on the shared drive. Each line
contains a unique ID and a shell command. The worker appends
`--job_tag VALUE` to that command. Set separate output paths per job and check
that every eligible host can run the program. Keep the project and queue paths
free of spaces and shell metacharacters.

Run these commands on the coordinator, with the generated environment sourced
and `project` set to your project directory:

```bash
scheduler="$PARALLEL_WORKERS_ROOT/adaptive_scheduler.sh"
"$scheduler" pool-status
"$scheduler" submit --model_dir "$project" --queue jobs.txt \
  --project my-analysis --run_name pilot \
  --cpu_units_per_job 1 --memory_mib_per_job 512 --max_workers 1
```

Use CPU and memory values measured for your job, including child processes.
Copy the printed `batch_dir` into a shell variable called `batch`.
`"$scheduler" queue-status --batch_dir "$batch"` shows the queue lifecycle.
Inspect `$batch/done/` and the application’s outputs before increasing concurrency.
`--after "$batch"` on a later submission makes it wait for successful completion.

## Monitor and recover

The direct dashboard samples the hosts over SSH. On a Linux dashboard host, the
optional systemd user service instead collects shared snapshots for the CLI and
Agent Bar. Install it from the checkout with the rendered environment sourced.
Check it with `systemctl --user status parallel-workers-collector` or
`journalctl --user -u parallel-workers-collector`. Rerun the installer after
changing inventories. Queue ETA uses measured job durations and current worker
occupancy. Long final jobs and differing job durations can still affect it.

To pause admissions for one queue, run `"$scheduler" pause --batch_dir "$batch"`.
Its running jobs finish. Resume with the same command using `resume`.
For maintenance, `"$scheduler" pool-drain` stops new admissions across the pool.
Check `pool-status` until drain completes before updating deployed scripts.

An unreachable worker may still be computing. After an interruption, restore
storage and connectivity first, inspect queue status, completion markers and
outputs, and reconcile uncertain jobs before retrying them. If the coordinator
process exited, record the pool ID from `pool-status` and confirm the original
process is gone. An active pool can be recovered with the same identity using
`"$scheduler" pool-start --recover-inactive --expected-pool-id "$pool_id"`.
For a pool already draining, use `pool-drain --recover-inactive` with the
same expected ID. Run `"$scheduler" --help` for the recovery and audit commands.

## Execution modes

The adaptive pool shares CPU and RAM among multiple queues. The fixed shared
queue starts a specified number of workers per host. Host-local shards reduce
shared scheduler traffic for short jobs and use GNU Parallel on the workers.
The deployed `start_workers.sh --help` describes both fixed modes. Hosts in a
mixed-architecture pool must have compatible job executables and dependencies.
