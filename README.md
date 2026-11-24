# Parallel workers

A lightweight SSH job scheduler for small, trusted macOS/Linux clusters.
Run independent shell jobs with fixed workers, node-local shards, or an adaptive
pool that shares CPU and memory budgets between queues. Jobs keep running when
admission is paused; dependencies, failure holds, and audited recovery are explicit.

The scheduler uses Bash, Python's standard library, and filesystem records rather
than a database. The tmux dashboard supports macOS and Linux CPU/memory sampling,
queue progress, shared storage, and network/block-device activity.

## Set up another cluster

1. On the storage host (the coordinator), clone this repository with Git and open a Bash
   shell in the checkout. Deployment records the Git commit and needs a clean
   working tree. GitHub’s source ZIP omits this metadata. Configure passwordless
   SSH from the coordinator to every configured target, including itself. The
   writable drive must have the same absolute path on every host. Install Bash,
   Python 3.9+, SSH and Perl/shasum. The dashboard
   also needs tmux. Host-local shards use GNU Parallel.
2. Copy the [example inventory](examples/cluster.json) to `cluster.local.json`
   and edit that one file. Set the shared mount path, coordinator ID, each
   host’s SSH target and its CPU and memory allowances. The scheduler, deployment
   tools and dashboard use settings generated from the inventory. Job commands,
   inputs and outputs belong to individual project queues. Keep SSH keys in
   your SSH configuration.
3. From the coordinator, run `./doctor cluster.local.json`. It checks each SSH
   route, runtime tools, writable storage and a marker shared with the coordinator.
   Then generate the configuration and activate it in this shell:

   ```bash
   env_file=$(python3 cluster_config.py cluster.local.json --render)
   source "$env_file"
   ```

   The generated file lives under the configured shared root. Source that same
   file in any new admin, coordinator or dashboard shell. If administering from
   another machine, `ssh "$PARALLEL_WORKERS_COORDINATOR"` opens the coordinator
   shell. Source the file there too.
4. From the coordinator checkout, deploy to an idle cluster and start its pool:

   ```bash
   ./deploy.sh --scheduler-only
   ./sync_parallel_workers_monitoring.sh
   "$PARALLEL_WORKERS_ROOT/adaptive_scheduler.sh" pool-start
   ```

5. **[Run your first two jobs and verify their results](examples/first-job/README.md).**
   The example includes the worker script and needs only Python. The
   [operations guide](docs/operations.md) covers your own workloads.

### Open the dashboard

On the dashboard host, source the generated environment and run
`./start_monitoring.sh --direct` from its checkout to sample the hosts over SSH.
For the shared snapshot used by the CLI and Agent Bar on Linux, run
`./admin/install_cluster_status_collector.sh` from that configured shell, then
`./start_monitoring.sh`. The installer keeps a local copy of the inventory
settings for the service and `pw-control`. Rerun the installer after selecting
a different inventory. [Monitor and recover](docs/operations.md#monitor-and-recover)

## Supported workloads

The scheduler runs independent shell jobs across trusted macOS and Linux
machines. Each eligible host needs the workload’s dependencies. An unreachable
worker may still be running its job. Confirm its state before rerunning it.
See the [operations guide](docs/operations.md) for placement, monitoring and recovery.

## Tests

GitHub Actions runs inventory, shell compatibility and ETA tests on Linux and
macOS. Linux also runs native monitoring and the first-job lifecycle fixture.
Run the portable suite with `python3 -m unittest discover -s tests` in a quiet
Linux test environment with tmux installed. The fresh-cluster test starts local scheduler processes and
uses a fixture in place of SSH.

## License

Parallel Workers is licensed under the GNU General Public License, version 2
or any later version (SPDX `GPL-2.0-or-later`). See [LICENSE](LICENSE).
