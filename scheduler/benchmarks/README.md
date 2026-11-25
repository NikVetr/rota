# Benchmark suite (ROTA)

Use the CPU, I/O and Stan jobs to put the queue runner under load and compare
how long the same work takes on different hosts.

## Files

- `bench_job.R`: runs one benchmark job (CPU / IO / Stan SV)
- `make_queue_stan_sv_box.sh`: generates a parameterized Stan benchmark queue
- `monitor_cluster_cpu.sh`: prints compact per-host CPU summaries while a run is active
- `summarize_results.R`: summarizes per-job results written by `bench_job.R`
- `Stan/models/stochastic_volatility.stan`: simulation-based Stan model used by `--task stan_sv`

## Where outputs go

- Per-job metrics: `results/job_<id>_<hostname>.rds`
- CmdStan CSVs: `Stan/output/sv_job_<id>_<hostname>-*.csv`

## Typical usage

Source the rendered environment on the coordinator. The CPU example below
needs only base R. If you choose the Stan workload, install cmdstanr and
CmdStan on every host that can take a job. Give each run a fresh ID.

```bash
project="$ROTA_ROOT/stan_projects/test"
bench="$project/benchmarks"
run_id="cpu-$(date +%s)"
mkdir -p "$project/queues"
printf 'job1 env ROTA_ROOT=%q Rscript %q --run_id %q --job_id 1 --task cpu --cpu_n 2500 --cpu_reps 10\n' \
  "$ROTA_ROOT" "$bench/bench_job.R" "$run_id" > "$project/queues/$run_id.txt"
"$ROTA_ROOT/adaptive_scheduler.sh" submit --model_dir "$project" \
  --queue "$run_id.txt" --project benchmark --run_name "$run_id" \
  --cpu_units_per_job 1 --memory_mib_per_job 1024
```

After the job completes, summarize it with
`Rscript "$bench/summarize_results.R" --run_id "$run_id"`.
