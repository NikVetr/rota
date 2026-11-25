# Benchmark suite (ROTA)

This directory contains a mixed-workload benchmark intended to stress-test the queue runner and estimate per-node performance.

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

Source the rendered environment on the coordinator. The following CPU example
uses base R. The Stan workloads also require cmdstanr and a working CmdStan
installation on every eligible host. Use a new run ID for each run.

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
