#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  idx <- which(args == flag)
  if (length(idx) == 0) return(default)
  if (idx[length(idx)] == length(args)) return(default)
  args[idx[length(idx)] + 1]
}

shared_root <- Sys.getenv("PARALLEL_WORKERS_ROOT")
if (!nzchar(shared_root)) stop("Source the inventory environment or export PARALLEL_WORKERS_ROOT for the job")
project_dir <- file.path(shared_root, "stan_projects/test/benchmarks")
run_id <- get_arg("--run_id", NA)
if (is.na(run_id) || !nzchar(run_id)) stop("Missing --run_id")
results_dir <- file.path(project_dir, "results", run_id)

files <- list.files(results_dir, pattern = "\\.rds$", full.names = TRUE)
if (length(files) == 0) {
  cat("No results found in", results_dir, "\n")
  quit(status = 0)
}

read_one <- function(f) {
  x <- readRDS(f)
  cpu_units <- NA_real_
  io_units <- NA_real_
  if (!is.null(x$cpu_box$cpu_units)) cpu_units <- as.numeric(x$cpu_box$cpu_units)
  if (!is.null(x$io_box$io_units)) io_units <- as.numeric(x$io_box$io_units)
  if (!is.null(x$mixed_box$cpu_box$cpu_units)) cpu_units <- as.numeric(x$mixed_box$cpu_box$cpu_units)
  data.frame(
    job_id = x$job_id,
    hostname = x$hostname,
    task = x$task,
    target_minutes = as.numeric(x$target_minutes %||% NA_real_),
    elapsed_sec = as.numeric(x$elapsed_sec %||% NA_real_),
    cpu_units = cpu_units,
    io_units = io_units,
    error = if (!is.null(x$error)) as.character(x$error) else NA_character_,
    scripts_git_sha = as.character(x$scripts_git_sha %||% NA_character_),
    stringsAsFactors = FALSE
  )
}

`%||%` <- function(a, b) if (!is.null(a)) a else b

df <- do.call(rbind, lapply(files, read_one))

cat("Total results:", nrow(df), "\n")
cat("Errors:", sum(!is.na(df$error)), "\n")

by_host <- aggregate(elapsed_sec ~ hostname, df, function(v) c(n = length(v), med = median(v, na.rm = TRUE), mean = mean(v, na.rm = TRUE)))
cat("\nPer-host elapsed_sec summary:\n")
print(by_host)

by_host_task <- aggregate(elapsed_sec ~ hostname + task, df, function(v) c(n = length(v), med = median(v, na.rm = TRUE), mean = mean(v, na.rm = TRUE)))
cat("\nPer-host per-task elapsed_sec summary:\n")
print(by_host_task)

# Units-based summaries (useful for timeboxed tasks)
if (any(!is.na(df$cpu_units))) {
  by_cpu <- aggregate(cpu_units ~ hostname + task, df, function(v) c(n = sum(!is.na(v)), med = median(v, na.rm = TRUE), mean = mean(v, na.rm = TRUE)))
  cat("\nPer-host per-task cpu_units summary:\n")
  print(by_cpu)
}
if (any(!is.na(df$io_units))) {
  by_io <- aggregate(io_units ~ hostname + task, df, function(v) c(n = sum(!is.na(v)), med = median(v, na.rm = TRUE), mean = mean(v, na.rm = TRUE)))
  cat("\nPer-host per-task io_units summary:\n")
  print(by_io)
}

# Simple throughput estimate (jobs/hour) using mean elapsed per host
thr <- transform(by_host, jobs_per_hour = 3600 / elapsed_sec[, "mean"])
cat("\nApprox throughput (jobs/hour) by host (mean-based):\n")
print(thr[, c("hostname", "jobs_per_hour")])
