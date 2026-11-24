#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

get_arg <- function(flag, default = NULL) {
  idx <- which(args == flag)
  if (length(idx) == 0) return(default)
  if (idx[length(idx)] == length(args)) return(default)
  args[idx[length(idx)] + 1]
}

job_id <- as.integer(get_arg("--job_id", NA))
if (is.na(job_id)) stop("Missing --job_id")

seed <- as.integer(get_arg("--seed", 1L))
task <- get_arg("--task", "cpu")
run_id <- get_arg("--run_id", NA)
if (is.na(run_id) || !nzchar(run_id)) stop("Missing --run_id")
job_tag <- get_arg("--job_tag", NA_character_)

cpu_n <- as.integer(get_arg("--cpu_n", 2500L))
cpu_p <- as.integer(get_arg("--cpu_p", 800L))
cpu_reps <- as.integer(get_arg("--cpu_reps", 3L))

io_mb <- as.integer(get_arg("--io_mb", 64L))
io_reps <- as.integer(get_arg("--io_reps", 1L))
io_buf_mb <- as.integer(get_arg("--io_buf_mb", 8L))

stan_T <- as.integer(get_arg("--stan_T", 1500L))
stan_warmup <- as.integer(get_arg("--stan_warmup", 500L))
stan_sampling <- as.integer(get_arg("--stan_sampling", 500L))

target_minutes <- as.numeric(get_arg("--target_minutes", NA))
target_sec <- if (is.na(target_minutes)) NA_real_ else target_minutes * 60

# Prevent oversubscription (esp. BLAS)
Sys.setenv(
  OMP_NUM_THREADS = "1",
  OPENBLAS_NUM_THREADS = "1",
  MKL_NUM_THREADS = "1",
  VECLIB_MAXIMUM_THREADS = "1"
)

hostname <- Sys.info()[["nodename"]]
shared_root <- Sys.getenv("PARALLEL_WORKERS_ROOT")
if (!nzchar(shared_root)) stop("Source the inventory environment or export PARALLEL_WORKERS_ROOT for the job")
project_dir <- file.path(shared_root, "stan_projects/test/benchmarks")
results_dir <- file.path(project_dir, "results", run_id)
tmp_dir <- file.path(project_dir, "tmp")

if (!dir.exists(results_dir)) dir.create(results_dir, recursive = TRUE)
if (!dir.exists(tmp_dir)) dir.create(tmp_dir, recursive = TRUE)

result_path <- file.path(results_dir, sprintf("job_%04d_%s.rds", job_id, hostname))
status_dir <- file.path(results_dir, "status")
if (!dir.exists(status_dir)) dir.create(status_dir, recursive = TRUE)
status_start_path <- file.path(status_dir, sprintf("job_%04d_%s.start.txt", job_id, hostname))
status_end_path <- file.path(status_dir, sprintf("job_%04d_%s.end.txt", job_id, hostname))

get_scripts_git_sha <- function() {
  version_file <- file.path(shared_root, "PARALLEL_WORKERS_VERSION")
  if (!file.exists(version_file)) return(NA_character_)
  out <- readLines(version_file, n = 1L, warn = FALSE)
  if (length(out) == 0L || !grepl("^[0-9a-f]{40,64}$", out[[1]])) return(NA_character_)
  out[[1]]
}

metrics <- list(
  job_id = job_id,
  hostname = hostname,
  task = task,
  run_id = run_id,
  seed = seed,
  job_tag = job_tag,
  start_time = as.character(Sys.time()),
  scripts_git_sha = get_scripts_git_sha()
)

cat(sprintf("[%s] job_id=%d host=%s task=%s\n", format(Sys.time(), "%F %T"), job_id, hostname, task))
writeLines(
  c(
    sprintf("state=running"),
    sprintf("job_id=%d", job_id),
    sprintf("hostname=%s", hostname),
    sprintf("task=%s", task),
    sprintf("run_id=%s", run_id),
    sprintf("start_time=%s", metrics$start_time),
    sprintf("job_tag=%s", ifelse(is.na(job_tag), "", job_tag))
  ),
  con = status_start_path
)

set.seed(seed + job_id)
pt0 <- proc.time()
t0 <- Sys.time()

run_cpu <- function() {
  # Fixed-work CPU: repeated crossprod of a random matrix.
  # Uses 1 thread via VECLIB_MAXIMUM_THREADS.
  cpu_times <- numeric(cpu_reps)
  set.seed(seed + job_id + 11L)
  x <- matrix(rnorm(cpu_n * cpu_p), nrow = cpu_n, ncol = cpu_p)
  for (i in seq_len(cpu_reps)) {
    cpu_times[i] <- system.time({
      z <- crossprod(x)
      s <- sum(z)
      if (!is.finite(s)) stop("non-finite")
    })[["elapsed"]]
  }
  list(cpu_reps = cpu_reps, cpu_n = cpu_n, cpu_p = cpu_p, cpu_elapsed = cpu_times, cpu_elapsed_total = sum(cpu_times))
}

run_cpu_box <- function(deadline) {
  set.seed(seed + job_id + 11L)
  x <- matrix(rnorm(cpu_n * cpu_p), nrow = cpu_n, ncol = cpu_p)
  units <- 0L
  unit_elapsed_total <- 0
  while (Sys.time() < deadline) {
    et <- system.time({
      z <- crossprod(x)
      s <- sum(z)
      if (!is.finite(s)) stop("non-finite")
    })[["elapsed"]]
    units <- units + 1L
    unit_elapsed_total <- unit_elapsed_total + et
  }
  list(
    cpu_n = cpu_n,
    cpu_p = cpu_p,
    cpu_units = units,
    cpu_unit_elapsed_mean = if (units > 0) unit_elapsed_total / units else NA_real_,
    cpu_units_per_sec = units / target_sec
  )
}

run_io <- function() {
  # Fixed-work IO: write and read back a file (then delete).
  sizes <- integer(io_reps)
  write_secs <- numeric(io_reps)
  read_secs <- numeric(io_reps)
  # Pre-generate a small deterministic buffer; don't time RNG cost.
  buf_bytes <- io_buf_mb * 1024L * 1024L
  set.seed(123)
  buf <- as.raw(sample.int(256L, size = buf_bytes, replace = TRUE) - 1L)
  for (i in seq_len(io_reps)) {
    path <- file.path(tmp_dir, sprintf("job_%04d_%s_rep_%02d.bin", job_id, hostname, i))
    bytes <- as.integer(io_mb) * 1024L * 1024L
    reps <- max(1L, as.integer(ceiling(bytes / length(buf))))

    write_secs[i] <- system.time({
      con <- file(path, open = "wb")
      for (r in seq_len(reps)) writeBin(buf, con = con)
      close(con)
    })[["elapsed"]]
    sizes[i] <- as.integer(file.info(path)$size)

    read_secs[i] <- system.time({
      y <- readBin(path, what = "raw", n = sizes[i])
      if (length(y) != sizes[i]) stop("short read")
    })[["elapsed"]]

    unlink(path)
  }

  list(io_mb = io_mb, io_reps = io_reps, io_bytes = sizes, io_write_elapsed = write_secs, io_read_elapsed = read_secs,
       io_write_mb_s = ifelse(write_secs > 0, (io_mb / write_secs), NA_real_),
       io_read_mb_s = ifelse(read_secs > 0, (io_mb / read_secs), NA_real_))
}

run_io_box <- function(deadline) {
  buf_bytes <- io_buf_mb * 1024L * 1024L
  set.seed(123)
  buf <- as.raw(sample.int(256L, size = buf_bytes, replace = TRUE) - 1L)
  bytes <- as.integer(io_mb) * 1024L * 1024L
  reps <- max(1L, as.integer(ceiling(bytes / length(buf))))

  units <- 0L
  write_total <- 0
  read_total <- 0
  while (Sys.time() < deadline) {
    path <- file.path(tmp_dir, sprintf("job_%04d_%s_unit_%06d.bin", job_id, hostname, units + 1L))
    wt <- system.time({
      con <- file(path, open = "wb")
      for (r in seq_len(reps)) writeBin(buf, con = con)
      close(con)
    })[["elapsed"]]
    size <- as.integer(file.info(path)$size)
    rt <- system.time({
      y <- readBin(path, what = "raw", n = size)
      if (length(y) != size) stop("short read")
    })[["elapsed"]]
    unlink(path)
    units <- units + 1L
    write_total <- write_total + wt
    read_total <- read_total + rt
  }
  list(
    io_mb = io_mb, io_buf_mb = io_buf_mb,
    io_units = units,
    io_write_elapsed_total = write_total,
    io_read_elapsed_total = read_total,
    io_write_mb_s = ifelse(write_total > 0, (io_mb * units) / write_total, NA_real_),
    io_read_mb_s = ifelse(read_total > 0, (io_mb * units) / read_total, NA_real_)
  )
}

run_stan_sv <- function() {
  suppressPackageStartupMessages(library(cmdstanr))

  model_path <- file.path(project_dir, "Stan", "models", "stochastic_volatility.stan")
  if (!file.exists(model_path)) stop("Missing Stan model: ", model_path)

  # Simulate SV data
  T <- stan_T
  mu <- -1
  phi <- 0.98
  sigma <- 0.15
  h <- numeric(T)
  h[1] <- rnorm(1, mu, sigma / sqrt(1 - phi^2))
  for (t in 2:T) h[t] <- rnorm(1, mu + phi * (h[t - 1] - mu), sigma)
  y <- rnorm(T, 0, exp(0.5 * h))

  data_list <- list(T = T, y = y)

  # Compile per-host locally
  local_build_root <- path.expand("~/repos/cmdstan_models/parallel_workers_bench")
  build_dir <- file.path(local_build_root, paste0("sv_", hostname))
  lock_dir <- file.path(build_dir, ".compile_lock")

  if (!dir.exists(build_dir)) dir.create(build_dir, recursive = TRUE)

  acquire_lock <- function() {
    repeat {
      if (dir.create(lock_dir, showWarnings = FALSE)) break
      Sys.sleep(0.2)
    }
  }
  release_lock <- function() {
    if (dir.exists(lock_dir)) unlink(lock_dir, recursive = TRUE, force = TRUE)
  }

  acquire_lock()
  on.exit(release_lock(), add = TRUE)
  mod <- cmdstan_model(stan_file = model_path, dir = build_dir, quiet = TRUE)
  release_lock()

  out_dir <- file.path(project_dir, "Stan", "output")
  if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

  fit <- mod$sample(
    data = data_list,
    chains = 1,
    parallel_chains = 1,
    iter_warmup = stan_warmup,
    iter_sampling = stan_sampling,
    seed = seed + job_id,
    refresh = 0,
    save_warmup = FALSE,
    output_dir = out_dir,
    output_basename = sprintf("sv_job_%04d_%s", job_id, hostname)
  )

  # Minimal summary
  summ <- fit$summary(c("mu", "sigma", "phi"))
  t <- fit$time()

  list(
    stan_T = stan_T,
    stan_warmup = stan_warmup,
    stan_sampling = stan_sampling,
    cmdstan_version = as.character(cmdstan_version()),
    cmdstan_path = cmdstan_path(),
    stan_time = t,
    stan_summary = summ
  )
}

fill_to_deadline_with_cpu <- function(deadline) {
  if (is.na(target_sec)) return(NULL)
  if (Sys.time() >= deadline) return(NULL)
  run_cpu_box(deadline)
}

run_mixed_box <- function(deadline) {
  out <- list()
  out$stan_sv <- run_stan_sv()
  out$cpu_box <- fill_to_deadline_with_cpu(deadline)
  out
}

tryCatch({
  deadline <- if (is.na(target_sec)) NA else (Sys.time() + target_sec)

  if (task == "cpu") {
    metrics$cpu <- run_cpu()
  } else if (task == "io") {
    metrics$io <- run_io()
  } else if (task == "stan_sv") {
    metrics$stan_sv <- run_stan_sv()
  } else if (task == "mixed") {
    metrics$cpu <- run_cpu()
    metrics$io <- run_io()
    metrics$stan_sv <- run_stan_sv()
  } else if (task == "cpu_box") {
    if (is.na(target_sec)) stop("cpu_box requires --target_minutes")
    metrics$cpu_box <- run_cpu_box(deadline)
  } else if (task == "io_box") {
    if (is.na(target_sec)) stop("io_box requires --target_minutes")
    metrics$io_box <- run_io_box(deadline)
  } else if (task == "stan_sv_box") {
    if (is.na(target_sec)) stop("stan_sv_box requires --target_minutes")
    metrics$stan_sv <- run_stan_sv()
    metrics$cpu_box <- fill_to_deadline_with_cpu(deadline)
  } else if (task == "mixed_box") {
    if (is.na(target_sec)) stop("mixed_box requires --target_minutes")
    metrics$mixed_box <- run_mixed_box(deadline)
  } else {
    stop("Unknown --task: ", task)
  }

  metrics$target_minutes <- target_minutes
  metrics$end_time <- as.character(Sys.time())
  metrics$elapsed_sec <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  pt1 <- proc.time() - pt0
  metrics$cpu_user_sec <- unname(pt1[["user.self"]])
  metrics$cpu_sys_sec <- unname(pt1[["sys.self"]])

  saveRDS(metrics, file = result_path)
  writeLines(
    c(
      sprintf("state=done"),
      sprintf("job_id=%d", job_id),
      sprintf("hostname=%s", hostname),
      sprintf("task=%s", task),
      sprintf("run_id=%s", run_id),
      sprintf("start_time=%s", metrics$start_time),
      sprintf("end_time=%s", metrics$end_time),
      sprintf("elapsed_sec=%.3f", metrics$elapsed_sec)
    ),
    con = status_end_path
  )
  cat(sprintf("[%s] DONE job_id=%d host=%s elapsed_sec=%.2f wrote=%s\n",
              format(Sys.time(), "%F %T"), job_id, hostname, metrics$elapsed_sec, result_path))
}, error = function(e) {
  metrics$error <- conditionMessage(e)
  metrics$end_time <- as.character(Sys.time())
  metrics$elapsed_sec <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  saveRDS(metrics, file = result_path)
  writeLines(
    c(
      sprintf("state=error"),
      sprintf("job_id=%d", job_id),
      sprintf("hostname=%s", hostname),
      sprintf("task=%s", task),
      sprintf("run_id=%s", run_id),
      sprintf("start_time=%s", metrics$start_time),
      sprintf("end_time=%s", metrics$end_time),
      sprintf("elapsed_sec=%.3f", metrics$elapsed_sec),
      sprintf("error=%s", metrics$error)
    ),
    con = status_end_path
  )
  message("ERROR: ", metrics$error)
  quit(status = 1)
})
