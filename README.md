# Qubit Execution Services

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-execution-services.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-execution-services)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-execution-services/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-execution-services?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-execution-services.svg?color=blue)](https://crates.io/crates/qubit-execution-services)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Aggregate execution-service facade for Rust applications.

## Overview

Qubit Execution Services wires together the Qubit executor implementations that
an application commonly needs: blocking OS-thread work, CPU-bound Rayon work,
Tokio blocking work, and async IO futures.

This crate is an application-level convenience facade. It is not the foundational
abstraction layer. Libraries should usually depend on smaller crates such as
`qubit-executor`, `qubit-thread-pool`, `qubit-rayon-executor`, or
`qubit-tokio-executor` directly.

## Features

- `ExecutionServices` facade with separate blocking, CPU, Tokio blocking, and async IO domains.
- `ExecutionServicesBuilder` for configuring the blocking thread-pool domain and CPU Rayon domain.
- `submit_blocking`, `submit_cpu`, and `submit_tokio_blocking` for fire-and-forget runnable work.
- `submit_blocking_callable`, `submit_cpu_callable`, and `submit_tokio_blocking_callable` for result-bearing callable work.
- `submit_tracked_*` variants for work that needs status or cancellation handles.
- `spawn_io` for async futures routed through Tokio's async scheduler.
- `ExecutionServicesStopReport` for aggregating queued, running, and cancelled counts across all domains.
- Type aliases and re-exports for the underlying executor services and task handles.

## Execution Domains

The blocking domain uses `ThreadPool` from `qubit-thread-pool`. It is intended
for synchronous work that may block OS threads, such as filesystem operations or
legacy blocking APIs.

The CPU domain uses `RayonExecutorService` from `qubit-rayon-executor`. It is
intended for CPU-heavy work where Rayon scheduling is the right execution model.

The Tokio blocking domain uses `TokioExecutorService` from
`qubit-tokio-executor`. It is intended for blocking functions submitted from a
Tokio application and executed through `spawn_blocking`.

The IO domain uses `TokioIoExecutorService` from `qubit-tokio-executor`. It is
intended for async futures and non-blocking IO work executed through
`tokio::spawn`.

## Builder Configuration

`ExecutionServicesBuilder` delegates blocking-domain settings to
`ThreadPoolBuilder` and CPU-domain settings to `RayonExecutorServiceBuilder`.
Tokio-backed domains currently use their default constructors because Tokio owns
the runtime and scheduler configuration.

The builder exposes common blocking-pool controls such as pool size, core size,
maximum size, queue capacity, thread-name prefix, stack size, keep-alive,
core-thread timeout, and prestart behavior. It also exposes CPU-domain controls
for Rayon worker count, thread-name prefix, and stack size.

## Shutdown Behavior

`shutdown` requests orderly shutdown for every domain. New tasks are rejected,
and accepted work is allowed to complete according to each underlying service.

`stop` requests abrupt stop for every domain and returns an
`ExecutionServicesStopReport` containing one `StopReport` per domain.
The report also provides `total_queued`, `total_running`, and `total_cancelled`
helpers for aggregate accounting.

`await_termination` resolves after every underlying service has terminated.

## Quick Start

```rust
use std::io;

use qubit_execution_services::ExecutionServices;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let services = ExecutionServices::builder()
    .blocking_pool_size(4)
    .blocking_queue_capacity(1024)
    .cpu_threads(4)
    .build()?;

let blocking = services.submit_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))?;
let cpu = services.submit_cpu_callable(|| Ok::<usize, io::Error>((1..=10).sum()))?;
let io = services.spawn_io(async { Ok::<usize, io::Error>(6 * 7) })?;

assert_eq!(blocking.get()?, 42);
assert_eq!(cpu.get()?, 55);
assert_eq!(io.await?, 42);

services.shutdown();
services.await_termination().await;
# Ok(())
# }
```

## Choosing a Dependency

Use `qubit-execution-services` at application boundaries when one owned facade
should route different task types to different execution domains.

Use a smaller crate directly when you only need one layer or one runtime:

- `qubit-executor` for traits, task handles, and shared lifecycle types.
- `qubit-thread-pool` for runtime-independent OS-thread pools.
- `qubit-rayon-executor` for CPU-bound Rayon execution.
- `qubit-tokio-executor` for Tokio blocking and async IO execution.

## Testing

A minimal local run:

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

To mirror what continuous integration enforces, run the repository scripts from
the project root: `./align-ci.sh` brings local tooling and configuration in line
with CI, then `./ci-check.sh` runs the same checks the pipeline uses. For test
coverage, use `./coverage.sh` to generate or open reports.

## Contributing

Issues and pull requests are welcome.

- Open an issue for bug reports, design questions, or larger feature proposals when it helps align on direction.
- Keep pull requests scoped to one behavior change, fix, or documentation update when practical.
- Before submitting, run `./align-ci.sh` and then `./ci-check.sh` so your branch matches CI rules and passes the same checks as the pipeline.
- Add or update tests when you change runtime behavior, and update this README or public rustdoc when user-visible API behavior changes.
- If you change routing or shutdown behavior, include tests that verify every affected execution domain.

By contributing, you agree to license your contributions under the [Apache License, Version 2.0](LICENSE), the same license as this project.

## License

Copyright (c) 2026. Haixing Hu.

This project is licensed under the [Apache License, Version 2.0](LICENSE). See the `LICENSE` file in the repository for the full text.

## Author

**Haixing Hu** — Qubit Co. Ltd.

| | |
| --- | --- |
| **Repository** | [github.com/qubit-ltd/rs-execution-services](https://github.com/qubit-ltd/rs-execution-services) |
| **Documentation** | [docs.rs/qubit-execution-services](https://docs.rs/qubit-execution-services) |
| **Crate** | [crates.io/crates/qubit-execution-services](https://crates.io/crates/qubit-execution-services) |
