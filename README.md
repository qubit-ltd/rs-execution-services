# Qubit Execution Services

[![Rust CI](https://github.com/qubit-ltd/rs-execution-services/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-execution-services/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-execution-services/coverage-badge.json)](https://qubit-ltd.github.io/rs-execution-services/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-execution-services.svg?color=blue)](https://crates.io/crates/qubit-execution-services)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Qubit Execution Services gives Rust applications one owner for routing synchronous blocking work, CPU-bound work, Tokio blocking work, and asynchronous futures to suitable execution domains. It helps application code keep these workloads separate without assembling and shutting down each service on its own; libraries that need only one execution layer can use the smaller executor crates directly.

## Installation

Add the crate to your application's `Cargo.toml`:

```toml
[dependencies]
qubit-execution-services = "0.10"
tokio = { version = "1.53", features = ["rt", "time"] }
```

## Quick Start

An application can submit a synchronous blocking operation, a CPU calculation, and an async task through one facade, then wait for their results and shut down the enabled domains:

```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates submitting and awaiting work in separate execution domains.

use std::future::Future;
#[cfg(not(test))]
use std::io;

use qubit_execution_services::ExecutionServices;

async fn shutdown_after<F>(services: &ExecutionServices, work: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let result = work.await;
    services.shutdown();
    services.await_termination().await;
    result
}

#[cfg(not(test))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

    runtime.block_on(async {
        let services = ExecutionServices::builder()
            .runtime(runtime.handle().clone())
            .enable_blocking()
            .enable_cpu()
            .enable_io()
            .blocking_pool_size(4)
            .blocking_queue_capacity(1024)
            .cpu_threads(4)
            .cpu_task_capacity(1024)
            .build()?;

        shutdown_after(&services, async {
            let blocking = services.submit_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))?;
            let cpu = services.submit_cpu_callable(|| Ok::<usize, io::Error>((1..=10).sum()))?;
            let io = services.spawn_io(async { Ok::<usize, io::Error>(6 * 7) })?;

            assert_eq!(blocking.await?, 42);
            assert_eq!(cpu.await?, 55);
            assert_eq!(io.await?, 42);
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await
    })?;

    Ok(())
}
```

The handles are awaited so the Tokio worker can continue polling. `TaskHandle::get()` blocks the calling thread and should be used only from synchronous code.

## What It Provides

- Optional blocking, CPU-bound, Tokio blocking, and Tokio async execution domains.
- A builder for managed pools and finite capacities for Tokio blocking and IO work; Tokio runtime and scheduler settings remain application-owned.
- Runnable submissions, result-bearing callable submissions, and tracked task variants.
- Aggregate lifecycle operations, graceful shutdown, abrupt stop, and per-domain stop counts.

The blocking pool has configurable worker and queue limits; the CPU and Tokio domains bound accepted unfinished work. The user guide explains the defaults, capacity errors, cancellation, and shutdown behavior.

The blocking and CPU domains each create a separate pool, while the application configures its Tokio runtime separately. Their defaults are per-domain starting points, not a total process thread or memory budget. The blocking queue defaults to 1024 waiting tasks; CPU and Tokio capacities default to 1024 unfinished tasks. Tune each domain for expected concurrency and apply back pressure before its capacity is reached.

`tokio_blocking` uses the application's shared Tokio blocking pool. Its task capacity bounds accepted, unfinished tasks; it does not reserve threads or configure a dedicated running-concurrency limit. Actual concurrency also depends on the runtime's shared blocking pool and its other users. See the runnable [resource budget example](examples/resource_budget.rs) for one explicit configuration. Its numbers are illustrative, not universal defaults.

Use `submit_blocking_callable_wait`, `submit_cpu_callable_wait`, `submit_tokio_blocking_callable_wait`, or `spawn_io_wait` to wait for capacity. Each is lazy, retains one task across retries, and submits it at most once. Dropping the future before acceptance drops the unaccepted task. The synchronous wait methods return a result-only `TaskHandle`; it has no cancellation method. IO returns a `TokioTaskHandle` whose abort request can race normal completion. See the [design documents](doc/design.md) for admission, capacity, and cancellation contracts. `snapshot()` returns independent per-domain monitoring data, not an atomic cross-domain view.

For workload-based domain selection and a step-by-step resource budget, see [Choosing a Domain and Sizing Resources](doc/user_guide.md#choosing-a-domain-and-sizing-resources).

The facade coordinates submission and lifecycle operations across the enabled domains. Its stop report contains optional per-domain observations and exposes no cross-domain totals.

Use this facade when an application needs one owner to submit work to several execution domains and coordinate their shutdown. A component that needs only one domain or its specific controls can depend directly on that executor crate.

For application shutdown, stop entry points and wait for accepted producers that can submit child tasks to finish before calling `shutdown()`. Those later child submissions are rejected after admission closes. Keep the Tokio runtime active until `await_termination()` resolves; see the [cross-domain application shutdown example](examples/application_shutdown.rs). Per-domain resource limits are not a process-wide thread or memory budget; see the [resource budget guide](doc/user_guide.md#choosing-a-domain-and-sizing-resources).

## Learn More

- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-execution-services)
- [Design documents](doc/design.md) · [设计文档](doc/design.zh_CN.md)
- [Daily report example](examples/daily_report.rs)
- [中文 README](README.zh_CN.md)

## Development Setup

The working tree uses the latest local `rs-executor` and `rs-tokio-executor` checkouts through versioned path dependencies in `Cargo.toml`. `.cargo/config.toml` patches `qubit-executor`, `qubit-rayon-executor`, and `qubit-tokio-executor` to local checkouts, so keep all three executor repositories beside this one to build the crate. The source-integration documentation consumer also uses these local executor siblings. The IoC integration fixture additionally uses local path dependencies: check out `rs-ioc`, `rs-event-bus`, `rs-config`, `rs-fs-registry`, `rs-fs-local`, and `rs-fs` beside this repository, together with the transitive path dependencies referenced by those manifests (`rs-value`, `rs-local-files`, `rs-redact`, and `rs-fs-testkit`). The pinned integration workflow records a fixed dependency set; the current integration workflow follows `rs-ioc/main`, supplies the repository-pinned lockfile for its current consumer, and keeps its other sibling repositories pinned. These fixture checks validate local integration layouts, not registry publication.

The [packaged documentation consumer workflow](.github/workflows/packaged-documentation-consumer.yml) runs a source-integration check and a registry-only check. The source-integration consumer check permits local overrides for `qubit-executor`, `qubit-rayon-executor`, and `qubit-tokio-executor`. The registry-only check builds and runs the archived examples with registry dependencies, allowing only the consumer and extracted target crate as local packages. Both checks are required for release acceptance; missing registry dependencies fail the check and block release acceptance. Neither proves minimum-semver compatibility. Run the strict check with `python3 .infra/tools/check-packaged-documentation-consumer.py --registry-only` before release acceptance. Use `cargo test --locked` and `cargo test --locked --all-features` to check that the committed lockfile resolves without changes.

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./.infra/bin/ci-check.sh

# Check code coverage
./.infra/bin/coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./.infra/bin/align-ci.sh` to format code and
`./.infra/bin/ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-execution-services](https://github.com/qubit-ltd/rs-execution-services)
