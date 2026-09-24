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
qubit-execution-services = "0.9"
tokio = { version = "1.53", features = ["rt", "time"] }
```

## Quick Start

An application can submit a synchronous blocking operation, a CPU calculation, and an async task through one facade, then wait for their results and shut down all domains:

```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;

use qubit_execution_services::ExecutionServices;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

    runtime.block_on(async {
        let services = ExecutionServices::builder(runtime.handle().clone())
            .blocking_pool_size(4)
            .blocking_queue_capacity(1024)
            .cpu_threads(4)
            .cpu_task_capacity(1024)
            .build()?;

        let blocking = services.submit_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))?;
        let cpu = services.submit_cpu_callable(|| Ok::<usize, io::Error>((1..=10).sum()))?;
        let io = services.spawn_io(async { Ok::<usize, io::Error>(6 * 7) })?;

        assert_eq!(blocking.get()?, 42);
        assert_eq!(cpu.get()?, 55);
        assert_eq!(io.await?, 42);

        services.shutdown();
        services.await_termination().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
```

## What It Provides

- Separate blocking, CPU-bound, Tokio blocking, and Tokio async execution domains.
- A builder for managed pools and finite capacities for Tokio blocking and IO work; Tokio runtime and scheduler settings remain application-owned.
- Runnable submissions, result-bearing callable submissions, and tracked task variants.
- Aggregate lifecycle operations, graceful shutdown, abrupt stop, and per-domain stop counts.

The blocking pool defaults both its core and maximum thread counts to the detected CPU parallelism, and its queue defaults to 1024 waiting tasks. Long blocking calls can occupy every worker; waiting tasks do not trigger growth while the queue still has room. Set `blocking_core_pool_size`, `blocking_maximum_pool_size`, and a finite `blocking_queue_capacity` based on expected simultaneous blocking work and acceptable backlog. A full bounded queue can trigger additional workers up to the configured maximum; an unbounded queue does not trigger growth beyond the core size. Queue capacity does not cap task memory. The CPU, Tokio blocking, and Tokio IO domains each default to 1024 unfinished accepted tasks and report `SubmissionError::Saturated` at capacity. Configure the Tokio limits with `tokio_blocking_task_capacity` and `io_task_capacity`. Once shutdown or stop closes facade admission, `SubmissionError::Shutdown` takes precedence over saturation. See the user guide for cancellation and shutdown details.

The facade serializes submissions against shutdown and stop. Once either operation closes admission, every domain rejects new facade submissions. Stop counts are per-domain observations taken sequentially. In particular, the Tokio IO `running` count includes accepted futures that have not completed, whether or not they are currently being polled; `total_running()` is not a global concurrency snapshot.

Use this facade when an application needs one owner to submit to and close several execution domains together. A component that needs only one domain can depend directly on its executor crate. The workspace's `rs-task` and `rs-event-bus` currently use lower-level crates directly; no production downstream consumer of this facade has been confirmed. The application consumer fixture tests the published API boundary and does not establish production adoption.

## Learn More

- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-execution-services)
- [中文 README](README.zh_CN.md)

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-execution-services](https://github.com/qubit-ltd/rs-execution-services)
