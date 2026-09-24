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
qubit-execution-services = "0.8"
```

## Quick Start

An application can submit a synchronous blocking operation, a CPU calculation, and an async task through one facade, then wait for their results and shut down all domains:

```rust
use std::io;

use qubit_execution_services::ExecutionServices;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let services = ExecutionServices::builder(tokio::runtime::Handle::current())
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
services.await_termination().await;
# Ok(())
# }
```

## What It Provides

- Separate blocking, CPU-bound, Tokio blocking, and Tokio async execution domains.
- A builder for the blocking thread pool and CPU Rayon pool; Tokio runtime and scheduler settings remain application-owned.
- Runnable submissions, result-bearing callable submissions, and tracked task variants.
- Aggregate lifecycle operations, graceful shutdown, abrupt stop, and per-domain stop counts.

The CPU domain can bound accepted unfinished work and returns `SubmissionError::Saturated` when that capacity is full. A bounded blocking queue can allow the blocking pool to grow beyond its core size; an unbounded queue continues queueing after the core size is reached. See the user guide for configuration and shutdown details.

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
