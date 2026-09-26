// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public API consumer regression; this fixture is not a production downstream.

use std::io;

use qubit_execution_services::ExecutionServices;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let services = ExecutionServices::builder()
            .enable_all(runtime.handle().clone())
            .blocking_pool_size(2)
            .blocking_queue_capacity(2)
            .cpu_threads(2)
            .build()?;
        let blocking = services.submit_blocking_callable(|| Ok::<u8, io::Error>(40))?;
        let cpu = services.submit_cpu_callable(|| Ok::<u8, io::Error>(41))?;
        let tokio_blocking =
            services.submit_tokio_blocking_callable(|| Ok::<u8, io::Error>(42))?;
        let io = services.spawn_io(async { Ok::<u8, io::Error>(43) })?;

        assert_eq!(blocking.await?, 40);
        assert_eq!(cpu.await?, 41);
        assert_eq!(tokio_blocking.await?, 42);
        assert_eq!(io.await?, 43);

        services.shutdown();
        assert!(services.submit_blocking(|| Ok::<(), io::Error>(())).is_err());
        assert!(services.submit_cpu(|| Ok::<(), io::Error>(())).is_err());
        assert!(services
            .submit_tokio_blocking(|| Ok::<(), io::Error>(() ))
            .is_err());
        assert!(services
            .spawn_io(async { Ok::<(), io::Error>(()) })
            .is_err());
        services.await_termination().await;
        assert!(services.is_terminated());
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
