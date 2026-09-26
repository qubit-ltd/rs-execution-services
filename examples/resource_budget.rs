// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;
use std::num::NonZeroUsize;

use qubit_execution_services::ExecutionServices;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(4)
        .build()?;

    runtime.block_on(async {
        let services = ExecutionServices::builder()
            .runtime(runtime.handle().clone())
            .enable_blocking()
            .blocking_core_pool_size(2)
            .blocking_maximum_pool_size(4)
            .blocking_queue_capacity(32)
            .enable_cpu()
            .cpu_threads(2)
            .cpu_task_capacity(32)
            .enable_tokio_blocking()
            .tokio_blocking_task_capacity(NonZeroUsize::new(8).expect("nonzero capacity"))
            .enable_io()
            .io_task_capacity(NonZeroUsize::new(64).expect("nonzero capacity"))
            .build()?;

        let blocking = services.submit_blocking_callable(|| Ok::<u8, io::Error>(40))?;
        let cpu = services.submit_cpu_callable(|| Ok::<u8, io::Error>(41))?;
        let tokio_blocking = services.submit_tokio_blocking_callable(|| Ok::<u8, io::Error>(42))?;
        let io = services.spawn_io(async { Ok::<u8, io::Error>(43) })?;

        assert_eq!(blocking.await?, 40);
        assert_eq!(cpu.await?, 41);
        assert_eq!(tokio_blocking.await?, 42);
        assert_eq!(io.await?, 43);

        services.shutdown();
        services.await_termination().await;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
