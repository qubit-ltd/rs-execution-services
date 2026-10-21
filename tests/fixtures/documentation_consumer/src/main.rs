// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies the documented minimal external dependency and runtime setup.

use std::error::Error;
use std::io::Error as IoError;

use qubit_execution_services::ExecutionServices;
use tokio::runtime::Builder;

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async {
        let services = ExecutionServices::builder()
            .enable_io()
            .runtime(runtime.handle().clone())
            .build()?;
        let task = services.spawn_io(async { Ok::<u8, IoError>(42) })?;
        assert_eq!(task.await?, 42);
        services.shutdown();
        services.await_termination().await;
        Ok::<(), Box<dyn Error>>(())
    })?;
    Ok(())
}
