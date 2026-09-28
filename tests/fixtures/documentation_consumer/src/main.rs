// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies the documented minimal external dependency and runtime setup.

use qubit_execution_services::ExecutionServices;
use tokio::runtime::Handle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let services = ExecutionServices::builder().enable_io().runtime(Handle::current()).build()?;
    let task = services.spawn_io(async { Ok::<u8, std::io::Error>(42) })?;
    assert_eq!(task.await?, 42);
    services.shutdown();
    services.await_termination().await;
    Ok(())
}
