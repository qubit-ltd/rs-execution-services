// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Lifecycle adapter keeping execution admission open until consumers finish.
use std::sync::Arc;

use qubit_execution_services::ExecutionServices;
use qubit_ioc::FactoryError;
use qubit_ioc::Managed;
use tokio::runtime::Handle;

/// Creates IO services on `runtime`; construction errors retain their source.
/// The caller must keep the runtime alive through the managed termination wait.
pub fn managed_execution_services(runtime: Handle) -> Result<Managed<ExecutionServices>, FactoryError> {
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(runtime)
        .build()
        .map_err(FactoryError::new)?;
    Ok(Managed::asynchronous_with_graceful(Arc::new(services), |services| {
        let _ = services.stop();
        Ok(())
    }, |services| {
        services.shutdown();
        Ok(())
    }, |services| {
        Box::pin(async move {
            services.await_termination().await;
            Ok(())
        })
    }))
}
