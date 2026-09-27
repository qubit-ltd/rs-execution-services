// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates draining an accepted producer before shutting down services.
//!
//! Stop application entry points first. An accepted task that can submit child
//! work must finish before `shutdown()` closes facade admission.

use std::io;
use std::sync::Arc;

use qubit_execution_services::ExecutionServices;
use tokio::sync::oneshot;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async {
        let services = Arc::new(
            ExecutionServices::builder()
                .enable_io()
                .enable_cpu()
                .runtime(runtime.handle().clone())
                .cpu_threads(1)
                .build()?,
        );
        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let child_services = Arc::clone(&services);
        let producer = services.spawn_io(async move {
            started_tx
                .send(())
                .map_err(|_| io::Error::other("startup receiver closed"))?;
            release_rx.await.map_err(io::Error::other)?;
            let child = child_services
                .submit_cpu_callable(|| Ok::<u8, io::Error>(42))
                .map_err(io::Error::other)?;
            child.await.map_err(io::Error::other)
        })?;

        // The application stops new producers, then lets this accepted producer
        // submit and await its CPU child before closing the facade.
        started_rx.await?;
        release_tx.send(()).map_err(|_| io::Error::other("producer stopped"))?;
        assert_eq!(producer.await?, 42);

        services.shutdown();
        services.await_termination().await;
        assert!(services.is_terminated());
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
