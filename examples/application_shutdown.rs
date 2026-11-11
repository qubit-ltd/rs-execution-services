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

use std::future::Future;
use std::io;
use std::sync::Arc;

use qubit_execution_services::ExecutionServices;
use tokio::sync::oneshot;

async fn shutdown_after<F>(services: &ExecutionServices, work: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let result = work.await;
    services.shutdown();
    services.await_termination().await;
    result
}

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
        shutdown_after(&services, async {
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

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await?;
        assert!(services.is_terminated());
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::task::Poll;

    use qubit_execution_services::ExecutionServices;

    use super::shutdown_after;

    #[tokio::test]
    async fn test_error_waits_for_accepted_work_and_preserves_error() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let services = ExecutionServices::builder()
                .enable_io()
                .runtime(tokio::runtime::Handle::current())
                .build()
                .expect("services");
            let (release, gate) = tokio::sync::oneshot::channel::<()>();
            let accepted = services
                .spawn_io(async move {
                    let _ = gate.await;
                    Ok::<(), std::io::Error>(())
                })
                .expect("accepted work");
            let cleanup = shutdown_after(&services, async {
                Err::<(), Box<dyn std::error::Error>>(Box::new(std::io::Error::other("business failed")))
            });
            tokio::pin!(cleanup);
            let observed = std::future::poll_fn(|cx| Poll::Ready(cleanup.as_mut().poll(cx))).await;
            assert!(observed.is_pending());
            release.send(()).expect("release work");
            let error = cleanup.await.expect_err("original failure");
            assert_eq!(error.to_string(), "business failed");
            accepted.await.expect("accepted task completed");
            assert!(services.is_terminated());
        })
        .await
        .expect("scenario must finish");
    }
}
