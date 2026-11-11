// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates independent worker and task-capacity limits per domain.

use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::task::Poll;

use qubit_execution_services::ExecutionServices;

async fn shutdown_after<F>(
    services: &ExecutionServices,
    work: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let result = work.await;
    services.shutdown();
    services.await_termination().await;
    result
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // These values illustrate separate resource limits; they are not global
    // thread or memory budgets for an application.
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
            .io_task_capacity(NonZeroUsize::new(1).expect("nonzero capacity"))
            .build()?;

        shutdown_after(&services, async {
            let blocking = services.submit_blocking_callable(|| Ok::<u8, io::Error>(40))?;
            let cpu = services.submit_cpu_callable(|| Ok::<u8, io::Error>(41))?;
            let tokio_blocking =
                services.submit_tokio_blocking_callable(|| Ok::<u8, io::Error>(42))?;
            let io = services.spawn_io(async { Ok::<u8, io::Error>(43) })?;

            assert_eq!(blocking.await?, 40);
            assert_eq!(cpu.await?, 41);
            assert_eq!(tokio_blocking.await?, 42);
            assert_eq!(io.await?, 43);

            let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
            let first_io = services.spawn_io(async move {
                release_rx.await.map_err(io::Error::other)?;
                Ok::<u8, io::Error>(44)
            })?;
            tokio::task::yield_now().await;
            let waiting_value = String::from("one-shot IO task");
            let waiting_io =
                services.spawn_io_wait(async move { Ok::<usize, io::Error>(waiting_value.len()) });
            tokio::pin!(waiting_io);
            let first_wait_poll =
                std::future::poll_fn(|context| Poll::Ready(waiting_io.as_mut().poll(context)))
                    .await;
            assert!(first_wait_poll.is_pending());
            assert_eq!(
                services
                    .snapshot()
                    .io
                    .expect("IO domain enabled")
                    .accepted_unfinished,
                1
            );
            release_tx.send(()).expect("IO producer should be released");
            assert_eq!(first_io.await?, 44);
            assert_eq!(waiting_io.await?.await?, 16);
            let snapshot = services.snapshot();
            assert_eq!(
                snapshot.io.expect("IO domain enabled").accepted_unfinished,
                0
            );

            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await?;
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
                Err::<(), Box<dyn std::error::Error>>(Box::new(std::io::Error::other(
                    "business failed",
                )))
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
