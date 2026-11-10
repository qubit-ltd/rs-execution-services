// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates submitting and awaiting work in separate execution domains.

use std::future::Future;
#[cfg(not(test))]
use std::io;

use qubit_execution_services::ExecutionServices;

async fn shutdown_after<F>(services: &ExecutionServices, work: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let result = work.await;
    services.shutdown();
    services.await_termination().await;
    result
}

#[cfg(not(test))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

    runtime.block_on(async {
        let services = ExecutionServices::builder()
            .runtime(runtime.handle().clone())
            .enable_blocking()
            .enable_cpu()
            .enable_io()
            .blocking_pool_size(4)
            .blocking_queue_capacity(1024)
            .cpu_threads(4)
            .cpu_task_capacity(1024)
            .build()?;

        shutdown_after(&services, async {
            let blocking = services.submit_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))?;
            let cpu = services.submit_cpu_callable(|| Ok::<usize, io::Error>((1..=10).sum()))?;
            let io = services.spawn_io(async { Ok::<usize, io::Error>(6 * 7) })?;

            assert_eq!(blocking.await?, 42);
            assert_eq!(cpu.await?, 55);
            assert_eq!(io.await?, 42);
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io;
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::task::Poll;
    use std::time::Duration;

    use qubit_execution_services::ExecutionServices;
    use tokio::runtime::Handle;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::shutdown_after;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
    }

    #[tokio::test]
    async fn test_quick_start_waits_for_accepted_work_and_preserves_business_error() {
        let services = ExecutionServices::builder()
            .runtime(Handle::current())
            .enable_io()
            .io_task_capacity(NonZeroUsize::new(2).expect("capacity is nonzero"))
            .build()
            .expect("execution services should build");

        let (started_tx, started_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let accepted_task = services
            .spawn_io(async move {
                let _ = started_tx.send(());
                release_rx.await.map_err(io::Error::other)?;
                Ok::<(), io::Error>(())
            })
            .expect("IO task should be accepted");
        started_rx.await.expect("accepted task should start");

        let cleanup = shutdown_after(&services, async {
            Err::<(), Box<dyn std::error::Error>>(Box::new(io::Error::other("business failure")))
        });
        tokio::pin!(cleanup);

        assert!(matches!(poll_once(cleanup.as_mut()).await, Poll::Pending));

        release_tx.send(()).expect("accepted task should still be gated");
        let result = timeout(TEST_TIMEOUT, cleanup)
            .await
            .expect("shutdown should finish after accepted work is released");

        let error = result.expect_err("the original business failure should be returned");
        assert_eq!(error.to_string(), "business failure");
        assert!(services.is_terminated());
        accepted_task.await.expect("accepted task should complete");
    }
}
