// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests lifecycle termination-wait ownership contracts.

use std::future::Future;
use std::sync::Arc;
use std::task::Poll;

use qubit_execution_services::ExecutionServices;

#[tokio::test]
async fn test_managed_task_cannot_wait_for_its_own_termination() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let services = Arc::new(
            ExecutionServices::builder()
                .enable_io()
                .runtime(tokio::runtime::Handle::current())
                .build()
                .expect("services"),
        );
        let (observed_tx, observed_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let owned = Arc::clone(&services);
        let task = services
            .spawn_io(async move {
                owned.shutdown();
                let wait = owned.await_termination();
                tokio::pin!(wait);
                let pending = std::future::poll_fn(|cx| Poll::Ready(wait.as_mut().poll(cx).is_pending())).await;
                let _ = observed_tx.send(pending);
                let _ = release_rx.await;
                Ok::<(), std::io::Error>(())
            })
            .expect("accepted managed task");
        assert!(observed_rx.await.expect("internal observation"));
        let external_wait = services.await_termination();
        tokio::pin!(external_wait);
        let pending = std::future::poll_fn(|cx| Poll::Ready(external_wait.as_mut().poll(cx).is_pending())).await;
        assert!(pending);
        release_tx.send(()).expect("release managed task");
        task.await.expect("task exits");
        external_wait.await;
        assert!(services.is_terminated());
    })
    .await
    .expect("scenario must finish");
}
