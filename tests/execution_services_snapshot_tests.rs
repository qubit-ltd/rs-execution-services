// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests snapshot values and disabled-domain representation.

use std::num::NonZeroUsize;

use qubit_execution_services::ExecutionServices;
use qubit_executor::TaskExecutionError;
use qubit_executor::service::ExecutorServiceLifecycle;

#[tokio::test]
async fn snapshot_reports_only_enabled_domains_and_configured_capacities() {
    let runtime = tokio::runtime::Handle::current();
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime)
        .blocking_pool_size(1)
        .blocking_queue_capacity(2)
        .cpu_threads(1)
        .cpu_task_capacity(3)
        .tokio_blocking_task_capacity(NonZeroUsize::new(4).unwrap())
        .io_task_capacity(NonZeroUsize::new(5).unwrap())
        .build()
        .expect("all domains should build");
    let io = services
        .spawn_io(async {
            std::future::pending::<()>().await;
            Ok::<(), std::io::Error>(())
        })
        .expect("pending IO task should be accepted");
    let snapshot = services.snapshot();
    assert_eq!(snapshot.blocking.unwrap().queue_capacity, Some(2));
    assert_eq!(snapshot.cpu.unwrap().task_capacity, 3);
    assert_eq!(snapshot.tokio_blocking.unwrap().task_capacity, 4);
    assert_eq!(snapshot.io.unwrap().task_capacity, 5);
    assert_eq!(snapshot.io.unwrap().accepted_unfinished, 1);
    let report = services.stop();
    assert!(report.io.expect("IO domain should be enabled").cancelled > 0);
    services.await_termination().await;
    assert!(matches!(io.await, Err(TaskExecutionError::Cancelled)));
    assert_eq!(
        services.snapshot().io.unwrap().lifecycle,
        ExecutorServiceLifecycle::Terminated
    );
}

#[tokio::test]
async fn snapshot_uses_none_for_disabled_domains() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .build()
        .expect("blocking domain should build");
    let snapshot = services.snapshot();
    assert!(snapshot.blocking.is_some());
    assert!(snapshot.cpu.is_none());
    assert!(snapshot.tokio_blocking.is_none());
    assert!(snapshot.io.is_none());
    services.shutdown();
    services.await_termination().await;
}
