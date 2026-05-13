/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ExecutionServices`](qubit_execution_services::ExecutionServices).

use std::{
    io,
    sync::mpsc,
    time::Duration,
};

use qubit_execution_services::{
    ExecutionServices,
    ExecutorServiceLifecycle,
    SubmissionError,
};
use qubit_executor::TaskExecutionError;

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for execution services tests")
}

#[test]
fn test_execution_services_submit_blocking_and_cpu_tasks() {
    let services = ExecutionServices::builder()
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Running);

    let blocking = services
        .submit_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))
        .expect("blocking domain should accept callable");
    let cpu = services
        .submit_cpu_callable(|| Ok::<usize, io::Error>(6 * 7))
        .expect("cpu domain should accept callable");

    assert_eq!(
        blocking
            .get()
            .expect("blocking task should complete successfully"),
        42,
    );
    assert_eq!(
        cpu.get().expect("cpu task should complete successfully"),
        42
    );
    services.shutdown();
    assert!(services.is_not_running());
    create_runtime().block_on(services.await_termination());
    assert!(services.is_not_running());
    assert!(services.is_terminated());
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Terminated);
}

#[test]
fn test_execution_services_submit_sync_runnables_and_tracked_callables() {
    let services = ExecutionServices::builder()
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (sender, receiver) = mpsc::channel();
    let blocking_sender = sender.clone();
    let cpu_sender = sender;

    services
        .submit_blocking(move || {
            blocking_sender
                .send("blocking")
                .expect("blocking runnable should report completion");
            Ok::<(), io::Error>(())
        })
        .expect("blocking domain should accept runnable");
    let blocking_callable = services
        .submit_tracked_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))
        .expect("blocking domain should accept tracked callable");
    services
        .submit_cpu(move || {
            cpu_sender
                .send("cpu")
                .expect("cpu runnable should report completion");
            Ok::<(), io::Error>(())
        })
        .expect("cpu domain should accept runnable");
    let cpu_callable = services
        .submit_tracked_cpu_callable(|| Ok::<usize, io::Error>(6 * 7))
        .expect("cpu domain should accept tracked callable");

    let mut completed = [
        receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("first runnable should complete"),
        receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("second runnable should complete"),
    ];
    completed.sort_unstable();
    assert_eq!(completed, ["blocking", "cpu"]);
    assert_eq!(
        blocking_callable
            .get()
            .expect("blocking tracked callable should complete"),
        42,
    );
    assert_eq!(
        cpu_callable
            .get()
            .expect("cpu tracked callable should complete"),
        42,
    );

    services.shutdown();
    create_runtime().block_on(services.await_termination());
}

#[test]
fn test_execution_services_reports_shutdown_while_task_is_running() {
    let services = ExecutionServices::builder()
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();

    services
        .submit_blocking(move || {
            started_sender
                .send(())
                .expect("blocking task should report start");
            release_receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("blocking task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("blocking domain should accept runnable");
    started_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("blocking task should start");

    services.shutdown();
    assert!(services.is_shutting_down());
    assert!(services.is_not_running());
    release_sender
        .send(())
        .expect("blocking task release should be sent");
    create_runtime().block_on(services.await_termination());
    assert!(services.is_terminated());
}

#[tokio::test]
async fn test_execution_services_submit_tokio_blocking_and_io_tasks() {
    let services = ExecutionServices::new().expect("execution services should be created");

    let blocking = services
        .submit_tokio_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))
        .expect("tokio blocking domain should accept callable");
    let io = services
        .spawn_io(async { Ok::<usize, io::Error>(42) })
        .expect("io domain should accept future");

    assert_eq!(
        blocking
            .await
            .expect("tokio blocking task should complete successfully"),
        42,
    );
    assert_eq!(io.await.expect("io task should complete successfully"), 42);
    services.shutdown();
    services.await_termination().await;
}

#[tokio::test]
async fn test_execution_services_submit_tokio_runnable_and_tracked_callable() {
    let services = ExecutionServices::new().expect("execution services should be created");
    let (sender, receiver) = mpsc::channel();

    assert!(services.is_running());
    assert!(!services.is_shutting_down());
    assert!(!services.is_stopping());

    services
        .submit_tokio_blocking(move || {
            sender
                .send("tokio-blocking")
                .expect("tokio blocking runnable should report completion");
            Ok::<(), io::Error>(())
        })
        .expect("tokio blocking domain should accept runnable");
    let callable = services
        .submit_tracked_tokio_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))
        .expect("tokio blocking domain should accept tracked callable");

    assert_eq!(
        receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("tokio blocking runnable should complete"),
        "tokio-blocking",
    );
    assert_eq!(
        callable
            .await
            .expect("tokio blocking tracked callable should complete"),
        42,
    );

    services.shutdown();
    assert!(!services.is_running());
    assert!(services.is_not_running());
    services.await_termination().await;
    assert!(services.is_terminated());
}

#[tokio::test]
async fn test_execution_services_stop_aggregates_reports() {
    let services = ExecutionServices::builder()
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");

    let blocking = services
        .submit_tracked_tokio_blocking(|| {
            std::thread::sleep(Duration::from_secs(1));
            Ok::<(), io::Error>(())
        })
        .expect("tokio blocking domain should accept task");
    let io = services
        .spawn_io(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<(), io::Error>(())
        })
        .expect("io domain should accept task");

    tokio::task::yield_now().await;
    let report = services.stop();
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Stopping);
    assert!(services.is_stopping());
    services.await_termination().await;

    let total_active = report.total_queued() + report.total_running();
    assert!(total_active >= 2);
    assert!(report.total_cancelled() >= 2);
    assert!(services.is_not_running());
    assert!(services.is_terminated());
    assert!(matches!(
        blocking.await,
        Ok(()) | Err(TaskExecutionError::Cancelled)
    ));
    assert!(matches!(io.await, Err(TaskExecutionError::Cancelled)));
}

#[tokio::test]
async fn test_execution_services_shutdown_rejects_new_tasks() {
    let services = ExecutionServices::new().expect("execution services should be created");

    services.shutdown();
    let result = services.spawn_io(async { Ok::<(), io::Error>(()) });

    assert!(matches!(result, Err(SubmissionError::Shutdown)));
    services.await_termination().await;
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Terminated);
}
