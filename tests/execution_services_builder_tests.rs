// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ExecutionServicesBuilder`](qubit_execution_services::ExecutionServicesBuilder).

use std::io;
use std::num::NonZeroUsize;
use std::sync::mpsc;
use std::time::Duration;

use qubit_execution_services::ExecutionServices;
use qubit_execution_services::ExecutionServicesBuildError;
use qubit_executor::TaskExecutionError;
use qubit_executor::service::ExecutorServiceLifecycle;
use qubit_executor::service::SubmissionError;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
use tokio::test as tokio_test;

fn create_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for execution services builder tests")
}

#[test]
fn test_execution_services_builder_debug_does_not_expose_configuration() {
    let runtime = create_runtime();
    let builder = ExecutionServices::builder(runtime.handle().clone())
        .blocking_thread_name_prefix("private-thread-prefix")
        .cpu_threads(1);

    assert_eq!(format!("{builder:?}"), "ExecutionServicesBuilder");
}

#[test]
fn test_execution_services_builder_rejects_invalid_blocking_domain() {
    let runtime = create_runtime();
    let error = match ExecutionServices::builder(runtime.handle().clone())
        .blocking_maximum_pool_size(0)
        .build()
    {
        Ok(_) => panic!("builder should reject invalid blocking domain"),
        Err(error) => error,
    };

    assert!(matches!(error, ExecutionServicesBuildError::Blocking { .. }));
}

#[test]
fn test_execution_services_builder_rejects_invalid_cpu_domain() {
    let runtime = create_runtime();
    let error = match ExecutionServices::builder(runtime.handle().clone())
        .cpu_threads(0)
        .build()
    {
        Ok(_) => panic!("builder should reject invalid cpu domain"),
        Err(error) => error,
    };

    assert!(matches!(error, ExecutionServicesBuildError::Cpu { .. }));
}

#[test]
fn test_execution_services_builder_options_and_accessors() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder(runtime.handle().clone())
        .blocking_core_pool_size(1)
        .blocking_maximum_pool_size(1)
        .blocking_queue_capacity(8)
        .blocking_unbounded_queue()
        .blocking_thread_name_prefix("exec-blocking")
        .blocking_stack_size(2 * 1024 * 1024)
        .blocking_keep_alive(Duration::from_millis(25))
        .blocking_allow_core_thread_timeout(false)
        .blocking_prestart_core_threads()
        .cpu_threads(1)
        .cpu_task_capacity(8)
        .cpu_thread_name_prefix("exec-cpu")
        .cpu_stack_size(2 * 1024 * 1024)
        .build()
        .expect("execution services should be created with custom options");

    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Running);

    services
        .submit_tracked_blocking(|| Ok::<(), io::Error>(()))
        .expect("blocking domain should accept runnable")
        .get()
        .expect("blocking runnable should complete");
    services
        .submit_tracked_cpu(|| Ok::<(), io::Error>(()))
        .expect("cpu domain should accept runnable")
        .get()
        .expect("cpu runnable should complete");

    services.shutdown();
    create_runtime()
        .block_on(services.await_termination())
        .expect("all execution domains should terminate");
}

#[test]
fn test_execution_services_builder_default_blocking_queue_is_bounded() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();

    services
        .submit_blocking(move || {
            started_sender.send(()).expect("blocking task should start");
            release_receiver.recv().expect("blocking task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("blocking task should be accepted");
    started_receiver
        .recv()
        .expect("blocking task should report that it started");

    for _ in 0..1024 {
        services
            .submit_blocking(|| Ok::<(), io::Error>(()))
            .expect("default blocking queue should accept up to 1024 waiting tasks");
    }
    assert!(matches!(
        services.submit_blocking(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Saturated)
    ));

    let report = services.stop();
    assert_eq!(report.blocking.queued, 1024);
    release_sender.send(()).expect("blocking task release should be sent");
    runtime
        .block_on(services.await_termination())
        .expect("all execution domains should terminate");
}

#[test]
fn test_execution_services_builder_can_use_unbounded_blocking_queue() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder(runtime.handle().clone())
        .blocking_pool_size(1)
        .blocking_unbounded_queue()
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();

    services
        .submit_blocking(move || {
            started_sender.send(()).expect("blocking task should start");
            release_receiver.recv().expect("blocking task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("blocking task should be accepted");
    started_receiver
        .recv()
        .expect("blocking task should report that it started");

    for _ in 0..1025 {
        services
            .submit_blocking(|| Ok::<(), io::Error>(()))
            .expect("explicitly unbounded queue should accept more than 1024 waiting tasks");
    }

    let report = services.stop();
    assert_eq!(report.blocking.queued, 1025);
    release_sender.send(()).expect("blocking task release should be sent");
    runtime
        .block_on(services.await_termination())
        .expect("all execution domains should terminate");
}

#[tokio_test]
async fn test_execution_services_builder_configures_independent_tokio_capacities() {
    let services = ExecutionServices::builder(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).expect("capacity should be nonzero"))
        .io_task_capacity(NonZeroUsize::new(1).expect("capacity should be nonzero"))
        .build()
        .expect("execution services should be created");

    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let blocking = services
        .submit_tracked_tokio_blocking(move || {
            started_sender.send(()).expect("blocking task should start");
            release_receiver.recv().expect("blocking task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("Tokio blocking domain should accept one task");
    started_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("Tokio blocking task should start");
    assert!(matches!(
        services.submit_tokio_blocking(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Saturated)
    ));

    let io = services
        .spawn_io(async {
            std::future::pending::<()>().await;
            Ok::<(), io::Error>(())
        })
        .expect("IO domain should accept one future");
    assert!(matches!(
        services.spawn_io(async { Ok::<(), io::Error>(()) }),
        Err(SubmissionError::Saturated)
    ));

    let report = services.stop();
    assert_eq!(report.io.cancelled, 1);
    release_sender.send(()).expect("Tokio blocking task should be released");
    services
        .await_termination()
        .await
        .expect("execution domains should terminate");
    blocking.await.expect("running Tokio blocking task should finish");
    assert!(matches!(io.await, Err(TaskExecutionError::Cancelled)));
}

#[test]
fn test_execution_services_builder_grows_blocking_pool_when_bounded_queue_fills() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder(runtime.handle().clone())
        .blocking_core_pool_size(1)
        .blocking_maximum_pool_size(2)
        .blocking_queue_capacity(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (started_sender, started_receiver) = mpsc::channel();
    let (first_release_sender, first_release_receiver) = mpsc::channel();
    let (second_release_sender, second_release_receiver) = mpsc::channel();

    let blocking_started_sender = started_sender.clone();
    services
        .submit_blocking(move || {
            blocking_started_sender
                .send(1)
                .expect("core worker should report that it started");
            first_release_receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("core worker should be released");
            Ok::<(), io::Error>(())
        })
        .expect("core worker task should be accepted");
    assert_eq!(
        started_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("core worker should start"),
        1,
    );

    services
        .submit_blocking(move || {
            started_sender
                .send(2)
                .expect("additional worker should report that it started");
            second_release_receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("additional worker should be released");
            Ok::<(), io::Error>(())
        })
        .expect("one task should fit in the bounded queue");
    services
        .submit_blocking(|| Ok::<(), io::Error>(()))
        .expect("a full queue should trigger the configured additional worker");

    let second_started = started_receiver.recv_timeout(Duration::from_secs(2)) == Ok(2);
    first_release_sender.send(()).expect("core worker should be released");
    second_release_sender
        .send(())
        .expect("additional worker should be released");
    services.shutdown();
    runtime
        .block_on(services.await_termination())
        .expect("all execution domains should terminate");
    assert!(
        second_started,
        "the bounded queue should cause a second worker to start"
    );
}
