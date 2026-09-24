// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ExecutionServices`](qubit_execution_services::ExecutionServices).

use std::io;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::mpsc;
use std::time::Duration;

use qubit_execution_services::ExecutionServices;
use qubit_executor::CancelResult;
use qubit_executor::TaskExecutionError;
use qubit_executor::service::ExecutorServiceLifecycle;
use qubit_executor::service::SubmissionError;
use tokio::pin;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::test as tokio_test;

fn create_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for execution services tests")
}

#[test]
fn test_execution_services_submit_blocking_and_cpu_tasks() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder(runtime.handle().clone())
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

    assert_eq!(blocking.get().expect("blocking task should complete successfully"), 42,);
    assert_eq!(cpu.get().expect("cpu task should complete successfully"), 42);
    services.shutdown();
    assert!(services.is_not_running());
    create_runtime().block_on(services.await_termination());
    assert!(services.is_not_running());
    assert!(services.is_terminated());
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Terminated);
}

#[test]
fn test_execution_services_cpu_capacity_rejects_and_reuses_slots() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .cpu_task_capacity(2)
        .build()
        .expect("execution services should be created");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = services
        .submit_tracked_cpu(move || {
            started_tx.send(()).expect("cpu task should start");
            release_rx.recv().expect("cpu task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("running CPU task should be accepted");
    started_rx.recv().expect("cpu task should start");
    let queued = services
        .submit_tracked_cpu(|| Ok::<(), io::Error>(()))
        .expect("queued CPU task should be accepted");
    assert!(matches!(
        services.submit_cpu_callable(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Saturated)
    ));
    assert_eq!(queued.cancel(), CancelResult::Cancelled);
    queued.get().expect_err("queued task should be cancelled");
    let replacement = services
        .submit_cpu_callable(|| Ok::<(), io::Error>(()))
        .expect("cancelled CPU capacity should be reusable");
    release_tx.send(()).expect("running CPU task should be released");
    running.get().expect("running CPU task should finish");
    replacement.get().expect("replacement CPU task should finish");
    services.shutdown();
    create_runtime().block_on(services.await_termination());
}

#[test]
fn test_execution_services_submit_sync_runnables_and_tracked_callables() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder(runtime.handle().clone())
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
            cpu_sender.send("cpu").expect("cpu runnable should report completion");
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
    assert_eq!(cpu_callable.get().expect("cpu tracked callable should complete"), 42,);

    services.shutdown();
    create_runtime().block_on(services.await_termination());
}

#[test]
fn test_execution_services_reports_shutdown_while_task_is_running() {
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
            started_sender.send(()).expect("blocking task should report start");
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
    release_sender.send(()).expect("blocking task release should be sent");
    create_runtime().block_on(services.await_termination());
    assert!(services.is_terminated());
}

#[test]
fn test_await_termination_completes_with_one_tokio_blocking_thread() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("runtime should build");
    let services = ExecutionServices::builder(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (blocking_started_tx, blocking_started_rx) = mpsc::channel();
    let (blocking_release_tx, blocking_release_rx) = mpsc::channel();
    let (cpu_started_tx, cpu_started_rx) = mpsc::channel();
    let (cpu_release_tx, cpu_release_rx) = mpsc::channel();

    services
        .submit_blocking(move || {
            blocking_started_tx.send(()).expect("blocking task should start");
            blocking_release_rx.recv().expect("blocking task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("blocking task should be accepted");
    services
        .submit_cpu(move || {
            cpu_started_tx.send(()).expect("CPU task should start");
            cpu_release_rx.recv().expect("CPU task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("CPU task should be accepted");
    blocking_started_rx.recv().expect("blocking task should start");
    cpu_started_rx.recv().expect("CPU task should start");
    services.shutdown();

    runtime.block_on(async {
        let waiter = services.await_termination();
        pin!(waiter);
        select! {
            result = &mut waiter => panic!("held tasks should keep termination pending: {result:?}"),
            _ = async {
                yield_now().await;
                blocking_release_tx.send(()).expect("blocking task release should be sent");
                cpu_release_tx.send(()).expect("CPU task release should be sent");
            } => {}
        }
        waiter.await;
    });

    assert!(services.is_terminated());
}

#[test]
fn test_await_termination_does_not_starve_io_spawn_blocking() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("runtime should build");
    let services = ExecutionServices::builder(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("services should build");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    services
        .submit_blocking(move || {
            started_tx.send(()).expect("task should signal start");
            release_rx.recv().expect("task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("blocking task should be accepted");
    started_rx.recv().expect("task should start");
    let (io_gate_tx, io_gate_rx) = oneshot::channel();
    let io_release_tx = release_tx.clone();
    services
        .spawn_io(async move {
            io_gate_rx.await.expect("IO gate should open");
            tokio::task::spawn_blocking(move || {
                let _ = io_release_tx.send(());
            })
            .await
            .expect("IO blocking task should join");
            Ok::<(), io::Error>(())
        })
        .expect("IO task should be accepted");
    services.shutdown();

    runtime.block_on(async {
        let mut wait = Box::pin(services.await_termination());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut wait)
                .await
                .is_err()
        );
        io_gate_tx.send(()).expect("IO gate should open");
        let completed = tokio::time::timeout(Duration::from_secs(2), &mut wait).await;
        if completed.is_err() {
            let _ = release_tx.send(());
            let _ = tokio::time::timeout(Duration::from_secs(2), &mut wait).await;
        }
        assert!(completed.is_ok(), "termination wait starved IO spawn_blocking");
    });
}

#[test]
fn test_cancelled_termination_wait_releases_tokio_blocking_capacity() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .expect("runtime should build");
    let services = ExecutionServices::builder(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("services should build");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    services
        .submit_blocking(move || {
            started_tx.send(()).expect("task should signal start");
            release_rx.recv().expect("task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("blocking task should be accepted");
    started_rx.recv().expect("task should start");
    services.shutdown();

    runtime.block_on(async {
        let mut wait = Box::pin(services.await_termination());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut wait)
                .await
                .is_err()
        );
        drop(wait);
        let sentinel = tokio::task::spawn_blocking(|| 42);
        let completed = tokio::time::timeout(Duration::from_secs(2), sentinel).await;
        release_tx.send(()).expect("blocking task should release");
        services.await_termination().await;
        assert!(completed.is_ok(), "cancelled wait retained a blocking worker");
    });
}

#[tokio_test]
async fn test_execution_services_submit_tokio_blocking_and_io_tasks() {
    let services = ExecutionServices::new(Handle::current()).expect("execution services should be created");

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

#[tokio_test]
async fn test_execution_services_submit_tokio_runnable_and_tracked_callable() {
    let services = ExecutionServices::new(Handle::current()).expect("execution services should be created");
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
        callable.await.expect("tokio blocking tracked callable should complete"),
        42,
    );

    services.shutdown();
    assert!(!services.is_running());
    assert!(services.is_not_running());
    services.await_termination().await;
    assert!(services.is_terminated());
}

#[tokio_test]
async fn test_execution_services_stop_aggregates_reports() {
    let services = ExecutionServices::builder(Handle::current())
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (blocking_started_sender, blocking_started_receiver) = mpsc::channel();
    let (blocking_release_sender, blocking_release_receiver) = mpsc::channel();

    let running = services
        .submit_tracked_blocking(move || {
            blocking_started_sender
                .send(())
                .expect("blocking task should report that it started");
            blocking_release_receiver
                .recv()
                .expect("blocking task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("blocking domain should accept running task");
    blocking_started_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("blocking task should start before stop");
    let queued = services
        .submit_tracked_blocking(|| Ok::<(), io::Error>(()))
        .expect("blocking domain should accept queued task");

    let (tokio_blocking_started_sender, tokio_blocking_started_receiver) = mpsc::channel();
    let (tokio_blocking_release_sender, tokio_blocking_release_receiver) = mpsc::channel();

    let blocking = services
        .submit_tracked_tokio_blocking(move || {
            tokio_blocking_started_sender
                .send(())
                .expect("Tokio blocking task should report that it started");
            tokio_blocking_release_receiver
                .recv()
                .expect("Tokio blocking task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("tokio blocking domain should accept task");
    tokio_blocking_started_receiver
        .recv_timeout(Duration::from_secs(2))
        .expect("Tokio blocking task should start before stop");

    let (io_started_sender, io_started_receiver) = oneshot::channel();
    let io = services
        .spawn_io(async move {
            io_started_sender
                .send(())
                .expect("IO future should report that it was polled");
            std::future::pending::<()>().await;
            Ok::<(), io::Error>(())
        })
        .expect("io domain should accept task");
    io_started_receiver.await.expect("IO future should start before stop");

    let report = services.stop();
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Stopping);
    assert!(services.is_stopping());
    assert_eq!(report.blocking.queued, 1);
    assert_eq!(report.blocking.running, 1);
    assert_eq!(report.blocking.cancelled, 1);
    assert!(report.tokio_blocking.running >= 1);
    assert_eq!(report.io.running, 1);
    assert_eq!(report.io.cancelled, 1);

    blocking_release_sender
        .send(())
        .expect("blocking task release should be sent");
    tokio_blocking_release_sender
        .send(())
        .expect("Tokio blocking task release should be sent");
    services.await_termination().await;

    assert_eq!(
        report.total_queued(),
        report.blocking.queued + report.cpu.queued + report.tokio_blocking.queued + report.io.queued
    );
    assert_eq!(
        report.total_running(),
        report.blocking.running + report.cpu.running + report.tokio_blocking.running + report.io.running
    );
    assert_eq!(
        report.total_cancelled(),
        report.blocking.cancelled + report.cpu.cancelled + report.tokio_blocking.cancelled + report.io.cancelled
    );
    assert!(report.total_cancelled() >= 1);
    assert!(services.is_not_running());
    assert!(services.is_terminated());
    running
        .get()
        .expect("running blocking task should finish after release");
    assert!(matches!(queued.get(), Err(TaskExecutionError::Cancelled)));
    assert!(matches!(blocking.await, Ok(()) | Err(TaskExecutionError::Cancelled)));
    assert!(matches!(io.await, Err(TaskExecutionError::Cancelled)));
}

#[tokio_test]
async fn test_execution_services_shutdown_rejects_new_tasks() {
    let services = ExecutionServices::new(Handle::current()).expect("execution services should be created");

    services.shutdown();
    let result = services.spawn_io(async { Ok::<(), io::Error>(()) });

    assert!(matches!(result, Err(SubmissionError::Shutdown)));
    services.await_termination().await;
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Terminated);
}

#[tokio_test]
async fn test_execution_services_shutdown_rejects_submissions_in_every_domain() {
    let services = ExecutionServices::builder(Handle::current())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");

    services.shutdown();
    assert!(matches!(
        services.submit_blocking(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Shutdown)
    ));
    assert!(matches!(
        services.submit_cpu(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Shutdown)
    ));
    assert!(matches!(
        services.submit_tokio_blocking(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Shutdown)
    ));
    assert!(matches!(
        services.spawn_io(async { Ok::<(), io::Error>(()) }),
        Err(SubmissionError::Shutdown)
    ));
    services.await_termination().await;
}

#[tokio_test]
async fn test_execution_services_stop_keeps_all_domains_closed_on_repeated_shutdown() {
    let services = ExecutionServices::builder(Handle::current())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");

    let _stop_report = services.stop();
    services.shutdown();
    assert!(matches!(
        services.submit_blocking(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Shutdown)
    ));
    assert!(matches!(
        services.submit_cpu(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Shutdown)
    ));
    assert!(matches!(
        services.submit_tokio_blocking(|| Ok::<(), io::Error>(())),
        Err(SubmissionError::Shutdown)
    ));
    assert!(matches!(
        services.spawn_io(async { Ok::<(), io::Error>(()) }),
        Err(SubmissionError::Shutdown)
    ));
    services.await_termination().await;
}

#[tokio_test]
async fn test_execution_services_stop_intent_survives_shutdown() {
    let services = ExecutionServices::builder(Handle::current())
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
                .expect("blocking task should report that it started");
            release_receiver
                .recv_timeout(Duration::from_secs(5))
                .expect("blocking task should be released");
            Ok::<(), io::Error>(())
        })
        .expect("blocking domain should accept the task");
    started_receiver
        .recv_timeout(Duration::from_secs(5))
        .expect("blocking task should start");

    services.shutdown();
    let _report = services.stop();
    services.shutdown();
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Stopping);
    assert!(services.is_stopping());

    release_sender.send(()).expect("blocking task should be released");
    services.await_termination().await;
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Terminated);
}

#[test]
fn test_execution_services_serializes_concurrent_submissions_and_shutdown() {
    let runtime = create_runtime();
    for _ in 0..8 {
        let services = Arc::new(
            ExecutionServices::builder(runtime.handle().clone())
                .blocking_pool_size(1)
                .cpu_threads(1)
                .build()
                .expect("execution services should be created"),
        );
        let barrier = Arc::new(Barrier::new(6));
        let mut workers = Vec::new();
        for submit in 0..4 {
            let services = Arc::clone(&services);
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                match submit {
                    0 => services
                        .submit_blocking_callable(|| Ok::<(), io::Error>(()))
                        .map(|_| ()),
                    1 => services.submit_cpu_callable(|| Ok::<(), io::Error>(())).map(|_| ()),
                    2 => services
                        .submit_tokio_blocking_callable(|| Ok::<(), io::Error>(()))
                        .map(|_| ()),
                    _ => services.spawn_io(async { Ok::<(), io::Error>(()) }).map(|_| ()),
                }
            }));
        }
        let shutdown_services = Arc::clone(&services);
        let shutdown_barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            shutdown_barrier.wait();
            shutdown_services.shutdown();
            Ok::<(), SubmissionError>(())
        }));
        let stop_services = Arc::clone(&services);
        let stop_barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            stop_barrier.wait();
            let _stop_report = stop_services.stop();
            Ok::<(), SubmissionError>(())
        }));

        for worker in workers {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(SubmissionError::Shutdown)) => {}
                Ok(Err(error)) => panic!("unexpected concurrent submission error: {error:?}"),
                Err(_) => panic!("concurrent submission or shutdown worker panicked"),
            }
        }

        assert!(matches!(
            services.submit_blocking(|| Ok::<(), io::Error>(())),
            Err(SubmissionError::Shutdown)
        ));
        assert!(matches!(
            services.submit_cpu(|| Ok::<(), io::Error>(())),
            Err(SubmissionError::Shutdown)
        ));
        assert!(matches!(
            services.submit_tokio_blocking(|| Ok::<(), io::Error>(())),
            Err(SubmissionError::Shutdown)
        ));
        assert!(matches!(
            services.spawn_io(async { Ok::<(), io::Error>(()) }),
            Err(SubmissionError::Shutdown)
        ));
    }
    drop(runtime);
}
