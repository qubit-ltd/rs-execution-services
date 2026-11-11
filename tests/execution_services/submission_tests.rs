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
use qubit_execution_services::ExecutionServicesSubmissionError;
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
use tokio::task::spawn_blocking;
use tokio::task::yield_now;
use tokio::test as tokio_test;
use tokio::time::timeout;

use super::GateRelease;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn test_gate_release_drop_unblocks_waiting_worker() {
    let (release_tx, release_rx) = mpsc::channel();
    let gate = GateRelease(Some(release_tx));
    let worker = std::thread::spawn(move || release_rx.recv_timeout(TEST_TIMEOUT));

    drop(gate);

    worker
        .join()
        .expect("gate worker should exit")
        .expect("dropping the gate should send a release signal");
}

fn create_runtime() -> Runtime {
    Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for execution services tests")
}

#[test]
fn test_execution_services_submit_blocking_and_cpu_tasks() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime.handle().clone())
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
        runtime.block_on(async {
            timeout(TEST_TIMEOUT, async { blocking.await })
                .await
                .expect("blocking task should finish before the diagnostic timeout")
                .expect("blocking task should complete successfully")
        }),
        42,
    );
    assert_eq!(
        runtime.block_on(async {
            timeout(TEST_TIMEOUT, async { cpu.await })
                .await
                .expect("CPU task should finish before the diagnostic timeout")
                .expect("CPU task should complete successfully")
        }),
        42
    );
    services.shutdown();
    assert!(services.is_not_running());
    runtime.block_on(async {
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .expect("services should terminate before the diagnostic timeout");
    });
    assert!(services.is_not_running());
    assert!(services.is_terminated());
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Terminated);
}

#[test]
fn test_execution_services_cpu_capacity_rejects_and_reuses_slots() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .cpu_task_capacity(2)
        .build()
        .expect("execution services should be created");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_tracked_cpu(move || {
            started_tx.send(()).expect("cpu task should start");
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("CPU task release gate should open");
            Ok::<(), io::Error>(())
        })
        .expect("running CPU task should be accepted");
    started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("CPU task should signal that it started");
    let queued = services
        .submit_tracked_cpu(|| Ok::<(), io::Error>(()))
        .expect("queued CPU task should be accepted");
    assert!(matches!(
        services.submit_cpu_callable(|| Ok::<(), io::Error>(())),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Saturated
        })
    ));
    assert_eq!(queued.cancel(), CancelResult::Cancelled);
    let queued_result = runtime.block_on(async {
        timeout(TEST_TIMEOUT, async { queued.await })
            .await
            .expect("cancelled queued CPU task should complete")
    });
    assert!(matches!(queued_result, Err(TaskExecutionError::Cancelled)));
    let replacement = services
        .submit_cpu_callable(|| Ok::<(), io::Error>(()))
        .expect("cancelled CPU capacity should be reusable");
    gate.release();
    runtime.block_on(async {
        timeout(TEST_TIMEOUT, async { running.await })
            .await
            .expect("running CPU task should finish before the diagnostic timeout")
            .expect("running CPU task should complete successfully");
        timeout(TEST_TIMEOUT, async { replacement.await })
            .await
            .expect("replacement CPU task should finish before the diagnostic timeout")
            .expect("replacement CPU task should complete successfully");
    });
    services.shutdown();
    runtime.block_on(async {
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .expect("execution services should terminate before the diagnostic timeout");
    });
}

#[test]
fn test_execution_services_submit_sync_runnables_and_tracked_callables() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime.handle().clone())
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
        runtime.block_on(async {
            timeout(TEST_TIMEOUT, async { blocking_callable.await })
                .await
                .expect("blocking tracked callable should finish before the diagnostic timeout")
                .expect("blocking tracked callable should complete")
        }),
        42,
    );
    assert_eq!(
        runtime.block_on(async {
            timeout(TEST_TIMEOUT, async { cpu_callable.await })
                .await
                .expect("CPU tracked callable should finish before the diagnostic timeout")
                .expect("CPU tracked callable should complete")
        }),
        42,
    );

    services.shutdown();
    runtime.block_on(async {
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .expect("services should terminate before the diagnostic timeout");
    });
}

#[test]
fn test_execution_services_reports_shutdown_while_task_is_running() {
    let runtime = create_runtime();
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let mut gate = GateRelease(Some(release_sender));

    services
        .submit_blocking(move || {
            started_sender.send(()).expect("blocking task should report start");
            release_receiver
                .recv_timeout(TEST_TIMEOUT)
                .expect("blocking task release gate should open");
            Ok::<(), io::Error>(())
        })
        .expect("blocking domain should accept runnable");
    started_receiver
        .recv_timeout(TEST_TIMEOUT)
        .expect("blocking task should signal that it started");

    services.shutdown();
    assert!(services.is_shutting_down());
    assert!(services.is_not_running());
    gate.release();
    runtime.block_on(async {
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .expect("services should terminate before the diagnostic timeout");
    });
    assert!(services.is_terminated());
}

#[test]
fn test_rejected_blocking_task_drop_may_shutdown_facade() {
    struct ShutdownOnDrop(std::sync::Weak<ExecutionServices>);
    impl Drop for ShutdownOnDrop {
        fn drop(&mut self) {
            if let Some(services) = self.0.upgrade() {
                services.shutdown();
            }
        }
    }

    let runtime = create_runtime();
    let services = Arc::new(
        ExecutionServices::builder()
            .enable_blocking()
            .enable_cpu()
            .enable_tokio_blocking()
            .enable_io()
            .runtime(runtime.handle().clone())
            .blocking_pool_size(1)
            .blocking_queue_capacity(1)
            .cpu_threads(1)
            .build()
            .expect("services should build"),
    );
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    services
        .submit_blocking(move || {
            started_tx.send(()).expect("start signal");
            release_rx.recv_timeout(TEST_TIMEOUT).expect("release signal");
            Ok::<(), io::Error>(())
        })
        .expect("running task should be accepted");
    started_rx.recv_timeout(Duration::from_secs(2)).expect("worker started");
    services
        .submit_blocking(|| Ok::<(), io::Error>(()))
        .expect("queued task should be accepted");

    let mut on_drop = Some(ShutdownOnDrop(Arc::downgrade(&services)));
    let result = services.submit_blocking(move || {
        drop(on_drop.take());
        Ok::<(), io::Error>(())
    });
    assert!(matches!(
        result,
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Saturated
        })
    ));
    assert!(services.is_shutting_down());
    gate.release();
    runtime.block_on(async {
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .expect("services should terminate before the diagnostic timeout");
    });
    assert!(services.is_terminated());
}

#[test]
fn test_closed_facade_rejected_task_drop_may_request_stop() {
    struct StopOnDrop {
        services: std::sync::Weak<ExecutionServices>,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }
    impl Drop for StopOnDrop {
        fn drop(&mut self) {
            self.dropped.store(true, std::sync::atomic::Ordering::SeqCst);
            if let Some(services) = self.services.upgrade() {
                let _ = services.stop();
            }
        }
    }

    let runtime = create_runtime();
    let services = Arc::new(
        ExecutionServices::builder()
            .enable_blocking()
            .enable_cpu()
            .enable_tokio_blocking()
            .enable_io()
            .runtime(runtime.handle().clone())
            .blocking_pool_size(1)
            .cpu_threads(1)
            .build()
            .expect("services should build"),
    );
    services.shutdown();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut on_drop = Some(StopOnDrop {
        services: Arc::downgrade(&services),
        dropped: Arc::clone(&dropped),
    });
    let result = services.submit_blocking(move || {
        drop(on_drop.take());
        Ok::<(), io::Error>(())
    });
    assert!(matches!(
        result,
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    runtime.block_on(async {
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .expect("services should terminate before the diagnostic timeout");
    });
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
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (blocking_started_tx, blocking_started_rx) = mpsc::channel();
    let (blocking_release_tx, blocking_release_rx) = mpsc::channel();
    let mut blocking_gate = GateRelease(Some(blocking_release_tx));
    let (cpu_started_tx, cpu_started_rx) = mpsc::channel();
    let (cpu_release_tx, cpu_release_rx) = mpsc::channel();
    let mut cpu_gate = GateRelease(Some(cpu_release_tx));

    services
        .submit_blocking(move || {
            blocking_started_tx.send(()).expect("blocking task should start");
            blocking_release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("blocking task release gate should open");
            Ok::<(), io::Error>(())
        })
        .expect("blocking task should be accepted");
    services
        .submit_cpu(move || {
            cpu_started_tx.send(()).expect("CPU task should start");
            cpu_release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("CPU task release gate should open");
            Ok::<(), io::Error>(())
        })
        .expect("CPU task should be accepted");
    blocking_started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("blocking task should signal that it started");
    cpu_started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("CPU task should signal that it started");
    services.shutdown();

    runtime.block_on(async {
        let waiter = services.await_termination();
        pin!(waiter);
        select! {
            result = &mut waiter => panic!("held tasks should keep termination pending: {result:?}"),
            _ = async {
                yield_now().await;
                blocking_gate.release();
                cpu_gate.release();
            } => {}
        }
        timeout(TEST_TIMEOUT, &mut waiter)
            .await
            .expect("services should terminate before the diagnostic timeout");
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
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("services should build");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    services
        .submit_blocking(move || {
            started_tx.send(()).expect("task should signal start");
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("blocking task release gate should open");
            Ok::<(), io::Error>(())
        })
        .expect("blocking task should be accepted");
    started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("blocking task should signal that it started");
    let (io_gate_tx, io_gate_rx) = oneshot::channel();
    let io_release_tx = gate.sender();
    services
        .spawn_io(async move {
            io_gate_rx.await.expect("IO gate should open");
            spawn_blocking(move || {
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
        assert!(timeout(Duration::from_millis(20), &mut wait).await.is_err());
        io_gate_tx.send(()).expect("IO gate should open");
        let completed = timeout(Duration::from_secs(2), &mut wait).await;
        if completed.is_err() {
            gate.release();
            let _ = timeout(Duration::from_secs(2), &mut wait).await;
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
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime.handle().clone())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("services should build");
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    services
        .submit_blocking(move || {
            started_tx.send(()).expect("task should signal start");
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("blocking task release gate should open");
            Ok::<(), io::Error>(())
        })
        .expect("blocking task should be accepted");
    started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("blocking task should signal that it started");
    services.shutdown();

    runtime.block_on(async {
        let mut wait = Box::pin(services.await_termination());
        assert!(timeout(Duration::from_millis(20), &mut wait).await.is_err());
        drop(wait);
        let sentinel = spawn_blocking(|| 42);
        let completed = timeout(Duration::from_secs(2), sentinel).await;
        gate.release();
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .expect("services should terminate before the diagnostic timeout");
        assert!(completed.is_ok(), "cancelled wait retained a blocking worker");
    });
}

#[tokio_test]
async fn test_execution_services_submit_tokio_blocking_and_io_tasks() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(Handle::current())
        .build()
        .expect("execution services should be created");

    let blocking = services
        .submit_tokio_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))
        .expect("tokio blocking domain should accept callable");
    let io = services
        .spawn_io(async { Ok::<usize, io::Error>(42) })
        .expect("io domain should accept future");

    assert_eq!(
        timeout(TEST_TIMEOUT, async { blocking.await })
            .await
            .expect("Tokio blocking task should finish before the diagnostic timeout")
            .expect("tokio blocking task should complete successfully"),
        42,
    );
    assert_eq!(
        timeout(TEST_TIMEOUT, io)
            .await
            .expect("IO task should finish before the diagnostic timeout")
            .expect("io task should complete successfully"),
        42
    );
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_execution_services_submit_tokio_runnable_and_tracked_callable() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(Handle::current())
        .build()
        .expect("execution services should be created");
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
        timeout(TEST_TIMEOUT, async { callable.await })
            .await
            .expect("Tokio blocking callable should finish before the diagnostic timeout")
            .expect("tokio blocking tracked callable should complete"),
        42,
    );

    services.shutdown();
    assert!(!services.is_running());
    assert!(services.is_not_running());
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
    assert!(services.is_terminated());
}

#[tokio_test]
async fn test_execution_services_stop_aggregates_reports() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(Handle::current())
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (blocking_started_sender, blocking_started_receiver) = mpsc::channel();
    let (blocking_release_sender, blocking_release_receiver) = mpsc::channel();
    let mut blocking_gate = GateRelease(Some(blocking_release_sender));

    let running = services
        .submit_tracked_blocking(move || {
            blocking_started_sender
                .send(())
                .expect("blocking task should report that it started");
            blocking_release_receiver
                .recv_timeout(TEST_TIMEOUT)
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
    let mut tokio_blocking_gate = GateRelease(Some(tokio_blocking_release_sender));

    let blocking = services
        .submit_tracked_tokio_blocking(move || {
            tokio_blocking_started_sender
                .send(())
                .expect("Tokio blocking task should report that it started");
            tokio_blocking_release_receiver
                .recv_timeout(TEST_TIMEOUT)
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
    timeout(TEST_TIMEOUT, io_started_receiver)
        .await
        .expect("IO future should start before the diagnostic timeout")
        .expect("IO future should start before stop");

    let report = services.stop();
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Stopping);
    assert!(services.is_stopping());
    let blocking_report = report.blocking.expect("blocking enabled");
    let io_report = report.io.expect("IO enabled");
    let tokio_blocking_report = report.tokio_blocking.expect("Tokio blocking enabled");
    assert_eq!(blocking_report.queued, 1);
    assert_eq!(blocking_report.running, 1);
    assert_eq!(blocking_report.cancelled, 1);
    assert!(tokio_blocking_report.running >= 1);
    assert_eq!(io_report.running, 1);
    assert_eq!(io_report.cancelled, 1);

    blocking_gate.release();
    tokio_blocking_gate.release();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");

    let cpu_report = report.cpu.expect("CPU enabled");
    assert_eq!(cpu_report.queued, 0);
    assert_eq!(cpu_report.running, 0);
    assert_eq!(cpu_report.cancelled, 0);
    assert!(services.is_not_running());
    assert!(services.is_terminated());
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running blocking task should finish after release")
        .expect("running blocking task should complete");
    let queued_result = timeout(TEST_TIMEOUT, async { queued.await })
        .await
        .expect("queued blocking task should finish before the diagnostic timeout");
    assert!(matches!(queued_result, Err(TaskExecutionError::Cancelled)));
    let blocking_result = timeout(TEST_TIMEOUT, async { blocking.await })
        .await
        .expect("Tokio blocking task should finish before the diagnostic timeout");
    assert!(matches!(blocking_result, Ok(()) | Err(TaskExecutionError::Cancelled)));
    let io_result = timeout(TEST_TIMEOUT, io)
        .await
        .expect("IO task should finish before the diagnostic timeout");
    assert!(matches!(io_result, Err(TaskExecutionError::Cancelled)));
}

#[tokio_test]
async fn test_execution_services_shutdown_rejects_new_tasks() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(Handle::current())
        .build()
        .expect("execution services should be created");

    services.shutdown();
    let result = services.spawn_io(async { Ok::<(), io::Error>(()) });

    assert!(matches!(
        result,
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Terminated);
}

#[tokio_test]
async fn test_execution_services_shutdown_rejects_submissions_in_every_domain() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(Handle::current())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");

    services.shutdown();
    assert!(matches!(
        services.submit_blocking(|| Ok::<(), io::Error>(())),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert!(matches!(
        services.submit_cpu(|| Ok::<(), io::Error>(())),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert!(matches!(
        services.submit_tokio_blocking(|| Ok::<(), io::Error>(())),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert!(matches!(
        services.spawn_io(async { Ok::<(), io::Error>(()) }),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_execution_services_stop_keeps_all_domains_closed_on_repeated_shutdown() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(Handle::current())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");

    let _stop_report = services.stop();
    services.shutdown();
    assert!(matches!(
        services.submit_blocking(|| Ok::<(), io::Error>(())),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert!(matches!(
        services.submit_cpu(|| Ok::<(), io::Error>(())),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert!(matches!(
        services.submit_tokio_blocking(|| Ok::<(), io::Error>(())),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert!(matches!(
        services.spawn_io(async { Ok::<(), io::Error>(()) }),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_execution_services_stop_intent_survives_shutdown() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(Handle::current())
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .expect("execution services should be created");
    let (started_sender, started_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let mut gate = GateRelease(Some(release_sender));

    services
        .submit_blocking(move || {
            started_sender
                .send(())
                .expect("blocking task should report that it started");
            release_receiver
                .recv_timeout(TEST_TIMEOUT)
                .expect("blocking task release gate should open");
            Ok::<(), io::Error>(())
        })
        .expect("blocking domain should accept the task");
    started_receiver
        .recv_timeout(TEST_TIMEOUT)
        .expect("blocking task should signal that it started");

    services.shutdown();
    let _report = services.stop();
    services.shutdown();
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Stopping);
    assert!(services.is_stopping());

    gate.release();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
    assert_eq!(services.lifecycle(), ExecutorServiceLifecycle::Terminated);
}

#[test]
fn test_execution_services_rejects_after_concurrent_shutdown_and_stop() {
    let runtime = create_runtime();
    for _ in 0..8 {
        let services = Arc::new(
            ExecutionServices::builder()
                .enable_blocking()
                .enable_cpu()
                .enable_tokio_blocking()
                .enable_io()
                .runtime(runtime.handle().clone())
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
            Ok::<(), ExecutionServicesSubmissionError>(())
        }));
        let stop_services = Arc::clone(&services);
        let stop_barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            stop_barrier.wait();
            let _stop_report = stop_services.stop();
            Ok::<(), ExecutionServicesSubmissionError>(())
        }));

        for worker in workers {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(ExecutionServicesSubmissionError::Rejected {
                    source: SubmissionError::Shutdown,
                })) => {}
                Ok(Err(error)) => panic!("unexpected concurrent submission error: {error:?}"),
                Err(_) => panic!("concurrent submission or shutdown worker panicked"),
            }
        }

        assert!(matches!(
            services.submit_blocking(|| Ok::<(), io::Error>(())),
            Err(ExecutionServicesSubmissionError::Rejected {
                source: SubmissionError::Shutdown
            })
        ));
        assert!(matches!(
            services.submit_cpu(|| Ok::<(), io::Error>(())),
            Err(ExecutionServicesSubmissionError::Rejected {
                source: SubmissionError::Shutdown
            })
        ));
        assert!(matches!(
            services.submit_tokio_blocking(|| Ok::<(), io::Error>(())),
            Err(ExecutionServicesSubmissionError::Rejected {
                source: SubmissionError::Shutdown
            })
        ));
        assert!(matches!(
            services.spawn_io(async { Ok::<(), io::Error>(()) }),
            Err(ExecutionServicesSubmissionError::Rejected {
                source: SubmissionError::Shutdown
            })
        ));
    }
    drop(runtime);
}

#[test]
fn test_stop_waits_for_queued_cpu_task_cleanup_on_calling_thread() {
    struct CleanupProbe {
        cleanup_started: mpsc::Sender<std::thread::ThreadId>,
        release: mpsc::Receiver<()>,
        dropped: Arc<std::sync::atomic::AtomicUsize>,
        released: Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for CleanupProbe {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = self.cleanup_started.send(std::thread::current().id());
            if self.release.recv_timeout(TEST_TIMEOUT).is_ok() {
                self.released.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    let runtime = create_runtime();
    let services = Arc::new(
        ExecutionServices::builder()
            .enable_cpu()
            .runtime(runtime.handle().clone())
            .cpu_threads(1)
            .build()
            .expect("execution services should be created"),
    );
    let (running_started_tx, running_started_rx) = mpsc::channel();
    let (running_release_tx, running_release_rx) = mpsc::channel();
    let mut running_gate = GateRelease(Some(running_release_tx));
    let running = services
        .submit_tracked_cpu(move || {
            running_started_tx
                .send(())
                .expect("running CPU task should report start");
            running_release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("running CPU task release gate should open");
            Ok::<(), io::Error>(())
        })
        .expect("running CPU task should be accepted");
    running_started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("CPU worker should start the first task");

    let (cleanup_started_tx, cleanup_started_rx) = mpsc::channel();
    let (cleanup_release_tx, cleanup_release_rx) = mpsc::channel();
    let mut cleanup_gate = GateRelease(Some(cleanup_release_tx));
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let task_ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut probe = Some(CleanupProbe {
        cleanup_started: cleanup_started_tx,
        release: cleanup_release_rx,
        dropped: Arc::clone(&dropped),
        released: Arc::clone(&released),
    });
    let task_ran_by_callable = Arc::clone(&task_ran);
    let queued = services
        .submit_tracked_cpu(move || {
            drop(probe.take());
            task_ran_by_callable.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<(), io::Error>(())
        })
        .expect("second CPU task should be queued");

    let (stop_returned_tx, stop_returned_rx) = mpsc::channel();
    let stop_services = Arc::clone(&services);
    let stop_thread = std::thread::spawn(move || {
        let thread_id = std::thread::current().id();
        let report = stop_services.stop();
        stop_returned_tx
            .send((thread_id, report))
            .expect("stop result receiver should remain available");
    });

    let cleanup_thread = cleanup_started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("queued task cleanup should start during stop");
    assert!(matches!(stop_returned_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert!(matches!(
        services.submit_cpu_callable(|| Ok::<(), io::Error>(())),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));

    cleanup_gate.release();
    let (stop_thread_id, report) = stop_returned_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("stop should return after queued task cleanup is released");
    assert_eq!(cleanup_thread, stop_thread_id);
    assert!(released.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(task_ran.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(report.cpu.expect("CPU domain should be enabled").queued, 1);

    running_gate.release();
    runtime.block_on(async {
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .expect("services should terminate before the diagnostic timeout");
        timeout(TEST_TIMEOUT, async { running.await })
            .await
            .expect("running CPU task should finish after release")
            .expect("running CPU task should complete successfully");
        assert!(matches!(
            timeout(TEST_TIMEOUT, async { queued.await })
                .await
                .expect("queued CPU task should resolve after stop"),
            Err(TaskExecutionError::Cancelled)
        ));
    });
    stop_thread.join().expect("stop thread should exit cleanly");
}

#[tokio_test]
async fn test_shutdown_rejects_child_submission_from_accepted_io_task() {
    let services = Arc::new(
        ExecutionServices::builder()
            .enable_io()
            .enable_cpu()
            .runtime(Handle::current())
            .cpu_threads(1)
            .build()
            .expect("execution services should be created"),
    );
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let child_services = Arc::clone(&services);
    let parent = services
        .spawn_io(async move {
            started_tx.send(()).expect("parent task should report that it started");
            timeout(TEST_TIMEOUT, release_rx)
                .await
                .expect("IO parent release gate should open before the diagnostic timeout")
                .expect("parent task should be released");
            let result = child_services.submit_cpu_callable(|| Ok::<u8, io::Error>(42));
            assert!(matches!(
                result,
                Err(ExecutionServicesSubmissionError::Rejected {
                    source: SubmissionError::Shutdown
                })
            ));
            Ok::<(), io::Error>(())
        })
        .expect("IO parent task should be accepted");

    timeout(TEST_TIMEOUT, started_rx)
        .await
        .expect("parent task should start before the diagnostic timeout")
        .expect("parent task should report that it started");
    services.shutdown();
    release_tx.send(()).expect("parent task should be released");
    timeout(TEST_TIMEOUT, parent)
        .await
        .expect("parent task should complete before the diagnostic timeout")
        .expect("parent task should complete");
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
    assert!(services.is_terminated());
}

#[tokio_test]
async fn test_drain_producer_before_shutdown_allows_child_submission() {
    let services = Arc::new(
        ExecutionServices::builder()
            .enable_io()
            .enable_cpu()
            .runtime(Handle::current())
            .cpu_threads(1)
            .build()
            .expect("execution services should be created"),
    );
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let child_services = Arc::clone(&services);
    let producer = services
        .spawn_io(async move {
            started_tx
                .send(())
                .expect("producer task should report that it started");
            timeout(TEST_TIMEOUT, release_rx)
                .await
                .expect("IO producer release gate should open before the diagnostic timeout")
                .expect("producer task should be released");
            let child = child_services
                .submit_cpu_callable(|| Ok::<u8, io::Error>(42))
                .expect("child CPU task should be accepted before shutdown");
            let child_result = timeout(TEST_TIMEOUT, async { child.await })
                .await
                .expect("child CPU task should finish before the diagnostic timeout")
                .expect("child CPU task should complete");
            Ok::<u8, io::Error>(child_result)
        })
        .expect("IO producer task should be accepted");

    timeout(TEST_TIMEOUT, started_rx)
        .await
        .expect("producer task should start before the diagnostic timeout")
        .expect("producer task should report that it started");
    release_tx.send(()).expect("producer task should be released");
    assert_eq!(
        timeout(TEST_TIMEOUT, producer)
            .await
            .expect("producer task should complete before the diagnostic timeout")
            .expect("producer task should complete"),
        42
    );
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
    assert!(services.is_terminated());
}
