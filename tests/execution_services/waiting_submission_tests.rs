// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests waiting submission and cancellation across execution domains.

use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Poll;
use std::time::Duration;

use qubit_execution_services::ExecutionDomain;
use qubit_execution_services::ExecutionServices;
use qubit_execution_services::ExecutionServicesSubmissionError;
use qubit_executor::TaskExecutionError;
use qubit_executor::service::SubmissionError;
use tokio::join;
use tokio::pin;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::test as tokio_test;
use tokio::time::timeout;

use super::GateRelease;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn poll_once<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PollCounterFuture {
    polls: Arc<AtomicUsize>,
    _drop_counter: DropCounter,
}

impl Future for PollCounterFuture {
    type Output = Result<usize, io::Error>;

    fn poll(self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(42))
    }
}

#[tokio_test]
async fn test_wait_apis_accept_non_clone_one_shot_tasks_for_every_domain() {
    let runtime = Handle::current();
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime)
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .unwrap();

    let blocking_value = String::from("blocking");
    let blocking = services
        .submit_blocking_callable_wait(move || Ok::<usize, io::Error>(blocking_value.len()))
        .await
        .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { blocking.await })
            .await
            .expect("blocking task should finish before the diagnostic timeout")
            .unwrap(),
        8
    );

    let cpu_value = String::from("cpu");
    let cpu = services
        .submit_cpu_callable_wait(move || Ok::<usize, io::Error>(cpu_value.len()))
        .await
        .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { cpu.await })
            .await
            .expect("CPU task should finish before the diagnostic timeout")
            .unwrap(),
        3
    );

    let tokio_blocking_value = String::from("tokio-blocking");
    let tokio_blocking = services
        .submit_tokio_blocking_callable_wait(move || Ok::<usize, io::Error>(tokio_blocking_value.len()))
        .await
        .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { tokio_blocking.await })
            .await
            .expect("Tokio blocking task should finish before the diagnostic timeout")
            .unwrap(),
        14
    );

    let io_value = String::from("io");
    let io = services
        .spawn_io_wait(async move { Ok::<usize, io::Error>(io_value.len()) })
        .await
        .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, io)
            .await
            .expect("IO task should finish before the diagnostic timeout")
            .unwrap(),
        2
    );

    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_waits_for_blocking_capacity_without_recreating_the_task() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    services.submit_blocking_callable(|| Ok::<(), io::Error>(())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let payload = String::from("one-shot blocking task");
    let waiting = services.submit_blocking_callable_wait(move || {
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<usize, io::Error>(payload.len())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    gate.release();
    let handle = timeout(TEST_TIMEOUT, waiting).await.unwrap().unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { handle.await })
            .await
            .expect("accepted blocking task should finish before the diagnostic timeout")
            .unwrap(),
        22
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_blocking_full_capacity_wait_wakes_on_shutdown() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let queued = services.submit_blocking_callable(|| Ok::<(), io::Error>(())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    services.shutdown();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, async { queued.await })
        .await
        .expect("queued task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("blocking service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_blocking_full_capacity_wait_wakes_on_stop() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let queued = services.submit_blocking_callable(|| Ok::<(), io::Error>(())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let _stop_report = services.stop();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    let _queued_result = timeout(TEST_TIMEOUT, async { queued.await })
        .await
        .expect("queued task should finish before the diagnostic timeout");
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("blocking service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_waits_for_cpu_tokio_blocking_and_io_capacity() {
    // CPU domain.
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let waiting = services.submit_cpu_callable_wait(move || {
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<usize, io::Error>(42)
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    gate.release();
    let handle = timeout(TEST_TIMEOUT, waiting).await.unwrap().unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { handle.await })
            .await
            .expect("accepted CPU task should finish before the diagnostic timeout")
            .unwrap(),
        42
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");

    // Tokio blocking domain.
    let services = ExecutionServices::builder()
        .enable_tokio_blocking()
        .runtime(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_tokio_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let waiting = services.submit_tokio_blocking_callable_wait(|| Ok::<usize, io::Error>(42));
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    gate.release();
    let handle = timeout(TEST_TIMEOUT, waiting).await.unwrap().unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { handle.await })
            .await
            .expect("accepted Tokio blocking task should finish before the diagnostic timeout")
            .unwrap(),
        42
    );
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");

    // IO domain.
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let running = services
        .spawn_io(async move {
            release_rx.await.unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let future = PollCounterFuture {
        polls: Arc::clone(&polls),
        _drop_counter: DropCounter(Arc::clone(&drops)),
    };
    let waiting = services.spawn_io_wait(future);
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, waiting).await.unwrap().unwrap().await.unwrap(),
        42,
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("accepted CPU task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dropping_accepted_cpu_wait_handle_does_not_cancel_task() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let (finished_tx, finished_rx) = mpsc::channel();
    let handle = services
        .submit_cpu_callable_wait(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            finished_tx.send(()).unwrap();
            Ok::<(), io::Error>(())
        })
        .await
        .unwrap();
    drop(handle);
    started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("accepted task should start");
    gate.release();
    finished_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("dropping result handle must not cancel task");
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("CPU service should terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cpu_full_capacity_wait_wakes_on_shutdown() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_cpu_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    services.shutdown();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("CPU service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cpu_full_capacity_wait_wakes_on_stop() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_cpu_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let _stop_report = services.stop();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("CPU service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tokio_blocking_full_capacity_wait_wakes_on_shutdown() {
    let services = ExecutionServices::builder()
        .enable_tokio_blocking()
        .runtime(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_tokio_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_tokio_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    services.shutdown();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("Tokio blocking service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tokio_blocking_full_capacity_wait_wakes_on_stop() {
    let services = ExecutionServices::builder()
        .enable_tokio_blocking()
        .runtime(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_tokio_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_tokio_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let _stop_report = services.stop();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("Tokio blocking service must terminate");
}

#[tokio_test]
async fn test_wait_prioritizes_shutdown_over_disabled_domain() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .build()
        .expect("CPU-only services should build");
    services.shutdown();

    let blocking_result = services.submit_blocking_callable_wait(|| Ok::<(), io::Error>(())).await;
    let cpu_result = services.submit_cpu_callable_wait(|| Ok::<(), io::Error>(())).await;
    let tokio_blocking_result = services
        .submit_tokio_blocking_callable_wait(|| Ok::<(), io::Error>(()))
        .await;
    let io_result = services.spawn_io_wait(async { Ok::<(), io::Error>(()) }).await;

    for result in [
        blocking_result.map(|_| ()),
        cpu_result.map(|_| ()),
        tokio_blocking_result.map(|_| ()),
        io_result.map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(ExecutionServicesSubmissionError::Rejected {
                source: SubmissionError::Shutdown,
            })
        ));
    }
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_disabled_domain_does_not_run_task_and_shutdown_rejects_waiter() {
    let disabled = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .build()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let result = disabled
        .submit_cpu_callable_wait(move || {
            task_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), io::Error>(())
        })
        .await;
    let error = match result {
        Ok(_) => panic!("disabled CPU domain must reject submission"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ExecutionServicesSubmissionError::DomainDisabled {
            domain: ExecutionDomain::Cpu
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    disabled.shutdown();
    timeout(TEST_TIMEOUT, disabled.await_termination())
        .await
        .expect("disabled services should terminate before the diagnostic timeout");

    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (release_tx, _release_rx) = oneshot::channel::<()>();
    let _running = services
        .spawn_io(async move {
            std::future::pending::<()>().await;
            let _ = release_tx;
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;
    let drop_count = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drop_count));
    let waiting = services.spawn_io_wait(async move {
        let _drop_counter = drop_counter;
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    services.shutdown();
    let result = timeout(TEST_TIMEOUT, waiting).await.unwrap();
    assert!(matches!(
        result,
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    let _report = services.stop();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_cancelling_a_full_capacity_wait_drops_the_unaccepted_task_once() {
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let running = services
        .spawn_io(async move {
            release_rx.await.unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;
    let drop_count = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drop_count));
    let mut waiting = Box::pin(services.spawn_io_wait(async move {
        let _drop_counter = drop_counter;
        Ok::<(), io::Error>(())
    }));
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(drop_count.load(Ordering::SeqCst), 0);
    drop(waiting);
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    release_tx.send(()).unwrap();
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running CPU task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_cancelling_a_full_cpu_capacity_wait_drops_the_unaccepted_task_once() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let (finished_tx, finished_rx) = mpsc::channel();
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            finished_tx.send(()).unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("CPU worker should start the capacity blocker");

    let calls = Arc::new(AtomicUsize::new(0));
    let call_count = Arc::clone(&calls);
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drops));
    let mut waiting = Box::pin(services.submit_cpu_callable_wait(move || {
        let _drop_counter = drop_counter;
        call_count.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    }));

    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(waiting);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    gate.release();
    finished_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("capacity blocker should finish after release");
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("CPU service should terminate");
}

#[tokio_test]
async fn test_stopping_a_full_capacity_wait_rejects_and_drops_the_unaccepted_task_once() {
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (_release_tx, release_rx) = oneshot::channel::<()>();
    let running = services
        .spawn_io(async move {
            release_rx.await.unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;

    let drop_count = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drop_count));
    let waiting = services.spawn_io_wait(async move {
        let _drop_counter = drop_counter;
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(drop_count.load(Ordering::SeqCst), 0);

    let _stop_report = services.stop();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.unwrap(),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    let cancelled = timeout(TEST_TIMEOUT, running)
        .await
        .expect("stopped CPU task should finish before the diagnostic timeout");
    assert!(matches!(cancelled, Err(TaskExecutionError::Cancelled)));
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_two_waiters_compete_for_capacity_without_losing_wakeups() {
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let running = services
        .spawn_io(async move {
            release_rx.await.unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;

    let first = services.spawn_io_wait(async { Ok::<usize, io::Error>(1) });
    let second = services.spawn_io_wait(async { Ok::<usize, io::Error>(2) });
    pin!(first);
    pin!(second);
    assert!(poll_once(first.as_mut()).await.is_pending());
    assert!(poll_once(second.as_mut()).await.is_pending());
    release_tx.send(()).unwrap();
    let (first, second) = timeout(TEST_TIMEOUT, async { join!(&mut first, &mut second) })
        .await
        .expect("both waiters should eventually acquire capacity");
    let first = first.unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, first)
            .await
            .expect("first waiting task should finish before the diagnostic timeout")
            .unwrap(),
        1
    );
    let second = second.unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, second)
            .await
            .expect("second waiting task should finish before the diagnostic timeout")
            .unwrap(),
        2
    );
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running IO task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_io_parent_waiting_on_same_full_domain_remains_pending() {
    let services = Arc::new(
        ExecutionServices::builder()
            .enable_io()
            .runtime(Handle::current())
            .io_task_capacity(NonZeroUsize::new(1).expect("capacity is nonzero"))
            .build()
            .expect("IO services should build"),
    );
    let (pending_tx, pending_rx) = oneshot::channel();
    let child_services = Arc::clone(&services);
    let parent = services
        .spawn_io(async move {
            let child_wait = child_services.spawn_io_wait(async { Ok::<(), io::Error>(()) });
            tokio::pin!(child_wait);
            let first_poll = poll_once(child_wait.as_mut()).await;
            pending_tx
                .send(first_poll.is_pending())
                .expect("test receiver should remain alive");
            let child = child_wait.await.map_err(io::Error::other)?;
            child.await.map_err(io::Error::other)
        })
        .expect("parent task should be accepted");

    assert!(
        timeout(TEST_TIMEOUT, pending_rx)
            .await
            .expect("parent should report the child wait state")
            .expect("parent should report its state")
    );
    assert_eq!(services.snapshot().io.unwrap().accepted_unfinished, 1);

    let _stop_report = services.stop();
    let _parent_result = timeout(TEST_TIMEOUT, parent)
        .await
        .expect("stop should release the accepted parent task");
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("stopped IO service should terminate");
    assert!(services.is_terminated());
}
