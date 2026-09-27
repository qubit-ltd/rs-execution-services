// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests waiting submission and cancellation across execution domains.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use qubit_execution_services::ExecutionDomain;
use qubit_execution_services::ExecutionServices;
use qubit_execution_services::ExecutionServicesSubmissionError;
use qubit_executor::service::SubmissionError;
use tokio::join;
use tokio::pin;
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::test as tokio_test;
use tokio::time::timeout;

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn waits_for_blocking_capacity_and_calls_factory_only_on_attempts() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = services
        .submit_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    services.submit_blocking_callable(|| Ok::<(), io::Error>(())).unwrap();
    let calls = AtomicUsize::new(0);
    let waiting = services.submit_blocking_callable_wait(|| {
        calls.fetch_add(1, Ordering::SeqCst);
        || Ok::<usize, io::Error>(42)
    });
    pin!(waiting);
    assert!(timeout(Duration::from_millis(20), &mut waiting).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .get()
            .unwrap(),
        42
    );
    running.get().unwrap();
    services.shutdown();
    services.await_termination().await;
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn waits_for_cpu_tokio_blocking_and_io_capacity() {
    // CPU domain.
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let waiting = services.submit_cpu_callable_wait(|| || Ok::<usize, io::Error>(42));
    pin!(waiting);
    assert!(timeout(Duration::from_millis(20), &mut waiting).await.is_err());
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .get()
            .unwrap(),
        42
    );
    running.get().unwrap();
    services.shutdown();
    services.await_termination().await;

    // Tokio blocking domain.
    let services = ExecutionServices::builder()
        .enable_tokio_blocking()
        .runtime(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let running = services
        .submit_tokio_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let waiting = services.submit_tokio_blocking_callable_wait(|| || Ok::<usize, io::Error>(42));
    pin!(waiting);
    assert!(timeout(Duration::from_millis(20), &mut waiting).await.is_err());
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .get()
            .unwrap(),
        42
    );
    running.get().unwrap();
    services.shutdown();
    services.await_termination().await;

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
    let waiting = services.spawn_io_wait(|| async { Ok::<usize, io::Error>(42) });
    pin!(waiting);
    assert!(timeout(Duration::from_millis(20), &mut waiting).await.is_err());
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap()
            .await
            .unwrap(),
        42
    );
    running.await.unwrap();
    services.shutdown();
    services.await_termination().await;
}

#[tokio_test]
async fn disabled_domain_does_not_call_factory_and_shutdown_rejects_waiter() {
    let disabled = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .build()
        .unwrap();
    let calls = AtomicUsize::new(0);
    let result = disabled
        .submit_cpu_callable_wait(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            || Ok::<(), io::Error>(())
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
    disabled.await_termination().await;

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
    let waiting = services.spawn_io_wait(|| async { Ok::<(), io::Error>(()) });
    pin!(waiting);
    assert!(timeout(Duration::from_millis(20), &mut waiting).await.is_err());
    services.shutdown();
    let result = timeout(Duration::from_secs(1), waiting).await.unwrap();
    assert!(matches!(
        result,
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    let _report = services.stop();
    services.await_termination().await;
}

#[tokio_test]
async fn cancelling_a_full_capacity_wait_does_not_call_factory_again() {
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
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = Arc::clone(&calls);
    let mut waiting = Box::pin(services.spawn_io_wait(move || {
        factory_calls.fetch_add(1, Ordering::SeqCst);
        async { Ok::<(), io::Error>(()) }
    }));
    assert!(timeout(Duration::from_millis(20), &mut waiting).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(waiting);
    release_tx.send(()).unwrap();
    running.await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    services.shutdown();
    services.await_termination().await;
}

#[tokio_test]
async fn two_waiters_compete_for_capacity_without_losing_wakeups() {
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

    let calls = Arc::new(AtomicUsize::new(0));
    let first_calls = Arc::clone(&calls);
    let second_calls = Arc::clone(&calls);
    let first = services.spawn_io_wait(move || {
        first_calls.fetch_add(1, Ordering::SeqCst);
        async { Ok::<usize, io::Error>(1) }
    });
    let second = services.spawn_io_wait(move || {
        second_calls.fetch_add(1, Ordering::SeqCst);
        async { Ok::<usize, io::Error>(2) }
    });
    pin!(first);
    pin!(second);
    timeout(Duration::from_secs(1), async {
        while calls.load(Ordering::SeqCst) < 2 {
            select! {
                _ = &mut first => panic!("waiter must remain pending while IO capacity is occupied"),
                _ = &mut second => panic!("waiter must remain pending while IO capacity is occupied"),
                _ = yield_now() => {}
            }
        }
    })
    .await
    .expect("both waiters should make an initial saturated attempt");
    release_tx.send(()).unwrap();
    let (first, second) = timeout(Duration::from_secs(1), async { join!(&mut first, &mut second) })
        .await
        .expect("both waiters should eventually acquire capacity");
    assert_eq!(first.unwrap().await.unwrap(), 1);
    assert_eq!(second.unwrap().await.unwrap(), 2);
    running.await.unwrap();
    services.shutdown();
    services.await_termination().await;
}
