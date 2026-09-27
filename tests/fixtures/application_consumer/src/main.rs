// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public API consumer regression; this fixture is not a production downstream.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_execution_services::ExecutionServices;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    runtime.block_on(async {
        let services = ExecutionServices::builder()
            .enable_blocking()
            .enable_cpu()
            .enable_tokio_blocking()
            .enable_io()
            .runtime(runtime.handle().clone())
            .blocking_pool_size(2)
            .blocking_queue_capacity(2)
            .cpu_threads(2)
            .io_task_capacity(NonZeroUsize::new(1).expect("capacity is nonzero"))
            .build()?;
        let blocking = services.submit_blocking_callable(|| Ok::<u8, io::Error>(40))?;
        let cpu = services.submit_cpu_callable(|| Ok::<u8, io::Error>(41))?;
        let tokio_blocking = services.submit_tokio_blocking_callable(|| Ok::<u8, io::Error>(42))?;
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
        let future_polls = Arc::new(AtomicUsize::new(0));
        let captured_polls = Arc::clone(&future_polls);
        let waiting_io = services.spawn_io_wait(async move {
            captured_polls.fetch_add(1, Ordering::SeqCst);
            Ok::<u8, io::Error>(45)
        });
        tokio::task::yield_now().await;
        assert_eq!(future_polls.load(Ordering::SeqCst), 0);
        release_tx.send(()).expect("first IO producer should be released");
        assert_eq!(first_io.await?, 44);
        assert_eq!(waiting_io.await?.await?, 45);
        assert_eq!(future_polls.load(Ordering::SeqCst), 1);
        assert_eq!(services.snapshot().io.unwrap().accepted_unfinished, 0);

        services.shutdown();
        assert!(services.submit_blocking(|| Ok::<(), io::Error>(())).is_err());
        assert!(services.submit_cpu(|| Ok::<(), io::Error>(())).is_err());
        assert!(services.submit_tokio_blocking(|| Ok::<(), io::Error>(())).is_err());
        assert!(services.spawn_io(async { Ok::<(), io::Error>(()) }).is_err());
        services.await_termination().await;
        assert!(services.is_terminated());
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
