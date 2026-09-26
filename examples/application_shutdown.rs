// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;
use std::sync::Arc;

use qubit_execution_services::ExecutionServices;
use qubit_execution_services::ExecutionServicesSubmissionError;
use qubit_executor::TaskHandle;
use qubit_tokio_executor::TokioTaskHandle;
use tokio::sync::oneshot;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    runtime.block_on(async {
        let services = Arc::new(
            ExecutionServices::builder()
                .enable_blocking()
                .enable_io()
                .runtime(runtime.handle().clone())
                .blocking_pool_size(1)
                .build()?,
        );
        let (ready_tx, ready_rx) = oneshot::channel();
        let (stop_tx, stop_rx) = oneshot::channel();
        let producer_services = Arc::clone(&services);
        let producer = tokio::spawn(async move {
            let blocking: TaskHandle<u8, io::Error> =
                producer_services.submit_blocking_callable(|| Ok::<u8, io::Error>(42))?;
            let io: TokioTaskHandle<u8, io::Error> = producer_services.spawn_io(async { Ok::<u8, io::Error>(43) })?;
            ready_tx.send(()).expect("application must still be waiting");
            let _ = stop_rx.await;
            Ok::<_, ExecutionServicesSubmissionError>((blocking, io))
        });

        ready_rx.await?;
        stop_tx.send(()).expect("producer must still be waiting");
        let (blocking, io) = producer.await??;
        services.shutdown();
        assert_eq!(blocking.await?, 42);
        assert_eq!(io.await?, 43);
        services.await_termination().await;
        assert!(services.is_terminated());
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;
    Ok(())
}
