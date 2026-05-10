/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ExecutionServicesBuilder`](qubit_execution_services::ExecutionServicesBuilder).

use std::{io, time::Duration};

use qubit_execution_services::{
    ExecutionServices, ExecutionServicesBuildError, ExecutorService, ExecutorServiceLifecycle,
};

fn create_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime for execution services builder tests")
}

#[test]
fn test_execution_services_builder_rejects_invalid_blocking_domain() {
    let error = match ExecutionServices::builder()
        .blocking_maximum_pool_size(0)
        .build()
    {
        Ok(_) => panic!("builder should reject invalid blocking domain"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        ExecutionServicesBuildError::Blocking { .. }
    ));
}

#[test]
fn test_execution_services_builder_rejects_invalid_cpu_domain() {
    let error = match ExecutionServices::builder().cpu_threads(0).build() {
        Ok(_) => panic!("builder should reject invalid cpu domain"),
        Err(error) => error,
    };

    assert!(matches!(error, ExecutionServicesBuildError::Cpu { .. }));
}

#[test]
fn test_execution_services_builder_options_and_accessors() {
    let services = ExecutionServices::builder()
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
        .cpu_thread_name_prefix("exec-cpu")
        .cpu_stack_size(2 * 1024 * 1024)
        .build()
        .expect("execution services should be created with custom options");

    assert!(!services.blocking().is_not_running());
    assert!(!services.cpu().is_not_running());
    assert!(!services.tokio_blocking().is_not_running());
    assert!(!services.io().is_not_running());
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
    create_runtime().block_on(services.await_termination());
}
