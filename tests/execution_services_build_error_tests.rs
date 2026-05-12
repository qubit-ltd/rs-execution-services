/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ExecutionServicesBuildError`](qubit_execution_services::ExecutionServicesBuildError).

use std::error::Error;

use qubit_execution_services::{
    ExecutionServices,
    ExecutionServicesBuildError,
};

/// Test build error variants expose the underlying builder failure.
#[test]
fn test_execution_services_build_error_display_and_source() {
    let blocking_error = match ExecutionServices::builder()
        .blocking_maximum_pool_size(0)
        .build()
    {
        Ok(_) => panic!("invalid blocking pool size should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        blocking_error,
        ExecutionServicesBuildError::Blocking { .. }
    ));
    assert!(
        blocking_error
            .to_string()
            .starts_with("failed to build blocking executor service:"),
    );
    assert!(blocking_error.source().is_some());

    let cpu_error = match ExecutionServices::builder().cpu_threads(0).build() {
        Ok(_) => panic!("invalid cpu thread count should fail"),
        Err(error) => error,
    };

    assert!(matches!(cpu_error, ExecutionServicesBuildError::Cpu { .. }));
    assert!(
        cpu_error
            .to_string()
            .starts_with("failed to build cpu executor service:"),
    );
    assert!(cpu_error.source().is_some());
}
