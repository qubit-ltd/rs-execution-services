// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for public execution-services submission errors.

use std::error::Error;

use qubit_execution_services::ExecutionDomain;
use qubit_execution_services::ExecutionServicesSubmissionError;
use qubit_executor::service::SubmissionError;

#[test]
fn test_rejected_error_preserves_domain_display_and_source() {
    let error = ExecutionServicesSubmissionError::rejected(ExecutionDomain::Cpu, SubmissionError::Saturated);

    assert!(matches!(
        error,
        ExecutionServicesSubmissionError::Rejected {
            domain: ExecutionDomain::Cpu,
            source: SubmissionError::Saturated,
        }
    ));
    assert_eq!(
        error.to_string(),
        "execution domain Cpu rejected the task: task rejected because the executor service is saturated"
    );
    assert!(Error::source(&error).is_some());
}
