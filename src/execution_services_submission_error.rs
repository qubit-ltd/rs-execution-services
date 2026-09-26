// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Submission error returned by the execution-services facade.

use qubit_executor::service::SubmissionError;
use thiserror::Error;

use crate::ExecutionDomain;

/// Error returned when a facade submission cannot be accepted.
///
/// # Examples
///
/// ```
/// use qubit_execution_services::ExecutionDomain;
/// use qubit_execution_services::ExecutionServices;
/// use qubit_execution_services::ExecutionServicesSubmissionError;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let services = ExecutionServices::builder()
///     .enable_blocking()
///     .build()?;
/// let result = services.spawn_io(async {
///     Ok::<(), std::io::Error>(())
/// });
///
/// assert!(matches!(
///     result,
///     Err(ExecutionServicesSubmissionError::DomainDisabled {
///         domain: ExecutionDomain::Io,
///     })
/// ));
/// services.shutdown();
/// # Ok(())
/// # }
/// ```
#[must_use]
#[derive(Debug, Clone, Error)]
pub enum ExecutionServicesSubmissionError {
    /// The requested execution domain was not enabled when the facade was
    /// built.
    #[error("execution domain {domain:?} is disabled")]
    DomainDisabled {
        /// Domain selected by the submission method.
        domain: ExecutionDomain,
    },

    /// An enabled domain rejected the task, or the facade has shut down.
    #[error("execution domain rejected the task: {source}")]
    Rejected {
        /// Rejection returned by the facade gate or underlying execution
        /// domain.
        #[from]
        source: SubmissionError,
    },
}
