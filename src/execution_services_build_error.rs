// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Build error for the execution-services facade.

use qubit_executor::service::ExecutorServiceBuilderError;
use qubit_rayon_executor::RayonExecutorServiceBuildError;
use thiserror::Error;

/// Error returned when [`super::ExecutionServicesBuilder`] cannot build the
/// facade.
///
/// # Examples
///
/// ```
/// use qubit_execution_services::ExecutionServices;
/// use qubit_execution_services::ExecutionServicesBuildError;
///
/// let result = ExecutionServices::builder()
///     .enable_cpu()
///     .cpu_threads(0)
///     .build();
/// assert!(matches!(result, Err(ExecutionServicesBuildError::Cpu { .. })));
/// ```
#[must_use]
#[derive(Debug, Error)]
pub enum ExecutionServicesBuildError {
    /// No execution domains were enabled in the builder.
    #[error("at least one execution domain must be enabled")]
    NoDomains,

    /// A Tokio-backed execution domain was enabled without a runtime handle.
    #[error("a Tokio runtime handle is required for the enabled Tokio domains")]
    MissingTokioRuntime,

    /// The blocking executor-service configuration is invalid.
    #[error("failed to build blocking executor service: {source}")]
    Blocking {
        /// Error returned by the underlying blocking executor builder.
        #[from]
        source: ExecutorServiceBuilderError,
    },

    /// The CPU executor-service configuration is invalid.
    #[error("failed to build cpu executor service: {source}")]
    Cpu {
        /// Error returned by the underlying Rayon executor builder.
        #[from]
        source: RayonExecutorServiceBuildError,
    },
}
