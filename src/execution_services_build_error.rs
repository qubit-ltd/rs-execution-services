// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Build error for the execution-services facade.

use thiserror::Error;

use super::{
    ExecutorServiceBuilderError,
    RayonExecutorServiceBuildError,
};

/// Error returned when [`super::ExecutionServicesBuilder`] cannot build the
/// facade.
#[derive(Debug, Error)]
pub enum ExecutionServicesBuildError {
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
