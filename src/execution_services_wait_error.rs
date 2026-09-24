// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

/// Error raised when an asynchronous bridge fails while waiting for a managed
/// execution domain to terminate.
///
/// # Examples
///
/// ```
/// use qubit_execution_services::ExecutionServices;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let runtime = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()?;
/// let services = ExecutionServices::builder(runtime.handle().clone())
///     .blocking_pool_size(1)
///     .cpu_threads(1)
///     .build()?;
/// services.shutdown();
/// runtime.block_on(services.await_termination())?;
/// # Ok(())
/// # }
/// ```
#[must_use]
#[derive(Debug, Error)]
pub enum ExecutionServicesWaitError {
    /// The blocking executor termination waiter failed to join.
    #[error("blocking executor termination waiter failed: {source}")]
    BlockingWaitJoin {
        /// Failure returned when joining the blocking-domain waiter.
        #[source]
        source: tokio::task::JoinError,
    },

    /// The CPU executor termination waiter failed to join.
    #[error("CPU executor termination waiter failed: {source}")]
    CpuWaitJoin {
        /// Failure returned when joining the CPU-domain waiter.
        #[source]
        source: tokio::task::JoinError,
    },
}
