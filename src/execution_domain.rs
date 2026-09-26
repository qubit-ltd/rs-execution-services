// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Execution domains exposed by the facade.

/// Identifies one independently configurable execution domain.
///
/// # Examples
///
/// ```
/// use qubit_execution_services::ExecutionDomain;
/// use qubit_execution_services::ExecutionServices;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let services = ExecutionServices::builder()
///     .enable_blocking()
///     .build()?;
///
/// assert!(services.has_domain(ExecutionDomain::Blocking));
/// assert!(!services.has_domain(ExecutionDomain::Io));
/// services.shutdown();
/// assert!(services.is_terminated());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionDomain {
    /// Managed pool for synchronous tasks that may block OS threads.
    Blocking,
    /// Rayon pool for CPU-bound synchronous tasks.
    Cpu,
    /// Tokio `spawn_blocking` service.
    TokioBlocking,
    /// Tokio async task service.
    Io,
}
