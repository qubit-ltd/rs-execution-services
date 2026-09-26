// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Best-effort monitoring snapshot for the enabled execution domains.

/// Independent best-effort snapshots of each enabled execution domain.
///
/// The fields are sampled independently. They do not represent one atomic
/// instant and must not be summed or used to synchronize submissions.
///
/// # Examples
///
/// ```
/// use qubit_execution_services::ExecutionServices;
///
/// let runtime = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
/// let services = ExecutionServices::builder()
///     .enable_blocking()
///     .blocking_pool_size(1)
///     .build().expect("services should build");
/// assert!(services.snapshot().blocking.is_some());
/// services.shutdown();
/// runtime.block_on(services.await_termination());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionServicesSnapshot {
    /// Blocking thread pool snapshot, when enabled.
    pub blocking: Option<qubit_thread_pool::ThreadPoolStats>,
    /// CPU executor snapshot, when enabled.
    pub cpu: Option<qubit_rayon_executor::RayonExecutorServiceStats>,
    /// Tokio blocking executor snapshot, when enabled.
    pub tokio_blocking: Option<qubit_tokio_executor::TokioExecutorServiceStats>,
    /// Tokio IO executor snapshot, when enabled.
    pub io: Option<qubit_tokio_executor::TokioIoExecutorServiceStats>,
}
