// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Best-effort monitoring snapshot for the enabled execution domains.

use qubit_rayon_executor::RayonExecutorServiceStats;
use qubit_thread_pool::ThreadPoolStats;
use qubit_tokio_executor::TokioExecutorServiceStats;
use qubit_tokio_executor::TokioIoExecutorServiceStats;

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
    /// `Some` contains the blocking thread-pool snapshot; `None` means the
    /// blocking domain is disabled.
    pub blocking: Option<ThreadPoolStats>,
    /// `Some` contains the CPU executor snapshot; `None` means the CPU domain
    /// is disabled.
    pub cpu: Option<RayonExecutorServiceStats>,
    /// `Some` contains the Tokio blocking executor snapshot; `None` means the
    /// Tokio blocking domain is disabled.
    pub tokio_blocking: Option<TokioExecutorServiceStats>,
    /// `Some` contains the Tokio IO executor snapshot; `None` means the IO
    /// domain is disabled.
    pub io: Option<TokioIoExecutorServiceStats>,
}
