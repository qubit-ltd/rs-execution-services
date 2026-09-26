// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
mod internal;
// Coordinates facade-wide shutdown, stop, and termination waits.
mod lifecycle;
// Routes submissions to the enabled execution domains.
mod submission;

use std::sync::Arc;

use qubit_rayon_executor::RayonExecutorService;
use qubit_thread_pool::ThreadPool;
use qubit_tokio_executor::TokioExecutorService;
use qubit_tokio_executor::TokioIoExecutorService;

use self::internal::execution_services_admission::ExecutionServicesAdmission;
use super::ExecutionDomain;
use super::ExecutionServicesBuilder;

/// Unified facade exposing separate execution domains through one owner.
///
/// The facade does not implement a single scheduling core. Instead it routes
/// work to any enabled subset of four dedicated execution domains:
///
/// - `blocking`: synchronous tasks that may block an OS thread.
/// - `cpu`: CPU-bound synchronous tasks backed by Rayon.
/// - `tokio_blocking`: blocking tasks routed through Tokio `spawn_blocking`.
/// - `io`: async futures spawned on Tokio's async runtime.
///
/// The `blocking` and `cpu` domains own separate worker pools. Both Tokio
/// domains use the caller's runtime; `tokio_blocking` uses that runtime's
/// shared `spawn_blocking` pool. Its task capacity limits accepted, unfinished
/// tasks; it is not a thread reservation or a setting for execution
/// concurrency. Enabling multiple domains does not establish a process-wide
/// resource budget.
///
/// Each submission checks facade admission before delegating to its domain.
/// The facade releases its admission lock before invoking a domain submission
/// or dropping a rejected task. A submission that passed admission may overlap
/// shutdown or stop; its domain decides whether to accept or reject it. Once
/// either operation returns, all enabled domains have been closed to new work.
///
/// # Examples
///
/// ```
/// use qubit_execution_services::ExecutionServices;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let services = ExecutionServices::builder()
///     .enable_blocking()
///     .blocking_pool_size(1)
///     .build()?;
/// let task = services.submit_blocking_callable(|| Ok::<u8, std::io::Error>(42))?;
/// assert_eq!(task.get()?, 42);
/// services.shutdown();
/// let runtime = tokio::runtime::Builder::new_current_thread().build()?;
/// runtime.block_on(services.await_termination());
/// # Ok(())
/// # }
/// ```
pub struct ExecutionServices {
    /// Facade-wide gate shared by submissions and shutdown operations.
    admission: ExecutionServicesAdmission,
    /// Managed service for synchronous tasks that may block OS threads.
    blocking: Option<Arc<ThreadPool>>,
    /// Managed service for CPU-bound synchronous tasks.
    cpu: Option<RayonExecutorService>,
    /// Tokio-backed blocking service using `spawn_blocking`.
    tokio_blocking: Option<TokioExecutorService>,
    /// Tokio-backed async service for Future-based tasks.
    io: Option<TokioIoExecutorService>,
}

impl ExecutionServices {
    /// Creates an empty builder for selecting execution domains.
    #[inline]
    pub fn builder() -> ExecutionServicesBuilder {
        ExecutionServicesBuilder::new()
    }

    /// Returns whether the requested execution domain was enabled.
    #[must_use]
    #[inline]
    pub fn has_domain(&self, domain: ExecutionDomain) -> bool {
        match domain {
            ExecutionDomain::Blocking => self.blocking.is_some(),
            ExecutionDomain::Cpu => self.cpu.is_some(),
            ExecutionDomain::TokioBlocking => self.tokio_blocking.is_some(),
            ExecutionDomain::Io => self.io.is_some(),
        }
    }

    /// Creates an execution-services facade from its enabled execution domains.
    ///
    /// # Parameters
    ///
    /// * `blocking` - Blocking executor domain.
    /// * `cpu` - CPU-bound executor domain.
    /// * `tokio_blocking` - Tokio blocking executor domain.
    /// * `io` - Tokio async IO executor domain.
    ///
    /// # Returns
    ///
    /// An execution-services facade owning all supplied domains.
    pub(crate) fn from_parts(
        blocking: Option<ThreadPool>,
        cpu: Option<RayonExecutorService>,
        tokio_blocking: Option<TokioExecutorService>,
        io: Option<TokioIoExecutorService>,
    ) -> Self {
        Self {
            admission: ExecutionServicesAdmission::new(),
            blocking: blocking.map(Arc::new),
            cpu,
            tokio_blocking,
            io,
        }
    }
}
