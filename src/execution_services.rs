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
use super::ExecutionServicesSnapshot;

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
/// An accepted task may continue running after shutdown is requested, but any
/// later submission it makes through this facade is rejected with
/// [`SubmissionError::Shutdown`](qubit_executor::service::SubmissionError::Shutdown).
/// Stop and await producers that can submit child work before calling
/// [`Self::shutdown`].
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
    ///
    /// # Returns
    ///
    /// A builder with all execution domains disabled.
    #[inline]
    pub fn builder() -> ExecutionServicesBuilder {
        ExecutionServicesBuilder::new()
    }

    /// Creates an execution-services facade from its enabled execution domains.
    ///
    /// # Parameters
    ///
    /// * `blocking` - Optional blocking executor domain.
    /// * `cpu` - Optional CPU-bound executor domain.
    /// * `tokio_blocking` - Optional Tokio blocking executor domain.
    /// * `io` - Optional Tokio async IO executor domain.
    ///
    /// # Returns
    ///
    /// A facade owning every supplied domain and accepting submissions.
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

    /// Returns whether the requested execution domain was enabled.
    ///
    /// # Parameters
    ///
    /// * `domain` - Execution domain to check.
    ///
    /// # Returns
    ///
    /// `true` if the facade owns that domain.
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

    /// Returns an independent best-effort snapshot of each enabled domain.
    ///
    /// The domain snapshots are not simultaneous and are intended for
    /// monitoring, not admission or synchronization decisions.
    ///
    /// # Returns
    ///
    /// Independent statistics for enabled domains. Disabled domains have no
    /// snapshot.
    #[must_use]
    pub fn snapshot(&self) -> ExecutionServicesSnapshot {
        ExecutionServicesSnapshot {
            blocking: self.blocking.as_deref().map(ThreadPool::stats),
            cpu: self.cpu.as_ref().map(RayonExecutorService::stats),
            tokio_blocking: self.tokio_blocking.as_ref().map(TokioExecutorService::stats),
            io: self.io.as_ref().map(TokioIoExecutorService::stats),
        }
    }
}
