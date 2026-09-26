// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Builder for the execution-services facade.

use std::fmt;
use std::num::NonZeroUsize;
use std::thread;
use std::time::Duration;

use qubit_rayon_executor::RayonExecutorService;
use qubit_rayon_executor::RayonExecutorServiceBuilder;
use qubit_tokio_executor::TokioIoExecutorService;
use tokio::runtime::Handle;

use super::BlockingExecutorService;
use super::BlockingExecutorServiceBuilder;
use super::ExecutionServices;
use super::ExecutionServicesBuildError;
use super::TokioBlockingExecutorService;

/// Maximum number of blocking tasks waiting in the default queue.
const DEFAULT_BLOCKING_QUEUE_CAPACITY: usize = 1024;

/// Maximum unfinished-task capacity for each Tokio-backed domain by default.
const DEFAULT_TOKIO_TASK_CAPACITY: usize = 1024;

/// Builder for explicitly selecting and configuring [`ExecutionServices`]
/// domains.
///
/// The builder exposes blocking-pool options by delegating to
/// [`BlockingExecutorServiceBuilder`] and CPU-pool options by delegating to
/// [`RayonExecutorServiceBuilder`]. It also configures finite accepted-task
/// capacities for the Tokio-backed domains; Tokio runtime settings remain
/// application-owned.
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
/// let services = ExecutionServices::builder()
///     .enable_all(runtime.handle().clone())
///     .blocking_pool_size(1)
///     .cpu_threads(1)
///     .build()?;
/// services.shutdown();
/// # runtime.block_on(services.await_termination());
/// # Ok(())
/// # }
/// ```
#[must_use]
#[derive(Clone)]
pub struct ExecutionServicesBuilder {
    /// Optional Tokio runtime used by enabled Tokio-backed domains.
    runtime: Option<Handle>,
    /// Whether the blocking domain is included.
    blocking_enabled: bool,
    /// Whether the CPU domain is included.
    cpu_enabled: bool,
    /// Whether the Tokio blocking domain is included.
    tokio_blocking_enabled: bool,
    /// Whether the Tokio IO domain is included.
    io_enabled: bool,
    /// Builder for the blocking executor domain.
    blocking: BlockingExecutorServiceBuilder,
    /// Builder for the CPU executor domain.
    cpu: RayonExecutorServiceBuilder,
    /// Maximum accepted unfinished tasks in the Tokio blocking domain.
    tokio_blocking_task_capacity: NonZeroUsize,
    /// Maximum accepted unfinished futures in the Tokio IO domain.
    io_task_capacity: NonZeroUsize,
}

impl fmt::Debug for ExecutionServicesBuilder {
    /// Formats the builder without exposing its runtime handle or pool
    /// settings.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ExecutionServicesBuilder").finish()
    }
}

impl ExecutionServicesBuilder {
    /// Creates an empty builder with default settings for each domain.
    ///
    /// No domain is enabled initially. A Tokio runtime is required only if a
    /// Tokio-backed domain is enabled. The default blocking queue and accepted
    /// task capacities are 1024.
    pub fn new() -> Self {
        let pool_size = default_pool_size();
        Self {
            runtime: None,
            blocking_enabled: false,
            cpu_enabled: false,
            tokio_blocking_enabled: false,
            io_enabled: false,
            blocking: BlockingExecutorService::builder()
                .pool_size(pool_size)
                .queue_capacity(DEFAULT_BLOCKING_QUEUE_CAPACITY),
            cpu: RayonExecutorService::builder().num_threads(pool_size),
            tokio_blocking_task_capacity: NonZeroUsize::new(DEFAULT_TOKIO_TASK_CAPACITY)
                .expect("default Tokio task capacity should be nonzero"),
            io_task_capacity: NonZeroUsize::new(DEFAULT_TOKIO_TASK_CAPACITY)
                .expect("default Tokio task capacity should be nonzero"),
        }
    }

    /// Enables all four execution domains and sets their shared Tokio runtime.
    pub fn enable_all(mut self, runtime: Handle) -> Self {
        self.runtime = Some(runtime);
        self.blocking_enabled = true;
        self.cpu_enabled = true;
        self.tokio_blocking_enabled = true;
        self.io_enabled = true;
        self
    }

    /// Enables the managed blocking execution domain.
    #[inline]
    pub fn enable_blocking(mut self) -> Self {
        self.blocking_enabled = true;
        self
    }

    /// Enables the Rayon-backed CPU execution domain.
    #[inline]
    pub fn enable_cpu(mut self) -> Self {
        self.cpu_enabled = true;
        self
    }

    /// Enables the Tokio `spawn_blocking` execution domain.
    #[inline]
    pub fn enable_tokio_blocking(mut self) -> Self {
        self.tokio_blocking_enabled = true;
        self
    }

    /// Enables the Tokio async IO execution domain.
    #[inline]
    pub fn enable_io(mut self) -> Self {
        self.io_enabled = true;
        self
    }

    /// Sets the runtime used by enabled Tokio-backed execution domains.
    #[inline]
    pub fn runtime(mut self, runtime: Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Sets the maximum accepted unfinished tasks in the Tokio blocking domain.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Nonzero limit for queued and running blocking tasks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn tokio_blocking_task_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.tokio_blocking_task_capacity = capacity;
        self
    }

    /// Sets the maximum accepted unfinished futures in the Tokio IO domain.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Nonzero limit for async futures not yet completed.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn io_task_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.io_task_capacity = capacity;
        self
    }

    /// Sets both the blocking core and maximum pool sizes to the same value.
    ///
    /// # Parameters
    ///
    /// * `pool_size` - Pool size applied as both core and maximum limits.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_pool_size(mut self, pool_size: usize) -> Self {
        self.blocking = self.blocking.pool_size(pool_size);
        self
    }

    /// Sets the blocking core pool size.
    ///
    /// # Parameters
    ///
    /// * `core_pool_size` - Core pool size for the blocking domain.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_core_pool_size(mut self, core_pool_size: usize) -> Self {
        self.blocking = self.blocking.core_pool_size(core_pool_size);
        self
    }

    /// Sets the blocking maximum pool size.
    ///
    /// The blocking pool grows beyond its core size only when its queue is
    /// bounded and full. With `blocking_unbounded_queue()`, tasks continue to
    /// queue after the core size is reached, so this setting alone does not
    /// create burst workers.
    ///
    /// # Parameters
    ///
    /// * `maximum_pool_size` - Maximum pool size for the blocking domain.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_maximum_pool_size(mut self, maximum_pool_size: usize) -> Self {
        self.blocking = self.blocking.maximum_pool_size(maximum_pool_size);
        self
    }

    /// Sets a bounded queue capacity for the blocking domain.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Maximum number of queued blocking tasks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_queue_capacity(mut self, capacity: usize) -> Self {
        self.blocking = self.blocking.queue_capacity(capacity);
        self
    }

    /// Configures the blocking domain to use an unbounded queue.
    ///
    /// Submissions continue to queue after the blocking core size is reached;
    /// increasing `blocking_maximum_pool_size` does not change that behavior.
    /// Use `blocking_queue_capacity` when bounded back pressure and elastic
    /// worker growth are desired.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_unbounded_queue(mut self) -> Self {
        self.blocking = self.blocking.unbounded_queue();
        self
    }

    /// Sets the blocking worker-thread name prefix.
    ///
    /// # Parameters
    ///
    /// * `prefix` - Prefix appended with the worker index.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_thread_name_prefix(mut self, prefix: &str) -> Self {
        self.blocking = self.blocking.thread_name_prefix(prefix);
        self
    }

    /// Sets the blocking worker-thread stack size.
    ///
    /// # Parameters
    ///
    /// * `stack_size` - Stack size in bytes for each blocking worker.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_stack_size(mut self, stack_size: usize) -> Self {
        self.blocking = self.blocking.stack_size(stack_size);
        self
    }

    /// Sets the blocking worker keep-alive timeout.
    ///
    /// # Parameters
    ///
    /// * `keep_alive` - Idle timeout for blocking workers allowed to retire.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.blocking = self.blocking.keep_alive(keep_alive);
        self
    }

    /// Allows blocking core workers to retire after keep-alive timeout.
    ///
    /// # Parameters
    ///
    /// * `allow` - Whether idle blocking core workers may time out.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_allow_core_thread_timeout(mut self, allow: bool) -> Self {
        self.blocking = self.blocking.allow_core_thread_timeout(allow);
        self
    }

    /// Starts all blocking core workers during build.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn blocking_prestart_core_threads(mut self) -> Self {
        self.blocking = self.blocking.prestart_core_threads();
        self
    }

    /// Sets the number of Rayon worker threads in the CPU domain.
    ///
    /// # Parameters
    ///
    /// * `num_threads` - Number of Rayon worker threads.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn cpu_threads(mut self, num_threads: usize) -> Self {
        self.cpu = self.cpu.num_threads(num_threads);
        self
    }

    /// Sets the maximum number of accepted unfinished CPU tasks.
    ///
    /// # Parameters
    ///
    /// * `capacity` - Maximum accepted CPU tasks that have not completed.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn cpu_task_capacity(mut self, capacity: usize) -> Self {
        self.cpu = self.cpu.task_capacity(capacity);
        self
    }

    /// Sets the Rayon worker-thread name prefix in the CPU domain.
    ///
    /// # Parameters
    ///
    /// * `prefix` - Prefix appended with the worker index.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn cpu_thread_name_prefix(mut self, prefix: &str) -> Self {
        self.cpu = self.cpu.thread_name_prefix(prefix);
        self
    }

    /// Sets the Rayon worker-thread stack size in the CPU domain.
    ///
    /// # Parameters
    ///
    /// * `stack_size` - Stack size in bytes for each Rayon worker.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn cpu_stack_size(mut self, stack_size: usize) -> Self {
        self.cpu = self.cpu.stack_size(stack_size);
        self
    }

    /// Builds the configured execution-services facade.
    ///
    /// # Returns
    ///
    /// `Ok(ExecutionServices)` if the blocking and CPU domains build
    /// successfully.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServicesBuildError`] if either the blocking or CPU
    /// domain rejects its builder configuration.
    pub fn build(self) -> Result<ExecutionServices, ExecutionServicesBuildError> {
        let any_enabled = self.blocking_enabled || self.cpu_enabled || self.tokio_blocking_enabled || self.io_enabled;
        if !any_enabled {
            return Err(ExecutionServicesBuildError::NoDomains);
        }
        if (self.tokio_blocking_enabled || self.io_enabled) && self.runtime.is_none() {
            return Err(ExecutionServicesBuildError::MissingTokioRuntime);
        }
        let blocking = if self.blocking_enabled {
            Some(
                self.blocking
                    .build()
                    .map_err(|source| ExecutionServicesBuildError::Blocking { source })?,
            )
        } else {
            None
        };
        let cpu = if self.cpu_enabled {
            Some(
                self.cpu
                    .build()
                    .map_err(|source| ExecutionServicesBuildError::Cpu { source })?,
            )
        } else {
            None
        };
        let runtime = self.runtime;
        let tokio_blocking = if self.tokio_blocking_enabled {
            Some(TokioBlockingExecutorService::with_task_capacity(
                runtime
                    .as_ref()
                    .ok_or(ExecutionServicesBuildError::MissingTokioRuntime)?
                    .clone(),
                self.tokio_blocking_task_capacity,
            ))
        } else {
            None
        };
        let io = if self.io_enabled {
            Some(TokioIoExecutorService::with_task_capacity(
                runtime.ok_or(ExecutionServicesBuildError::MissingTokioRuntime)?,
                self.io_task_capacity,
            ))
        } else {
            None
        };
        Ok(ExecutionServices::from_parts(blocking, cpu, tokio_blocking, io))
    }
}

impl Default for ExecutionServicesBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the default pool size for blocking and CPU domains.
///
/// # Returns
///
/// The available CPU parallelism, or `1` if it cannot be detected.
fn default_pool_size() -> usize {
    thread::available_parallelism().map(usize::from).unwrap_or(1)
}
