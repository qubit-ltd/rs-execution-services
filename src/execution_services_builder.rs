/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Builder for the execution-services facade.

use std::{
    thread,
    time::Duration,
};

use super::{
    BlockingExecutorService,
    BlockingExecutorServiceBuilder,
    ExecutionServices,
    ExecutionServicesBuildError,
    RayonExecutorService,
    RayonExecutorServiceBuilder,
    TokioBlockingExecutorService,
    TokioIoExecutorService,
};

/// Builder for [`ExecutionServices`].
///
/// The builder exposes blocking-pool options by delegating to
/// [`BlockingExecutorServiceBuilder`] and CPU-pool options by delegating to
/// [`RayonExecutorServiceBuilder`]. Tokio-backed domains are created with their
/// default constructors because they do not currently expose custom builders.
#[derive(Debug, Clone)]
pub struct ExecutionServicesBuilder {
    /// Builder for the blocking executor domain.
    blocking: BlockingExecutorServiceBuilder,
    /// Builder for the CPU executor domain.
    cpu: RayonExecutorServiceBuilder,
}

impl ExecutionServicesBuilder {
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
        let blocking = self
            .blocking
            .build()
            .map_err(|source| ExecutionServicesBuildError::Blocking { source })?;
        let cpu = self
            .cpu
            .build()
            .map_err(|source| ExecutionServicesBuildError::Cpu { source })?;
        let tokio_blocking = TokioBlockingExecutorService::new();
        let io = TokioIoExecutorService::new();
        Ok(ExecutionServices::from_parts(blocking, cpu, tokio_blocking, io))
    }
}

impl Default for ExecutionServicesBuilder {
    /// Creates a builder with CPU-parallelism defaults.
    ///
    /// # Returns
    ///
    /// A builder configured with available parallelism for both blocking and
    /// CPU domains.
    fn default() -> Self {
        let pool_size = default_pool_size();
        Self {
            blocking: BlockingExecutorService::builder().pool_size(pool_size),
            cpu: RayonExecutorService::builder().num_threads(pool_size),
        }
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
