/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
// qubit-style: allow multiple-public-types
use std::{
    future::Future,
    pin::Pin,
    thread,
    time::Duration,
};

use qubit_function::{
    Callable,
    Runnable,
};
use thiserror::Error;

use qubit_executor::TaskHandle;

use super::{
    BlockingExecutorService,
    BlockingExecutorServiceBuilder,
    ExecutorService,
    RayonExecutorService,
    RayonExecutorServiceBuildError,
    RayonExecutorServiceBuilder,
    RayonTaskHandle,
    RejectedExecution,
    ShutdownReport,
    TokioBlockingExecutorService,
    TokioIoExecutorService,
    TokioTaskHandle,
};

/// Error returned when [`ExecutionServicesBuilder`] cannot build the facade.
#[derive(Debug, Error)]
pub enum ExecutionServicesBuildError {
    /// The blocking executor-service configuration is invalid.
    #[error("failed to build blocking executor service: {source}")]
    Blocking {
        /// Error returned by the underlying blocking executor builder.
        #[from]
        source: super::ThreadPoolBuildError,
    },

    /// The CPU executor-service configuration is invalid.
    #[error("failed to build cpu executor service: {source}")]
    Cpu {
        /// Error returned by the underlying Rayon executor builder.
        #[from]
        source: RayonExecutorServiceBuildError,
    },
}

/// Aggregate report returned by [`ExecutionServices::shutdown_now`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionServicesShutdownReport {
    /// Shutdown report for the blocking executor domain.
    pub blocking: ShutdownReport,
    /// Shutdown report for the CPU executor domain.
    pub cpu: ShutdownReport,
    /// Shutdown report for the Tokio blocking executor domain.
    pub tokio_blocking: ShutdownReport,
    /// Shutdown report for the Tokio async IO executor domain.
    pub io: ShutdownReport,
}

impl ExecutionServicesShutdownReport {
    /// Returns the total queued task count across all execution domains.
    ///
    /// # Returns
    ///
    /// The sum of every domain's queued-task count.
    #[inline]
    pub const fn total_queued(&self) -> usize {
        self.blocking.queued + self.cpu.queued + self.tokio_blocking.queued + self.io.queued
    }

    /// Returns the total running task count across all execution domains.
    ///
    /// # Returns
    ///
    /// The sum of every domain's running-task count.
    #[inline]
    pub const fn total_running(&self) -> usize {
        self.blocking.running + self.cpu.running + self.tokio_blocking.running + self.io.running
    }

    /// Returns the total cancellation count across all execution domains.
    ///
    /// # Returns
    ///
    /// The sum of every domain's cancelled-task count.
    #[inline]
    pub const fn total_cancelled(&self) -> usize {
        self.blocking.cancelled
            + self.cpu.cancelled
            + self.tokio_blocking.cancelled
            + self.io.cancelled
    }
}

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
        Ok(ExecutionServices {
            blocking,
            cpu,
            tokio_blocking,
            io,
        })
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

/// Unified facade exposing separate execution domains through one owner.
///
/// The facade does not implement a single scheduling core. Instead it routes
/// work to one of four dedicated execution domains:
///
/// - `blocking`: synchronous tasks that may block an OS thread.
/// - `cpu`: CPU-bound synchronous tasks backed by Rayon.
/// - `tokio_blocking`: blocking tasks routed through Tokio `spawn_blocking`.
/// - `io`: async futures spawned on Tokio's async runtime.
pub struct ExecutionServices {
    /// Managed service for synchronous tasks that may block OS threads.
    blocking: BlockingExecutorService,
    /// Managed service for CPU-bound synchronous tasks.
    cpu: RayonExecutorService,
    /// Tokio-backed blocking service using `spawn_blocking`.
    tokio_blocking: TokioBlockingExecutorService,
    /// Tokio-backed async service for Future-based tasks.
    io: TokioIoExecutorService,
}

impl ExecutionServices {
    /// Creates an execution-services facade with default builder settings.
    ///
    /// # Returns
    ///
    /// `Ok(ExecutionServices)` if the default blocking and CPU domains build
    /// successfully.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServicesBuildError`] if the default builder
    /// configuration is rejected.
    #[inline]
    pub fn new() -> Result<Self, ExecutionServicesBuildError> {
        Self::builder().build()
    }

    /// Creates a builder for configuring the execution-services facade.
    ///
    /// # Returns
    ///
    /// A builder configured with CPU-parallelism defaults.
    #[inline]
    pub fn builder() -> ExecutionServicesBuilder {
        ExecutionServicesBuilder::default()
    }

    /// Returns the blocking execution domain.
    ///
    /// # Returns
    ///
    /// A shared reference to the blocking executor service.
    #[inline]
    pub fn blocking(&self) -> &BlockingExecutorService {
        &self.blocking
    }

    /// Returns the CPU execution domain.
    ///
    /// # Returns
    ///
    /// A shared reference to the Rayon-backed CPU executor service.
    #[inline]
    pub fn cpu(&self) -> &RayonExecutorService {
        &self.cpu
    }

    /// Returns the Tokio blocking execution domain.
    ///
    /// # Returns
    ///
    /// A shared reference to the Tokio blocking executor service.
    #[inline]
    pub fn tokio_blocking(&self) -> &TokioBlockingExecutorService {
        &self.tokio_blocking
    }

    /// Returns the Tokio async IO execution domain.
    ///
    /// # Returns
    ///
    /// A shared reference to the Tokio IO executor service.
    #[inline]
    pub fn io(&self) -> &TokioIoExecutorService {
        &self.io
    }

    /// Submits a blocking runnable task to the blocking domain.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable task that may block an OS thread.
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedExecution`] if the blocking domain refuses the task.
    #[inline]
    pub fn submit_blocking<T, E>(&self, task: T) -> Result<TaskHandle<(), E>, RejectedExecution>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.blocking.submit(task)
    }

    /// Submits a blocking callable task to the blocking domain.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable task that may block an OS thread.
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedExecution`] if the blocking domain refuses the task.
    #[inline]
    pub fn submit_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, RejectedExecution>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.blocking.submit_callable(task)
    }

    /// Submits a CPU-bound runnable task to the Rayon domain.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable CPU task.
    ///
    /// # Returns
    ///
    /// A [`RayonTaskHandle`] for the accepted CPU task.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedExecution`] if the CPU domain refuses the task.
    #[inline]
    pub fn submit_cpu<T, E>(&self, task: T) -> Result<RayonTaskHandle<(), E>, RejectedExecution>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.cpu.submit(task)
    }

    /// Submits a CPU-bound callable task to the Rayon domain.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable CPU task.
    ///
    /// # Returns
    ///
    /// A [`RayonTaskHandle`] for the accepted CPU task.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedExecution`] if the CPU domain refuses the task.
    #[inline]
    pub fn submit_cpu_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<RayonTaskHandle<R, E>, RejectedExecution>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.cpu.submit_callable(task)
    }

    /// Submits a blocking runnable task to Tokio `spawn_blocking`.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable task to execute on Tokio's blocking pool.
    ///
    /// # Returns
    ///
    /// A [`TokioTaskHandle`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedExecution`] if the Tokio blocking domain refuses the
    /// task.
    #[inline]
    pub fn submit_tokio_blocking<T, E>(
        &self,
        task: T,
    ) -> Result<TokioTaskHandle<(), E>, RejectedExecution>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.tokio_blocking.submit(task)
    }

    /// Submits a blocking callable task to Tokio `spawn_blocking`.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable task to execute on Tokio's blocking pool.
    ///
    /// # Returns
    ///
    /// A [`TokioTaskHandle`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedExecution`] if the Tokio blocking domain refuses the
    /// task.
    #[inline]
    pub fn submit_tokio_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TokioTaskHandle<R, E>, RejectedExecution>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.tokio_blocking.submit_callable(task)
    }

    /// Spawns an async IO or Future-based task on Tokio's async runtime.
    ///
    /// # Parameters
    ///
    /// * `future` - Future to execute on Tokio's async scheduler.
    ///
    /// # Returns
    ///
    /// A [`TokioTaskHandle`] for the accepted async task.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedExecution`] if the Tokio IO domain refuses the task.
    #[inline]
    pub fn spawn_io<F, R, E>(&self, future: F) -> Result<TokioTaskHandle<R, E>, RejectedExecution>
    where
        F: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.io.spawn(future)
    }

    /// Requests graceful shutdown for every execution domain.
    pub fn shutdown(&self) {
        self.blocking.shutdown();
        self.cpu.shutdown();
        self.tokio_blocking.shutdown();
        self.io.shutdown();
    }

    /// Requests immediate shutdown for every execution domain.
    ///
    /// # Returns
    ///
    /// A per-domain aggregate report describing queued, running, and cancelled
    /// work observed during shutdown.
    pub fn shutdown_now(&self) -> ExecutionServicesShutdownReport {
        ExecutionServicesShutdownReport {
            blocking: self.blocking.shutdown_now(),
            cpu: self.cpu.shutdown_now(),
            tokio_blocking: self.tokio_blocking.shutdown_now(),
            io: self.io.shutdown_now(),
        }
    }

    /// Returns whether every execution domain has been shut down.
    ///
    /// # Returns
    ///
    /// `true` only if all execution domains no longer accept new tasks.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        self.blocking.is_shutdown()
            && self.cpu.is_shutdown()
            && self.tokio_blocking.is_shutdown()
            && self.io.is_shutdown()
    }

    /// Returns whether every execution domain has terminated.
    ///
    /// # Returns
    ///
    /// `true` only after all execution domains have terminated.
    #[inline]
    pub fn is_terminated(&self) -> bool {
        self.blocking.is_terminated()
            && self.cpu.is_terminated()
            && self.tokio_blocking.is_terminated()
            && self.io.is_terminated()
    }

    /// Waits until every execution domain has terminated.
    ///
    /// # Returns
    ///
    /// A future that resolves after all execution domains have terminated.
    pub fn await_termination(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            self.blocking.await_termination().await;
            self.cpu.await_termination().await;
            self.tokio_blocking.await_termination().await;
            self.io.await_termination().await;
        })
    }
}

/// Returns the default pool size for blocking and CPU domains.
///
/// # Returns
///
/// The available CPU parallelism, or `1` if it cannot be detected.
fn default_pool_size() -> usize {
    thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}
