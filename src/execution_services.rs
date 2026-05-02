/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    future::Future,
    pin::Pin,
};

use qubit_executor::TaskHandle;
use qubit_function::{
    Callable,
    Runnable,
};

use super::{
    BlockingExecutorService,
    ExecutionServicesBuildError,
    ExecutionServicesBuilder,
    ExecutionServicesShutdownReport,
    ExecutorService,
    RayonExecutorService,
    RayonTaskHandle,
    RejectedExecution,
    TokioBlockingExecutorService,
    TokioIoExecutorService,
    TokioTaskHandle,
};

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
    /// Creates an execution-services facade from its four execution domains.
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
        blocking: BlockingExecutorService,
        cpu: RayonExecutorService,
        tokio_blocking: TokioBlockingExecutorService,
        io: TokioIoExecutorService,
    ) -> Self {
        Self {
            blocking,
            cpu,
            tokio_blocking,
            io,
        }
    }

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
