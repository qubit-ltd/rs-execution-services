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
    sync::Arc,
};

use qubit_executor::{
    TaskHandle,
    TrackedTask,
};
use qubit_function::{
    Callable,
    Runnable,
};
use qubit_thread_pool::{
    ThreadPool,
    ThreadPoolBuilder,
};
use qubit_tokio_executor::TokioExecutorService;

use super::{
    ExecutionServicesBuildError,
    ExecutionServicesBuilder,
    ExecutionServicesStopReport,
    ExecutorService,
    ExecutorServiceLifecycle,
    RayonExecutorService,
    RayonTaskHandle,
    SubmissionError,
    TokioIoExecutorService,
    TokioTaskHandle,
};

/// Default managed service for synchronous tasks that may block an OS thread.
pub type BlockingExecutorService = ThreadPool;

/// Builder alias for configuring [`BlockingExecutorService`].
pub type BlockingExecutorServiceBuilder = ThreadPoolBuilder;

/// Tokio-backed blocking executor service routed through `spawn_blocking`.
pub type TokioBlockingExecutorService = TokioExecutorService;

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
    blocking: Arc<BlockingExecutorService>,
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
            blocking: Arc::new(blocking),
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
        self.blocking.as_ref()
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
    /// `Ok(())` if the blocking domain accepts the task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the blocking domain refuses the task.
    #[inline]
    pub fn submit_blocking<T, E>(&self, task: T) -> Result<(), SubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.blocking.submit(task)
    }

    /// Submits a blocking runnable task and returns a tracked handle.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable task that may block an OS thread.
    ///
    /// # Returns
    ///
    /// A [`TrackedTask`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the blocking domain refuses the task.
    #[inline]
    pub fn submit_tracked_blocking<T, E>(
        &self,
        task: T,
    ) -> Result<TrackedTask<(), E>, SubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.blocking.submit_tracked(task)
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
    /// Returns [`SubmissionError`] if the blocking domain refuses the task.
    #[inline]
    pub fn submit_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, SubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.blocking.submit_callable(task)
    }

    /// Submits a blocking callable task and returns a tracked handle.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable task that may block an OS thread.
    ///
    /// # Returns
    ///
    /// A [`TrackedTask`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the blocking domain refuses the task.
    #[inline]
    pub fn submit_tracked_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TrackedTask<R, E>, SubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.blocking.submit_tracked_callable(task)
    }

    /// Submits a CPU-bound runnable task to the Rayon domain.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable CPU task.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the CPU domain accepts the task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the CPU domain refuses the task.
    #[inline]
    pub fn submit_cpu<T, E>(&self, task: T) -> Result<(), SubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.cpu.submit(task)
    }

    /// Submits a CPU-bound runnable task and returns a tracked handle.
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
    /// Returns [`SubmissionError`] if the CPU domain refuses the task.
    #[inline]
    pub fn submit_tracked_cpu<T, E>(
        &self,
        task: T,
    ) -> Result<RayonTaskHandle<(), E>, SubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.cpu.submit_tracked(task)
    }

    /// Submits a CPU-bound callable task to the Rayon domain.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable CPU task.
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] for the accepted CPU task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the CPU domain refuses the task.
    #[inline]
    pub fn submit_cpu_callable<C, R, E>(&self, task: C) -> Result<TaskHandle<R, E>, SubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.cpu.submit_callable(task)
    }

    /// Submits a CPU-bound callable task and returns a tracked handle.
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
    /// Returns [`SubmissionError`] if the CPU domain refuses the task.
    #[inline]
    pub fn submit_tracked_cpu_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<RayonTaskHandle<R, E>, SubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.cpu.submit_tracked_callable(task)
    }

    /// Submits a blocking runnable task to Tokio `spawn_blocking`.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable task to execute on Tokio's blocking pool.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the Tokio blocking domain accepts the task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the Tokio blocking domain refuses the
    /// task.
    #[inline]
    pub fn submit_tokio_blocking<T, E>(&self, task: T) -> Result<(), SubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.tokio_blocking.submit(task)
    }

    /// Submits a blocking runnable task to Tokio and returns a tracked handle.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable task to execute on Tokio's blocking pool.
    ///
    /// # Returns
    ///
    /// A [`TrackedTask`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the Tokio blocking domain refuses the
    /// task.
    #[inline]
    pub fn submit_tracked_tokio_blocking<T, E>(
        &self,
        task: T,
    ) -> Result<TrackedTask<(), E>, SubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.tokio_blocking.submit_tracked(task)
    }

    /// Submits a blocking callable task to Tokio `spawn_blocking`.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable task to execute on Tokio's blocking pool.
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the Tokio blocking domain refuses the
    /// task.
    #[inline]
    pub fn submit_tokio_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, SubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.tokio_blocking.submit_callable(task)
    }

    /// Submits a blocking callable task to Tokio and returns a tracked handle.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable task to execute on Tokio's blocking pool.
    ///
    /// # Returns
    ///
    /// A [`TrackedTask`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the Tokio blocking domain refuses the
    /// task.
    #[inline]
    pub fn submit_tracked_tokio_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TrackedTask<R, E>, SubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.tokio_blocking.submit_tracked_callable(task)
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
    /// Returns [`SubmissionError`] if the Tokio IO domain refuses the task.
    #[inline]
    pub fn spawn_io<F, R, E>(&self, future: F) -> Result<TokioTaskHandle<R, E>, SubmissionError>
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

    /// Requests abrupt stop for every execution domain.
    ///
    /// # Returns
    ///
    /// A per-domain aggregate report describing queued, running, and cancelled
    /// work observed during shutdown.
    pub fn stop(&self) -> ExecutionServicesStopReport {
        ExecutionServicesStopReport {
            blocking: self.blocking.stop(),
            cpu: self.cpu.stop(),
            tokio_blocking: self.tokio_blocking.stop(),
            io: self.io.stop(),
        }
    }

    /// Returns the aggregate lifecycle state.
    ///
    /// # Returns
    ///
    /// [`ExecutorServiceLifecycle::Terminated`] if all domains have
    /// terminated; [`ExecutorServiceLifecycle::Stopping`] if any domain is
    /// stopping; [`ExecutorServiceLifecycle::ShuttingDown`] if any domain is no
    /// longer running; otherwise [`ExecutorServiceLifecycle::Running`].
    pub fn lifecycle(&self) -> ExecutorServiceLifecycle {
        let lifecycles = [
            self.blocking.lifecycle(),
            self.cpu.lifecycle(),
            self.tokio_blocking.lifecycle(),
            self.io.lifecycle(),
        ];
        if lifecycles
            .iter()
            .all(|state| *state == ExecutorServiceLifecycle::Terminated)
        {
            ExecutorServiceLifecycle::Terminated
        } else if lifecycles.contains(&ExecutorServiceLifecycle::Stopping) {
            ExecutorServiceLifecycle::Stopping
        } else if lifecycles
            .iter()
            .any(|state| *state != ExecutorServiceLifecycle::Running)
        {
            ExecutorServiceLifecycle::ShuttingDown
        } else {
            ExecutorServiceLifecycle::Running
        }
    }

    /// Returns whether every execution domain is running.
    ///
    /// # Returns
    ///
    /// `true` only if all execution domains are running.
    #[inline]
    pub fn is_running(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::Running
    }

    /// Returns whether any execution domain is gracefully shutting down.
    ///
    /// # Returns
    ///
    /// `true` when the aggregate lifecycle is
    /// [`ExecutorServiceLifecycle::ShuttingDown`].
    #[inline]
    pub fn is_shutting_down(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::ShuttingDown
    }

    /// Returns whether any execution domain is stopping abruptly.
    ///
    /// # Returns
    ///
    /// `true` when the aggregate lifecycle is
    /// [`ExecutorServiceLifecycle::Stopping`].
    #[inline]
    pub fn is_stopping(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::Stopping
    }

    /// Returns whether the facade is no longer fully running.
    ///
    /// # Returns
    ///
    /// `true` after any execution domain starts shutdown, stop, or has already
    /// terminated.
    #[inline]
    pub fn is_not_running(&self) -> bool {
        self.lifecycle() != ExecutorServiceLifecycle::Running
    }

    /// Returns whether every execution domain has terminated.
    ///
    /// # Returns
    ///
    /// `true` only after all execution domains have terminated.
    #[inline]
    pub fn is_terminated(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::Terminated
    }

    /// Waits until every execution domain has terminated.
    ///
    /// # Returns
    ///
    /// A future that resolves after all execution domains have terminated.
    pub fn await_termination(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let blocking = Arc::clone(&self.blocking);
            let cpu = self.cpu.clone();
            let tokio_blocking = self.tokio_blocking.clone();
            let blocking_wait = tokio::task::spawn_blocking(move || blocking.wait_termination());
            let cpu_wait = tokio::task::spawn_blocking(move || cpu.wait_termination());
            let tokio_blocking_wait =
                tokio::task::spawn_blocking(move || tokio_blocking.wait_termination());
            self.io.await_termination().await;
            let _ = blocking_wait.await;
            let _ = cpu_wait.await;
            let _ = tokio_blocking_wait.await;
        })
    }
}
