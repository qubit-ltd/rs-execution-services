// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
mod internal;

use std::future::Future;
use std::sync::Arc;

use qubit_executor::TaskHandle;
use qubit_executor::TrackedTask;
use qubit_executor::service::ExecutorService;
use qubit_executor::service::ExecutorServiceLifecycle;
use qubit_executor::service::SubmissionError;
use qubit_function::Callable;
use qubit_function::Runnable;
use qubit_rayon_executor::RayonExecutorService;
use qubit_rayon_executor::RayonTaskHandle;
use qubit_thread_pool::ThreadPool;
use qubit_thread_pool::ThreadPoolBuilder;
use qubit_tokio_executor::TokioBlockingTaskHandle;
use qubit_tokio_executor::TokioExecutorService;
use qubit_tokio_executor::TokioIoExecutorService;
use qubit_tokio_executor::TokioTaskHandle;
use tokio::join;

use self::internal::execution_services_admission::ExecutionServicesAdmission;
use super::ExecutionDomain;
use super::ExecutionServicesBuilder;
use super::ExecutionServicesStopReport;
use super::ExecutionServicesSubmissionError;

/// Default managed service for synchronous tasks that may block an OS thread.
pub type BlockingExecutorService = ThreadPool;

/// Builder alias for configuring [`BlockingExecutorService`].
pub type BlockingExecutorServiceBuilder = ThreadPoolBuilder;

/// Tokio-backed blocking executor service routed through `spawn_blocking`.
pub type TokioBlockingExecutorService = TokioExecutorService;

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
/// let runtime = tokio::runtime::Builder::new_current_thread()
///     .enable_all()
///     .build()?;
/// let services = ExecutionServices::builder()
///     .enable_blocking()
///     .enable_cpu()
///     .enable_tokio_blocking()
///     .enable_io()
///     .runtime(runtime.handle().clone())
///     .blocking_pool_size(1)
///     .cpu_threads(1)
///     .build()?;
/// services.shutdown();
/// # runtime.block_on(services.await_termination());
/// # Ok(())
/// # }
/// ```
pub struct ExecutionServices {
    /// Facade-wide gate shared by submissions and shutdown operations.
    admission: ExecutionServicesAdmission,
    /// Managed service for synchronous tasks that may block OS threads.
    blocking: Option<Arc<BlockingExecutorService>>,
    /// Managed service for CPU-bound synchronous tasks.
    cpu: Option<RayonExecutorService>,
    /// Tokio-backed blocking service using `spawn_blocking`.
    tokio_blocking: Option<TokioBlockingExecutorService>,
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
        blocking: Option<BlockingExecutorService>,
        cpu: Option<RayonExecutorService>,
        tokio_blocking: Option<TokioBlockingExecutorService>,
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

    /// Submits a blocking runnable task to the blocking domain.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type submitted to the blocking domain.
    /// * `E` - Error type produced by the runnable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_blocking<T, E>(&self, task: T) -> Result<(), ExecutionServicesSubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Blocking, self.blocking.as_deref(), |service| {
            service.submit(task)
        })
    }

    /// Submits a blocking runnable task and returns a tracked handle.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type submitted to the blocking domain.
    /// * `E` - Error type produced by the runnable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_tracked_blocking<T, E>(&self, task: T) -> Result<TrackedTask<(), E>, ExecutionServicesSubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Blocking, self.blocking.as_deref(), |service| {
            service.submit_tracked(task)
        })
    }

    /// Submits a blocking callable task to the blocking domain.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type submitted to the blocking domain.
    /// * `R` - Successful result type produced by the callable task.
    /// * `E` - Error type produced by the callable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Blocking, self.blocking.as_deref(), |service| {
            service.submit_callable(task)
        })
    }

    /// Submits a blocking callable task and returns a tracked handle.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type submitted to the blocking domain.
    /// * `R` - Successful result type produced by the callable task.
    /// * `E` - Error type produced by the callable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_tracked_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TrackedTask<R, E>, ExecutionServicesSubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Blocking, self.blocking.as_deref(), |service| {
            service.submit_tracked_callable(task)
        })
    }

    /// Submits a CPU-bound runnable task to the Rayon domain.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type submitted to the CPU domain.
    /// * `E` - Error type produced by the runnable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_cpu<T, E>(&self, task: T) -> Result<(), ExecutionServicesSubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Cpu, self.cpu.as_ref(), |service| service.submit(task))
    }

    /// Submits a CPU-bound runnable task and returns a tracked handle.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type submitted to the CPU domain.
    /// * `E` - Error type produced by the runnable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_tracked_cpu<T, E>(&self, task: T) -> Result<RayonTaskHandle<(), E>, ExecutionServicesSubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Cpu, self.cpu.as_ref(), |service| {
            service.submit_tracked(task)
        })
    }

    /// Submits a CPU-bound callable task to the Rayon domain.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type submitted to the CPU domain.
    /// * `R` - Successful result type produced by the callable task.
    /// * `E` - Error type produced by the callable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_cpu_callable<C, R, E>(&self, task: C) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Cpu, self.cpu.as_ref(), |service| {
            service.submit_callable(task)
        })
    }

    /// Submits a CPU-bound callable task and returns a tracked handle.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type submitted to the CPU domain.
    /// * `R` - Successful result type produced by the callable task.
    /// * `E` - Error type produced by the callable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_tracked_cpu_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<RayonTaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Cpu, self.cpu.as_ref(), |service| {
            service.submit_tracked_callable(task)
        })
    }

    /// Submits a blocking runnable task to Tokio `spawn_blocking`.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type submitted to Tokio's blocking pool.
    /// * `E` - Error type produced by the runnable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_tokio_blocking<T, E>(&self, task: T) -> Result<(), ExecutionServicesSubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(
            ExecutionDomain::TokioBlocking,
            self.tokio_blocking.as_ref(),
            |service| service.submit(task),
        )
    }

    /// Submits a blocking runnable task to Tokio and returns a tracked handle.
    ///
    /// # Type Parameters
    ///
    /// * `T` - Runnable task type submitted to Tokio's blocking pool.
    /// * `E` - Error type produced by the runnable task.
    ///
    /// # Parameters
    ///
    /// * `task` - Runnable task to execute on Tokio's blocking pool.
    ///
    /// # Returns
    ///
    /// A [`TokioBlockingTaskHandle`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_tracked_tokio_blocking<T, E>(
        &self,
        task: T,
    ) -> Result<TokioBlockingTaskHandle<(), E>, ExecutionServicesSubmissionError>
    where
        T: Runnable<E> + Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(
            ExecutionDomain::TokioBlocking,
            self.tokio_blocking.as_ref(),
            |service| service.submit_tracked(task),
        )
    }

    /// Submits a blocking callable task to Tokio `spawn_blocking`.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type submitted to Tokio's blocking pool.
    /// * `R` - Successful result type produced by the callable task.
    /// * `E` - Error type produced by the callable task.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_tokio_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(
            ExecutionDomain::TokioBlocking,
            self.tokio_blocking.as_ref(),
            |service| service.submit_callable(task),
        )
    }

    /// Submits a blocking callable task to Tokio and returns a tracked handle.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Callable task type submitted to Tokio's blocking pool.
    /// * `R` - Successful result type produced by the callable task.
    /// * `E` - Error type produced by the callable task.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable task to execute on Tokio's blocking pool.
    ///
    /// # Returns
    ///
    /// A [`TokioBlockingTaskHandle`] for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn submit_tracked_tokio_blocking_callable<C, R, E>(
        &self,
        task: C,
    ) -> Result<TokioBlockingTaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(
            ExecutionDomain::TokioBlocking,
            self.tokio_blocking.as_ref(),
            |service| service.submit_tracked_callable(task),
        )
    }

    /// Spawns an async IO or Future-based task on Tokio's async runtime.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Future submitted to the Tokio async scheduler.
    /// * `R` - Successful output type produced by the future.
    /// * `E` - Error type produced by the future.
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
    /// Returns [`ExecutionServicesSubmissionError`] if the domain is disabled
    /// or refuses the task.
    #[inline]
    pub fn spawn_io<F, R, E>(&self, future: F) -> Result<TokioTaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        F: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        self.submit_to(ExecutionDomain::Io, self.io.as_ref(), |service| service.spawn(future))
    }

    /// Checks facade admission and domain availability before submitting.
    fn submit_to<S, R>(
        &self,
        domain: ExecutionDomain,
        service: Option<&S>,
        submit: impl FnOnce(&S) -> Result<R, SubmissionError>,
    ) -> Result<R, ExecutionServicesSubmissionError> {
        self.admission.admit(|| match service {
            Some(service) => submit(service).map_err(Into::into),
            None => Err(ExecutionServicesSubmissionError::DomainDisabled { domain }),
        })
    }

    /// Requests graceful shutdown for every execution domain.
    ///
    /// The facade records the shutdown intent before closing the domains. A
    /// submission that already passed the facade admission check may overlap
    /// domain shutdown and may be accepted or rejected by that domain. When
    /// this method returns, every enabled domain rejects new submissions.
    pub fn shutdown(&self) {
        self.admission.request_shutdown();
        if let Some(service) = &self.blocking {
            service.shutdown();
        }
        if let Some(service) = &self.cpu {
            service.shutdown();
        }
        if let Some(service) = &self.tokio_blocking {
            service.shutdown();
        }
        if let Some(service) = &self.io {
            service.shutdown();
        }
    }

    /// Requests abrupt stop for every execution domain.
    ///
    /// The facade records the stop intent before stopping the domains. A
    /// submission that already passed the facade admission check may overlap
    /// domain shutdown and may be accepted or rejected by that domain. When
    /// this method returns, every enabled domain rejects new submissions. The
    /// report samples domains sequentially in facade order.
    ///
    /// # Returns
    ///
    /// A per-domain aggregate report describing queued, running, and cancelled
    /// work observed during shutdown.
    pub fn stop(&self) -> ExecutionServicesStopReport {
        self.admission.request_stop();
        ExecutionServicesStopReport {
            blocking: self.blocking.as_ref().map(|service| service.stop()),
            cpu: self.cpu.as_ref().map(|service| service.stop()),
            tokio_blocking: self.tokio_blocking.as_ref().map(|service| service.stop()),
            io: self.io.as_ref().map(|service| service.stop()),
        }
    }

    /// Returns the aggregate lifecycle state.
    ///
    /// # Returns
    ///
    /// [`ExecutorServiceLifecycle::Terminated`] if all domains have terminated.
    /// Before termination, an aggregate stop request remains
    /// [`ExecutorServiceLifecycle::Stopping`] even if some domains have already
    /// terminated. A graceful shutdown request reports
    /// [`ExecutorServiceLifecycle::ShuttingDown`]. If a domain is stopped
    /// independently while aggregate admission is still open, its stopping
    /// state takes precedence over graceful shutdown in the aggregate result.
    #[must_use]
    pub fn lifecycle(&self) -> ExecutorServiceLifecycle {
        self.admission.lifecycle([
            self.blocking.as_ref().map(|service| service.lifecycle()),
            self.cpu.as_ref().map(|service| service.lifecycle()),
            self.tokio_blocking.as_ref().map(|service| service.lifecycle()),
            self.io.as_ref().map(|service| service.lifecycle()),
        ])
    }

    /// Returns whether every execution domain is running.
    ///
    /// # Returns
    ///
    /// `true` only if all execution domains are running.
    #[must_use]
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
    #[must_use]
    #[inline]
    pub fn is_shutting_down(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::ShuttingDown
    }

    /// Returns whether any execution domain is stopping abruptly.
    ///
    /// # Returns
    ///
    /// `true` after an aggregate stop request and until every domain is
    /// terminated, or while any domain is independently stopping.
    #[must_use]
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
    #[must_use]
    #[inline]
    pub fn is_not_running(&self) -> bool {
        self.lifecycle() != ExecutorServiceLifecycle::Running
    }

    /// Returns whether every execution domain has terminated.
    ///
    /// # Returns
    ///
    /// `true` only after all execution domains have terminated.
    #[must_use]
    #[inline]
    pub fn is_terminated(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::Terminated
    }

    /// Waits until every execution domain has terminated.
    ///
    /// # Returns
    ///
    /// This future resolves after all execution domains have terminated.
    /// It occupies no Tokio blocking threads or background wait tasks.
    /// Dropping it cancels only this wait and leaves domain shutdown intact.
    /// The runtime supplied to the builder must remain active while its Tokio
    /// tasks are running.
    /// Request shutdown or stop before awaiting termination; this method only
    /// waits for the domains to finish.
    pub async fn await_termination(&self) {
        join!(
            async {
                if let Some(service) = &self.blocking {
                    service.await_termination().await;
                }
            },
            async {
                if let Some(service) = &self.cpu {
                    service.await_termination().await;
                }
            },
            async {
                if let Some(service) = &self.tokio_blocking {
                    service.await_termination().await;
                }
            },
            async {
                if let Some(service) = &self.io {
                    service.await_termination().await;
                }
            },
        );
    }
}
