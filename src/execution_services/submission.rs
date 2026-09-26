// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Submission APIs for the execution-services facade.

use std::future::Future;

use qubit_executor::TaskHandle;
use qubit_executor::TrackedTask;
use qubit_executor::service::ExecutorService;
use qubit_executor::service::SubmissionError;
use qubit_function::Callable;
use qubit_function::Runnable;
use qubit_rayon_executor::RayonTaskHandle;
use qubit_tokio_executor::TokioBlockingTaskHandle;
use qubit_tokio_executor::TokioTaskHandle;

use super::super::ExecutionDomain;
use super::super::ExecutionServicesSubmissionError;
use super::ExecutionServices;

impl ExecutionServices {
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
}
