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
use super::internal::owned_wait_task::OwnedWaitTask;

impl ExecutionServices {
    /// Submits a blocking callable, waiting and retrying when its queue is
    /// full.
    ///
    /// The task is retained unchanged while capacity is full and runs only
    /// after an attempt is accepted. Cancelling this future while it waits
    /// drops the unaccepted task. The returned task handle controls
    /// cancellation after acceptance.
    ///
    /// # Type Parameters
    ///
    /// * `C` - One-shot task submitted to the blocking domain.
    /// * `R` - Successful result type produced by the task.
    /// * `E` - Error type produced by the task.
    ///
    /// # Parameters
    ///
    /// * `task` - One-shot callable that may block an OS thread.
    ///
    /// # Returns
    ///
    /// A handle for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns `DomainDisabled` if blocking is disabled, or `Rejected` when
    /// shutdown or another submission error prevents acceptance.
    pub async fn submit_blocking_callable_wait<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: FnOnce() -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self
            .blocking
            .as_deref()
            .ok_or(ExecutionServicesSubmissionError::DomainDisabled {
                domain: ExecutionDomain::Blocking,
            })?;
        let task = OwnedWaitTask::new(task);
        let mut changes = service.capacity_changes();
        loop {
            let attempt = task.callable_attempt();
            let result = self.admission.admit(|| service.submit_callable(attempt));
            match result {
                Ok(handle) => return Ok(handle),
                Err(SubmissionError::Saturated) => changes.changed().await.map_err(|_| SubmissionError::Shutdown)?,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Submits a CPU callable, waiting and retrying when its capacity is full.
    ///
    /// The task is retained unchanged while capacity is full and runs only
    /// after an attempt is accepted. Cancelling this future while it waits
    /// drops the unaccepted task. The returned task handle controls
    /// cancellation after acceptance.
    ///
    /// # Type Parameters
    ///
    /// * `C` - One-shot task submitted to the CPU domain.
    /// * `R` - Successful result type produced by the task.
    /// * `E` - Error type produced by the task.
    ///
    /// # Parameters
    ///
    /// * `task` - One-shot CPU-bound callable.
    ///
    /// # Returns
    ///
    /// A handle for the accepted CPU task.
    ///
    /// # Errors
    ///
    /// Returns `DomainDisabled` if CPU is disabled, or `Rejected` when
    /// shutdown or another submission error prevents acceptance.
    pub async fn submit_cpu_callable_wait<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: FnOnce() -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self
            .cpu
            .as_ref()
            .ok_or(ExecutionServicesSubmissionError::DomainDisabled {
                domain: ExecutionDomain::Cpu,
            })?;
        let task = OwnedWaitTask::new(task);
        let mut changes = service.capacity_changes();
        loop {
            let attempt = task.callable_attempt();
            let result = self.admission.admit(|| service.submit_callable(attempt));
            match result {
                Ok(handle) => return Ok(handle),
                Err(SubmissionError::Saturated) => changes.changed().await.map_err(|_| SubmissionError::Shutdown)?,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Submits a Tokio blocking callable, waiting and retrying when capacity is
    /// full.
    ///
    /// Capacity notifications are advisory, so a competing submitter can make
    /// the next attempt saturate again. The same one-shot task remains owned by
    /// this future until an attempt is accepted. Cancelling while it waits
    /// drops the unaccepted task; the returned handle controls an accepted
    /// task.
    ///
    /// # Type Parameters
    ///
    /// * `C` - One-shot task submitted to the Tokio blocking domain.
    /// * `R` - Successful result type produced by the task.
    /// * `E` - Error type produced by the task.
    ///
    /// # Parameters
    ///
    /// * `task` - One-shot blocking callable.
    ///
    /// # Returns
    ///
    /// A handle for the accepted task.
    ///
    /// # Errors
    ///
    /// Returns `DomainDisabled` if Tokio blocking is disabled, or `Rejected`
    /// when shutdown or another submission error prevents acceptance.
    pub async fn submit_tokio_blocking_callable_wait<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: FnOnce() -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self
            .tokio_blocking
            .as_ref()
            .ok_or(ExecutionServicesSubmissionError::DomainDisabled {
                domain: ExecutionDomain::TokioBlocking,
            })?;
        let task = OwnedWaitTask::new(task);
        let mut changes = service.capacity_changes();
        loop {
            let attempt = task.callable_attempt();
            let result = self.admission.admit(|| service.submit_callable(attempt));
            match result {
                Ok(handle) => return Ok(handle),
                Err(SubmissionError::Saturated) => changes.changed().await.map_err(|_| SubmissionError::Shutdown)?,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Submits an IO future, waiting and retrying when accepted-future capacity
    /// is full. The same future remains owned by this method until an attempt
    /// is accepted.
    ///
    /// Cancelling this future while it waits drops the unaccepted IO future.
    /// The returned task handle controls the future after acceptance.
    ///
    /// # Type Parameters
    ///
    /// * `F` - One-shot future submitted to the Tokio IO domain.
    /// * `R` - Successful output type produced by the future.
    /// * `E` - Error type produced by the future.
    ///
    /// # Parameters
    ///
    /// * `future` - One-shot future to run on the Tokio runtime.
    ///
    /// # Returns
    ///
    /// A handle for the accepted IO task.
    ///
    /// # Errors
    ///
    /// Returns `DomainDisabled` if IO is disabled, or `Rejected` when shutdown
    /// or another submission error prevents acceptance.
    pub async fn spawn_io_wait<F, R, E>(
        &self,
        future: F,
    ) -> Result<TokioTaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        F: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self
            .io
            .as_ref()
            .ok_or(ExecutionServicesSubmissionError::DomainDisabled {
                domain: ExecutionDomain::Io,
            })?;
        let future = OwnedWaitTask::new(future);
        let mut changes = service.capacity_changes();
        loop {
            let attempt = future.future_attempt();
            let result = self.admission.admit(|| service.spawn(attempt));
            match result {
                Ok(handle) => return Ok(handle),
                Err(SubmissionError::Saturated) => changes.changed().await.map_err(|_| SubmissionError::Shutdown)?,
                Err(error) => return Err(error.into()),
            }
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
    ///
    /// # Parameters
    ///
    /// * `domain` - Domain requested by the submission method.
    /// * `service` - Enabled domain service, or `None` when disabled.
    /// * `submit` - Closure that submits the task to an enabled service.
    ///
    /// # Returns
    ///
    /// The service's submission result or a facade error for a disabled or
    /// closed domain.
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
