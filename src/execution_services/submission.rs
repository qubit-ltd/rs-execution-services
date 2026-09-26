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
    /// Submits a blocking callable, waiting and retrying when its queue is
    /// full.
    ///
    /// `make` may be called more than once after a saturation race and must not
    /// perform external side effects. Cancelling this future while it waits
    /// does not invoke the factory again. The returned task handle controls
    /// cancellation after acceptance.
    ///
    /// # Parameters
    ///
    /// * `make` - Factory for a fresh blocking callable on each attempt.
    ///
    /// # Returns
    ///
    /// A handle for the accepted blocking task.
    ///
    /// # Errors
    ///
    /// Returns `DomainDisabled` if blocking is disabled, or `Rejected` when
    /// shutdown or another submission error prevents acceptance.
    pub async fn submit_blocking_callable_wait<Make, C, R, E>(
        &self,
        make: Make,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        Make: Fn() -> C + Send + Sync,
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self
            .blocking
            .as_deref()
            .ok_or(ExecutionServicesSubmissionError::DomainDisabled {
                domain: ExecutionDomain::Blocking,
            })?;
        let mut changes = service.capacity_changes();
        loop {
            let result = self.admission.admit(|| service.submit_callable(make()));
            match result {
                Ok(handle) => return Ok(handle),
                Err(SubmissionError::Saturated) => changes.changed().await.map_err(|_| SubmissionError::Shutdown)?,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Submits a CPU callable, waiting and retrying when its capacity is full.
    ///
    /// `make` may be called more than once after a saturation race and must be
    /// side-effect free. Cancelling the wait future leaves accepted work and
    /// returned task handles under the caller's control.
    ///
    /// # Parameters
    ///
    /// * `make` - Factory for a fresh CPU callable on each attempt.
    ///
    /// # Returns
    ///
    /// A handle for the accepted CPU task.
    ///
    /// # Errors
    ///
    /// Returns `DomainDisabled` if CPU is disabled, or `Rejected` when
    /// shutdown or another submission error prevents acceptance.
    pub async fn submit_cpu_callable_wait<Make, C, R, E>(
        &self,
        make: Make,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        Make: Fn() -> C + Send + Sync,
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self
            .cpu
            .as_ref()
            .ok_or(ExecutionServicesSubmissionError::DomainDisabled {
                domain: ExecutionDomain::Cpu,
            })?;
        let mut changes = service.capacity_changes();
        loop {
            let result = self.admission.admit(|| service.submit_callable(make()));
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
    /// the next attempt saturate again. `make` may be called more than once and
    /// must not perform external side effects. Cancelling this future stops
    /// further attempts; the returned handle controls an accepted task.
    ///
    /// # Parameters
    ///
    /// * `make` - Factory for a fresh Tokio blocking callable on each attempt.
    ///
    /// # Returns
    ///
    /// A handle for the accepted task.
    ///
    /// # Errors
    ///
    /// Returns `DomainDisabled` if Tokio blocking is disabled, or `Rejected`
    /// when shutdown or another submission error prevents acceptance.
    pub async fn submit_tokio_blocking_callable_wait<Make, C, R, E>(
        &self,
        make: Make,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        Make: Fn() -> C + Send + Sync,
        C: Callable<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self
            .tokio_blocking
            .as_ref()
            .ok_or(ExecutionServicesSubmissionError::DomainDisabled {
                domain: ExecutionDomain::TokioBlocking,
            })?;
        let mut changes = service.capacity_changes();
        loop {
            let result = self.admission.admit(|| service.submit_callable(make()));
            match result {
                Ok(handle) => return Ok(handle),
                Err(SubmissionError::Saturated) => changes.changed().await.map_err(|_| SubmissionError::Shutdown)?,
                Err(error) => return Err(error.into()),
            }
        }
    }

    /// Spawns an IO future, waiting and retrying when the accepted-future
    /// capacity is full. The factory may be called repeatedly and must be
    /// side-effect free.
    ///
    /// Cancelling this future stops further attempts; the returned task handle
    /// controls the future after it has been accepted.
    ///
    /// # Parameters
    ///
    /// * `make` - Factory for a fresh IO future on each attempt.
    ///
    /// # Returns
    ///
    /// A handle for the accepted IO task.
    ///
    /// # Errors
    ///
    /// Returns `DomainDisabled` if IO is disabled, or `Rejected` when shutdown
    /// or another submission error prevents acceptance.
    pub async fn spawn_io_wait<Make, F, R, E>(
        &self,
        make: Make,
    ) -> Result<TokioTaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        Make: Fn() -> F + Send + Sync,
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
        let mut changes = service.capacity_changes();
        loop {
            let result = self.admission.admit(|| service.spawn(make()));
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
