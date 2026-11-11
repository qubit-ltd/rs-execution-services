// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Capacity-waiting submission APIs for the execution-services facade.

use std::future::Future;

use qubit_executor::TaskHandle;
use qubit_executor::service::ExecutorService;
use qubit_tokio_executor::TokioTaskHandle;

use super::super::ExecutionDomain;
use super::super::ExecutionServicesSubmissionError;
use super::ExecutionServices;
use super::internal::owned_wait_task::OwnedWaitTask;
use super::internal::submission_retry::retry_submission;
use super::internal::submission_retry::retry_submission_with_changes;

impl ExecutionServices {
    /// Submits a blocking callable, waiting and retrying when its queue is
    /// full.
    ///
    /// The future is lazy and must be polled to begin submission. It retains
    /// the callable until a submission is accepted. Dropping this future before
    /// acceptance drops the callable; after acceptance, this method returns a
    /// [`TaskHandle`] for observing the result. This handle does not cancel the
    /// accepted task.
    ///
    /// A parent task can prevent a same-domain child from running if it waits
    /// for that child while occupying the worker the child needs. Keep such
    /// orchestration outside the blocking domain or account for available
    /// workers and dependency depth.
    ///
    /// # Type Parameters
    ///
    /// * `C` - One-shot callable submitted to the blocking domain.
    /// * `R` - Successful result type produced by the callable.
    /// * `E` - Error type produced by the callable.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable that may block an OS thread.
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] for the accepted task.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServicesSubmissionError::Rejected`] if shutdown has
    /// closed admission, the domain rejects the task, or the capacity-change
    /// channel closes. Returns
    /// [`ExecutionServicesSubmissionError::DomainDisabled`] when the facade
    /// is running and blocking is disabled.
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
            .admission
            .resolve_domain(ExecutionDomain::Blocking, self.blocking.as_deref())?;
        let task = OwnedWaitTask::new(task);
        let changes = service.capacity_changes();
        retry_submission(&self.admission, changes, || {
            service.submit_callable(task.callable_attempt())
        })
        .await
    }

    /// Submits a CPU callable, waiting and retrying when capacity is full.
    ///
    /// The future is lazy and must be polled to begin submission. It retains
    /// the callable until a submission is accepted. Dropping this future before
    /// acceptance drops the callable; after acceptance, this method returns a
    /// [`TaskHandle`] for observing the result. This handle does not cancel the
    /// accepted task.
    ///
    /// A parent task can prevent a same-domain child from running if it waits
    /// for that child while occupying the worker the child needs. The parent
    /// also occupies unfinished-task capacity until it returns. With capacity
    /// one, waiting for a same-domain child submission prevents progress even
    /// when additional workers are available. Keep orchestration outside the
    /// CPU domain, or explicitly account for worker availability, task
    /// capacity, and dependency depth. More workers or capacity alone do
    /// not make arbitrary recursive dependencies safe.
    ///
    /// # Type Parameters
    ///
    /// * `C` - One-shot CPU callable.
    /// * `R` - Successful result type produced by the callable.
    /// * `E` - Error type produced by the callable.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable for CPU-bound work.
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] for the accepted task.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServicesSubmissionError::Rejected`] if shutdown has
    /// closed admission, the domain rejects the task, or the capacity-change
    /// channel closes. Returns
    /// [`ExecutionServicesSubmissionError::DomainDisabled`] when the facade
    /// is running and CPU is disabled.
    pub async fn submit_cpu_callable_wait<C, R, E>(
        &self,
        task: C,
    ) -> Result<TaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        C: FnOnce() -> Result<R, E> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self.admission.resolve_domain(ExecutionDomain::Cpu, self.cpu.as_ref())?;
        let mut callable = Some({
            let mut task = Some(task);
            move || task.take().expect("CPU wait task executes once")()
        });
        let changes = service.capacity_changes();
        retry_submission_with_changes(&self.admission, changes, || {
            let callable_attempt = callable.take().expect("CPU wait task remains available until accepted");
            match service.try_submit_callable(callable_attempt) {
                Ok(handle) => Ok(handle),
                Err(rejected) => {
                    let (reason, callable_attempt) = rejected.into_parts();
                    callable = Some(callable_attempt);
                    Err(reason)
                }
            }
        })
        .await
    }

    /// Submits a Tokio blocking callable, waiting and retrying when capacity is
    /// full.
    ///
    /// The future is lazy and must be polled to begin submission. It retains
    /// the callable until a submission is accepted. Dropping this future before
    /// acceptance drops the callable; after acceptance, this method returns a
    /// [`TaskHandle`] for observing the result. This handle does not cancel the
    /// accepted task.
    ///
    /// A parent task can prevent a same-domain child from running if it waits
    /// for that child while occupying a Tokio blocking worker the child needs.
    /// The parent also occupies unfinished-task capacity until it returns. With
    /// capacity one, waiting for a same-domain child submission prevents
    /// progress even when additional workers are available. Keep orchestration
    /// outside the blocking pool, or explicitly account for worker
    /// availability, task capacity, and dependency depth. More workers or
    /// capacity alone do not make arbitrary recursive dependencies safe.
    ///
    /// Capacity notifications are advisory: another submitter may use an
    /// available slot before this method retries. No FIFO or bounded-wait
    /// guarantee is provided.
    ///
    /// # Type Parameters
    ///
    /// * `C` - One-shot callable submitted to Tokio's blocking pool.
    /// * `R` - Successful result type produced by the callable.
    /// * `E` - Error type produced by the callable.
    ///
    /// # Parameters
    ///
    /// * `task` - Callable that may block a Tokio blocking thread.
    ///
    /// # Returns
    ///
    /// A [`TaskHandle`] for the accepted task.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServicesSubmissionError::Rejected`] if shutdown has
    /// closed admission, the domain rejects the task, or the capacity-change
    /// channel closes. Returns
    /// [`ExecutionServicesSubmissionError::DomainDisabled`] when the facade
    /// is running and Tokio blocking is disabled.
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
            .admission
            .resolve_domain(ExecutionDomain::TokioBlocking, self.tokio_blocking.as_ref())?;
        let mut callable = Some({
            let mut task = Some(task);
            move || task.take().expect("Tokio blocking wait task executes once")()
        });
        let changes = service.capacity_changes();
        retry_submission(&self.admission, changes, || {
            let callable_attempt = callable
                .take()
                .expect("Tokio blocking wait task remains available until accepted");
            match service.try_submit_callable(callable_attempt) {
                Ok(handle) => Ok(handle),
                Err(rejected) => {
                    let (reason, callable_attempt) = rejected.into_parts();
                    callable = Some(callable_attempt);
                    Err(reason)
                }
            }
        })
        .await
    }

    /// Submits an IO future, waiting and retrying when accepted-future capacity
    /// is full.
    ///
    /// This method's future is lazy and must be polled to begin submission. The
    /// same user future remains owned until a submission is accepted. Dropping
    /// this method's future before acceptance drops the user future. After
    /// acceptance, the returned [`TokioTaskHandle`] supports cancellation
    /// according to Tokio task cancellation semantics.
    ///
    /// Avoid waiting for same-domain child work while the parent holds the
    /// capacity that child needs. With IO capacity one, a parent awaiting a
    /// child submitted through `spawn_io_wait` keeps the only slot occupied, so
    /// the child cannot be accepted. Keep orchestration outside the IO domain
    /// or explicitly account for dependency depth and capacity.
    ///
    /// Capacity notifications are advisory: another submitter may use an
    /// available slot before this method retries. No FIFO or bounded-wait
    /// guarantee is provided.
    ///
    /// # Type Parameters
    ///
    /// * `F` - One-shot future submitted to the Tokio IO domain.
    /// * `R` - Successful output type produced by the future.
    /// * `E` - Error type produced by the future.
    ///
    /// # Parameters
    ///
    /// * `future` - Future to poll on the supplied Tokio runtime.
    ///
    /// # Returns
    ///
    /// A [`TokioTaskHandle`] for the accepted task.
    ///
    /// # Errors
    ///
    /// Returns [`ExecutionServicesSubmissionError::Rejected`] if shutdown has
    /// closed admission, the domain rejects the task, or the capacity-change
    /// channel closes. Returns
    /// [`ExecutionServicesSubmissionError::DomainDisabled`] when the facade
    /// is running and IO is disabled.
    pub async fn spawn_io_wait<F, R, E>(
        &self,
        future: F,
    ) -> Result<TokioTaskHandle<R, E>, ExecutionServicesSubmissionError>
    where
        F: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let service = self.admission.resolve_domain(ExecutionDomain::Io, self.io.as_ref())?;
        let mut future = Some(future);
        let changes = service.capacity_changes();
        retry_submission(&self.admission, changes, || {
            let future_attempt = future.take().expect("IO wait future remains available until accepted");
            match service.try_spawn(future_attempt) {
                Ok(handle) => Ok(handle),
                Err(rejected) => {
                    let (reason, future_attempt) = rejected.into_parts();
                    future = Some(future_attempt);
                    Err(reason)
                }
            }
        })
        .await
    }
}
