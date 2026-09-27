// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Owns a one-shot task while capacity-wait submission attempts are retried.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

/// Holds a task until one submission attempt is accepted and starts executing.
pub(in crate::execution_services) struct OwnedWaitTask<T> {
    /// Shared slot retained by the waiter while rejected attempt wrappers drop.
    task: Arc<Mutex<Option<T>>>,
}

impl<T> OwnedWaitTask<T> {
    /// Creates a task owner that can produce retryable submission wrappers.
    ///
    /// # Parameters
    ///
    /// * `task` - One-shot task retained until an accepted wrapper executes.
    ///
    /// # Returns
    ///
    /// An owner for the task and its future submission attempts.
    pub(in crate::execution_services) fn new(task: T) -> Self {
        Self {
            task: Arc::new(Mutex::new(Some(task))),
        }
    }
}

impl<T> OwnedWaitTask<T> {
    /// Creates a callable wrapper that takes the task only when it runs.
    ///
    /// Rejected wrappers leave the task in the shared slot for the next
    /// attempt. The mutex is released before invoking user code.
    pub(in crate::execution_services) fn callable_attempt<R, E>(&self) -> impl FnMut() -> Result<R, E> + Send + 'static
    where
        T: FnOnce() -> Result<R, E> + Send + 'static,
    {
        let task = Arc::clone(&self.task);
        move || take_task(&task)()
    }

    /// Creates an IO wrapper that takes the future on its first poll.
    ///
    /// Rejected wrappers leave the future in the shared slot for the next
    /// attempt. The mutex is released before polling user code.
    pub(in crate::execution_services) fn future_attempt<R, E>(
        &self,
    ) -> impl Future<Output = Result<R, E>> + Send + 'static
    where
        T: Future<Output = Result<R, E>> + Send + 'static,
    {
        let task = Arc::clone(&self.task);
        async move { take_task(&task).await }
    }
}

/// Takes the one-shot task while recovering its value from a poisoned mutex.
///
/// # Parameters
///
/// * `task` - Shared slot containing the task to execute.
///
/// # Returns
///
/// The task removed from the slot.
fn take_task<T>(task: &Mutex<Option<T>>) -> T {
    lock_task(task)
        .take()
        .expect("accepted wait task must be consumed exactly once")
}

/// Locks the task slot and recovers after poisoning.
///
/// # Parameters
///
/// * `task` - Shared slot containing the task.
///
/// # Returns
///
/// A guard for reading or taking the task.
fn lock_task<T>(task: &Mutex<Option<T>>) -> MutexGuard<'_, Option<T>> {
    task.lock().unwrap_or_else(|error| error.into_inner())
}
