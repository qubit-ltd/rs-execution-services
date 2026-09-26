// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Submission admission and aggregate lifecycle intent for the facade.

use std::sync::Mutex;
use std::sync::MutexGuard;

use qubit_executor::service::ExecutorServiceLifecycle;
use qubit_executor::service::SubmissionError;

use super::facade_intent::FacadeIntent;

/// Locks the aggregate intent, recovering the last recorded value if poisoned.
///
/// # Parameters
///
/// * `intent` - Mutex protecting the facade's aggregate lifecycle intent.
///
/// # Returns
///
/// A guard that keeps the intent locked for the caller's read or update.
fn lock_intent(intent: &Mutex<FacadeIntent>) -> MutexGuard<'_, FacadeIntent> {
    intent.lock().unwrap_or_else(|error| error.into_inner())
}

/// Tracks facade admission intent and aggregate shutdown requests.
pub struct ExecutionServicesAdmission {
    /// Current aggregate intent, protected while admission is checked or
    /// updated.
    intent: Mutex<FacadeIntent>,
}

impl ExecutionServicesAdmission {
    /// Creates an admission gate in the running state.
    pub fn new() -> Self {
        Self {
            intent: Mutex::new(FacadeIntent::Running),
        }
    }

    /// Checks facade admission, then calls `submit` without holding the intent
    /// lock. An overlapping shutdown may cause the underlying domain to
    /// reject work.
    pub fn admit<R, E: From<SubmissionError>>(&self, submit: impl FnOnce() -> Result<R, E>) -> Result<R, E> {
        let accepting = {
            let intent = lock_intent(&self.intent);
            *intent == FacadeIntent::Running
        };
        if !accepting {
            return Err(SubmissionError::Shutdown.into());
        }
        submit()
    }

    /// Requests graceful shutdown without downgrading an existing stop.
    #[inline]
    pub fn request_shutdown(&self) {
        let mut intent = lock_intent(&self.intent);
        if *intent == FacadeIntent::Running {
            *intent = FacadeIntent::ShuttingDown;
        }
    }

    /// Records an abrupt stop request, upgrading any graceful shutdown intent.
    #[inline]
    pub fn request_stop(&self) {
        *lock_intent(&self.intent) = FacadeIntent::Stopping;
    }

    /// Returns the current aggregate shutdown intent.
    #[inline]
    pub fn intent(&self) -> FacadeIntent {
        *lock_intent(&self.intent)
    }

    /// Returns the aggregate lifecycle for the current intent and domain
    /// states.
    pub fn lifecycle(&self, states: [Option<ExecutorServiceLifecycle>; 4]) -> ExecutorServiceLifecycle {
        aggregate_lifecycle(self.intent(), states)
    }
}

/// Combines aggregate shutdown intent with the latest state of each domain.
///
/// Termination takes precedence once every domain is terminated. Before then,
/// aggregate intent keeps an explicit stop visible even if individual domains
/// have already terminated.
fn aggregate_lifecycle(
    intent: FacadeIntent,
    states: [Option<ExecutorServiceLifecycle>; 4],
) -> ExecutorServiceLifecycle {
    use ExecutorServiceLifecycle::Running;
    use ExecutorServiceLifecycle::ShuttingDown;
    use ExecutorServiceLifecycle::Stopping;
    use ExecutorServiceLifecycle::Terminated;

    if states.iter().flatten().all(|state| *state == Terminated) {
        return Terminated;
    }
    match intent {
        FacadeIntent::Stopping => Stopping,
        FacadeIntent::ShuttingDown => ShuttingDown,
        FacadeIntent::Running if states.contains(&Some(Stopping)) => Stopping,
        FacadeIntent::Running if states.iter().flatten().any(|state| *state != Running) => ShuttingDown,
        FacadeIntent::Running => Running,
    }
}

#[cfg(test)]
mod tests {
    use std::panic::catch_unwind;
    use std::sync::Mutex;

    use qubit_executor::service::ExecutorServiceLifecycle;
    use qubit_executor::service::SubmissionError;

    use super::ExecutionServicesAdmission;
    use super::FacadeIntent;
    use super::aggregate_lifecycle;
    use super::lock_intent;

    #[test]
    fn test_aggregate_lifecycle_preserves_stop_intent_until_all_domains_terminate() {
        use ExecutorServiceLifecycle::Running;
        use ExecutorServiceLifecycle::ShuttingDown;
        use ExecutorServiceLifecycle::Stopping;
        use ExecutorServiceLifecycle::Terminated;

        assert_eq!(
            aggregate_lifecycle(
                FacadeIntent::Stopping,
                [Some(Terminated), Some(ShuttingDown), Some(Terminated), Some(Running)],
            ),
            Stopping,
        );
        assert_eq!(
            aggregate_lifecycle(FacadeIntent::Stopping, [Some(Terminated); 4]),
            Terminated,
        );
    }

    #[test]
    fn test_aggregate_lifecycle_prioritizes_stop_over_graceful_shutdown() {
        use ExecutorServiceLifecycle::Running;
        use ExecutorServiceLifecycle::ShuttingDown;
        use ExecutorServiceLifecycle::Stopping;
        use ExecutorServiceLifecycle::Terminated;

        assert_eq!(
            aggregate_lifecycle(
                FacadeIntent::Running,
                [Some(Running), Some(Stopping), Some(ShuttingDown), Some(Terminated)],
            ),
            Stopping,
        );
    }

    #[test]
    fn test_admission_closes_after_shutdown_and_stop_is_not_downgraded() {
        use ExecutorServiceLifecycle::Running;
        use ExecutorServiceLifecycle::Stopping;

        let admission = ExecutionServicesAdmission::new();
        let accepted = admission.admit(|| Ok::<_, SubmissionError>(7));
        assert_eq!(accepted, Ok(7));
        assert!(admission.intent() == FacadeIntent::Running);

        admission.request_shutdown();
        assert!(admission.intent() == FacadeIntent::ShuttingDown);
        assert_eq!(admission.admit(|| Ok(9)), Err(SubmissionError::Shutdown));

        admission.request_stop();
        assert!(admission.intent() == FacadeIntent::Stopping);
        admission.request_shutdown();
        assert!(admission.intent() == FacadeIntent::Stopping);
        assert_eq!(admission.lifecycle([Some(Running); 4]), Stopping);
    }

    #[test]
    fn test_admit_callback_may_request_shutdown() {
        let admission = ExecutionServicesAdmission::new();
        let result = admission.admit(|| {
            admission.request_shutdown();
            Err::<(), _>(SubmissionError::Saturated)
        });
        assert_eq!(result, Err(SubmissionError::Saturated));
        assert!(admission.intent() == FacadeIntent::ShuttingDown);
    }

    #[test]
    fn test_admission_recovers_a_poisoned_intent_lock() {
        let intent = Mutex::new(FacadeIntent::Running);
        let _ = catch_unwind(|| {
            let _guard = intent.lock().expect("initial lock");
            panic!("poison admission lock");
        });

        assert!(*lock_intent(&intent) == FacadeIntent::Running);
    }
}
