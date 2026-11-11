// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Retries native task submission after advisory capacity notifications.

use qubit_executor::service::SubmissionError;
use tokio::sync::watch::Receiver;

use super::capacity_change_receiver::CapacityChangeReceiver;
use super::execution_services_admission::ExecutionServicesAdmission;

/// Submits a task until accepted, rejected, or facade admission closes.
///
/// Capacity notifications only prompt another attempt; they do not reserve a
/// slot for this submission. The caller must subscribe before the first call.
///
/// # Type Parameters
///
/// * `H` - Native handle returned by the accepted submission.
/// * `F` - Reusable closure constructing one native submission attempt.
///
/// # Parameters
///
/// * `admission` - Facade gate checked before each native submission.
/// * `changes` - Capacity-change receiver subscribed before the first attempt.
/// * `attempt` - Closure submitting one retryable wrapper to the domain.
///
/// # Returns
///
/// The native handle for the accepted submission.
///
/// # Errors
///
/// Returns facade Shutdown if admission closes or the change channel closes;
/// propagates saturation only internally while retrying and returns all other
/// native submission errors immediately.
pub(crate) async fn retry_submission<H, F>(
    admission: &ExecutionServicesAdmission,
    changes: Receiver<u64>,
    attempt: F,
) -> Result<H, SubmissionError>
where
    H: Send,
    F: FnMut() -> Result<H, SubmissionError> + Send,
{
    retry_submission_with_changes(admission, changes, attempt).await
}

/// Shared retry loop for native capacity notification sources.
///
/// # Type Parameters
///
/// * `C` - Capacity notification source used between attempts.
/// * `H` - Native handle returned after a submission is accepted.
/// * `F` - Reusable closure that performs one native submission attempt.
///
/// # Parameters
///
/// * `admission` - Facade gate checked before each native submission.
/// * `changes` - Subscribed notification source used after saturation.
/// * `attempt` - Closure submitting one retryable wrapper to the domain.
///
/// # Returns
///
/// The native handle for the accepted submission.
///
/// # Errors
///
/// Returns Shutdown if admission closes or the notification source closes;
/// returns other native submission errors immediately.
pub(crate) async fn retry_submission_with_changes<C, H, F>(
    admission: &ExecutionServicesAdmission,
    mut changes: C,
    mut attempt: F,
) -> Result<H, SubmissionError>
where
    C: CapacityChangeReceiver,
    H: Send,
    F: FnMut() -> Result<H, SubmissionError> + Send,
{
    loop {
        match admission.admit(&mut attempt) {
            Ok(handle) => return Ok(handle),
            Err(SubmissionError::Saturated) => {
                changes.changed().await.map_err(|_| SubmissionError::Shutdown)?;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use qubit_executor::service::SubmissionError;
    use tokio::sync::watch;

    use super::ExecutionServicesAdmission;
    use super::retry_submission;

    #[tokio::test]
    async fn test_retry_returns_shutdown_when_capacity_channel_closes() {
        let admission = ExecutionServicesAdmission::new();
        let (sender, changes) = watch::channel(0);
        drop(sender);

        let result = retry_submission::<(), _>(&admission, changes, || Err(SubmissionError::Saturated)).await;

        assert_eq!(result, Err(SubmissionError::Shutdown));
    }
}
