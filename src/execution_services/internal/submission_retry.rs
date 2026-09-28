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

use super::execution_services_admission::ExecutionServicesAdmission;
use crate::ExecutionServicesSubmissionError;

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
pub async fn retry_submission<H, F>(
    admission: &ExecutionServicesAdmission,
    mut changes: Receiver<u64>,
    mut attempt: F,
) -> Result<H, ExecutionServicesSubmissionError>
where
    H: Send,
    F: FnMut() -> Result<H, SubmissionError> + Send,
{
    loop {
        match admission.admit(&mut attempt) {
            Ok(handle) => return Ok(handle),
            Err(SubmissionError::Saturated) => {
                changes.changed().await.map_err(|_| SubmissionError::Shutdown)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use qubit_executor::service::SubmissionError;
    use tokio::sync::watch;

    use super::ExecutionServicesAdmission;
    use super::retry_submission;
    use crate::ExecutionServicesSubmissionError;

    #[tokio::test]
    async fn test_retry_returns_shutdown_when_capacity_channel_closes() {
        let admission = ExecutionServicesAdmission::new();
        let (sender, changes) = watch::channel(0);
        drop(sender);

        let result = retry_submission::<(), _>(&admission, changes, || Err(SubmissionError::Saturated)).await;

        assert!(matches!(
            result,
            Err(ExecutionServicesSubmissionError::Rejected {
                source: SubmissionError::Shutdown,
            })
        ));
    }
}
