// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stop report for the execution-services facade.

use qubit_executor::service::StopReport;

/// Per-domain report returned by [`super::ExecutionServices::stop`].
///
/// # Examples
///
/// ```
/// use qubit_execution_services::ExecutionServicesStopReport;
/// use qubit_executor::service::StopReport;
///
/// let report = ExecutionServicesStopReport {
///     blocking: Some(StopReport::new(1, 0, 0)),
///     cpu: None,
///     tokio_blocking: None,
///     io: None,
/// };
/// assert_eq!(report.blocking.expect("enabled").queued, 1);
/// ```
#[must_use]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionServicesStopReport {
    /// Stop report for the blocking executor domain.
    /// `None` means the domain was not enabled.
    pub blocking: Option<StopReport>,
    /// Stop report for the CPU executor domain.
    /// `None` means the domain was not enabled.
    pub cpu: Option<StopReport>,
    /// Stop report for the Tokio blocking executor domain.
    /// `None` means the domain was not enabled.
    pub tokio_blocking: Option<StopReport>,
    /// Stop report for the Tokio async IO executor domain.
    ///
    /// `None` means this domain was not enabled. The `running` count covers
    /// accepted futures that had not completed when stop was requested; it does
    /// not indicate which futures were being polled.
    pub io: Option<StopReport>,
}
