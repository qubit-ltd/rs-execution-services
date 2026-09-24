// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stop report for the execution-services facade.

use qubit_executor::service::StopReport;

/// Aggregate report returned by [`super::ExecutionServices::stop`].
///
/// # Examples
///
/// ```
/// use qubit_execution_services::ExecutionServicesStopReport;
/// use qubit_executor::service::StopReport;
///
/// let report = ExecutionServicesStopReport {
///     blocking: StopReport::new(1, 0, 0),
///     cpu: StopReport::new(0, 0, 0),
///     tokio_blocking: StopReport::new(0, 0, 0),
///     io: StopReport::new(0, 0, 0),
/// };
/// assert_eq!(report.total_queued(), 1);
/// ```
#[must_use]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionServicesStopReport {
    /// Stop report for the blocking executor domain.
    pub blocking: StopReport,
    /// Stop report for the CPU executor domain.
    pub cpu: StopReport,
    /// Stop report for the Tokio blocking executor domain.
    pub tokio_blocking: StopReport,
    /// Stop report for the Tokio async IO executor domain.
    ///
    /// Its `running` field counts accepted futures that had not completed when
    /// stop was requested; it does not indicate which futures were being
    /// polled.
    pub io: StopReport,
}

impl ExecutionServicesStopReport {
    /// Returns the total queued task count across all execution domains.
    ///
    /// # Returns
    ///
    /// The sum of the per-domain queued counts observed during the sequential
    /// stop calls. The result is not an atomic cross-domain snapshot.
    #[must_use]
    #[inline]
    pub const fn total_queued(&self) -> usize {
        self.blocking.queued + self.cpu.queued + self.tokio_blocking.queued + self.io.queued
    }

    /// Returns the sum of the per-domain `running` counts.
    ///
    /// # Returns
    ///
    /// The arithmetic sum of the four stop reports' `running` fields. For the
    /// Tokio IO domain this includes accepted futures that had not completed,
    /// whether or not they were being polled. The per-domain counts are sampled
    /// sequentially and do not form an atomic cross-domain snapshot.
    #[must_use]
    #[inline]
    pub const fn total_running(&self) -> usize {
        self.blocking.running + self.cpu.running + self.tokio_blocking.running + self.io.running
    }

    /// Returns the total cancellation count across all execution domains.
    ///
    /// # Returns
    ///
    /// The sum of the per-domain cancellation counts observed during the
    /// sequential stop calls. The result is not an atomic cross-domain
    /// snapshot; each domain reports cancellation according to its own
    /// contract.
    #[must_use]
    #[inline]
    pub const fn total_cancelled(&self) -> usize {
        self.blocking.cancelled + self.cpu.cancelled + self.tokio_blocking.cancelled + self.io.cancelled
    }
}
