/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Shutdown report for the execution-services facade.

use super::ShutdownReport;

/// Aggregate report returned by [`super::ExecutionServices::shutdown_now`].
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionServicesShutdownReport {
    /// Shutdown report for the blocking executor domain.
    pub blocking: ShutdownReport,
    /// Shutdown report for the CPU executor domain.
    pub cpu: ShutdownReport,
    /// Shutdown report for the Tokio blocking executor domain.
    pub tokio_blocking: ShutdownReport,
    /// Shutdown report for the Tokio async IO executor domain.
    pub io: ShutdownReport,
}

impl ExecutionServicesShutdownReport {
    /// Returns the total queued task count across all execution domains.
    ///
    /// # Returns
    ///
    /// The sum of every domain's queued-task count.
    #[inline]
    pub const fn total_queued(&self) -> usize {
        self.blocking.queued + self.cpu.queued + self.tokio_blocking.queued + self.io.queued
    }

    /// Returns the total running task count across all execution domains.
    ///
    /// # Returns
    ///
    /// The sum of every domain's running-task count.
    #[inline]
    pub const fn total_running(&self) -> usize {
        self.blocking.running + self.cpu.running + self.tokio_blocking.running + self.io.running
    }

    /// Returns the total cancellation count across all execution domains.
    ///
    /// # Returns
    ///
    /// The sum of every domain's cancelled-task count.
    #[inline]
    pub const fn total_cancelled(&self) -> usize {
        self.blocking.cancelled
            + self.cpu.cancelled
            + self.tokio_blocking.cancelled
            + self.io.cancelled
    }
}
