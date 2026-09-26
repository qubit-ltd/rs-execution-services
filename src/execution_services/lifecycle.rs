// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Lifecycle operations for the execution-services facade.

use qubit_executor::service::ExecutorService;
use qubit_executor::service::ExecutorServiceLifecycle;
use tokio::join;

use super::super::ExecutionServicesStopReport;
use super::ExecutionServices;

impl ExecutionServices {
    /// Requests graceful shutdown for every execution domain.
    ///
    /// The facade records the shutdown intent before closing the domains. A
    /// submission that already passed the facade admission check may overlap
    /// domain shutdown and may be accepted or rejected by that domain. When
    /// this method returns, every enabled domain rejects new submissions.
    pub fn shutdown(&self) {
        self.admission.request_shutdown();
        if let Some(service) = &self.blocking {
            service.shutdown();
        }
        if let Some(service) = &self.cpu {
            service.shutdown();
        }
        if let Some(service) = &self.tokio_blocking {
            service.shutdown();
        }
        if let Some(service) = &self.io {
            service.shutdown();
        }
    }

    /// Requests abrupt stop for every execution domain.
    ///
    /// The facade records the stop intent before stopping the domains. A
    /// submission that already passed the facade admission check may overlap
    /// domain shutdown and may be accepted or rejected by that domain. When
    /// this method returns, every enabled domain rejects new submissions. The
    /// report samples domains sequentially in facade order.
    ///
    /// # Returns
    ///
    /// A per-domain aggregate report describing queued, running, and cancelled
    /// work observed during shutdown.
    pub fn stop(&self) -> ExecutionServicesStopReport {
        self.admission.request_stop();
        ExecutionServicesStopReport {
            blocking: self.blocking.as_ref().map(|service| service.stop()),
            cpu: self.cpu.as_ref().map(|service| service.stop()),
            tokio_blocking: self.tokio_blocking.as_ref().map(|service| service.stop()),
            io: self.io.as_ref().map(|service| service.stop()),
        }
    }

    /// Returns the aggregate lifecycle state.
    ///
    /// # Returns
    ///
    /// [`ExecutorServiceLifecycle::Terminated`] if all domains have terminated.
    /// Before termination, an aggregate stop request remains
    /// [`ExecutorServiceLifecycle::Stopping`] even if some domains have already
    /// terminated. A graceful shutdown request reports
    /// [`ExecutorServiceLifecycle::ShuttingDown`]. If a domain is stopped
    /// independently while aggregate admission is still open, its stopping
    /// state takes precedence over graceful shutdown in the aggregate result.
    #[must_use]
    pub fn lifecycle(&self) -> ExecutorServiceLifecycle {
        self.admission.lifecycle([
            self.blocking.as_ref().map(|service| service.lifecycle()),
            self.cpu.as_ref().map(|service| service.lifecycle()),
            self.tokio_blocking.as_ref().map(|service| service.lifecycle()),
            self.io.as_ref().map(|service| service.lifecycle()),
        ])
    }

    /// Returns whether every execution domain is running.
    ///
    /// # Returns
    ///
    /// `true` only if all execution domains are running.
    #[must_use]
    #[inline]
    pub fn is_running(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::Running
    }

    /// Returns whether any execution domain is gracefully shutting down.
    ///
    /// # Returns
    ///
    /// `true` when the aggregate lifecycle is
    /// [`ExecutorServiceLifecycle::ShuttingDown`].
    #[must_use]
    #[inline]
    pub fn is_shutting_down(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::ShuttingDown
    }

    /// Returns whether any execution domain is stopping abruptly.
    ///
    /// # Returns
    ///
    /// `true` after an aggregate stop request and until every domain is
    /// terminated, or while any domain is independently stopping.
    #[must_use]
    #[inline]
    pub fn is_stopping(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::Stopping
    }

    /// Returns whether the facade is no longer fully running.
    ///
    /// # Returns
    ///
    /// `true` after any execution domain starts shutdown, stop, or has already
    /// terminated.
    #[must_use]
    #[inline]
    pub fn is_not_running(&self) -> bool {
        self.lifecycle() != ExecutorServiceLifecycle::Running
    }

    /// Returns whether every execution domain has terminated.
    ///
    /// # Returns
    ///
    /// `true` only after all execution domains have terminated.
    #[must_use]
    #[inline]
    pub fn is_terminated(&self) -> bool {
        self.lifecycle() == ExecutorServiceLifecycle::Terminated
    }

    /// Waits until every execution domain has terminated.
    ///
    /// # Returns
    ///
    /// This future resolves after all execution domains have terminated.
    /// It occupies no Tokio blocking threads or background wait tasks.
    /// Dropping it cancels only this wait and leaves domain shutdown intact.
    /// The runtime supplied to the builder must remain active while its Tokio
    /// tasks are running.
    /// Request shutdown or stop before awaiting termination; this method only
    /// waits for the domains to finish.
    pub async fn await_termination(&self) {
        join!(
            async {
                if let Some(service) = &self.blocking {
                    service.await_termination().await;
                }
            },
            async {
                if let Some(service) = &self.cpu {
                    service.await_termination().await;
                }
            },
            async {
                if let Some(service) = &self.tokio_blocking {
                    service.await_termination().await;
                }
            },
            async {
                if let Some(service) = &self.io {
                    service.await_termination().await;
                }
            },
        );
    }
}
