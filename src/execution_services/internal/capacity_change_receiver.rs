// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;

use qubit_rayon_executor::CapacityChanges;
use tokio::sync::watch::Receiver;

/// Receiver abstraction shared by CPU and blocking service capacity waits.
pub(crate) trait CapacityChangeReceiver {
    /// Waits for a capacity notification or channel closure.
    ///
    /// # Returns
    ///
    /// A future that resolves when the receiver observes a channel change.
    fn changed(&mut self) -> impl Future<Output = Result<(), tokio::sync::watch::error::RecvError>> + Send + '_;
}

impl CapacityChangeReceiver for Receiver<u64> {
    /// Waits for the next Tokio watch notification.
    fn changed(&mut self) -> impl Future<Output = Result<(), tokio::sync::watch::error::RecvError>> + Send + '_ {
        Receiver::changed(self)
    }
}

impl CapacityChangeReceiver for CapacityChanges {
    /// Waits for the next isolated capacity notification.
    fn changed(&mut self) -> impl Future<Output = Result<(), tokio::sync::watch::error::RecvError>> + Send + '_ {
        CapacityChanges::changed(self)
    }
}
