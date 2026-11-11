// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Adapts capacity-change notification sources for the shared retry loop.

use std::future::Future;

use qubit_rayon_executor::CapacityChanges;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;

/// Capacity notification source accepted by the shared retry loop.
pub(crate) trait CapacityChangeReceiver {
    /// Waits for the next notification while preserving the source's behavior.
    fn changed(&mut self) -> impl Future<Output = Result<(), watch::error::RecvError>> + Send + '_;
}

impl CapacityChangeReceiver for Receiver<u64> {
    /// Waits for Tokio's next watch notification.
    fn changed(&mut self) -> impl Future<Output = Result<(), watch::error::RecvError>> + Send + '_ {
        Receiver::changed(self)
    }
}

impl CapacityChangeReceiver for CapacityChanges {
    /// Waits through Rayon’s waker-isolating notification wrapper.
    fn changed(&mut self) -> impl Future<Output = Result<(), watch::error::RecvError>> + Send + '_ {
        CapacityChanges::changed(self)
    }
}
