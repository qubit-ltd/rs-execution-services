// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Adapts capacity-change notification sources for the shared retry loop.

use std::future::Future;
use std::pin::Pin;

use qubit_rayon_executor::CapacityChanges;
use tokio::sync::watch;
use tokio::sync::watch::Receiver;

/// Capacity notification source accepted by the shared retry loop.
pub(crate) trait CapacityChangeReceiver {
    /// Future returned by [`Self::changed`].
    type Changed<'a>: Future<Output = Result<(), watch::error::RecvError>> + Send + 'a
    where
        Self: 'a;

    /// Waits for the next notification while preserving the source's behavior.
    fn changed(&mut self) -> Self::Changed<'_>;
}

impl CapacityChangeReceiver for Receiver<u64> {
    /// Boxed future returned by Tokio's watch receiver.
    type Changed<'a> = Pin<Box<dyn Future<Output = Result<(), watch::error::RecvError>> + Send + 'a>>;

    /// Waits for Tokio's next watch notification.
    fn changed(&mut self) -> Self::Changed<'_> {
        Box::pin(Receiver::changed(self))
    }
}

impl CapacityChangeReceiver for CapacityChanges {
    /// Panic-isolated future returned by Rayon capacity notifications.
    type Changed<'a> = qubit_rayon_executor::CapacityChangesChanged<'a>;

    /// Waits through Rayon’s waker-isolating notification wrapper.
    fn changed(&mut self) -> Self::Changed<'_> {
        CapacityChanges::changed(self)
    }
}
