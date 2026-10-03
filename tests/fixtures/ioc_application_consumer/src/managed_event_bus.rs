// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Nonblocking EventBus requests with one retained shutdown observer.
use std::sync::Arc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::spi::ShutdownMode;
use qubit_ioc::CleanupError;
use qubit_ioc::Managed;

/// Adapts an existing bus. Abort and graceful requests return shutdown tickets
/// without waiting for handlers. Their errors remain in the IoC shutdown report,
/// and a cancelled wait resumes the same ticket observation.
pub fn managed_event_bus(bus: Arc<EventBus>) -> Managed<EventBus> {
    Managed::asynchronous_with_graceful_ticket(
        bus,
        |bus| {
            bus.request_shutdown(ShutdownMode::Immediate)
                .map_err(CleanupError::new)
        },
        |bus| {
            bus.request_shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(30),
            })
            .map_err(CleanupError::new)
        },
        |_, ticket| {
            Box::pin(async move {
                ticket.wait_async().await.map(|_| ()).map_err(CleanupError::new)
            })
        },
    )
}
