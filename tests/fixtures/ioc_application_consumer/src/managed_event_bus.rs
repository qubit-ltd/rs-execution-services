// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Nonblocking EventBus requests with one retained shutdown observer.
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusShutdown;
use qubit_event_bus::spi::ShutdownMode;
use qubit_ioc::CleanupError;
use qubit_ioc::Managed;

/// Adapts an existing bus. Requests preserve errors and never block for
/// handlers. A cancellation of the IoC wait leaves its owned ticket future
/// resumable.
pub fn managed_event_bus(bus: Arc<EventBus>) -> Managed<EventBus> {
    let ticket = Arc::new(Mutex::new(None::<EventBusShutdown>));
    let active = Arc::new(AtomicBool::new(false));
    let abort_ticket = Arc::clone(&ticket);
    let abort_active = Arc::clone(&active);
    let graceful_ticket = Arc::clone(&ticket);
    Managed::new(bus, move |bus| {
        let requested = bus
            .request_shutdown(ShutdownMode::Immediate)
            .map_err(CleanupError::new)?;
        let mut slot = abort_ticket.lock().expect("shutdown ticket lock");
        if slot.is_none() && !abort_active.load(Ordering::SeqCst) {
            *slot = Some(requested);
        }
        Ok(())
    })
    .with_graceful_stop(move |bus| {
        let requested = bus
            .request_shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(30),
            })
            .map_err(CleanupError::new)?;
        *graceful_ticket.lock().expect("shutdown ticket lock") = Some(requested);
        Ok(())
    })
    .with_wait(move |_| {
        let mut slot = ticket.lock().expect("shutdown ticket lock");
        active.store(true, Ordering::SeqCst);
        let observer = slot.take();
        Box::pin(async move {
            let observer =
                observer.ok_or_else(|| CleanupError::new(std::io::Error::other("missing EventBus shutdown ticket")))?;
            observer.wait_async().await.map(|_| ()).map_err(CleanupError::new)
        })
    })
}
