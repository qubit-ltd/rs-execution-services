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
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusShutdown;
use qubit_event_bus::spi::ShutdownMode;
use qubit_ioc::CleanupError;
use qubit_ioc::Managed;

#[derive(Default)]
struct AdapterState {
    ticket: Option<EventBusShutdown>,
    wait_started: bool,
}

/// Adapts an existing bus. Requests preserve errors and never block for
/// handlers. A cancellation of the IoC wait leaves its owned ticket future
/// resumable.
pub fn managed_event_bus(bus: Arc<EventBus>) -> Managed<EventBus> {
    let state = Arc::new(Mutex::new(AdapterState::default()));
    let abort_state = Arc::clone(&state);
    let graceful_state = Arc::clone(&state);
    Managed::asynchronous(bus, move |bus| {
        let requested = bus
            .request_shutdown(ShutdownMode::Immediate)
            .map_err(CleanupError::new)?;
        let mut state = abort_state.lock().expect("shutdown ticket lock");
        if !state.wait_started && state.ticket.is_none() {
            state.ticket = Some(requested);
        }
        Ok(())
    }, move |_| {
        let observer = {
            let mut state = state.lock().expect("shutdown ticket lock");
            state.wait_started = true;
            state.ticket.take()
        };
        Box::pin(async move {
            let observer =
                observer.ok_or_else(|| CleanupError::new(std::io::Error::other("missing EventBus shutdown ticket")))?;
            observer.wait_async().await.map(|_| ()).map_err(CleanupError::new)
        })
    })
    .with_graceful_stop(move |bus| {
        let requested = bus
            .request_shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(30),
            })
            .map_err(CleanupError::new)?;
        let mut state = graceful_state.lock().expect("shutdown ticket lock");
        if !state.wait_started {
            state.ticket = Some(requested);
        }
        Ok(())
    })
}
