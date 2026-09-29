// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Application-owned real execution, messaging, and filesystem resources.
mod application;
mod flush_worker;
pub mod managed_event_bus;
pub mod managed_execution_services;

pub use application::build_application;
pub use flush_worker::FlushWorker;
