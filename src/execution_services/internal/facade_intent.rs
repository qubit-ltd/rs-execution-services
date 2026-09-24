// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate lifecycle intent for the execution-services facade.

/// Records whether the facade accepts work or has begun closing.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FacadeIntent {
    /// The facade still accepts new work.
    Running,
    /// Graceful shutdown has been requested.
    ShuttingDown,
    /// Abrupt stop has been requested and cannot be downgraded.
    Stopping,
}
