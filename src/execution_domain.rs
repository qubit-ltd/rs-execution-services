// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Execution domains exposed by the facade.

/// Identifies one independently configurable execution domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionDomain {
    /// Managed pool for synchronous tasks that may block OS threads.
    Blocking,
    /// Rayon pool for CPU-bound synchronous tasks.
    Cpu,
    /// Tokio `spawn_blocking` service.
    TokioBlocking,
    /// Tokio async task service.
    Io,
}
