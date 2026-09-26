// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Qubit Execution Services
//!
//! Aggregated execution services facade for blocking, CPU-bound, Tokio
//! blocking, and async IO tasks.

mod execution_domain;
mod execution_services;
mod execution_services_build_error;
mod execution_services_builder;
mod execution_services_stop_report;
mod execution_services_submission_error;

pub use execution_domain::ExecutionDomain;
pub use execution_services::ExecutionServices;
pub use execution_services_build_error::ExecutionServicesBuildError;
pub use execution_services_builder::ExecutionServicesBuilder;
pub use execution_services_stop_report::ExecutionServicesStopReport;
pub use execution_services_submission_error::ExecutionServicesSubmissionError;
