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

mod execution_services;
mod execution_services_build_error;
mod execution_services_builder;
mod execution_services_stop_report;

use qubit_executor::service::{
    ExecutorService, ExecutorServiceBuilderError, ExecutorServiceLifecycle, StopReport,
    SubmissionError,
};
use qubit_rayon_executor::{
    RayonExecutorService, RayonExecutorServiceBuildError, RayonExecutorServiceBuilder,
    RayonTaskHandle,
};
use qubit_tokio_executor::{TokioBlockingTaskHandle, TokioIoExecutorService, TokioTaskHandle};

pub use execution_services::{
    BlockingExecutorService, BlockingExecutorServiceBuilder, ExecutionServices,
    TokioBlockingExecutorService,
};
pub use execution_services_build_error::ExecutionServicesBuildError;
pub use execution_services_builder::ExecutionServicesBuilder;
pub use execution_services_stop_report::ExecutionServicesStopReport;
