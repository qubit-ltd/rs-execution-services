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

pub use execution_services::BlockingExecutorService;
pub use execution_services::BlockingExecutorServiceBuilder;
pub use execution_services::ExecutionServices;
pub use execution_services::TokioBlockingExecutorService;
pub use execution_services_build_error::ExecutionServicesBuildError;
pub use execution_services_builder::ExecutionServicesBuilder;
pub use execution_services_stop_report::ExecutionServicesStopReport;
use qubit_executor::service::ExecutorService;
use qubit_executor::service::ExecutorServiceBuilderError;
use qubit_executor::service::ExecutorServiceLifecycle;
use qubit_executor::service::StopReport;
use qubit_executor::service::SubmissionError;
use qubit_rayon_executor::RayonExecutorService;
use qubit_rayon_executor::RayonExecutorServiceBuildError;
use qubit_rayon_executor::RayonExecutorServiceBuilder;
use qubit_rayon_executor::RayonTaskHandle;
use qubit_tokio_executor::TokioBlockingTaskHandle;
use qubit_tokio_executor::TokioIoExecutorService;
use qubit_tokio_executor::TokioTaskHandle;
