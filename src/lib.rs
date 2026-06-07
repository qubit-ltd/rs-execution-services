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

pub use execution_services::{
    BlockingExecutorService,
    BlockingExecutorServiceBuilder,
    ExecutionServices,
    TokioBlockingExecutorService,
};
pub use execution_services_build_error::ExecutionServicesBuildError;
pub use execution_services_builder::ExecutionServicesBuilder;
pub use execution_services_stop_report::ExecutionServicesStopReport;
pub use qubit_executor::service::{
    ExecutorService,
    ExecutorServiceBuilderError,
    ExecutorServiceLifecycle,
    StopReport,
    SubmissionError,
};
pub use qubit_executor::task::spi::{
    TaskResultHandle,
    TrackedTaskHandle,
};
pub use qubit_executor::{
    CancelResult,
    TaskHandle,
    TaskResult,
    TaskStatus,
    TrackedTask,
    TryGet,
};
pub use qubit_rayon_executor::{
    RayonExecutorService,
    RayonExecutorServiceBuildError,
    RayonExecutorServiceBuilder,
    RayonTaskHandle,
};
pub use qubit_thread_pool::{
    ThreadPool,
    ThreadPoolBuilder,
};
pub use qubit_tokio_executor::{
    TokioBlockingTaskHandle,
    TokioExecutorService,
    TokioIoExecutorService,
    TokioTaskHandle,
};
