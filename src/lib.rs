/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Qubit Execution Services
//!
//! Aggregated execution services facade for blocking, CPU-bound, Tokio blocking,
//! and async IO tasks.
//!

mod execution_services;

pub use execution_services::{
    ExecutionServices,
    ExecutionServicesBuildError,
    ExecutionServicesBuilder,
    ExecutionServicesShutdownReport,
};
pub use qubit_executor::TaskHandle;
pub use qubit_executor::service::{
    ExecutorService,
    RejectedExecution,
    ShutdownReport,
};
pub use qubit_rayon_executor::{
    RayonExecutorService,
    RayonExecutorServiceBuildError,
    RayonExecutorServiceBuilder,
    RayonTaskHandle,
};
pub use qubit_thread_pool::{
    ThreadPool,
    ThreadPoolBuildError,
    ThreadPoolBuilder,
};
pub use qubit_tokio_executor::{
    TokioExecutorService,
    TokioIoExecutorService,
    TokioTaskHandle,
};

/// Default managed service for synchronous tasks that may block an OS thread.
pub type BlockingExecutorService = ThreadPool;

/// Builder alias for configuring [`BlockingExecutorService`].
pub type BlockingExecutorServiceBuilder = ThreadPoolBuilder;

/// Tokio-backed blocking executor service routed through `spawn_blocking`.
pub type TokioBlockingExecutorService = TokioExecutorService;
