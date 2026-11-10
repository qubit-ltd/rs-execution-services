// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Qubit Execution Services
//!
//! An application-owned facade for four independent execution domains:
//! blocking work, CPU-bound work, Tokio blocking work, and asynchronous IO.
//! [`ExecutionServicesBuilder`] configures the enabled domains, while
//! [`ExecutionServices`] coordinates admission, snapshots, and shutdown. The
//! application creates and configures the Tokio runtime; this crate does not
//! own it.
//!
//! ## Quick start
//!
//! Build the facade inside the runtime that will drive its Tokio services.
//! Await task handles for their results, then close admission and wait for all
//! enabled services to terminate while the runtime is still running. The
//! cleanup runs on both success and ordinary task errors.
//!
//! ```
//! use qubit_execution_services::ExecutionServices;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let runtime = tokio::runtime::Builder::new_current_thread().build()?;
//! runtime.block_on(async {
//!     let services = ExecutionServices::builder()
//!         .enable_blocking()
//!         .blocking_pool_size(1)
//!         .build()?;
//!     let result = async {
//!         let task = services.submit_blocking_callable(|| Ok::<u8, std::io::Error>(42))?;
//!         assert_eq!(task.await?, 42);
//!         Ok::<(), Box<dyn std::error::Error>>(())
//!     }
//!     .await;
//!     services.shutdown();
//!     services.await_termination().await;
//!     result
//! })?;
//! # Ok(())
//! # }
//! ```
//!
//! `shutdown()` closes facade admission and requests graceful completion;
//! [`ExecutionServices::await_termination`] waits for enabled services to
//! terminate. Dropping the facade alone does not coordinate shutdown or wait
//! for accepted work. Keep the application-owned Tokio runtime alive through
//! termination of Tokio blocking and IO services.
//!
//! Capacity-wait submission methods wait until the domain accepts a task, then
//! return a handle; they do not wait for task execution to finish. Capacity
//! notifications are advisory, and waiters have no FIFO, fairness, or
//! bounded-wait guarantee. See the [waiting submission
//! methods](ExecutionServices::submit_blocking_callable_wait) for cancellation
//! and same-domain waiting constraints.
//!
//! ## Documentation
//!
//! - [English README](https://github.com/qubit-ltd/rs-execution-services/blob/main/README.md)
//! - [中文 README](https://github.com/qubit-ltd/rs-execution-services/blob/main/README.zh_CN.md)
//! - [English user guide](https://github.com/qubit-ltd/rs-execution-services/blob/main/doc/user_guide.md)
//! - [中文用户手册](https://github.com/qubit-ltd/rs-execution-services/blob/main/doc/user_guide.zh_CN.md)
//! - [English design](https://github.com/qubit-ltd/rs-execution-services/blob/main/doc/design.md)
//! - [中文设计文档](https://github.com/qubit-ltd/rs-execution-services/blob/main/doc/design.zh_CN.md)

mod execution_domain;
mod execution_services;
mod execution_services_build_error;
mod execution_services_builder;
mod execution_services_snapshot;
mod execution_services_stop_report;
mod execution_services_submission_error;

pub use execution_domain::ExecutionDomain;
pub use execution_services::ExecutionServices;
pub use execution_services_build_error::ExecutionServicesBuildError;
pub use execution_services_builder::ExecutionServicesBuilder;
pub use execution_services_snapshot::ExecutionServicesSnapshot;
pub use execution_services_stop_report::ExecutionServicesStopReport;
pub use execution_services_submission_error::ExecutionServicesSubmissionError;
