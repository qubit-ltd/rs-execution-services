// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Real resource integration through application ownership.
use ioc_application_consumer::FlushWorker;
use ioc_application_consumer::build_application;
use ioc_application_consumer::write_report;
use qubit_execution_services::ExecutionServices;
use qubit_fs::path::ConnectionUri;
use qubit_fs_registry::FileSystemConfig;
use qubit_fs_registry::FileSystemRegistry;
use qubit_ioc::BindingKey;
use qubit_ioc::ShutdownMode;
use tokio::runtime::Builder;

/// Final flush must submit through services while dependencies still admit
/// work.
#[test]
fn test_graceful_shutdown_flushes_before_stopping_dependencies() {
    let runtime = Builder::new_multi_thread().enable_all().build().expect("runtime");
    let root = tempfile::tempdir().expect("temporary report root");
    let application = runtime
        .block_on(build_application(runtime.handle().clone(), root.path()))
        .expect("application");
    assert!(!root.path().join("report.csv").exists());
    let context = application.context();
    let retained = context.clone();
    let worker = context.get::<FlushWorker>().expect("worker");
    let services = context.get::<ExecutionServices>().expect("services");
    assert!(worker.messages().is_empty());
    let mut shutdown = application.begin_shutdown(ShutdownMode::Graceful);
    let report = runtime.block_on(shutdown.wait()).expect("report");
    assert!(report.is_success(), "{report:?}");
    assert_eq!(worker.messages(), ["final-report:22"]);
    assert_eq!(worker.final_task_result(), Some(22));
    assert_eq!(worker.graceful_requests(), 1);
    assert!(services.is_terminated());
    assert!(retained.binding_sources(&BindingKey::of::<FlushWorker>(None)).is_some());
}

/// Filesystem resolution must reach a rooted provider and return real metadata.
#[test]
fn test_registry_resolves_real_report_metadata() {
    let runtime = Builder::new_multi_thread().enable_all().build().expect("runtime");
    let root = tempfile::tempdir().expect("temporary report root");
    let application = runtime
        .block_on(build_application(runtime.handle().clone(), root.path()))
        .expect("application");
    assert!(!root.path().join("report.csv").exists());
    write_report(root.path()).expect("write report after build");
    let registry = application.context().get::<FileSystemRegistry>().expect("registry");
    let config = FileSystemConfig::new(ConnectionUri::parse("file:///report.csv").expect("uri"));
    let resolution = registry.resolve_config(&config).expect("resolve report");
    assert_eq!(resolution.file_system().stat(resolution.path()).expect("stat").len(), Some(22));
    assert_eq!(resolution.canonical_uri().as_str(), "file:///report.csv");
    let mut shutdown = application.begin_shutdown(ShutdownMode::Immediate);
    assert!(runtime.block_on(shutdown.wait()).expect("report").is_success());
}
