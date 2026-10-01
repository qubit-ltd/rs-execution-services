// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Assembles resources and declares the business consumer's dependency edges.
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_execution_services::ExecutionServices;
use qubit_fs::metadata::FileSystemId;
use qubit_fs_local::LocalCopyResourceLimits;
use qubit_fs_local::LocalDeleteResourceLimits;
use qubit_fs_local::LocalFileSystemProvider;
use qubit_fs_local::LocalListResourceLimits;
use qubit_fs_local::LocalResourcePolicy;
use qubit_fs_registry::FileSystemRegistry;
use qubit_ioc::Application;
use qubit_ioc::ContainerBuilder;
use qubit_ioc::FactoryError;
use qubit_ioc::WaitPolicy;
use tokio::runtime::Handle;

use crate::flush_worker::FlushWorker;
use crate::managed_event_bus::managed_event_bus;
use crate::managed_execution_services::managed_execution_services;

/// Creates a report under the caller-owned temporary `root` and builds its
/// application. Keep both root and runtime alive until shutdown completes.
/// Filesystem, registration, or construction failures preserve their error
/// source (including a BuildFailure owner).
pub fn build_application(runtime: Handle, root: &Path) -> Result<Application, Box<dyn Error>> {
    std::fs::write(root.join("report.csv"), b"name,total\nexample,42\n")?;
    let registry = FileSystemRegistry::default();
    let policy = LocalResourcePolicy::bounded(
        LocalListResourceLimits::new(16, 10_000, 8_388_608, 32, Duration::from_secs(30))?,
        LocalCopyResourceLimits::new(16, 10_000, 1_073_741_824, 32, Duration::from_secs(30))?,
        LocalDeleteResourceLimits::new(16, 10_000, 8_388_608, Duration::from_secs(30)),
    );
    registry.register(LocalFileSystemProvider::rooted(
        FileSystemId::new("reports")?,
        root,
        policy,
    )?)?;
    let mut builder = ContainerBuilder::new().wait_policy(WaitPolicy::bounded_with_total(
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(90),
        |duration| Box::pin(tokio::time::sleep(duration)),
    ));
    builder.register_instance(Arc::new(registry))?;
    builder.register_injected_managed_factory::<ExecutionServices, (), _>(|()| managed_execution_services(runtime))?;
    builder.register_injected_factory::<EventBusRegistry, (), _>(|()| {
        let registry = EventBusRegistry::with_local().map_err(FactoryError::new)?;
        registry.seal();
        Ok(Arc::new(registry))
    })?;
    builder.register_injected_managed_factory::<EventBus, (Arc<EventBusRegistry>,), _>(|(registry,)| {
        let bus = registry.create(&EventBusConfig::default()).map_err(FactoryError::new)?;
        Ok(managed_event_bus(Arc::new(bus)))
    })?;
    builder.register_injected_managed_factory::<FlushWorker, (Arc<ExecutionServices>, Arc<EventBus>), _>(
        |(services, bus)| FlushWorker::managed(services, bus),
    )?;
    builder.root::<FlushWorker>();
    builder.root::<FileSystemRegistry>();
    Ok(builder.build()?)
}
