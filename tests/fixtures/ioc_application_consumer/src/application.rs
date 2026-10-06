// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Assembles resources and declares the business consumer's dependency edges.
use std::path::{Path, PathBuf};
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
use qubit_ioc::BuildFailure;
use qubit_ioc::ContainerBuilder;
use qubit_ioc::FactoryError;
use qubit_ioc::RegistrationError;
use qubit_ioc::WaitPolicy;
use tokio::runtime::Handle;

use crate::flush_worker::FlushWorker;
use crate::managed_event_bus::managed_event_bus;
use crate::managed_execution_services::managed_execution_services;

/// Registration or construction failures, retaining any rollback owner.
#[derive(Debug, thiserror::Error)]
pub enum ApplicationBuildError {
    /// The builder rejected a component definition.
    #[error(transparent)]
    Registration(#[from] RegistrationError),
    /// Construction failed, possibly with managed cleanup still pending.
    #[error(transparent)]
    Build(#[from] BuildFailure),
}

/// Builds an application, creating a report under the caller-owned temporary
/// `root` after graph validation. Keep both root and runtime alive until shutdown
/// completes. Registration or construction failures preserve their error source
/// (including a BuildFailure owner). This synchronous function does not
/// enter the runtime; the caller borrows `BuildFailure` with `wait_cleanup`
/// if needed, keeping the original cause available while cleanup completes.
pub fn build_application(
    runtime: Handle,
    root: &Path,
) -> Result<Application, ApplicationBuildError> {
    let mut builder = ContainerBuilder::new().wait_policy(WaitPolicy::bounded_with_total(
        Duration::from_secs(30),
        Duration::from_secs(30),
        Duration::from_secs(90),
        |duration| Box::pin(tokio::time::sleep(duration)),
    ));
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
    register_file_system(&mut builder, root.to_path_buf())?;
    builder.root::<FlushWorker>();
    builder.root::<FileSystemRegistry>();
    Ok(builder.build()?)
}

/// Registers a factory that creates the report and registry under `root` only
/// after graph validation. Registration errors are returned immediately; setup
/// errors are retained as factory errors in a build failure.
fn register_file_system(
    builder: &mut ContainerBuilder,
    root: PathBuf,
) -> Result<(), RegistrationError> {
    builder.register_factory::<FileSystemRegistry, _>(&[], move |_| {
        let registry = FileSystemRegistry::default();
        let policy = LocalResourcePolicy::bounded(
            LocalListResourceLimits::new(16, 10_000, 8_388_608, 32, Duration::from_secs(30))
                .map_err(FactoryError::new)?,
            LocalCopyResourceLimits::new(16, 10_000, 1_073_741_824, 32, Duration::from_secs(30))
                .map_err(FactoryError::new)?,
            LocalDeleteResourceLimits::new(16, 10_000, 8_388_608, Duration::from_secs(30)),
        );
        let id = FileSystemId::new("reports").map_err(FactoryError::new)?;
        let provider =
            LocalFileSystemProvider::rooted(id, &root, policy).map_err(FactoryError::new)?;
        registry.register(provider).map_err(FactoryError::new)?;
        std::fs::write(root.join("report.csv"), b"name,total\nexample,42\n")
            .map_err(FactoryError::new)?;
        Ok(Arc::new(registry))
    })
}

#[cfg(test)]
mod tests {
    use qubit_fs_registry::FileSystemRegistry;
    use qubit_ioc::{BuildError, ContainerBuilder};

    use super::register_file_system;

    /// An invalid root must stop construction before the report is written.
    #[test]
    fn test_invalid_graph_does_not_create_report() {
        struct Missing;
        let root = tempfile::tempdir().expect("temporary root");
        let mut builder = ContainerBuilder::new();
        register_file_system(&mut builder, root.path().to_path_buf()).expect("register factory");
        builder.root::<FileSystemRegistry>();
        builder.root::<Missing>();
        let failure = match builder.build() {
            Ok(_) => panic!("missing root must fail before factories"),
            Err(failure) => failure,
        };
        assert!(matches!(failure.cause(), BuildError::MissingRoot { .. }));
        assert!(!root.path().join("report.csv").exists());
    }
}
