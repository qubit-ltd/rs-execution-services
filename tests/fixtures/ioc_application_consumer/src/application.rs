// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Assembles resources and declares the business consumer's dependency edges.
use std::path::Path;
use std::path::PathBuf;
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
use qubit_ioc::RegistrationError;
use qubit_ioc::SettledBuildFailure;
use qubit_ioc::ValidationScope;
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
    Registration(Box<RegistrationError>),
    /// Construction failed after rollback waiting, with its result preserved.
    #[error(transparent)]
    Build(Box<SettledBuildFailure>),
}

impl From<RegistrationError> for ApplicationBuildError {
    fn from(error: RegistrationError) -> Self {
        Self::Registration(Box::new(error))
    }
}

impl From<Box<RegistrationError>> for ApplicationBuildError {
    fn from(error: Box<RegistrationError>) -> Self {
        Self::Registration(error)
    }
}

impl From<SettledBuildFailure> for ApplicationBuildError {
    fn from(failure: SettledBuildFailure) -> Self {
        Self::Build(Box::new(failure))
    }
}

/// Builds an application using the caller-owned filesystem root. Keep both root
/// and runtime alive until shutdown completes. A construction failure is
/// returned only after managed rollback has finished, with its original cause
/// and cleanup report preserved.
pub async fn build_application(
    runtime: Handle,
    root: &Path,
) -> Result<Application, ApplicationBuildError> {
    let mut builder = ContainerBuilder::new()
        .wait_policy(WaitPolicy::bounded_with_total(
            Duration::from_secs(30),
            Duration::from_secs(30),
            Duration::from_secs(90),
            |duration| Box::pin(tokio::time::sleep(duration)),
        ))
        .validation_scope(ValidationScope::AllActive);
    builder.register_injected_managed_factory::<ExecutionServices, (), _>(|()| {
        managed_execution_services(runtime)
    })?;
    builder.register_injected_factory::<EventBusRegistry, (), _>(|()| {
        let registry = EventBusRegistry::with_local().map_err(FactoryError::new)?;
        registry.seal();
        Ok(Arc::new(registry))
    })?;
    builder.register_injected_managed_factory::<EventBus, (Arc<EventBusRegistry>,), _>(
        |(registry,)| {
            let bus = registry
                .create(&EventBusConfig::default())
                .map_err(FactoryError::new)?;
            Ok(managed_event_bus(Arc::new(bus)))
        },
    )?;
    builder.register_injected_managed_factory::<FlushWorker, (Arc<ExecutionServices>, Arc<EventBus>), _>(
        |(services, bus)| FlushWorker::managed(services, bus),
    )?;
    register_file_system(&mut builder, root.to_path_buf())?;
    builder.root::<FlushWorker>();
    builder.root::<FileSystemRegistry>();
    Ok(builder.build_settled().await?)
}

/// Writes the business report under the caller-provided root.
pub fn write_report(root: &Path) -> std::io::Result<()> {
    std::fs::write(root.join("report.csv"), b"name,total\nexample,42\n")
}

/// Registers a factory that creates a registry and rooted provider under
/// `root`. Registration errors are returned immediately; setup errors are
/// retained as factory errors in a settled build failure.
fn register_file_system(
    builder: &mut ContainerBuilder,
    root: PathBuf,
) -> Result<(), Box<RegistrationError>> {
    builder
        .register_factory::<FileSystemRegistry, _>(&[], move |_| {
            let registry = FileSystemRegistry::default();
            let policy = LocalResourcePolicy::bounded(
                LocalListResourceLimits::new(16, 10_000, 8_388_608, 32, Duration::from_secs(30))
                    .map_err(FactoryError::new)?,
                LocalCopyResourceLimits::new(
                    16,
                    10_000,
                    1_073_741_824,
                    32,
                    Duration::from_secs(30),
                )
                .map_err(FactoryError::new)?,
                LocalDeleteResourceLimits::new(16, 10_000, 8_388_608, Duration::from_secs(30)),
            );
            let id = FileSystemId::new("reports").map_err(FactoryError::new)?;
            let provider =
                LocalFileSystemProvider::rooted(id, &root, policy).map_err(FactoryError::new)?;
            registry.register(provider).map_err(FactoryError::new)?;
            Ok(Arc::new(registry))
        })
        .map_err(Box::new)
}

#[cfg(test)]
mod tests {
    use std::io;

    use qubit_fs_registry::FileSystemRegistry;
    use qubit_ioc::BuildError;
    use qubit_ioc::ContainerBuilder;
    use qubit_ioc::Dependency;
    use qubit_ioc::FactoryError;

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

    /// A later factory failure must not leave a report behind.
    #[tokio::test]
    async fn test_later_factory_failure_does_not_create_report() {
        let root = tempfile::tempdir().expect("temporary root");
        let mut builder = ContainerBuilder::new();
        register_file_system(&mut builder, root.path().to_path_buf()).expect("register filesystem");
        builder
            .register_factory::<u8, _>(&[Dependency::of::<FileSystemRegistry>()], |_| {
                Err(FactoryError::new(io::Error::other("expected failure")))
            })
            .expect("register failing factory");
        builder.root::<u8>();

        let failure = match builder.build_settled().await {
            Ok(_) => panic!("failing factory must fail construction"),
            Err(failure) => failure,
        };
        assert!(matches!(failure.cause(), BuildError::FactoryFailed { .. }));
        assert!(!root.path().join("report.csv").exists());
    }
}
