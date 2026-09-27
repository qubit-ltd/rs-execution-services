// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Downstream fixture showing managed execution services in an IoC application.

use std::error::Error;
use std::io;
use std::sync::Arc;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::spi::ShutdownMode;
use qubit_execution_services::ExecutionServices;
use qubit_fs_registry::FileSystemRegistry;
use qubit_ioc::ApplicationContext;
use qubit_ioc::BindingKey;
use qubit_ioc::bean;
use qubit_ioc::ContainerBuilder;
#[cfg(test)]
use qubit_ioc::Dependency;
use qubit_ioc::CleanupError;
use qubit_ioc::FactoryError;
use qubit_ioc::Managed;
use tokio::runtime::Builder;
use tokio::runtime::Handle;

#[bean(marker = ExecutionServicesBean)]
fn execution_services(runtime: Arc<Handle>) -> Result<Managed<ExecutionServices>, FactoryError> {
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime((*runtime).clone())
        .build()
        .map_err(FactoryError::new)?;
    Ok(Managed::new(Arc::new(services), |services| {
        services.shutdown();
        Ok(())
    })
    .with_wait(|services| {
        Box::pin(async move {
            services.await_termination().await;
            Ok(())
        })
    }))
}

#[bean(marker = EventBusBean)]
fn event_bus(registry: Arc<EventBusRegistry>) -> Result<Managed<EventBus>, FactoryError> {
    let bus = registry
        .create(&EventBusConfig::default())
        .map_err(FactoryError::new)?;
    Ok(Managed::new(Arc::new(bus), |bus| {
        bus.shutdown(ShutdownMode::Immediate)
            .map(|_| ())
            .map_err(CleanupError::new)
    }))
}

async fn build_application(runtime: Handle) -> Result<ApplicationContext, Box<dyn Error>> {
    let mut builder = ContainerBuilder::new();
    builder.register_instance(Arc::new(runtime))?;
    builder.register_instance(Arc::new(FileSystemRegistry::default()))?;
    builder.install::<ExecutionServicesBean>()?;
    builder.register_factory::<EventBusRegistry, _>(&[], |_| {
        let registry = EventBusRegistry::with_local().map_err(FactoryError::new)?;
        registry.seal();
        Ok(Arc::new(registry))
    })?;
    builder.install::<EventBusBean>()?;
    builder.root::<ExecutionServices>();
    builder.root::<EventBus>();
    builder.root::<FileSystemRegistry>();
    Ok(builder.build_async().await?)
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let context = runtime.block_on(build_application(runtime.handle().clone()))?;

    let bus = context.get::<EventBus>()?;
    let same_bus = context.get::<EventBus>()?;
    assert!(Arc::ptr_eq(&bus, &same_bus));
    assert!(context.binding_sources(&BindingKey::of::<EventBus>(None)).is_some());

    let services = context.get::<ExecutionServices>()?;
    let file_systems = context.get::<FileSystemRegistry>()?;
    assert!(file_systems.is_empty());
    let result = runtime.block_on(services.spawn_io(async {
        Ok::<u8, io::Error>(43)
    })?)?;
    assert_eq!(result, 43);

    let mut shutdown = context.begin_shutdown();
    runtime.block_on(shutdown.wait())?;
    assert!(services.is_terminated());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use qubit_ioc::BuildError;

    use super::ContainerBuilder;
    use super::Dependency;
    use super::EventBus;
    use super::EventBusRegistry;
    use super::FactoryError;
    use super::Managed;
    use super::Arc;

    #[test]
    fn test_event_bus_missing_registry_prevents_factory_execution() {
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let captured_calls = Arc::clone(&factory_calls);
        let mut builder = ContainerBuilder::new();
        builder
            .register_factory::<EventBus, _>(&[Dependency::of::<EventBusRegistry>()], move |_| {
                captured_calls.fetch_add(1, Ordering::SeqCst);
                Err(FactoryError::new(io::Error::other("factory must not run")))
            })
            .expect("register event bus factory");
        builder.root::<EventBus>();

        assert!(matches!(builder.build(), Err(BuildError::MissingDependency { .. })));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_build_failure_stops_an_already_created_managed_resource() {
        let stops = Arc::new(AtomicUsize::new(0));
        let captured = Arc::clone(&stops);
        let mut builder = ContainerBuilder::new();
        builder
            .register_managed_factory::<u8, _>(&[], move |_| {
                Ok(Managed::new(Arc::new(1), move |_| {
                    captured.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }))
            })
            .expect("register managed resource");
        builder
            .register_factory::<u16, _>(&[], |_| Err(FactoryError::new(io::Error::other("expected failure"))))
            .expect("register failing factory");
        builder.root::<u16>();
        builder.root::<u8>();

        assert!(matches!(builder.build(), Err(BuildError::FactoryFailed { .. })));
        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }
}
