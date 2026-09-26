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
use qubit_ioc::BuildError;
use qubit_ioc::ContainerBuilder;
use qubit_ioc::Dependency;
use qubit_ioc::FactoryError;
use tokio::runtime::Handle;

fn build_application(runtime: Handle) -> Result<ApplicationContext, Box<dyn Error>> {
    let mut builder = ContainerBuilder::new();
    builder.register_instance(Arc::new(runtime))?;
    builder.register_instance(Arc::new(FileSystemRegistry::default()))?;
    builder.register_factory::<ExecutionServices, _>(&[Dependency::of::<Handle>()], |context| {
        let runtime = context.get::<Handle>().map_err(FactoryError::new)?;
        let services = ExecutionServices::builder()
            .enable_io()
            .runtime((*runtime).clone())
            .build()
            .map_err(FactoryError::new)?;
        Ok(Arc::new(services))
    })?;
    builder.register_factory::<EventBusRegistry, _>(&[], |_| {
        let registry = EventBusRegistry::with_local().map_err(FactoryError::new)?;
        registry.seal();
        Ok(Arc::new(registry))
    })?;
    builder.register_factory::<EventBus, _>(&[Dependency::of::<EventBusRegistry>()], |context| {
        let registry = context.get::<EventBusRegistry>().map_err(FactoryError::new)?;
        registry
            .create(&EventBusConfig::default())
            .map(Arc::new)
            .map_err(FactoryError::new)
    })?;
    builder.root::<ExecutionServices>();
    builder.root::<EventBus>();
    builder.root::<FileSystemRegistry>();
    Ok(builder.build()?)
}

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let context = build_application(runtime.handle().clone())?;

    let bus = context.get::<EventBus>()?;
    let same_bus = context.get::<EventBus>()?;
    assert!(Arc::ptr_eq(&bus, &same_bus));

    let services = context.get::<ExecutionServices>()?;
    let file_systems = context.get::<FileSystemRegistry>()?;
    assert!(file_systems.is_empty());
    let result = runtime.block_on(services.spawn_io(async {
        Ok::<u8, io::Error>(43)
    })?)?;
    assert_eq!(result, 43);

    bus.shutdown(ShutdownMode::Immediate)?;
    services.shutdown();
    runtime.block_on(services.await_termination());
    assert!(services.is_terminated());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::BuildError;
    use super::ContainerBuilder;
    use super::Dependency;
    use super::EventBus;
    use super::EventBusRegistry;
    use super::FactoryError;
    use super::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::io;

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
}
