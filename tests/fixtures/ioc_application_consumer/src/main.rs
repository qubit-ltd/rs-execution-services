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
use qubit_ioc::CleanupError;
use qubit_ioc::ContainerBuilder;
#[cfg(test)]
use qubit_ioc::Dependency;
use qubit_ioc::FactoryError;
use qubit_ioc::Managed;
use qubit_ioc::bean;
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
        let _stop_report = services.stop();
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
    let bus = registry.create(&EventBusConfig::default()).map_err(FactoryError::new)?;
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
    let runtime = Builder::new_multi_thread().enable_all().build()?;
    let context = runtime.block_on(build_application(runtime.handle().clone()))?;

    let bus = context.get::<EventBus>()?;
    let same_bus = context.get::<EventBus>()?;
    assert!(Arc::ptr_eq(&bus, &same_bus));
    assert!(context.binding_sources(&BindingKey::of::<EventBus>(None)).is_some());

    let services = context.get::<ExecutionServices>()?;
    let file_systems = context.get::<FileSystemRegistry>()?;
    assert!(file_systems.is_empty());
    let result = runtime.block_on(services.spawn_io(async { Ok::<u8, io::Error>(43) })?)?;
    assert_eq!(result, 43);

    let mut shutdown = context.begin_shutdown();
    runtime.block_on(shutdown.wait())?;
    assert!(services.is_terminated());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use qubit_execution_services::ExecutionServicesSubmissionError;
    use qubit_ioc::BuildError;
    use tokio::sync::oneshot;
    use tokio::time::timeout;

    use super::Arc;
    use super::Builder;
    use super::ContainerBuilder;
    use super::Dependency;
    use super::EventBus;
    use super::EventBusRegistry;
    use super::ExecutionServices;
    use super::FactoryError;
    use super::Managed;
    use super::build_application;

    /// Notifies the test when a running IO future is released during stop.
    struct TaskGuard {
        ended: Option<oneshot::Sender<()>>,
    }

    impl Drop for TaskGuard {
        fn drop(&mut self) {
            if let Some(sender) = self.ended.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn test_ioc_shutdown_terminates_running_execution_services() {
        let runtime = Builder::new_multi_thread().enable_all().build().expect("create test runtime");
        runtime.block_on(async {
            let context = build_application(runtime.handle().clone()).await.expect("build application");
            let services = context.get::<ExecutionServices>().expect("resolve execution services");
            let (started_tx, started_rx) = oneshot::channel();
            let (ended_tx, ended_rx) = oneshot::channel();
            let task = services
                .spawn_io(async move {
                    let _guard = TaskGuard { ended: Some(ended_tx) };
                    let _ = started_tx.send(());
                    std::future::pending::<()>().await;
                    Ok::<(), io::Error>(())
                })
                .expect("submit pending IO task");
            timeout(Duration::from_secs(5), started_rx)
                .await
                .expect("IO task should start before timeout")
                .expect("IO task should notify startup");
            assert!(!services.is_terminated());

            let mut shutdown = context.begin_shutdown();
            timeout(Duration::from_secs(5), shutdown.wait())
                .await
                .expect("IoC shutdown should terminate IO services before timeout")
                .expect("managed shutdown should succeed");
            timeout(Duration::from_secs(5), ended_rx)
                .await
                .expect("IO task should be released before timeout")
                .expect("IO task guard should notify release");
            assert!(services.is_terminated());
            assert!(services.spawn_io(async { Ok::<(), io::Error>(()) }).is_err());
            drop(task);
        });
    }

    #[test]
    fn test_event_bus_missing_registry_prevents_factory_execution() {
        let runtime = Builder::new_multi_thread().enable_all().build().expect("create test runtime");
        let creates = Arc::new(AtomicUsize::new(0));
        let observed_services = Arc::new(Mutex::new(None::<Arc<ExecutionServices>>));
        let factory_calls = Arc::new(AtomicUsize::new(0));
        let captured_calls = Arc::clone(&factory_calls);
        let create_count = Arc::clone(&creates);
        let observed_slot = Arc::clone(&observed_services);
        let handle = runtime.handle().clone();
        let mut builder = ContainerBuilder::new();
        builder
            .register_managed_factory::<ExecutionServices, _>(&[], move |_| {
                create_count.fetch_add(1, Ordering::SeqCst);
                let services = Arc::new(
                    ExecutionServices::builder()
                        .enable_io()
                        .runtime(handle)
                        .build()
                        .map_err(FactoryError::new)?,
                );
                *observed_slot.lock().expect("lock observed services") = Some(Arc::clone(&services));
                Ok(Managed::new(services, |services| {
                    let _stop_report = services.stop();
                    Ok(())
                }))
            })
            .expect("register managed execution services");
        builder
            .register_factory::<EventBus, _>(&[
                Dependency::of::<ExecutionServices>(),
                Dependency::of::<EventBusRegistry>(),
            ], move |_| {
                captured_calls.fetch_add(1, Ordering::SeqCst);
                Err(FactoryError::new(io::Error::other("factory must not run")))
            })
            .expect("register event bus factory");
        builder.root::<EventBus>();

        assert!(matches!(builder.build(), Err(BuildError::MissingDependency { .. })));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        assert_eq!(creates.load(Ordering::SeqCst), 0);
        assert!(observed_services.lock().expect("lock observed services").is_none());
    }

    #[test]
    fn test_async_build_failure_stops_managed_execution_services_once() {
        let runtime = Builder::new_multi_thread().enable_all().build().expect("create test runtime");
        runtime.block_on(async {
            let creates = Arc::new(AtomicUsize::new(0));
            let observed_services = Arc::new(Mutex::new(None::<Arc<ExecutionServices>>));
            let stops = Arc::new(AtomicUsize::new(0));
            let waits = Arc::new(AtomicUsize::new(0));
            let mut builder = ContainerBuilder::new();
            let create_count = Arc::clone(&creates);
            let observed_slot = Arc::clone(&observed_services);
            let handle = runtime.handle().clone();
            let stop_count = Arc::clone(&stops);
            let wait_count = Arc::clone(&waits);
            builder
                .register_managed_factory::<ExecutionServices, _>(&[], move |_| {
                    create_count.fetch_add(1, Ordering::SeqCst);
                    let services = Arc::new(
                        ExecutionServices::builder()
                            .enable_io()
                            .runtime(handle)
                            .build()
                            .map_err(FactoryError::new)?,
                    );
                    *observed_slot.lock().expect("lock observed services") = Some(Arc::clone(&services));
                    Ok(Managed::new(services, move |services| {
                        stop_count.fetch_add(1, Ordering::SeqCst);
                        let _stop_report = services.stop();
                        Ok(())
                    })
                    .with_wait(move |services| {
                        wait_count.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async move {
                            services.await_termination().await;
                            Ok(())
                        })
                    }))
                })
                .expect("register managed execution services");
            let expected_slot = Arc::clone(&observed_services);
            builder
                .register_async_factory::<u8, _>(&[Dependency::of::<ExecutionServices>()], move |context| {
                    let dependency = context.get::<ExecutionServices>();
                    Box::pin(async move {
                        let services = dependency.map_err(FactoryError::new)?;
                        assert!(Arc::ptr_eq(
                            &services,
                            expected_slot.lock().expect("lock observed services").as_ref().expect("resource was handed off"),
                        ));
                        let (task_started_tx, task_started_rx) = oneshot::channel();
                        let _task = services
                            .spawn_io(async move {
                                let _ = task_started_tx.send(());
                                std::future::pending::<()>().await;
                                Ok::<(), io::Error>(())
                            })
                            .map_err(FactoryError::new)?;
                        timeout(Duration::from_secs(5), task_started_rx)
                            .await
                            .expect("IO task should start before timeout")
                            .expect("IO task should notify startup");
                        Err(FactoryError::new(io::Error::other("expected downstream failure")))
                    })
                })
                .expect("register dependent failing factory");
            builder.root::<u8>();

            assert!(matches!(
                timeout(Duration::from_secs(5), builder.build_async()).await.expect("failed build cleanup should complete"),
                Err(BuildError::FactoryFailed { .. })
            ));
            let services = observed_services.lock().expect("lock observed services").clone().expect("resource was handed off");
            assert_eq!(creates.load(Ordering::SeqCst), 1);
            assert_eq!(stops.load(Ordering::SeqCst), 1);
            assert_eq!(waits.load(Ordering::SeqCst), 1);
            assert!(services.is_terminated());
            assert!(matches!(
                services.spawn_io(async { Ok::<(), io::Error>(()) }),
                Err(ExecutionServicesSubmissionError::Rejected { .. })
            ));
        });
    }

    #[test]
    fn test_cancelling_async_build_stops_managed_execution_services_once() {
        let runtime = Builder::new_multi_thread().enable_all().build().expect("create test runtime");
        runtime.block_on(async {
            let creates = Arc::new(AtomicUsize::new(0));
            let observed_services = Arc::new(Mutex::new(None::<Arc<ExecutionServices>>));
            let stops = Arc::new(AtomicUsize::new(0));
            let waits = Arc::new(AtomicUsize::new(0));
            let (factory_started_tx, factory_started_rx) = oneshot::channel();
            let mut builder = ContainerBuilder::new();
            let create_count = Arc::clone(&creates);
            let observed_slot = Arc::clone(&observed_services);
            let handle = runtime.handle().clone();
            let stop_count = Arc::clone(&stops);
            let wait_count = Arc::clone(&waits);
            builder
                .register_managed_factory::<ExecutionServices, _>(&[], move |_| {
                    create_count.fetch_add(1, Ordering::SeqCst);
                    let services = Arc::new(
                        ExecutionServices::builder()
                            .enable_io()
                            .runtime(handle)
                            .build()
                            .map_err(FactoryError::new)?,
                    );
                    *observed_slot.lock().expect("lock observed services") = Some(Arc::clone(&services));
                    Ok(Managed::new(services, move |services| {
                        stop_count.fetch_add(1, Ordering::SeqCst);
                        let _stop_report = services.stop();
                        Ok(())
                    })
                    .with_wait(move |services| {
                        wait_count.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async move {
                            services.await_termination().await;
                            Ok(())
                        })
                    }))
                })
                .expect("register managed execution services");
            builder
                .register_async_factory::<u8, _>(&[Dependency::of::<ExecutionServices>()], move |context| {
                    let dependency = context.get::<ExecutionServices>();
                    Box::pin(async move {
                        let _services = dependency.map_err(FactoryError::new)?;
                        factory_started_tx.send(()).expect("test receiver should be alive");
                        std::future::pending::<()>().await;
                        Ok(Arc::new(1))
                    })
                })
                .expect("register pending async factory");
            builder.root::<u8>();

            let mut build = Box::pin(builder.build_async());
            let mut factory_started = Box::pin(factory_started_rx);
            timeout(Duration::from_secs(5), std::future::poll_fn(|context| {
                if build.as_mut().poll(context).is_ready() {
                    panic!("pending async factory unexpectedly completed");
                }
                factory_started.as_mut().poll(context).map(|result| {
                    result.expect("dependent async factory should start")
                })
            }))
            .await
            .expect("dependent async factory should start before timeout");
            drop(build);

            let services = observed_services.lock().expect("lock observed services").clone().expect("resource was handed off");
            assert_eq!(creates.load(Ordering::SeqCst), 1);
            assert_eq!(stops.load(Ordering::SeqCst), 1);
            assert_eq!(waits.load(Ordering::SeqCst), 0);
            timeout(Duration::from_secs(5), services.await_termination())
                .await
                .expect("cancelled build should stop execution services before timeout");
            assert!(services.is_terminated());
        });
    }

    #[test]
    fn test_build_failure_stops_an_already_created_managed_resource() {
        let runtime = Builder::new_multi_thread().enable_all().build().expect("create test runtime");
        let creates = Arc::new(AtomicUsize::new(0));
        let observed_services = Arc::new(Mutex::new(None::<Arc<ExecutionServices>>));
        let stops = Arc::new(AtomicUsize::new(0));
        let waits = Arc::new(AtomicUsize::new(0));
        let create_count = Arc::clone(&creates);
        let observed_slot = Arc::clone(&observed_services);
        let handle = runtime.handle().clone();
        let stop_count = Arc::clone(&stops);
        let wait_count = Arc::clone(&waits);
        let mut builder = ContainerBuilder::new();
        builder
            .register_managed_factory::<ExecutionServices, _>(&[], move |_| {
                create_count.fetch_add(1, Ordering::SeqCst);
                let services = Arc::new(
                    ExecutionServices::builder()
                        .enable_io()
                        .runtime(handle)
                        .build()
                        .map_err(FactoryError::new)?,
                );
                *observed_slot.lock().expect("lock observed services") = Some(Arc::clone(&services));
                Ok(Managed::new(services, move |services| {
                    stop_count.fetch_add(1, Ordering::SeqCst);
                    let _stop_report = services.stop();
                    Ok(())
                })
                .with_wait(move |services| {
                    wait_count.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async move {
                        services.await_termination().await;
                        Ok(())
                    })
                }))
            })
            .expect("register managed resource");
        builder
            .register_factory::<u16, _>(&[Dependency::of::<ExecutionServices>()], |context| {
                let _services = context.get::<ExecutionServices>().map_err(FactoryError::new)?;
                Err(FactoryError::new(io::Error::other("expected failure")))
            })
            .expect("register failing factory");
        builder.root::<u16>();

        assert!(matches!(builder.build(), Err(BuildError::FactoryFailed { .. })));
        let services = observed_services.lock().expect("lock observed services").clone().expect("resource was handed off");
        assert_eq!(creates.load(Ordering::SeqCst), 1);
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        assert_eq!(waits.load(Ordering::SeqCst), 0);
        runtime.block_on(async {
            timeout(Duration::from_secs(5), services.await_termination())
                .await
                .expect("synchronous build failure should stop execution services before timeout");
        });
        assert!(services.is_terminated());
    }
}
