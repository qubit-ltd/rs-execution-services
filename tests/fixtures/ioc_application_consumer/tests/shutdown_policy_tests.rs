// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Verifies shutdown deadlines through public IoC and real execution services.

#[path = "internal/shutdown_timer.rs"]
mod shutdown_timer;

use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ioc_application_consumer::managed_execution_services::managed_execution_services;
use ioc_application_consumer::managed_event_bus::managed_event_bus;
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_execution_services::ExecutionServices;
use qubit_execution_services::ExecutionServicesSubmissionError;
use qubit_ioc::BindingKey;
use qubit_ioc::ContainerBuilder;
use qubit_ioc::Dependency;
use qubit_ioc::Managed;
use qubit_ioc::ShutdownMode;
use qubit_ioc::ShutdownPhase;
use shutdown_timer::Gate;
use shutdown_timer::Timers;
use shutdown_timer::poll_once;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::time::timeout;

/// Reports destruction of a running task after execution services abort it.
struct TaskGuard {
    ended: Option<oneshot::Sender<()>>,
}

impl Drop for TaskGuard {
    /// Notifies the test without blocking, including during task cancellation.
    fn drop(&mut self) {
        if let Some(sender) = self.ended.take() {
            let _ = sender.send(());
        }
    }
}

/// Represents a consumer whose completion is controlled independently of time.
struct WaitingConsumer;

/// Creates the runtime needed by real services; panics on runtime setup
/// failure.
fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create shutdown policy runtime")
}

/// Registers the production fixture adapter with manually controlled deadlines.
fn builder(runtime: Handle, timers: &Timers) -> ContainerBuilder {
    let mut builder = ContainerBuilder::new().wait_policy(timers.policy());
    builder
        .register_managed_factory::<ExecutionServices, _>(&[], move |_| managed_execution_services(runtime))
        .expect("register real execution services adapter");
    builder
}

/// Registers the production adapter with an application-wide shutdown budget.
fn builder_with_total(runtime: Handle, timers: &Timers, total: Duration) -> ContainerBuilder {
    let mut builder = ContainerBuilder::new().wait_policy(timers.policy_with_total(total));
    builder
        .register_managed_factory::<ExecutionServices, _>(&[], move |_| managed_execution_services(runtime))
        .expect("register real execution services adapter");
    builder
}

/// Adds a hang guard; successful assertions depend on signals, never elapsed
/// time.
async fn guarded<F: Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(5), future)
        .await
        .expect("shutdown operation exceeded the test hang guard")
}

/// A graceful deadline must abort the actual IO task and retain its failure
/// report.
#[test]
fn test_grace_timeout_aborts_running_execution_service_task() {
    let runtime = runtime();
    runtime.block_on(async {
        let timers = Timers::default();
        let application = builder(runtime.handle().clone(), &timers)
            .build_all()
            .expect("build execution services application");
        let services = application.context().get::<ExecutionServices>().expect("services");
        let (started_tx, started_rx) = oneshot::channel();
        let (ended_tx, ended_rx) = oneshot::channel();
        let task = services
            .spawn_io(async move {
                let _guard = TaskGuard { ended: Some(ended_tx) };
                started_tx.send(()).expect("startup receiver alive");
                std::future::pending::<()>().await;
                Ok::<(), io::Error>(())
            })
            .expect("submit real pending IO task");
        guarded(started_rx).await.expect("task has started");

        let mut shutdown = application.begin_shutdown(ShutdownMode::Graceful);
        assert!(poll_once(shutdown.wait()).is_pending());
        assert!(!services.is_terminated());
        assert_eq!(timers.durations(), [Duration::from_secs(13)]);
        timers.trigger(0);

        let error = guarded(shutdown.wait()).await.expect_err("grace deadline recorded");
        guarded(ended_rx).await.expect("aborted task released its guard");
        assert!(guarded(task).await.is_err(), "aborted IO task cannot succeed");
        assert!(services.is_terminated());
        assert_eq!(error.report().failures().len(), 1);
        assert_eq!(error.report().failures()[0].phase, ShutdownPhase::GracefulWait);
        assert_eq!(
            error.report().failures()[0].key,
            BindingKey::of::<ExecutionServices>(None)
        );
        assert!(error.report().incomplete().is_empty());
        assert!(error.report().is_complete());
        assert!(!error.report().is_success());
        assert_eq!(timers.durations(), [Duration::from_secs(13), Duration::from_secs(7)]);
        assert!(matches!(
            services.spawn_io(async { Ok::<(), io::Error>(()) }),
            Err(ExecutionServicesSubmissionError::Rejected { .. })
        ));
    });
}

/// An incomplete consumer must not prevent its real services dependency
/// closing.
#[test]
fn test_termination_timeout_continues_to_execution_services_dependency() {
    let runtime = runtime();
    runtime.block_on(async {
        let timers = Timers::default();
        let aborts = Arc::new(AtomicUsize::new(0));
        let observed_aborts = Arc::clone(&aborts);
        let mut builder = builder(runtime.handle().clone(), &timers);
        builder
            .register_managed_factory::<WaitingConsumer, _>(&[Dependency::of::<ExecutionServices>()], move |_| {
                Ok(Managed::asynchronous_with_graceful(Arc::new(WaitingConsumer), move |_| {
                    observed_aborts.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }, |_| Ok(()), |_| Box::pin(std::future::pending())))
            })
            .expect("register pending dependent consumer");
        let application = builder.build_all().expect("build dependency graph");
        let services = application.context().get::<ExecutionServices>().expect("services");
        let (started_tx, started_rx) = oneshot::channel();
        let release_task = Gate::default();
        let task_gate = release_task.clone();
        let task = services
            .spawn_io(async move {
                started_tx.send(()).expect("startup receiver alive");
                task_gate.await;
                Ok::<u32, io::Error>(43)
            })
            .expect("submit task to dependency");
        guarded(started_rx).await.expect("dependency task has started");

        let mut shutdown = application.begin_shutdown(ShutdownMode::Graceful);
        assert!(poll_once(shutdown.wait()).is_pending());
        assert!(
            services.is_running(),
            "dependency remains available while consumer waits"
        );
        assert_eq!(timers.durations(), [Duration::from_secs(13)]);
        timers.trigger(0);
        assert!(poll_once(shutdown.wait()).is_pending());
        assert!(
            services.is_running(),
            "consumer abort does not close dependencies early"
        );
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        assert_eq!(timers.durations(), [Duration::from_secs(13), Duration::from_secs(7)]);
        release_task.trigger();
        assert_eq!(guarded(task).await.expect("dependency task completes"), 43);
        timers.trigger(1);

        let error = guarded(shutdown.wait()).await.expect_err("consumer remains incomplete");
        assert!(services.is_terminated());
        assert_eq!(aborts.load(Ordering::SeqCst), 1);
        assert_eq!(error.report().failures().len(), 2);
        assert_eq!(error.report().failures()[0].phase, ShutdownPhase::GracefulWait);
        assert_eq!(error.report().failures()[1].phase, ShutdownPhase::TerminationWait);
        assert_eq!(
            error.report().failures()[0].key,
            BindingKey::of::<WaitingConsumer>(None)
        );
        assert_eq!(
            error.report().failures()[1].key,
            BindingKey::of::<WaitingConsumer>(None)
        );
        assert_eq!(error.report().incomplete(), [BindingKey::of::<WaitingConsumer>(None)]);
        assert!(error.report().fallbacks().is_empty());
        assert!(!error.report().is_complete());
        assert_eq!(
            timers.durations(),
            [Duration::from_secs(13), Duration::from_secs(7), Duration::from_secs(13)]
        );
    });
}

/// Cancelling caller waits preserves the active consumer future and its
/// deadline.
#[test]
fn test_cancelled_wait_reuses_pending_consumer_and_grace_deadline() {
    let runtime = runtime();
    runtime.block_on(async {
        let timers = Timers::default();
        let gate = Gate::default();
        let waiting = gate.clone();
        let starts = Arc::new(AtomicUsize::new(0));
        let observed_starts = Arc::clone(&starts);
        let graceful_requests = Arc::new(AtomicUsize::new(0));
        let observed_graceful = Arc::clone(&graceful_requests);
        let aborts = Arc::new(AtomicUsize::new(0));
        let observed_aborts = Arc::clone(&aborts);
        let mut builder = builder(runtime.handle().clone(), &timers);
        builder
            .register_managed_factory::<WaitingConsumer, _>(&[Dependency::of::<ExecutionServices>()], move |_| {
                Ok(Managed::asynchronous_with_graceful(Arc::new(WaitingConsumer), move |_| {
                    observed_aborts.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }, move |_| {
                    observed_graceful.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }, move |_| {
                    observed_starts.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async move {
                        waiting.await;
                        Ok(())
                    })
                }))
            })
            .expect("register resumable consumer");
        let application = builder.build_all().expect("build resumable application");
        let services = application.context().get::<ExecutionServices>().expect("services");
        let mut shutdown = application.begin_shutdown(ShutdownMode::Graceful);
        assert!(poll_once(shutdown.wait()).is_pending());
        assert!(poll_once(shutdown.wait()).is_pending());
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(graceful_requests.load(Ordering::SeqCst), 1);
        assert_eq!(timers.durations(), [Duration::from_secs(13)]);
        assert!(services.is_running());

        let task = services
            .spawn_io(async { Ok::<u32, io::Error>(42) })
            .expect("dependency accepts work while consumer waits");
        assert_eq!(guarded(task).await.expect("real task result"), 42);
        gate.trigger();
        let report = guarded(shutdown.wait()).await.expect("resume graceful cleanup");
        assert!(report.is_success());
        assert!(services.is_terminated());
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(graceful_requests.load(Ordering::SeqCst), 1);
        assert_eq!(aborts.load(Ordering::SeqCst), 0);
    });
}

/// The application-wide deadline aborts real services and is retained in the
/// public shutdown report.
#[test]
fn test_total_timeout_aborts_execution_services_and_is_reported() {
    let runtime = runtime();
    runtime.block_on(async {
        let timers = Timers::default();
        let bus = Arc::new(EventBus::local(LocalEventBusConfig::new()).expect("local EventBus"));
        let observed_bus = Arc::clone(&bus);
        let mut builder = builder_with_total(
            runtime.handle().clone(),
            &timers,
            Duration::from_secs(19),
        );
        builder
            .register_managed_factory::<EventBus, _>(&[], move |_| {
                Ok(managed_event_bus(bus))
            })
            .expect("register EventBus lifecycle adapter");
        let application = builder
            .build_all()
            .expect("build execution services application");
        let services = application.context().get::<ExecutionServices>().expect("services");
        let (started_tx, started_rx) = oneshot::channel();
        let (ended_tx, ended_rx) = oneshot::channel();
        let task = services
            .spawn_io(async move {
                let _guard = TaskGuard { ended: Some(ended_tx) };
                started_tx.send(()).expect("startup receiver alive");
                std::future::pending::<()>().await;
                Ok::<(), io::Error>(())
            })
            .expect("submit real pending IO task");
        guarded(started_rx).await.expect("task has started");
        let mut shutdown = application.begin_shutdown(ShutdownMode::Graceful);
        assert!(poll_once(shutdown.wait()).is_pending());
        assert_eq!(timers.durations(), [Duration::from_secs(19), Duration::from_secs(13)]);
        assert!(poll_once(shutdown.wait()).is_pending());
        assert_eq!(timers.durations(), [Duration::from_secs(19), Duration::from_secs(13)]);

        timers.trigger(0);
        let error = guarded(shutdown.wait()).await.expect_err("total deadline recorded");
        guarded(ended_rx).await.expect("aborted task released its guard");
        assert!(guarded(task).await.is_err(), "aborted IO task cannot succeed");
        assert!(services.is_terminated());
        assert!(error.report().overall_failure().is_some());
        assert!(!error.report().is_success());
        assert_eq!(error.report().mode(), ShutdownMode::Graceful);
        assert!(error
            .report()
            .incomplete()
            .contains(&BindingKey::of::<EventBus>(None)));
        let ticket = observed_bus
            .request_shutdown(qubit_event_bus::spi::ShutdownMode::Immediate)
            .expect("retrieve the same EventBus shutdown generation");
        let report = guarded(ticket.wait_async())
            .await
            .expect("EventBus completes after the Immediate upgrade");
        assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
    });
}
