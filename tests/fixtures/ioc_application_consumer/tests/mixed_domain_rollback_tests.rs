// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::error::Error as _;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Poll;
use std::time::Duration;

use qubit_execution_services::ExecutionDomain;
use qubit_execution_services::ExecutionServices;
use qubit_execution_services::ExecutionServicesBuilder;
use qubit_execution_services::ExecutionServicesSubmissionError;
use qubit_ioc::BuildError;
use qubit_ioc::BuildSessionError;
use qubit_ioc::ContainerBuilder;
use qubit_ioc::FactoryError;
use qubit_ioc::Managed;
use qubit_ioc::WaitPolicy;
use tokio::runtime::Builder;
use tokio::sync::oneshot;
use tokio::time::timeout;

const WATCHDOG: Duration = Duration::from_secs(15);
const EXPECTED_VALUE: usize = 73;

#[derive(Clone, Copy, Debug)]
enum SyncDomain {
    Blocking,
    Cpu,
    TokioBlocking,
}

impl SyncDomain {
    fn execution_domain(self) -> ExecutionDomain {
        match self {
            Self::Blocking => ExecutionDomain::Blocking,
            Self::Cpu => ExecutionDomain::Cpu,
            Self::TokioBlocking => ExecutionDomain::TokioBlocking,
        }
    }

    fn configure(self, services: ExecutionServicesBuilder) -> ExecutionServicesBuilder {
        match self {
            Self::Blocking => services
                .enable_blocking()
                .blocking_pool_size(1)
                .blocking_queue_capacity(1),
            Self::Cpu => services.enable_cpu().cpu_threads(1),
            Self::TokioBlocking => services
                .enable_tokio_blocking()
                .tokio_blocking_task_capacity(NonZeroUsize::new(1).expect("one is nonzero")),
        }
    }

    fn submit(
        self,
        services: &ExecutionServices,
        task: impl FnMut() -> Result<(), io::Error> + Send + 'static,
    ) -> Result<(), ExecutionServicesSubmissionError> {
        match self {
            Self::Blocking => services.submit_blocking(task),
            Self::Cpu => services.submit_cpu(task),
            Self::TokioBlocking => services.submit_tokio_blocking(task),
        }
    }

    fn submit_probe(self, services: &ExecutionServices) -> Result<(), ExecutionServicesSubmissionError> {
        self.submit(services, || Ok(()))
    }
}

struct TestResource {
    value: usize,
    drops: Arc<AtomicUsize>,
}

impl Drop for TestResource {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// The sender releases the worker explicitly and while unwinding.
struct GateRelease(Option<mpsc::Sender<()>>);

impl GateRelease {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for GateRelease {
    fn drop(&mut self) {
        self.release();
    }
}

struct IoDropNotice(Option<oneshot::Sender<()>>);

impl Drop for IoDropNotice {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn test_runtime() -> tokio::runtime::Runtime {
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create test runtime")
}

async fn await_signal<T>(receiver: oneshot::Receiver<T>, description: &str) -> T {
    timeout(WATCHDOG, receiver)
        .await
        .unwrap_or_else(|_| panic!("{description} before watchdog"))
        .unwrap_or_else(|_| panic!("{description} sender dropped"))
}

async fn observe_gated_state(
    domain: SyncDomain,
    services: &ExecutionServices,
    drops: &AtomicUsize,
    resource: &Weak<TestResource>,
    gate: &mut GateRelease,
    worker_result: oneshot::Receiver<usize>,
    io_dropped: oneshot::Receiver<()>,
) -> usize {
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "resource remains alive while worker owns it"
    );
    assert!(resource.upgrade().is_some(), "task-held resource remains upgradeable");
    assert!(!services.is_terminated(), "gated synchronous task prevents termination");
    assert!(
        matches!(
            domain.submit_probe(services),
            Err(ExecutionServicesSubmissionError::Rejected { domain: actual_domain, source })
                if actual_domain == domain.execution_domain() && format!("{source:?}") == "Shutdown"
        ),
        "new submissions must be rejected specifically as shutdown"
    );
    assert!(
        matches!(
            services.spawn_io(async { Ok::<(), io::Error>(()) }),
            Err(ExecutionServicesSubmissionError::Rejected { domain, source })
                if domain == ExecutionDomain::Io && format!("{source:?}") == "Shutdown"
        ),
        "new IO submissions must also be rejected specifically as shutdown"
    );
    await_signal(io_dropped, "pending IO future drop").await;
    assert!(
        !services.is_terminated(),
        "IO cancellation does not release the gated worker"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    gate.release();
    await_signal(worker_result, "worker result").await
}

fn run_scenario(domain: SyncDomain, cancel: bool) {
    let runtime = test_runtime();
    runtime.block_on(async {
        let drops = Arc::new(AtomicUsize::new(0));
        let observed_resource = Arc::new(Mutex::new(None::<Weak<TestResource>>));
        let observed_services = Arc::new(Mutex::new(None::<Arc<ExecutionServices>>));
        let gate_sender = Arc::new(Mutex::new(None::<mpsc::Sender<()>>));
        let worker_result = Arc::new(Mutex::new(None::<oneshot::Receiver<usize>>));
        let io_dropped = Arc::new(Mutex::new(None::<oneshot::Receiver<()>>));
        let stops = Arc::new(AtomicUsize::new(0));
        let waits = Arc::new(AtomicUsize::new(0));
        let (factory_started_tx, factory_started_rx) = oneshot::channel();
        let resource_drops = Arc::clone(&drops);
        let resource_slot = Arc::clone(&observed_resource);
        let services_slot = Arc::clone(&observed_services);
        let gate_slot = Arc::clone(&gate_sender);
        let result_slot = Arc::clone(&worker_result);
        let io_drop_slot = Arc::clone(&io_dropped);
        let stop_count = Arc::clone(&stops);
        let wait_count = Arc::clone(&waits);
        let handle = runtime.handle().clone();
        let mut builder = ContainerBuilder::new().wait_policy(WaitPolicy::unbounded());

        builder
            .register_factory::<TestResource, _>(&[], move |_| {
                let resource = Arc::new(TestResource {
                    value: EXPECTED_VALUE,
                    drops: resource_drops,
                });
                *resource_slot.lock().expect("lock resource slot") = Some(Arc::downgrade(&resource));
                Ok(resource)
            })
            .expect("register test resource");

        let service_domain = domain;
        builder
            .register_injected_managed_factory::<ExecutionServices, (Arc<TestResource>,), _>(move |(_resource,)| {
                let services = Arc::new(
                    service_domain
                        .configure(ExecutionServices::builder().enable_io())
                        .runtime(handle)
                        .io_task_capacity(NonZeroUsize::new(1).expect("one is nonzero"))
                        .build()
                        .map_err(FactoryError::new)?,
                );
                *services_slot.lock().expect("lock services slot") = Some(Arc::clone(&services));
                let stop_count = Arc::clone(&stop_count);
                let wait_count = Arc::clone(&wait_count);
                Ok(Managed::asynchronous(
                    services,
                    move |services| {
                        stop_count.fetch_add(1, Ordering::SeqCst);
                        let _report = services.stop();
                        Ok(())
                    },
                    move |services| {
                        wait_count.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async move {
                            services.await_termination().await;
                            Ok(())
                        })
                    },
                ))
            })
            .expect("register managed execution services");

        let scenario_domain = domain;
        builder
            .register_injected_async_factory::<usize, (Arc<ExecutionServices>, Arc<TestResource>), _>(
                move |(services, resource)| {
                    Box::pin(async move {
                        let (worker_started_tx, worker_started_rx) = oneshot::channel();
                        let (gate_tx, gate_rx) = mpsc::channel();
                        let (result_tx, result_rx) = oneshot::channel();
                        let (io_started_tx, io_started_rx) = oneshot::channel();
                        let (io_dropped_tx, io_dropped_rx) = oneshot::channel();
                        let worker_resource = Arc::clone(&resource);
                        let mut worker_started_tx = Some(worker_started_tx);
                        let mut result_tx = Some(result_tx);
                        scenario_domain
                            .submit(&services, move || {
                                if let Some(sender) = worker_started_tx.take() {
                                    let _ = sender.send(());
                                }
                                gate_rx
                                    .recv_timeout(WATCHDOG)
                                    .expect("worker gate must be released before watchdog");
                                let value = worker_resource.value;
                                if let Some(sender) = result_tx.take() {
                                    let _ = sender.send(value);
                                }
                                Ok(())
                            })
                            .map_err(FactoryError::new)?;
                        *gate_slot.lock().expect("lock gate sender") = Some(gate_tx);
                        let _ = result_slot.lock().expect("lock worker result").replace(result_rx);

                        let io_task = services
                            .spawn_io(async move {
                                let _notice = IoDropNotice(Some(io_dropped_tx));
                                let _ = io_started_tx.send(());
                                std::future::pending::<()>().await;
                                Ok::<(), io::Error>(())
                            })
                            .map_err(FactoryError::new)?;
                        let _ = io_drop_slot
                            .lock()
                            .expect("lock IO drop receiver")
                            .replace(io_dropped_rx);

                        let _io_task = io_task;
                        await_signal(worker_started_rx, "synchronous worker start").await;
                        await_signal(io_started_rx, "IO future start").await;
                        let _ = factory_started_tx.send(());
                        if cancel {
                            std::future::pending::<()>().await;
                            Ok(Arc::new(resource.value))
                        } else {
                            Err(FactoryError::new(io::Error::other("mixed-domain downstream failure")))
                        }
                    })
                },
            )
            .expect("register downstream factory");
        builder.root::<usize>();

        let (services, resource_weak) = if cancel {
            let mut session = builder.build_async_session();
            let mut run = Box::pin(session.run());
            let mut factory_started = Box::pin(factory_started_rx);
            timeout(
                WATCHDOG,
                std::future::poll_fn(|context| {
                    if run.as_mut().poll(context).is_ready() {
                        panic!("pending factory unexpectedly completed");
                    }
                    factory_started
                        .as_mut()
                        .poll(context)
                        .map(|result| result.expect("downstream factory started"))
                }),
            )
            .await
            .expect("downstream factory should start before watchdog");
            drop(run);
            assert!(
                matches!(session.run().await, Err(BuildSessionError::Cancelled)),
                "a cancelled session reports its cancellation state"
            );
            assert_eq!(stops.load(Ordering::SeqCst), 1, "cancellation calls stop once");
            assert_eq!(waits.load(Ordering::SeqCst), 0, "managed wait is not started yet");
            let services = observed_services
                .lock()
                .expect("lock services slot")
                .clone()
                .expect("managed services were created");
            let resource_weak = observed_resource
                .lock()
                .expect("lock resource slot")
                .clone()
                .expect("resource was created");
            let mut gate = GateRelease(gate_sender.lock().expect("lock gate sender").take());
            let worker_result = worker_result
                .lock()
                .expect("lock worker result")
                .take()
                .expect("worker result receiver");
            let io_drop_receiver = io_dropped
                .lock()
                .expect("lock IO drop receiver")
                .take()
                .expect("IO drop receiver");
            let report = {
                let cleanup = session.wait_cancelled_cleanup();
                let mut cleanup = pin!(cleanup);
                let poll = std::future::poll_fn(|context| Poll::Ready(cleanup.as_mut().poll(context))).await;
                assert!(
                    poll.is_pending(),
                    "cancelled cleanup stays pending while worker is gated"
                );
                let value = observe_gated_state(
                    domain,
                    &services,
                    &drops,
                    &resource_weak,
                    &mut gate,
                    worker_result,
                    io_drop_receiver,
                )
                .await;
                assert_eq!(value, EXPECTED_VALUE, "worker reads the retained resource");
                timeout(WATCHDOG, cleanup)
                    .await
                    .expect("cancelled cleanup should finish before watchdog")
                    .expect("managed cleanup report")
            };
            assert!(report.is_success(), "cleanup report: {report:?}");
            drop(session);
            (services, resource_weak)
        } else {
            let result = timeout(WATCHDOG, builder.build_async())
                .await
                .expect("failed build should return before watchdog");
            let mut failure = match result {
                Ok(_application) => panic!("downstream factory should fail"),
                Err(failure) => failure,
            };
            assert!(matches!(failure.cause(), BuildError::FactoryFailed { .. }));
            let BuildError::FactoryFailed { error, .. } = failure.cause() else {
                panic!("factory failure remains primary");
            };
            assert_eq!(
                error.source().expect("original failure source").to_string(),
                "mixed-domain downstream failure"
            );
            assert_eq!(stops.load(Ordering::SeqCst), 1, "failure calls stop once");
            assert_eq!(waits.load(Ordering::SeqCst), 0, "managed wait is not started yet");
            let services = observed_services
                .lock()
                .expect("lock services slot")
                .clone()
                .expect("managed services were created");
            let resource_weak = observed_resource
                .lock()
                .expect("lock resource slot")
                .clone()
                .expect("resource was created");
            let mut gate = GateRelease(gate_sender.lock().expect("lock gate sender").take());
            let worker_result = worker_result
                .lock()
                .expect("lock worker result")
                .take()
                .expect("worker result receiver");
            let io_drop_receiver = io_dropped
                .lock()
                .expect("lock IO drop receiver")
                .take()
                .expect("IO drop receiver");
            let report = {
                let cleanup = failure.wait_cleanup();
                let mut cleanup = pin!(cleanup);
                let poll = std::future::poll_fn(|context| Poll::Ready(cleanup.as_mut().poll(context))).await;
                assert!(poll.is_pending(), "failure cleanup stays pending while worker is gated");
                let value = observe_gated_state(
                    domain,
                    &services,
                    &drops,
                    &resource_weak,
                    &mut gate,
                    worker_result,
                    io_drop_receiver,
                )
                .await;
                assert_eq!(value, EXPECTED_VALUE, "worker reads the retained resource");
                timeout(WATCHDOG, cleanup)
                    .await
                    .expect("failure cleanup should finish before watchdog")
                    .expect("managed cleanup report")
            };
            assert!(report.is_success(), "cleanup report: {report:?}");
            drop(failure);
            (services, resource_weak)
        };

        timeout(WATCHDOG, services.await_termination())
            .await
            .expect("services should terminate before watchdog");
        assert!(services.is_terminated());
        assert_eq!(stops.load(Ordering::SeqCst), 1, "stop callback runs exactly once");
        assert_eq!(
            waits.load(Ordering::SeqCst),
            1,
            "termination callback runs exactly once"
        );
        drop(observed_services.lock().expect("lock services slot").take());
        drop(services);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "resource drops exactly once after owners leave"
        );
        assert!(
            resource_weak.upgrade().is_none(),
            "resource has no remaining strong owner"
        );
    });
}

#[test]
fn test_mixed_domain_blocking_failure_rolls_back_gated_task() {
    run_scenario(SyncDomain::Blocking, false);
}

#[test]
fn test_mixed_domain_blocking_cancellation_rolls_back_gated_task() {
    run_scenario(SyncDomain::Blocking, true);
}

#[test]
fn test_mixed_domain_cpu_failure_rolls_back_gated_task() {
    run_scenario(SyncDomain::Cpu, false);
}

#[test]
fn test_mixed_domain_cpu_cancellation_rolls_back_gated_task() {
    run_scenario(SyncDomain::Cpu, true);
}

#[test]
fn test_mixed_domain_tokio_blocking_failure_rolls_back_gated_task() {
    run_scenario(SyncDomain::TokioBlocking, false);
}

#[test]
fn test_mixed_domain_tokio_blocking_cancellation_rolls_back_gated_task() {
    run_scenario(SyncDomain::TokioBlocking, true);
}
