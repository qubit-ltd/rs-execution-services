// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Exercises real blocked handlers across cancellable nonblocking shutdown.
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Context;
use std::task::Waker;
use std::time::Duration;

use ioc_application_consumer::managed_event_bus::managed_event_bus;
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_ioc::ContainerBuilder;
use qubit_ioc::ShutdownMode as IocShutdownMode;
use qubit_ioc::WaitPolicy;
use tokio::runtime::Builder;
use tokio::sync::oneshot;

/// Ensures a failed assertion always releases the handler before unwinding.
struct Release {
    gate: Arc<(Mutex<bool>, Condvar)>,
}
impl Release {
    /// Wakes the handler without waiting for it to finish.
    fn release(&self) {
        let (open, ready) = &*self.gate;
        *open.lock().expect("gate lock") = true;
        ready.notify_all();
    }
}
impl Drop for Release {
    /// Releases a blocked handler even if a test assertion panics.
    fn drop(&mut self) {
        self.release();
    }
}

/// The gate proves requests return before handlers finish without elapsed
/// thresholds. Polling and cancelling the IoC observer must retain its ticket.
fn exercise_blocked_handler_shutdown(upgrade_to_abort: bool) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let bus = Arc::new(EventBus::local(LocalEventBusConfig::new()).expect("local bus"));
        let topic = Topic::<String>::new("blocked.final").expect("topic");
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = Release {
            gate: Arc::clone(&gate),
        };
        let (started_tx, started_rx) = oneshot::channel();
        let started = Mutex::new(Some(started_tx));
        let (ended_tx, mut ended_rx) = tokio::sync::mpsc::unbounded_channel();
        let _subscription = bus
            .subscribe(
                SubscribeRequest::new("blocked-handler", topic.clone()).expect("subscribe request"),
                move |delivery| {
                    started
                        .lock()
                        .expect("started lock")
                        .take()
                        .expect("one delivery")
                        .send(())
                        .expect("startup receiver");
                    let (open, ready) = &*gate;
                    let mut opened = open.lock().expect("handler gate");
                    while !*opened {
                        opened = ready.wait(opened).expect("handler gate wait");
                    }
                    ended_tx.send(delivery.payload().clone()).expect("message receiver");
                },
            )
            .expect("subscription");
        let _ = bus.publish(PublishRequest::new(topic, "real payload".to_owned()).expect("publish request"))
            .expect("publish");
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .expect("startup guard")
            .expect("handler started");
        let mut builder = ContainerBuilder::new().wait_policy(WaitPolicy::unbounded());
        builder
            .register_managed_factory::<EventBus, _>(&[], move |_| Ok(managed_event_bus(bus)))
            .expect("bus adapter");
        let application = builder.build_all().expect("application");
        let request_task = runtime.spawn(async move {
            let mut shutdown = application.begin_shutdown(IocShutdownMode::Graceful);
            assert!(
                pin!(shutdown.wait())
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            assert!(
                pin!(shutdown.wait())
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            if upgrade_to_abort {
                shutdown.abort();
                shutdown.abort();
            }
            assert!(
                pin!(shutdown.wait())
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
            );
            shutdown
        });
        let mut shutdown = tokio::time::timeout(Duration::from_secs(5), request_task)
            .await
            .expect("nonblocking request guard")
            .expect("request task");
        assert!(ended_rx.try_recv().is_err(), "handler remains gated after requests");
        release.release();
        let report = tokio::time::timeout(Duration::from_secs(5), shutdown.wait())
            .await
            .expect("shutdown guard")
            .expect("same generation completes");
        assert!(report.is_success());
        assert_eq!(report.mode(), IocShutdownMode::Graceful);
        assert!(report.failures().is_empty());
        assert_eq!(ended_rx.recv().await.as_deref(), Some("real payload"));
    });
}

/// A graceful request keeps observing its generation after caller cancellation.
#[test]
fn test_blocked_handler_graceful_request_cancel_resume() {
    exercise_blocked_handler_shutdown(false);
}

/// An Immediate upgrade preserves the already active graceful observation.
#[test]
fn test_blocked_handler_request_cancel_resume_and_abort_upgrade() {
    exercise_blocked_handler_shutdown(true);
}
