// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Business consumer whose final publication must precede dependency shutdown.
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_event_bus::EventBus;
use qubit_event_bus::Subscription;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_execution_services::ExecutionServices;
use qubit_ioc::CleanupError;
use qubit_ioc::FactoryError;
use qubit_ioc::Managed;
use tokio::sync::mpsc;

/// Owns the final-report subscription and observable business results.
pub struct FlushWorker {
    services: Arc<ExecutionServices>,
    bus: Arc<EventBus>,
    topic: Topic<String>,
    _subscription: Subscription,
    received: Mutex<Vec<String>>,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<String>>>,
    result: Mutex<Option<u64>>,
    graceful: AtomicUsize,
}
impl FlushWorker {
    /// Returns handler acknowledgements retained after the final flush.
    pub fn messages(&self) -> Vec<String> {
        self.received.lock().expect("messages lock").clone()
    }
    /// Returns the completed final task result, or `None` before graceful
    /// flush.
    pub fn final_task_result(&self) -> Option<u64> {
        *self.result.lock().expect("result lock")
    }
    /// Counts graceful requests; Immediate must leave this at zero.
    pub fn graceful_requests(&self) -> usize {
        self.graceful.load(Ordering::SeqCst)
    }

    /// Subscribes to real final-report messages; invalid bus state is a factory
    /// error.
    pub(crate) fn managed(services: Arc<ExecutionServices>, bus: Arc<EventBus>) -> Result<Managed<Self>, FactoryError> {
        let topic = Topic::<String>::new("reports.final").map_err(FactoryError::new)?;
        let (sender, receiver) = mpsc::unbounded_channel();
        let subscription = bus
            .subscribe(
                SubscribeRequest::new("report-recorder", topic.clone()).map_err(FactoryError::new)?,
                move |delivery| {
                    let _ = sender.send(delivery.payload().clone());
                },
            )
            .map_err(FactoryError::new)?;
        let worker = Arc::new(Self {
            services,
            bus,
            topic,
            _subscription: subscription,
            received: Mutex::new(Vec::new()),
            receiver: Mutex::new(Some(receiver)),
            result: Mutex::new(None),
            graceful: AtomicUsize::new(0),
        });
        Ok(Managed::new(worker, |_| Ok(()))
            .with_graceful_stop(|worker| {
                worker.graceful.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .with_wait(|worker| {
                Box::pin(async move {
                    if worker.graceful_requests() == 0 {
                        return Ok(());
                    }
                    assert!(
                        worker.services.is_running(),
                        "final flush requires open service admission"
                    );
                    let bus = Arc::clone(&worker.bus);
                    let topic = worker.topic.clone();
                    let task = worker
                        .services
                        .spawn_io(async move {
                            let request = PublishRequest::new(topic, "final-report:22".to_owned())
                                .map_err(std::io::Error::other)?;
                            bus.publish(request).map_err(std::io::Error::other)?;
                            Ok::<u64, std::io::Error>(22)
                        })
                        .map_err(CleanupError::new)?;
                    let result = task.await.map_err(CleanupError::new)?;
                    *worker.result.lock().expect("result lock") = Some(result);
                    let mut receiver =
                        worker.receiver.lock().expect("receiver lock").take().ok_or_else(|| {
                            CleanupError::new(std::io::Error::other("final receiver already consumed"))
                        })?;
                    let message = receiver
                        .recv()
                        .await
                        .ok_or_else(|| CleanupError::new(std::io::Error::other("final handler did not acknowledge")))?;
                    worker.received.lock().expect("messages lock").push(message);
                    Ok(())
                })
            }))
    }
}
