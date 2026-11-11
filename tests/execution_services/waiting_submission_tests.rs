// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests waiting submission and cancellation across execution domains.

use std::future::Future;
use std::future::IntoFuture;
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Poll;
use std::time::Duration;

use qubit_execution_services::ExecutionDomain;
use qubit_execution_services::ExecutionServices;
use qubit_execution_services::ExecutionServicesSubmissionError;
use qubit_executor::TaskExecutionError;
use qubit_executor::service::SubmissionError;
use tokio::join;
use tokio::pin;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::task::yield_now;
use tokio::test as tokio_test;
use tokio::time::timeout;

use super::GateRelease;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

async fn poll_once<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    std::future::poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PollCounterFuture {
    polls: Arc<AtomicUsize>,
    _drop_counter: DropCounter,
}

type ErasedTask = Pin<Box<dyn Future<Output = Result<(), TaskExecutionError<io::Error>>> + Send>>;
type ErasedWait<'a> = Pin<Box<dyn Future<Output = Result<ErasedTask, ExecutionServicesSubmissionError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug)]
enum WaitCompetitionDomain {
    Blocking,
    Cpu,
    TokioBlocking,
    Io,
}

impl WaitCompetitionDomain {
    fn all() -> [Self; 4] {
        [Self::Blocking, Self::Cpu, Self::TokioBlocking, Self::Io]
    }
}

#[derive(Clone, Copy)]
enum WaitCompetitionScenario {
    Progress,
    Shutdown,
    Stop,
}

impl WaitCompetitionScenario {
    fn name(self) -> &'static str {
        match self {
            Self::Progress => "progress",
            Self::Shutdown => "shutdown",
            Self::Stop => "stop",
        }
    }
}

struct ScenarioRelease(Option<Box<dyn FnOnce() + Send>>);

impl ScenarioRelease {
    fn release(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}

impl Drop for ScenarioRelease {
    fn drop(&mut self) {
        self.release();
    }
}

enum WaiterGate {
    Blocking(mpsc::Receiver<()>),
    Io(oneshot::Receiver<()>),
}

fn mpsc_gate() -> (ScenarioRelease, mpsc::Receiver<()>) {
    let (sender, receiver) = mpsc::channel();
    let release = ScenarioRelease(Some(Box::new(move || {
        let _ = sender.send(());
    })));
    (release, receiver)
}

fn oneshot_gate() -> (ScenarioRelease, oneshot::Receiver<()>) {
    let (sender, receiver) = oneshot::channel();
    let release = ScenarioRelease(Some(Box::new(move || {
        let _ = sender.send(());
    })));
    (release, receiver)
}

fn waiter_gate(domain: WaitCompetitionDomain) -> (ScenarioRelease, WaiterGate) {
    match domain {
        WaitCompetitionDomain::Io => {
            let (release, gate) = oneshot_gate();
            (release, WaiterGate::Io(gate))
        }
        _ => {
            let (release, gate) = mpsc_gate();
            (release, WaiterGate::Blocking(gate))
        }
    }
}

fn erase_task_handle<H>(handle: H) -> ErasedTask
where
    H: IntoFuture<Output = Result<(), TaskExecutionError<io::Error>>> + Send + 'static,
    H::IntoFuture: Send + 'static,
{
    Box::pin(async move { handle.await })
}

fn build_competition_services(domain: WaitCompetitionDomain) -> ExecutionServices {
    let builder = ExecutionServices::builder();
    match domain {
        WaitCompetitionDomain::Blocking => builder
            .enable_blocking()
            .blocking_pool_size(1)
            .blocking_queue_capacity(1)
            .build()
            .expect("blocking competition services should build"),
        WaitCompetitionDomain::Cpu => builder
            .enable_cpu()
            .cpu_threads(1)
            .cpu_task_capacity(1)
            .build()
            .expect("CPU competition services should build"),
        WaitCompetitionDomain::TokioBlocking => builder
            .enable_tokio_blocking()
            .runtime(Handle::current())
            .tokio_blocking_task_capacity(NonZeroUsize::new(1).expect("capacity is nonzero"))
            .build()
            .expect("Tokio blocking competition services should build"),
        WaitCompetitionDomain::Io => builder
            .enable_io()
            .runtime(Handle::current())
            .io_task_capacity(NonZeroUsize::new(1).expect("capacity is nonzero"))
            .build()
            .expect("IO competition services should build"),
    }
}

struct OccupiedCapacity {
    releases: Vec<ScenarioRelease>,
    queued_started: Option<mpsc::Receiver<()>>,
    tasks: Vec<ErasedTask>,
}

fn occupy_competition_capacity(services: &ExecutionServices, domain: WaitCompetitionDomain) -> OccupiedCapacity {
    let (started_tx, started_rx) = mpsc::channel();
    match domain {
        WaitCompetitionDomain::Blocking => {
            let (running_release, running_gate) = mpsc_gate();
            let running_started = started_tx.clone();
            let running = services
                .submit_blocking_callable(move || {
                    running_started
                        .send(())
                        .expect("running blocker observer should remain available");
                    running_gate
                        .recv_timeout(TEST_TIMEOUT)
                        .expect("blocking running gate should open");
                    Ok::<(), io::Error>(())
                })
                .expect("blocking running placeholder should be accepted");
            started_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("blocking running placeholder should start");

            let (queued_started_tx, queued_started_rx) = mpsc::channel();
            let (queued_release, queued_gate) = mpsc_gate();
            let queued = services
                .submit_blocking_callable(move || {
                    queued_started_tx
                        .send(())
                        .expect("queued blocker observer should remain available");
                    queued_gate
                        .recv_timeout(TEST_TIMEOUT)
                        .expect("blocking queued gate should open");
                    Ok::<(), io::Error>(())
                })
                .expect("blocking queue placeholder should be accepted");
            OccupiedCapacity {
                releases: vec![running_release, queued_release],
                queued_started: Some(queued_started_rx),
                tasks: vec![erase_task_handle(running), erase_task_handle(queued)],
            }
        }
        WaitCompetitionDomain::Cpu => {
            let (release, gate) = mpsc_gate();
            let started = started_tx;
            let task = services
                .submit_cpu_callable(move || {
                    started.send(()).expect("CPU blocker observer should remain available");
                    gate.recv_timeout(TEST_TIMEOUT).expect("CPU gate should open");
                    Ok::<(), io::Error>(())
                })
                .expect("CPU capacity placeholder should be accepted");
            started_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("CPU placeholder should start");
            OccupiedCapacity {
                releases: vec![release],
                queued_started: None,
                tasks: vec![erase_task_handle(task)],
            }
        }
        WaitCompetitionDomain::TokioBlocking => {
            let (release, gate) = mpsc_gate();
            let started = started_tx;
            let task = services
                .submit_tokio_blocking_callable(move || {
                    started
                        .send(())
                        .expect("Tokio blocking observer should remain available");
                    gate.recv_timeout(TEST_TIMEOUT)
                        .expect("Tokio blocking gate should open");
                    Ok::<(), io::Error>(())
                })
                .expect("Tokio blocking capacity placeholder should be accepted");
            started_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("Tokio blocking placeholder should start");
            OccupiedCapacity {
                releases: vec![release],
                queued_started: None,
                tasks: vec![erase_task_handle(task)],
            }
        }
        WaitCompetitionDomain::Io => {
            let (release, gate) = oneshot_gate();
            let started = started_tx;
            let task = services
                .spawn_io(async move {
                    started.send(()).expect("IO blocker observer should remain available");
                    gate.await.expect("IO gate should open");
                    Ok::<(), io::Error>(())
                })
                .expect("IO capacity placeholder should be accepted");
            started_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("IO placeholder should start");
            OccupiedCapacity {
                releases: vec![release],
                queued_started: None,
                tasks: vec![erase_task_handle(task)],
            }
        }
    }
}

fn make_competition_waiter<'a>(
    services: &'a ExecutionServices,
    domain: WaitCompetitionDomain,
    task_id: usize,
    started: mpsc::Sender<usize>,
    gate: WaiterGate,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
) -> ErasedWait<'a> {
    let drop_counter = DropCounter(drops);
    match domain {
        WaitCompetitionDomain::Blocking => {
            let WaiterGate::Blocking(gate) = gate else {
                unreachable!("blocking waiter needs an mpsc gate")
            };
            Box::pin(async move {
                services
                    .submit_blocking_callable_wait(move || {
                        let _drop_counter = drop_counter;
                        calls.fetch_add(1, Ordering::SeqCst);
                        started
                            .send(task_id)
                            .expect("blocking waiter observer should remain available");
                        gate.recv_timeout(TEST_TIMEOUT)
                            .expect("blocking waiter gate should open");
                        Ok::<(), io::Error>(())
                    })
                    .await
                    .map(erase_task_handle)
            })
        }
        WaitCompetitionDomain::Cpu => {
            let WaiterGate::Blocking(gate) = gate else {
                unreachable!("CPU waiter needs an mpsc gate")
            };
            Box::pin(async move {
                services
                    .submit_cpu_callable_wait(move || {
                        let _drop_counter = drop_counter;
                        calls.fetch_add(1, Ordering::SeqCst);
                        started
                            .send(task_id)
                            .expect("CPU waiter observer should remain available");
                        gate.recv_timeout(TEST_TIMEOUT).expect("CPU waiter gate should open");
                        Ok::<(), io::Error>(())
                    })
                    .await
                    .map(erase_task_handle)
            })
        }
        WaitCompetitionDomain::TokioBlocking => {
            let WaiterGate::Blocking(gate) = gate else {
                unreachable!("Tokio blocking waiter needs an mpsc gate")
            };
            Box::pin(async move {
                services
                    .submit_tokio_blocking_callable_wait(move || {
                        let _drop_counter = drop_counter;
                        calls.fetch_add(1, Ordering::SeqCst);
                        started
                            .send(task_id)
                            .expect("Tokio blocking waiter observer should remain available");
                        gate.recv_timeout(TEST_TIMEOUT)
                            .expect("Tokio blocking waiter gate should open");
                        Ok::<(), io::Error>(())
                    })
                    .await
                    .map(erase_task_handle)
            })
        }
        WaitCompetitionDomain::Io => {
            let WaiterGate::Io(gate) = gate else {
                unreachable!("IO waiter needs a oneshot gate")
            };
            Box::pin(async move {
                services
                    .spawn_io_wait(async move {
                        let _drop_counter = drop_counter;
                        calls.fetch_add(1, Ordering::SeqCst);
                        started
                            .send(task_id)
                            .expect("IO waiter observer should remain available");
                        gate.await.expect("IO waiter gate should open");
                        Ok::<(), io::Error>(())
                    })
                    .await
                    .map(erase_task_handle)
            })
        }
    }
}

fn wait_for_started(rx: &mpsc::Receiver<usize>, expected: usize, domain: WaitCompetitionDomain, scenario: &str) {
    assert_eq!(
        rx.recv_timeout(TEST_TIMEOUT)
            .unwrap_or_else(|error| panic!("{domain:?}/{scenario}: waiter did not start: {error}")),
        expected,
        "{domain:?}/{scenario}: the accepted waiter should report its id",
    );
}

struct WaiterTaskGuard(Vec<JoinHandle<()>>);

impl Drop for WaiterTaskGuard {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

async fn receive_waiter_result(
    completed: &mut tokio::sync::mpsc::Receiver<(usize, Result<ErasedTask, ExecutionServicesSubmissionError>)>,
    domain: WaitCompetitionDomain,
    scenario: &str,
) -> (usize, Result<ErasedTask, ExecutionServicesSubmissionError>) {
    timeout(TEST_TIMEOUT, completed.recv())
        .await
        .unwrap_or_else(|_| panic!("{domain:?}/{scenario}: waiter made no progress before timeout"))
        .unwrap_or_else(|| panic!("{domain:?}/{scenario}: waiter result channel closed"))
}

async fn expect_successful_waiter_task(
    task: ErasedTask,
    domain: WaitCompetitionDomain,
    scenario: &str,
    task_id: usize,
) {
    let result = timeout(TEST_TIMEOUT, task)
        .await
        .unwrap_or_else(|_| panic!("{domain:?}/{scenario}: accepted waiter {task_id} did not finish"));
    assert!(
        result.is_ok(),
        "{domain:?}/{scenario}: accepted waiter {task_id} should finish successfully: {result:?}",
    );
}

async fn wait_for_payload_drop(drops: &AtomicUsize, domain: WaitCompetitionDomain, scenario: &str, task_id: usize) {
    timeout(TEST_TIMEOUT, async {
        while drops.load(Ordering::SeqCst) == 0 {
            yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{domain:?}/{scenario}: waiter {task_id} payload was not dropped"));
}

async fn run_wait_competition(domain: WaitCompetitionDomain, scenario: WaitCompetitionScenario) {
    let label = scenario.name();
    let mut waiter_tasks = WaiterTaskGuard(Vec::new());
    let services = Arc::new(build_competition_services(domain));
    let mut occupied = occupy_competition_capacity(&services, domain);

    let (started_1_tx, started_1_rx) = mpsc::channel();
    let (started_2_tx, started_2_rx) = mpsc::channel();
    let (release_1, gate_1) = waiter_gate(domain);
    let (release_2, gate_2) = waiter_gate(domain);
    let calls: [Arc<AtomicUsize>; 2] = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
    let drops: [Arc<AtomicUsize>; 2] = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
    let (armed_1_tx, armed_1_rx) = oneshot::channel();
    let (armed_2_tx, armed_2_rx) = oneshot::channel();
    let (completed_tx, mut completed_rx) = tokio::sync::mpsc::channel(2);
    let spawn_waiter = |waiter_id: usize,
                        started,
                        gate,
                        calls: Arc<AtomicUsize>,
                        drops: Arc<AtomicUsize>,
                        armed_tx: oneshot::Sender<()>,
                        completed_tx: tokio::sync::mpsc::Sender<(
        usize,
        Result<ErasedTask, ExecutionServicesSubmissionError>,
    )>| {
        let services = Arc::clone(&services);
        tokio::spawn(async move {
            let mut waiter = make_competition_waiter(&services, domain, waiter_id + 1, started, gate, calls, drops);
            let mut armed = Some(armed_tx);
            let result = std::future::poll_fn(|cx| {
                let state = waiter.as_mut().poll(cx);
                if state.is_pending()
                    && let Some(sender) = armed.take()
                {
                    let _ = sender.send(());
                }
                state
            })
            .await
            .map(erase_task_handle);
            let _ = completed_tx.send((waiter_id, result)).await;
        })
    };
    waiter_tasks.0.extend([
        spawn_waiter(
            0,
            started_1_tx,
            gate_1,
            Arc::clone(&calls[0]),
            Arc::clone(&drops[0]),
            armed_1_tx,
            completed_tx.clone(),
        ),
        spawn_waiter(
            1,
            started_2_tx,
            gate_2,
            Arc::clone(&calls[1]),
            Arc::clone(&drops[1]),
            armed_2_tx,
            completed_tx,
        ),
    ]);
    let mut waiter_releases = [release_1, release_2];
    let waiter_started = [started_1_rx, started_2_rx];

    timeout(TEST_TIMEOUT, async {
        let (armed_1, armed_2) = tokio::join!(armed_1_rx, armed_2_rx);
        armed_1.expect("waiter 1 should register its pending wait");
        armed_2.expect("waiter 2 should register its pending wait");
    })
    .await
    .unwrap_or_else(|_| panic!("{domain:?}/{label}: waiters did not both register"));
    assert_eq!(
        calls[0].load(Ordering::SeqCst),
        0,
        "{domain:?}/{label}: waiter 1 must not run before admission"
    );
    assert_eq!(
        calls[1].load(Ordering::SeqCst),
        0,
        "{domain:?}/{label}: waiter 2 must not run before admission"
    );

    occupied.releases[0].release();
    if let Some(queued_started) = occupied.queued_started.take() {
        queued_started
            .recv_timeout(TEST_TIMEOUT)
            .unwrap_or_else(|error| panic!("{domain:?}/{label}: queued blocker did not start: {error}"));
    }

    let (accepted_id, accepted_result) = receive_waiter_result(&mut completed_rx, domain, label).await;
    let accepted_task = accepted_result
        .unwrap_or_else(|error| panic!("{domain:?}/{label}: waiter {accepted_id} unexpectedly rejected: {error}"));
    let pending_id = 1 - accepted_id;
    assert_eq!(
        calls[pending_id].load(Ordering::SeqCst),
        0,
        "{domain:?}/{label}: the unaccepted waiter must not execute",
    );

    match scenario {
        WaitCompetitionScenario::Progress => {
            for release in &mut occupied.releases {
                release.release();
            }
            wait_for_started(&waiter_started[accepted_id], accepted_id + 1, domain, label);
            waiter_releases[accepted_id].release();
            expect_successful_waiter_task(accepted_task, domain, label, accepted_id + 1).await;

            let (second_id, second_result) = receive_waiter_result(&mut completed_rx, domain, label).await;
            assert_eq!(
                second_id, pending_id,
                "{domain:?}/{label}: only the pending waiter should complete"
            );
            let second_task = second_result
                .unwrap_or_else(|error| panic!("{domain:?}/{label}: remaining waiter was rejected: {error}"));
            wait_for_started(&waiter_started[pending_id], pending_id + 1, domain, label);
            waiter_releases[pending_id].release();
            expect_successful_waiter_task(second_task, domain, label, pending_id + 1).await;
        }
        WaitCompetitionScenario::Shutdown | WaitCompetitionScenario::Stop => {
            if matches!(scenario, WaitCompetitionScenario::Shutdown) {
                services.shutdown();
            } else {
                let _stop_report = services.stop();
            }

            let (rejected_id, rejected) = receive_waiter_result(&mut completed_rx, domain, label).await;
            assert_eq!(
                rejected_id, pending_id,
                "{domain:?}/{label}: closed pending waiter should complete"
            );
            assert!(
                matches!(
                    rejected,
                    Err(ExecutionServicesSubmissionError::Rejected {
                        source: SubmissionError::Shutdown
                    })
                ),
                "{domain:?}/{label}: remaining waiter should be rejected with Shutdown",
            );
            for release in &mut occupied.releases {
                release.release();
            }
            waiter_releases[accepted_id].release();
            let accepted_result = timeout(TEST_TIMEOUT, accepted_task)
                .await
                .unwrap_or_else(|_| panic!("{domain:?}/{label}: accepted task did not reach a terminal result"));
            if matches!(scenario, WaitCompetitionScenario::Shutdown) {
                assert!(
                    accepted_result.is_ok(),
                    "{domain:?}/{label}: shutdown must let accepted work finish: {accepted_result:?}",
                );
            }
        }
    }

    if !matches!(scenario, WaitCompetitionScenario::Progress) {
        for task in occupied.tasks {
            let result = timeout(TEST_TIMEOUT, task)
                .await
                .unwrap_or_else(|_| panic!("{domain:?}/{label}: initial placeholder did not terminate"));
            if matches!(scenario, WaitCompetitionScenario::Shutdown) {
                assert!(
                    result.is_ok(),
                    "{domain:?}/{label}: shutdown should finish placeholders: {result:?}"
                );
            }
        }
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .unwrap_or_else(|_| panic!("{domain:?}/{label}: services did not terminate"));
    } else {
        for task in occupied.tasks {
            timeout(TEST_TIMEOUT, task)
                .await
                .unwrap_or_else(|_| panic!("{domain:?}/{label}: initial placeholder did not terminate"))
                .unwrap_or_else(|error| panic!("{domain:?}/{label}: initial placeholder failed: {error:?}"));
        }
        services.shutdown();
        timeout(TEST_TIMEOUT, services.await_termination())
            .await
            .unwrap_or_else(|_| panic!("{domain:?}/{label}: services did not terminate"));
    }
    for task in waiter_tasks.0.drain(..) {
        timeout(TEST_TIMEOUT, task)
            .await
            .unwrap_or_else(|_| panic!("{domain:?}/{label}: waiter task did not join"))
            .unwrap_or_else(|error| panic!("{domain:?}/{label}: waiter task panicked: {error}"));
    }

    if !matches!(scenario, WaitCompetitionScenario::Progress) {
        assert_eq!(
            calls[pending_id].load(Ordering::SeqCst),
            0,
            "{domain:?}/{label}: rejected waiter must never execute",
        );
    }
    match scenario {
        WaitCompetitionScenario::Progress | WaitCompetitionScenario::Shutdown => {
            assert_eq!(
                calls[accepted_id].load(Ordering::SeqCst),
                1,
                "{domain:?}/{label}: each accepted task must execute once",
            );
            if matches!(scenario, WaitCompetitionScenario::Progress) {
                assert_eq!(
                    calls[pending_id].load(Ordering::SeqCst),
                    1,
                    "{domain:?}/{label}: the second task must execute once after capacity returns",
                );
            }
        }
        WaitCompetitionScenario::Stop => assert!(
            calls[accepted_id].load(Ordering::SeqCst) <= 1,
            "{domain:?}/{label}: stop must not execute accepted work more than once",
        ),
    }
    for (index, drop_count) in drops.iter().enumerate() {
        wait_for_payload_drop(drop_count, domain, label, index + 1).await;
        assert_eq!(
            drop_count.load(Ordering::SeqCst),
            1,
            "{domain:?}/{label}: waiter {} payload drops once (accepted={accepted_id}, rejected/pending={pending_id})",
            index + 1,
        );
    }
}

impl Future for PollCounterFuture {
    type Output = Result<usize, io::Error>;

    fn poll(self: Pin<&mut Self>, _context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(42))
    }
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wait_competition_progress_across_domains() {
    for domain in WaitCompetitionDomain::all() {
        run_wait_competition(domain, WaitCompetitionScenario::Progress).await;
    }
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wait_competition_shutdown_across_domains() {
    for domain in WaitCompetitionDomain::all() {
        run_wait_competition(domain, WaitCompetitionScenario::Shutdown).await;
    }
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_wait_competition_stop_across_domains() {
    for domain in WaitCompetitionDomain::all() {
        run_wait_competition(domain, WaitCompetitionScenario::Stop).await;
    }
}

#[tokio_test]
async fn test_wait_apis_accept_non_clone_one_shot_tasks_for_every_domain() {
    let runtime = Handle::current();
    let services = ExecutionServices::builder()
        .enable_blocking()
        .enable_cpu()
        .enable_tokio_blocking()
        .enable_io()
        .runtime(runtime)
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()
        .unwrap();

    let blocking_value = String::from("blocking");
    let blocking = services
        .submit_blocking_callable_wait(move || Ok::<usize, io::Error>(blocking_value.len()))
        .await
        .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { blocking.await })
            .await
            .expect("blocking task should finish before the diagnostic timeout")
            .unwrap(),
        8
    );

    let cpu_value = String::from("cpu");
    let cpu = services
        .submit_cpu_callable_wait(move || Ok::<usize, io::Error>(cpu_value.len()))
        .await
        .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { cpu.await })
            .await
            .expect("CPU task should finish before the diagnostic timeout")
            .unwrap(),
        3
    );

    let tokio_blocking_value = String::from("tokio-blocking");
    let tokio_blocking = services
        .submit_tokio_blocking_callable_wait(move || Ok::<usize, io::Error>(tokio_blocking_value.len()))
        .await
        .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { tokio_blocking.await })
            .await
            .expect("Tokio blocking task should finish before the diagnostic timeout")
            .unwrap(),
        14
    );

    let io_value = String::from("io");
    let io = services
        .spawn_io_wait(async move { Ok::<usize, io::Error>(io_value.len()) })
        .await
        .unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, io)
            .await
            .expect("IO task should finish before the diagnostic timeout")
            .unwrap(),
        2
    );

    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_waits_for_blocking_capacity_without_recreating_the_task() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    services.submit_blocking_callable(|| Ok::<(), io::Error>(())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let payload = String::from("one-shot blocking task");
    let waiting = services.submit_blocking_callable_wait(move || {
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<usize, io::Error>(payload.len())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    gate.release();
    let handle = timeout(TEST_TIMEOUT, waiting).await.unwrap().unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { handle.await })
            .await
            .expect("accepted blocking task should finish before the diagnostic timeout")
            .unwrap(),
        22
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancelling_full_blocking_wait_drops_task_and_keeps_service_running() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(TEST_TIMEOUT).expect("worker gate must open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).expect("worker must start");
    let queued = services.submit_blocking_callable(|| Ok::<(), io::Error>(())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drops));
    let mut waiting = Box::pin(services.submit_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    }));

    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(waiting);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(services.is_running());

    gate.release();
    timeout(TEST_TIMEOUT, running).await.unwrap().unwrap();
    timeout(TEST_TIMEOUT, queued).await.unwrap().unwrap();
    let next = timeout(
        TEST_TIMEOUT,
        services.submit_blocking_callable_wait(|| Ok::<u8, io::Error>(42)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(timeout(TEST_TIMEOUT, next).await.unwrap().unwrap(), 42);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination()).await.unwrap();
    assert!(services.is_terminated());
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cancelling_full_tokio_blocking_wait_drops_task_and_keeps_service_running() {
    let services = ExecutionServices::builder()
        .enable_tokio_blocking()
        .runtime(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_tokio_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(TEST_TIMEOUT).expect("worker gate must open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).expect("worker must start");
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drops));
    let mut waiting = Box::pin(services.submit_tokio_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    }));

    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(waiting);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(services.is_running());

    gate.release();
    timeout(TEST_TIMEOUT, running).await.unwrap().unwrap();
    let next = timeout(
        TEST_TIMEOUT,
        services.submit_tokio_blocking_callable_wait(|| Ok::<u8, io::Error>(42)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(timeout(TEST_TIMEOUT, next).await.unwrap().unwrap(), 42);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination()).await.unwrap();
    assert!(services.is_terminated());
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_blocking_full_capacity_wait_wakes_on_shutdown() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let queued = services.submit_blocking_callable(|| Ok::<(), io::Error>(())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    services.shutdown();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, async { queued.await })
        .await
        .expect("queued task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("blocking service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_blocking_full_capacity_wait_wakes_on_stop() {
    let services = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .blocking_queue_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let queued = services.submit_blocking_callable(|| Ok::<(), io::Error>(())).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let _stop_report = services.stop();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    let _queued_result = timeout(TEST_TIMEOUT, async { queued.await })
        .await
        .expect("queued task should finish before the diagnostic timeout");
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("blocking service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_waits_for_cpu_tokio_blocking_and_io_capacity() {
    // CPU domain.
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let waiting = services.submit_cpu_callable_wait(move || {
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<usize, io::Error>(42)
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    gate.release();
    let handle = timeout(TEST_TIMEOUT, waiting).await.unwrap().unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { handle.await })
            .await
            .expect("accepted CPU task should finish before the diagnostic timeout")
            .unwrap(),
        42
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");

    // Tokio blocking domain.
    let services = ExecutionServices::builder()
        .enable_tokio_blocking()
        .runtime(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_tokio_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let waiting = services.submit_tokio_blocking_callable_wait(|| Ok::<usize, io::Error>(42));
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    gate.release();
    let handle = timeout(TEST_TIMEOUT, waiting).await.unwrap().unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, async { handle.await })
            .await
            .expect("accepted Tokio blocking task should finish before the diagnostic timeout")
            .unwrap(),
        42
    );
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");

    // IO domain.
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let running = services
        .spawn_io(async move {
            release_rx.await.unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let future = PollCounterFuture {
        polls: Arc::clone(&polls),
        _drop_counter: DropCounter(Arc::clone(&drops)),
    };
    let waiting = services.spawn_io_wait(future);
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, waiting).await.unwrap().unwrap().await.unwrap(),
        42,
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("accepted CPU task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dropping_accepted_cpu_wait_handle_does_not_cancel_task() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let (finished_tx, finished_rx) = mpsc::channel();
    let handle = services
        .submit_cpu_callable_wait(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            finished_tx.send(()).unwrap();
            Ok::<(), io::Error>(())
        })
        .await
        .unwrap();
    drop(handle);
    started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("accepted task should start");
    gate.release();
    finished_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("dropping result handle must not cancel task");
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("CPU service should terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cpu_full_capacity_wait_wakes_on_shutdown() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_cpu_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    services.shutdown();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("CPU service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cpu_full_capacity_wait_wakes_on_stop() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_cpu_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let _stop_report = services.stop();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("CPU service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tokio_blocking_full_capacity_wait_wakes_on_shutdown() {
    let services = ExecutionServices::builder()
        .enable_tokio_blocking()
        .runtime(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_tokio_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_tokio_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    services.shutdown();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("Tokio blocking service must terminate");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tokio_blocking_full_capacity_wait_wakes_on_stop() {
    let services = ExecutionServices::builder()
        .enable_tokio_blocking()
        .runtime(Handle::current())
        .tokio_blocking_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let running = services
        .submit_tokio_blocking_callable(move || {
            started_tx.send(()).unwrap();
            release_rx
                .recv_timeout(TEST_TIMEOUT)
                .expect("worker release gate should open");
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let drop_counter = DropCounter(Arc::clone(&drops));
    let waiting = services.submit_tokio_blocking_callable_wait(move || {
        let _drop_counter = drop_counter;
        task_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let _stop_report = services.stop();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.expect("wait must wake"),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    gate.release();
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("Tokio blocking service must terminate");
}

#[tokio_test]
async fn test_wait_prioritizes_shutdown_over_disabled_domain() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .build()
        .expect("CPU-only services should build");
    services.shutdown();

    let blocking_result = services.submit_blocking_callable_wait(|| Ok::<(), io::Error>(())).await;
    let cpu_result = services.submit_cpu_callable_wait(|| Ok::<(), io::Error>(())).await;
    let tokio_blocking_result = services
        .submit_tokio_blocking_callable_wait(|| Ok::<(), io::Error>(()))
        .await;
    let io_result = services.spawn_io_wait(async { Ok::<(), io::Error>(()) }).await;

    for result in [
        blocking_result.map(|_| ()),
        cpu_result.map(|_| ()),
        tokio_blocking_result.map(|_| ()),
        io_result.map(|_| ()),
    ] {
        assert!(matches!(
            result,
            Err(ExecutionServicesSubmissionError::Rejected {
                source: SubmissionError::Shutdown,
            })
        ));
    }
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_disabled_domain_does_not_run_task_and_shutdown_rejects_waiter() {
    let disabled = ExecutionServices::builder()
        .enable_blocking()
        .blocking_pool_size(1)
        .build()
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let task_calls = Arc::clone(&calls);
    let result = disabled
        .submit_cpu_callable_wait(move || {
            task_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), io::Error>(())
        })
        .await;
    let error = match result {
        Ok(_) => panic!("disabled CPU domain must reject submission"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ExecutionServicesSubmissionError::DomainDisabled {
            domain: ExecutionDomain::Cpu
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    disabled.shutdown();
    timeout(TEST_TIMEOUT, disabled.await_termination())
        .await
        .expect("disabled services should terminate before the diagnostic timeout");

    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (release_tx, _release_rx) = oneshot::channel::<()>();
    let _running = services
        .spawn_io(async move {
            std::future::pending::<()>().await;
            let _ = release_tx;
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;
    let drop_count = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drop_count));
    let waiting = services.spawn_io_wait(async move {
        let _drop_counter = drop_counter;
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    services.shutdown();
    let result = timeout(TEST_TIMEOUT, waiting).await.unwrap();
    assert!(matches!(
        result,
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    let _report = services.stop();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_cancelling_a_full_capacity_wait_drops_the_unaccepted_task_once() {
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let running = services
        .spawn_io(async move {
            release_rx.await.unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;
    let drop_count = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drop_count));
    let mut waiting = Box::pin(services.spawn_io_wait(async move {
        let _drop_counter = drop_counter;
        Ok::<(), io::Error>(())
    }));
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(drop_count.load(Ordering::SeqCst), 0);
    drop(waiting);
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    release_tx.send(()).unwrap();
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running CPU task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_cancelling_a_full_cpu_capacity_wait_drops_the_unaccepted_task_once() {
    let services = ExecutionServices::builder()
        .enable_cpu()
        .cpu_threads(1)
        .cpu_task_capacity(1)
        .build()
        .unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut gate = GateRelease(Some(release_tx));
    let (finished_tx, finished_rx) = mpsc::channel();
    let running = services
        .submit_cpu_callable(move || {
            started_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            finished_tx.send(()).unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    started_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("CPU worker should start the capacity blocker");

    let calls = Arc::new(AtomicUsize::new(0));
    let call_count = Arc::clone(&calls);
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drops));
    let mut waiting = Box::pin(services.submit_cpu_callable_wait(move || {
        let _drop_counter = drop_counter;
        call_count.fetch_add(1, Ordering::SeqCst);
        Ok::<(), io::Error>(())
    }));

    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(waiting);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    gate.release();
    finished_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("capacity blocker should finish after release");
    timeout(TEST_TIMEOUT, async { running.await })
        .await
        .expect("running task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("CPU service should terminate");
}

#[tokio_test]
async fn test_stopping_a_full_capacity_wait_rejects_and_drops_the_unaccepted_task_once() {
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (_release_tx, release_rx) = oneshot::channel::<()>();
    let running = services
        .spawn_io(async move {
            release_rx.await.unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;

    let drop_count = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(Arc::clone(&drop_count));
    let waiting = services.spawn_io_wait(async move {
        let _drop_counter = drop_counter;
        Ok::<(), io::Error>(())
    });
    pin!(waiting);
    assert!(poll_once(waiting.as_mut()).await.is_pending());
    assert_eq!(drop_count.load(Ordering::SeqCst), 0);

    let _stop_report = services.stop();
    assert!(matches!(
        timeout(TEST_TIMEOUT, waiting).await.unwrap(),
        Err(ExecutionServicesSubmissionError::Rejected {
            source: SubmissionError::Shutdown
        })
    ));
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    let cancelled = timeout(TEST_TIMEOUT, running)
        .await
        .expect("stopped CPU task should finish before the diagnostic timeout");
    assert!(matches!(cancelled, Err(TaskExecutionError::Cancelled)));
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test]
async fn test_two_waiters_compete_for_capacity_without_losing_wakeups() {
    let services = ExecutionServices::builder()
        .enable_io()
        .runtime(Handle::current())
        .io_task_capacity(NonZeroUsize::new(1).unwrap())
        .build()
        .unwrap();
    let (release_tx, release_rx) = oneshot::channel::<()>();
    let running = services
        .spawn_io(async move {
            release_rx.await.unwrap();
            Ok::<(), io::Error>(())
        })
        .unwrap();
    yield_now().await;

    let first = services.spawn_io_wait(async { Ok::<usize, io::Error>(1) });
    let second = services.spawn_io_wait(async { Ok::<usize, io::Error>(2) });
    pin!(first);
    pin!(second);
    assert!(poll_once(first.as_mut()).await.is_pending());
    assert!(poll_once(second.as_mut()).await.is_pending());
    release_tx.send(()).unwrap();
    let (first, second) = timeout(TEST_TIMEOUT, async { join!(&mut first, &mut second) })
        .await
        .expect("both waiters should eventually acquire capacity");
    let first = first.unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, first)
            .await
            .expect("first waiting task should finish before the diagnostic timeout")
            .unwrap(),
        1
    );
    let second = second.unwrap();
    assert_eq!(
        timeout(TEST_TIMEOUT, second)
            .await
            .expect("second waiting task should finish before the diagnostic timeout")
            .unwrap(),
        2
    );
    timeout(TEST_TIMEOUT, running)
        .await
        .expect("running IO task should finish before the diagnostic timeout")
        .unwrap();
    services.shutdown();
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate before the diagnostic timeout");
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_io_parent_waiting_on_same_full_domain_remains_pending() {
    let services = Arc::new(
        ExecutionServices::builder()
            .enable_io()
            .runtime(Handle::current())
            .io_task_capacity(NonZeroUsize::new(1).expect("capacity is nonzero"))
            .build()
            .expect("IO services should build"),
    );
    let (pending_tx, pending_rx) = oneshot::channel();
    let child_services = Arc::clone(&services);
    let parent = services
        .spawn_io(async move {
            let child_wait = child_services.spawn_io_wait(async { Ok::<(), io::Error>(()) });
            tokio::pin!(child_wait);
            let first_poll = poll_once(child_wait.as_mut()).await;
            pending_tx
                .send(first_poll.is_pending())
                .expect("test receiver should remain alive");
            let child = child_wait.await.map_err(io::Error::other)?;
            child.await.map_err(io::Error::other)
        })
        .expect("parent task should be accepted");

    assert!(
        timeout(TEST_TIMEOUT, pending_rx)
            .await
            .expect("parent should report the child wait state")
            .expect("parent should report its state")
    );
    assert_eq!(services.snapshot().io.unwrap().accepted_unfinished, 1);

    let _stop_report = services.stop();
    let _parent_result = timeout(TEST_TIMEOUT, parent)
        .await
        .expect("stop should release the accepted parent task");
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("stopped IO service should terminate");
    assert!(services.is_terminated());
}

async fn assert_parent_capacity_one_blocks_child(cpu: bool) {
    let runtime = Handle::current();
    let builder = ExecutionServices::builder().runtime(runtime.clone());
    let services = Arc::new(
        if cpu {
            builder.enable_cpu().cpu_threads(2).cpu_task_capacity(1).build()
        } else {
            builder
                .enable_tokio_blocking()
                .tokio_blocking_task_capacity(NonZeroUsize::new(1).expect("capacity is nonzero"))
                .build()
        }
        .expect("services should build"),
    );
    let child_runs = Arc::new(AtomicUsize::new(0));
    let child_services = Arc::clone(&services);
    let runs = Arc::clone(&child_runs);
    let (pending_tx, pending_rx) = oneshot::channel();
    let mut parent_work = Some(move || {
        runtime.block_on(async move {
            let child = move || {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok::<(), io::Error>(())
            };
            let mut wait: Pin<
                Box<
                    dyn Future<
                            Output = Result<
                                qubit_executor::TaskHandle<(), io::Error>,
                                ExecutionServicesSubmissionError,
                            >,
                        > + Send,
                >,
            > = if cpu {
                Box::pin(child_services.submit_cpu_callable_wait(child))
            } else {
                Box::pin(child_services.submit_tokio_blocking_callable_wait(child))
            };
            let pending = poll_once(wait.as_mut()).await.is_pending();
            let _ = pending_tx.send(pending);
            let rejected = timeout(TEST_TIMEOUT, wait).await.map_err(io::Error::other)?;
            assert!(matches!(
                rejected,
                Err(ExecutionServicesSubmissionError::Rejected {
                    source: SubmissionError::Shutdown
                })
            ));
            Ok::<(), io::Error>(())
        })
    });
    let parent = move || parent_work.take().expect("parent should execute once")();
    let parent_handle = if cpu {
        services.submit_cpu_callable(parent)
    } else {
        services.submit_tokio_blocking_callable(parent)
    }
    .expect("parent should be accepted");

    let observed = timeout(TEST_TIMEOUT, pending_rx).await;
    let snapshot = services.snapshot();
    services.shutdown();
    timeout(TEST_TIMEOUT, parent_handle)
        .await
        .expect("parent should exit after shutdown")
        .expect("parent should return successfully");
    timeout(TEST_TIMEOUT, services.await_termination())
        .await
        .expect("services should terminate");

    assert!(observed.expect("parent should report").expect("report should arrive"));
    let capacity = if cpu {
        snapshot.cpu.expect("CPU should be enabled").task_capacity
    } else {
        snapshot
            .tokio_blocking
            .expect("Tokio blocking should be enabled")
            .task_capacity
    };
    assert_eq!(capacity, 1);
    assert_eq!(child_runs.load(Ordering::SeqCst), 0);
    assert!(services.is_terminated());
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cpu_parent_capacity_one_blocks_child_with_spare_workers() {
    assert_parent_capacity_one_blocks_child(true).await;
}

#[tokio_test(flavor = "multi_thread", worker_threads = 2)]
async fn test_tokio_blocking_parent_capacity_one_blocks_child_with_spare_workers() {
    assert_parent_capacity_one_blocks_child(false).await;
}
