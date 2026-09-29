// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Manually controlled deadlines for downstream shutdown policy tests.

use std::future::Future;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use qubit_ioc::WaitPolicy;

/// A future completed only by an explicit test signal.
#[derive(Clone, Default)]
pub struct Gate {
    state: Arc<GateState>,
}

#[derive(Default)]
struct GateState {
    fired: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Gate {
    /// Releases this gate and wakes its observer outside the state lock.
    pub fn trigger(&self) {
        self.state.fired.store(true, Ordering::SeqCst);
        let waker = self.state.waker.lock().expect("gate lock").take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Future for Gate {
    type Output = ();

    /// Registers the observer and rechecks the signal to avoid a missed wakeup.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        if self.state.fired.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        *self.state.waker.lock().expect("gate lock") = Some(context.waker().clone());
        if self.state.fired.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Records timer creation and lets tests expire individual budgets explicitly.
#[derive(Clone, Default)]
pub struct Timers {
    calls: Arc<Mutex<Vec<TimerCall>>>,
}

struct TimerCall {
    duration: Duration,
    gate: Gate,
}

impl Timers {
    /// Returns distinct graceful and termination budgets without reading time.
    pub fn policy(&self) -> WaitPolicy {
        let timers = self.clone();
        WaitPolicy::bounded(Duration::from_secs(13), Duration::from_secs(7), move |duration| {
            let gate = Gate::default();
            timers.calls.lock().expect("timers lock").push(TimerCall {
                duration,
                gate: gate.clone(),
            });
            Box::pin(gate)
        })
    }

    /// Returns requested budgets in creation order for cancellation assertions.
    pub fn durations(&self) -> Vec<Duration> {
        self.calls
            .lock()
            .expect("timers lock")
            .iter()
            .map(|call| call.duration)
            .collect()
    }

    /// Expires the timer at `index`; panics if that timer was never installed.
    pub fn trigger(&self, index: usize) {
        let gate = self.calls.lock().expect("timers lock")[index].gate.clone();
        gate.trigger();
    }
}

/// Polls once and drops the borrowed wait future to model caller cancellation.
pub fn poll_once<F: Future>(future: F) -> Poll<F::Output> {
    pin!(future).as_mut().poll(&mut Context::from_waker(Waker::noop()))
}
