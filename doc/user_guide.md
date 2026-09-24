# Qubit Execution Services User Guide

[中文用户手册](user_guide.zh_CN.md) | [README](../README.md)

This guide is for Rust application developers using `qubit-execution-services` 0.9.0. It explains how to route different kinds of work through one facade, configure its managed pools, handle submission and task results, and shut the services down. The crate is an application-level facade; a library that needs only one execution layer can depend on that layer directly.

## Conceptual Model

`ExecutionServices` owns four execution domains:

| Domain | Work | Implementation and boundary |
| --- | --- | --- |
| `blocking` | Synchronous work that may block an OS thread | `ThreadPool` from `qubit-thread-pool`; its pool and queue are configured by this crate's builder. |
| `cpu` | CPU-bound synchronous work | Rayon-backed `RayonExecutorService`; this crate can bound accepted unfinished tasks. |
| `tokio_blocking` | Blocking functions in a Tokio application | Tokio `spawn_blocking`; runtime scheduling and blocking-pool settings belong to the application. |
| `io` | `Future` tasks | Tokio's async scheduler; this domain accepts futures that return `Result<R, E>`. |

The facade does not combine these into one scheduler. Choose a domain based on the work: blocking a Tokio worker with synchronous work can delay unrelated futures, while CPU-heavy work belongs in the Rayon domain. `spawn_io` can run any suitable async future; its name describes the domain, not a restriction to filesystem or network IO.

## Scenario: Route Three Kinds of Work

Suppose an application must perform a blocking operation, calculate a CPU-bound result, and await asynchronous work in the same request flow. The success criteria are that each task runs through its intended domain, each result can be observed, and the services can be shut down together.

The example below uses small computations so the return values are easy to verify. Replace the closures with application work while preserving the domain choice.

## Installation and Minimal Configuration

The crate requires Rust 1.94 or newer. Add the published crate and Tokio to the application. The Tokio runtime must be available before building the facade:

```toml
[dependencies]
qubit-execution-services = "0.9"
tokio = { version = "1.53", features = ["rt", "time"] }
```

For an application with a Tokio runtime, pass its handle to the builder and submit work to the appropriate domain:

```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;

use qubit_execution_services::ExecutionServices;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build()?;

    runtime.block_on(async {
        let services = ExecutionServices::builder(runtime.handle().clone())
            .blocking_pool_size(4)
            .blocking_queue_capacity(1024)
            .cpu_threads(4)
            .cpu_task_capacity(1024)
            .build()?;

        let blocking = services.submit_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))?;
        let cpu = services.submit_cpu_callable(|| Ok::<usize, io::Error>((1..=10).sum()))?;
        let io = services.spawn_io(async { Ok::<usize, io::Error>(6 * 7) })?;

        assert_eq!(blocking.get()?, 42);
        assert_eq!(cpu.get()?, 55);
        assert_eq!(io.await?, 42);

        services.shutdown();
        services.await_termination().await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
```

Here `get()` observes the blocking and CPU callable results, while awaiting the Tokio task handles observes the Tokio blocking or async result. Each callable or future returns `Result<R, E>`; the handle reports task completion separately from whether submission was accepted.

## Core Workflow

1. **Create the facade.** Call `ExecutionServices::builder(runtime_handle)` and configure the managed blocking and CPU pools as needed. `ExecutionServices::new(runtime_handle)` uses builder defaults.
2. **Route work by behavior.** Use `submit_blocking*` for synchronous tasks that may wait on blocking APIs, `submit_cpu*` for CPU-heavy synchronous work, `submit_tokio_blocking*` for blocking work integrated with Tokio, and `spawn_io` for async futures.
3. **Choose how to observe work.** `submit_*` runnable methods return after acceptance and do not return a result handle. Callable methods return a task handle for the result. `submit_tracked_*` methods return tracked tasks when status or cancellation is needed.
4. **Stop accepting work and wait.** `shutdown()` requests graceful shutdown across all domains. Then await `await_termination()` before releasing application resources that those tasks may use. Handle its `Result`; a failed Tokio join for either managed-domain waiter is returned as `ExecutionServicesWaitError`.

Submission itself can fail, so propagate or handle its `Result` at the call site. Accepted work can also finish with an error; inspect the task handle result when the task outcome matters.

## Advanced Configuration

### Blocking pool growth and queueing

The default blocking queue holds up to 1024 waiting tasks; running tasks are outside this queue limit. A full queue rejects another blocking submission with `SubmissionError::Saturated`. Set `blocking_queue_capacity(n)` to choose another finite limit, or call `blocking_unbounded_queue()` explicitly to restore unbounded queueing. Choose a limit based on expected task sizes and submission rates; this queue limit does not cap task memory.

The builder delegates blocking settings to `ThreadPoolBuilder`. `blocking_pool_size(n)` sets both core and maximum size. To let the pool grow under a burst, use a bounded queue with a maximum larger than the core size:

```rust
let services = ExecutionServices::builder(tokio::runtime::Handle::current())
    .blocking_core_pool_size(4)
    .blocking_maximum_pool_size(8)
    .blocking_queue_capacity(128)
    .build()?;
```

With `blocking_unbounded_queue()`, work continues to queue after the core size is reached; increasing the maximum alone does not create burst workers. Other blocking controls include thread-name prefix, stack size, keep-alive timeout, core-thread timeout, and prestarting core threads.

### CPU task capacity

`cpu_threads(n)` configures Rayon worker count. `cpu_task_capacity(n)` limits the total number of accepted CPU tasks that have not finished, including running and queued work. A submission made at capacity returns `SubmissionError::Saturated`; when accepted work finishes or a queued tracked task is cancelled, capacity can become available again.

### Tokio task capacities

The Tokio blocking and IO domains each default to 1024 accepted tasks that have not completed. This count includes queued and running work; for IO, it also includes futures that Tokio has accepted but has not polled yet. At capacity, submission returns `SubmissionError::Saturated`. Set `tokio_blocking_task_capacity(NonZeroUsize)` or `io_task_capacity(NonZeroUsize)` on the facade builder to choose another finite limit. A completed task releases its slot. Cancelling a queued blocking task or aborting an IO task releases its slot when the underlying task is dropped; a blocking closure that has already started cannot be forcibly stopped and keeps its slot until it returns.

### Tokio runtime ownership

The builder passes the supplied `tokio::runtime::Handle` to both Tokio-backed domains and configures their accepted-task capacities. Configure Tokio's runtime and scheduler in the application that creates the runtime. The facade owns the domains and does not expose direct domain accessors, so submissions pass through the facade-wide shutdown gate.

### Graceful shutdown and abrupt stop

`shutdown()` and `stop()` first close facade-wide submission admission, serialized against submissions already in progress, then request the corresponding operation from each domain. `shutdown()` asks accepted work to complete according to the underlying service's behavior. `stop()` requests abrupt stopping and returns an `ExecutionServicesStopReport` with one `StopReport` per domain. Its `total_queued()`, `total_running()`, and `total_cancelled()` methods add the corresponding fields from those reports. Each domain is sampled in sequence, so these totals are not an atomic snapshot across all domains. The Tokio IO `running` field counts accepted futures that had not completed when stop was requested; it does not say whether a future was being polled at that instant. Treat these values as per-domain stop accounting, not a global concurrency measurement.

Poll `await_termination()` from an active Tokio runtime. It uses the calling runtime's blocking pool to wait for the managed blocking and CPU domains, and asynchronously waits for both Tokio-backed domains. Polling without an active runtime panics when the blocking waiters are started. Keep the calling runtime alive with available blocking capacity, and keep the runtime supplied to the builder running until its Tokio tasks finish. Awaiting from another runtime does not drive a stopped current-thread runtime. Dropping the returned future after polling begins does not stop the services or already spawned blocking waiters; continue to manage service shutdown explicitly.

The facade also exposes `lifecycle()`, `is_running()`, `is_shutting_down()`, `is_stopping()`, `is_not_running()`, and `is_terminated()` for lifecycle checks.

## Errors and Diagnostics

- `ExecutionServicesBuilder::build()` returns `ExecutionServicesBuildError::Blocking` when the blocking builder rejects its configuration, or `ExecutionServicesBuildError::Cpu` when the CPU builder rejects its configuration. The error retains the underlying builder error as its source.
- Submission methods return `SubmissionError` when a domain refuses work. The CPU and Tokio domains report a full configured capacity as `SubmissionError::Saturated`. After shutdown starts, all facade submissions return `SubmissionError::Shutdown`, even if a domain is also at capacity.
- Once accepted, the task's result is obtained through its handle. A task's own error value is distinct from a submission error; inspect both layers rather than treating successful submission as successful completion.
- For aggregate shutdown diagnostics, inspect the per-domain fields of `ExecutionServicesStopReport` before relying only on totals.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `build()` returns an error | Inspect whether the error is the `Blocking` or `Cpu` variant, then validate the corresponding pool settings. For example, a zero maximum blocking size or zero CPU thread count is rejected. |
| CPU or Tokio submission returns `Saturated` | Increase the relevant task capacity, reduce outstanding work, or apply backpressure before submitting more tasks. |
| Blocking work queues instead of using more workers | Check whether the blocking queue is unbounded. Configure a bounded `blocking_queue_capacity` and a larger `blocking_maximum_pool_size` if elastic growth is desired. |
| A new submission fails during shutdown | Check `lifecycle()` and stop submitting once shutdown begins. |
| `await_termination()` does not resolve | Check whether accepted blocking work is still running or waiting on an external condition; graceful shutdown waits for underlying services to terminate. |

## Limitations and Best Practices

- The facade owns the blocking and CPU services it builds, but Tokio runtime lifetime and scheduler configuration remain the application's responsibility. Keep the runtime alive while using the Tokio-backed domains.
- Use a bounded queue when queued blocking work needs a finite limit. An unbounded queue has no configured queue-capacity limit.
- CPU capacity counts unfinished accepted work, not just queued work. Account for both running and queued tasks when choosing a limit.
- Graceful shutdown can wait for accepted work. Use `stop()` when abrupt stopping is required, then inspect its report and task handles for cancellation outcomes.
- This crate routes work and aggregates lifecycle state; it does not provide application-specific retry, prioritization, or Tokio runtime configuration.

## Further Reading

- [README](../README.md) and [中文 README](../README.zh_CN.md)
- [API documentation](https://docs.rs/qubit-execution-services)
- [中文用户手册](user_guide.zh_CN.md)
