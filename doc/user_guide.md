# Qubit Execution Services User Guide

[中文用户手册](user_guide.zh_CN.md) | [README](../README.md)

This guide is for Rust application developers using `qubit-execution-services` 0.8.0. It explains how to route different kinds of work through one facade, configure its managed pools, handle submission and task results, and shut the services down. The crate is an application-level facade; a library that needs only one execution layer can depend on that layer directly.

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
qubit-execution-services = "0.8"
tokio = { version = "1.53", features = ["rt", "time"] }
```

For an application with a Tokio runtime, pass its handle to the builder and submit work to the appropriate domain:

```rust
use std::io;

use qubit_execution_services::ExecutionServices;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let services = ExecutionServices::builder(tokio::runtime::Handle::current())
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
# Ok(())
# }
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

### Tokio runtime ownership

The builder passes the supplied `tokio::runtime::Handle` to both Tokio-backed domains. This crate does not expose builders for those domains. Configure Tokio's runtime and scheduler in the application that creates the runtime.

### Graceful shutdown and abrupt stop

`shutdown()` rejects new tasks and asks accepted work to complete according to the underlying service's behavior. `stop()` requests abrupt stopping and returns an `ExecutionServicesStopReport` with one `StopReport` per domain. Its `total_queued()`, `total_running()`, and `total_cancelled()` methods add the corresponding fields from those reports. Each domain is sampled in sequence, so these totals are not an atomic snapshot across all domains. The Tokio IO `running` field counts accepted futures that had not completed when stop was requested; it does not say whether a future was being polled at that instant. Treat these values as per-domain stop accounting, not a global concurrency measurement.

`await_termination()` uses the calling Tokio runtime's blocking pool to wait for the managed blocking and CPU domains, and asynchronously waits for both Tokio-backed domains. Keep the calling runtime alive with available blocking capacity, and keep the runtime supplied to the builder running until its Tokio tasks finish. Awaiting from another runtime does not drive a stopped current-thread runtime.

The facade also exposes `lifecycle()`, `is_running()`, `is_shutting_down()`, `is_stopping()`, `is_not_running()`, and `is_terminated()` for lifecycle checks.

## Errors and Diagnostics

- `ExecutionServicesBuilder::build()` returns `ExecutionServicesBuildError::Blocking` when the blocking builder rejects its configuration, or `ExecutionServicesBuildError::Cpu` when the CPU builder rejects its configuration. The error retains the underlying builder error as its source.
- Submission methods return `SubmissionError` when a domain refuses work. The CPU domain reports a full configured capacity as `SubmissionError::Saturated`. After shutdown, submissions are rejected.
- Once accepted, the task's result is obtained through its handle. A task's own error value is distinct from a submission error; inspect both layers rather than treating successful submission as successful completion.
- For aggregate shutdown diagnostics, inspect the per-domain fields of `ExecutionServicesStopReport` before relying only on totals.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `build()` returns an error | Inspect whether the error is the `Blocking` or `Cpu` variant, then validate the corresponding pool settings. For example, a zero maximum blocking size or zero CPU thread count is rejected. |
| CPU submission returns `Saturated` | Increase `cpu_task_capacity`, reduce outstanding work, or apply backpressure before submitting more tasks. |
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
