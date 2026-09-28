# Execution Services Design

[中文设计文档](design.zh_CN.md) · [User guide](user_guide.md)

## Purpose and boundaries

`qubit-execution-services` is an application-owned facade over four independent execution domains: a blocking thread pool, a Rayon CPU pool, Tokio `spawn_blocking`, and Tokio async IO. It centralizes construction, admission, snapshots, and shutdown while leaving Tokio runtime creation and configuration with the application.

Each domain owns its own capacity and queue. The facade is not a shared scheduler and does not create a process-wide thread or memory budget. Its builder requires at least one enabled domain and rejects options configured for disabled domains.

## Ownership and modules

`ExecutionServices` owns an `ExecutionServicesAdmission` and optional service values for each domain. The blocking service is held in `Arc` so accepted producers can submit child work through the same facade; the other service handles already provide shared ownership as required by their implementations.

- `execution_services.rs` owns the facade, builder entry point, domain inventory, and snapshot composition.
- `submission.rs` implements immediate submission and the shared native submission adapter.
- `waiting_submission.rs` implements the four capacity-wait APIs as thin adapters over the common retry loop.
- `internal/execution_services_admission.rs` owns monotonic facade intent and resolves a requested optional domain.
- `internal/submission_retry.rs` retries only saturation, waiting on a capacity-change receiver between attempts.
- `internal/owned_wait_task.rs` retains the one-shot callable or future while rejected attempt wrappers are dropped.
- `lifecycle.rs` closes each enabled domain and asynchronously waits for termination.

The Tokio `sync` feature is a direct dependency because the retry module names Tokio's `watch::Receiver` API directly.

## Submission and admission

The facade records one of three intents: `Running`, `ShuttingDown`, or `Stopping`. Shutdown and stop close admission before asking each enabled service to perform its lifecycle operation. Stop upgrades graceful shutdown and cannot be downgraded.

Both immediate and waiting submission first resolve the requested domain under admission. While intent is `Running`, an absent service returns `DomainDisabled`. Once intent has closed admission, `Rejected { source: Shutdown }` takes precedence, even when the requested domain is disabled. The admission mutex is released before native submission, rejected task destruction, user callable execution, or future polling.

Admission and native acceptance are not one atomic transaction. A call that passed the facade check may overlap shutdown or stop; the underlying service decides whether it accepts that task. The facade does not promise strict linearizability across independent services.

| Facade intent | Domain state | Immediate submission | Capacity-wait submission |
| --- | --- | --- | --- |
| Running | Disabled | `DomainDisabled` | `DomainDisabled` |
| Closed | Disabled or enabled | `Rejected(Shutdown)` | `Rejected(Shutdown)` |
| Running | Enabled with available capacity | Native handle | Native handle |
| Running | Enabled at capacity | `Rejected(Saturated)` | Pending; retry after notification |
| Closed during a wait | Previously enabled | Not applicable | Wake, recheck admission, return `Rejected(Shutdown)` |

## Capacity-wait algorithm

A wait API returns a lazy future. It starts when first polled, resolves the domain, stores the user task in `OwnedWaitTask`, and subscribes to capacity changes before its first attempt. Each attempt passes a fresh wrapper to the native executor. If accepted, the handle is returned. If saturated, the future waits for a watch notification and retries. Other submission errors return immediately. If the notification channel closes, the wait returns Shutdown instead of remaining pending forever.

Capacity notifications are advisory: they indicate a state change, not a reservation. A competing submitter can consume the available capacity before a waiter retries. No FIFO ordering, fairness, or wait-time bound is guaranteed.

`OwnedWaitTask` holds `Option<T>` behind a mutex shared with attempt wrappers. A rejected wrapper leaves the task in the slot. An accepted callable wrapper takes the callable when executed; an accepted IO wrapper takes the future on first poll. The slot lock is released before user code runs. This makes task movement independent of `Clone` and prevents retries from invoking a one-shot task more than once.

Capacity limits count accepted work only. Waiters that have not been accepted are not included in `accepted_unfinished` snapshots or task-capacity limits, even though their futures retain user data. Applications that need a memory bound must limit concurrent producers or add application-level timeouts/backpressure.

## Cancellation and lifecycle

| Stage or handle | Contract |
| --- | --- |
| Wait future dropped before acceptance | Stops retries and drops the unaccepted task. |
| `TaskHandle` from blocking, CPU, or Tokio-blocking wait | Observes task completion; it has no cancellation method. Dropping it does not cancel work. |
| `TokioTaskHandle` from IO wait | Can request Tokio task abort; abort may race normal completion and does not guarantee user cleanup code runs. |
| Started synchronous callable | Cannot be forcibly interrupted by a handle. |
| `shutdown()` | Closes admission and requests graceful completion according to each enabled service's contract. |
| `stop()` | Closes admission and requests abrupt stopping; it cannot forcibly interrupt already-running synchronous code. |

Keep the supplied Tokio runtime active until enabled Tokio services terminate. Stop external producers and wait for accepted producers that may submit child work before calling `shutdown()`.

A capacity-one IO task that awaits another submission to the same full IO domain can form a wait cycle: the parent holds the only accepted slot while the child waits for a slot. Put orchestration outside that domain, use distinct domains for independent work, or reason explicitly about dependency depth and capacity. Increasing capacity alone is not a proof against arbitrary recursive waits. The facade does not detect dependency cycles or create fallback workers.

## Downstream integration

The IoC fixture treats `ExecutionServices` as a managed resource. On build failure or cancellation, rollback uses `stop()` so cleanup closes admission without waiting on unbounded application work. A failed async build awaits each managed wait action and verifies the service terminates. If the build future itself is cancelled, its synchronous stop callback runs; the fixture then waits for termination outside the cancelled build because a dropped build cannot drive an async wait callback.

Normal application shutdown has a different order: stop producers, call `ExecutionServices::shutdown()`, await termination while Tokio remains active, then shut down the IoC context. Repeating stop during context cleanup does not reopen admission.

CI checks this integration against fixed sibling revisions: `rs-ioc` `762660f56e425a5b1b442528a9f979a2c41a1a9d`, `rs-event-bus` `bf7ab432947070e6e8bbc6ffe02560eb1957ad27`, and `rs-fs-registry` `d3a6cacbc05bea970175db9cf6211680aa39c87c`. These are reproducibility pins for the fixture, not a claim that production downstream services currently use this facade.

## Verification

The integration suite exercises the public error matrix, capacity waits for all four domains, repeated contention, dropping an unaccepted task, closure wakeups, and same-domain nested waiting. Tests poll a pinned wait future once and assert `Pending` while a separate gate holds capacity; a timeout is only a deadlock watchdog.

The application consumer and runnable resource-budget example also poll a wait future before releasing the accepted task. The documentation consumer verifies the minimal published dependency and Tokio features. The IoC integration job runs its pinned consumer, tests, and Clippy independently of the core crate's feature matrix.

The bilingual user guides describe these same error, ownership, cancellation, and capacity contracts. Runnable examples and the package file list verify that documented code and design documents are present in the crate archive.

## Non-goals

This facade does not provide a fair or FIFO wait queue, a global resource budget, automatic deadlock detection, priority scheduling, forced interruption of synchronous code, or a universal cancellable handle. Those capabilities require separate application demand and a dedicated API and lifecycle design.
