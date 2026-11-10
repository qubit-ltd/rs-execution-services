# Execution Services Design

[中文设计文档](design.zh_CN.md) · [User guide](user_guide.md)

## Purpose and boundaries

`qubit-execution-services` is an application-owned facade over four independent execution domains: a blocking thread pool, a Rayon CPU pool, Tokio `spawn_blocking`, and Tokio async IO. It centralizes construction, admission, snapshots, and shutdown while leaving Tokio runtime creation and configuration with the application.

Each domain owns its own capacity and queue. The facade is not a shared scheduler and does not create a process-wide thread or memory budget. Its builder requires at least one enabled domain and rejects options configured for disabled domains.

## Ownership and modules

`ExecutionServices` owns an `ExecutionServicesAdmission` and optional service values for each domain. The blocking service is held in `Arc` so accepted producers can submit child work through the same facade; the other service handles already provide shared ownership as required by their implementations.

- `execution_services.rs` owns the facade, builder entry point, domain inventory, and snapshot composition.
- `submission.rs` implements immediate submission and the shared native submission adapter.
- `waiting_submission.rs` implements the four capacity-wait APIs. Blocking uses the owned-task wrapper and shared retry loop; CPU, Tokio blocking, and IO retry through their native task-returning rejection APIs.
- `internal/execution_services_admission.rs` owns monotonic facade intent and resolves a requested optional domain.
- `internal/submission_retry.rs` retries blocking submissions only when they are saturated, waiting on a capacity-change receiver between attempts.
- `internal/owned_wait_task.rs` retains the blocking callable while rejected attempt wrappers are dropped.
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

A wait API returns a lazy future. It starts when first polled, resolves the domain, and subscribes to capacity changes before its first submission attempt. Blocking stores the callable in `OwnedWaitTask` and uses a fresh wrapper for each attempt; CPU, Tokio blocking, and IO retain the original callable or future in the waiting future and use native `try_*` APIs that return that task when rejected. All four return a handle after acceptance, retry only after saturation and a capacity-change notification, and return other errors immediately. If the notification channel closes, the wait returns Shutdown instead of remaining pending forever.

Capacity notifications are advisory: they indicate a state change, not a reservation. A competing submitter can consume the available capacity before a waiter retries. No FIFO ordering, fairness, or wait-time bound is guaranteed.

For blocking, `OwnedWaitTask` holds `Option<C>` behind a mutex shared with attempt wrappers. A rejected wrapper leaves the callable in the slot; an accepted wrapper takes it when executed. The other domains recover the original task directly from the native rejection value and retain it across retries. No path requires `Clone`, and a one-shot task is submitted for execution at most once.

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

Dropping the facade releases its service values; it does not coordinate `stop()` or wait for all accepted work. The resolved `qubit-thread-pool 0.12.1` implementation requests graceful shutdown when its `ThreadPool` value is dropped. The `qubit-rayon-executor 0.9.0` wrapper has no drop-time call to its explicit shutdown or stop methods; dropping the owned Rayon pool value signals pool termination, and Rayon worker threads terminate gradually after extant work completes. This is not a facade-level wait. The resolved `qubit-tokio-executor 0.12.0` blocking and IO service wrappers have no drop implementation that cancels accepted tasks. Tokio blocking tasks and IO futures are not aborted by dropping this facade. These differences do not provide a coordinated completion guarantee; call `shutdown()` and await `await_termination()` before dropping the facade when accepted work must finish before resources are released.

On success and ordinary error returns, preserve the operation result while requesting graceful shutdown and awaiting termination. If abrupt stop or Tokio abort is required, synchronous cleanup may run in the task's drop/cancellation path; asynchronous cleanup must be driven by a surviving owner before it releases the required resources. A future that has already been cancelled cannot resume to perform a later asynchronous cleanup wait. In particular, dropping an `await_termination()` future only cancels that waiter; a surviving owner may await termination again. Keep Tokio running until its accepted work and any required cleanup have completed.

A capacity-one IO task that awaits another submission to the same full IO domain can form a wait cycle: the parent holds the only accepted slot while the child waits for a slot. Put orchestration outside that domain, use distinct domains for independent work, or reason explicitly about dependency depth and capacity. Increasing capacity alone is not a proof against arbitrary recursive waits. The facade does not detect dependency cycles or create fallback workers.

## Downstream integration

The IoC fixture treats `ExecutionServices` as a managed resource. On build failure or cancellation, rollback uses `stop()` so cleanup closes admission without waiting on unbounded application work. A failed async build awaits each managed wait action and verifies the service terminates. If the build future itself is cancelled, its synchronous stop callback runs; the fixture then waits for termination outside the cancelled build because a dropped build cannot drive an async wait callback.

Normal application shutdown has a different order: stop producers, call `ExecutionServices::shutdown()`, await termination while Tokio remains active, then shut down the IoC context. Repeating stop during context cleanup does not reopen admission.

The existing [ioc-integration workflow](../.github/workflows/ci.yml) checks a fixed baseline. The [ioc-current-integration workflow](../.github/workflows/ioc-current-integration.yml) checks this repository revision from the triggering event together with `rs-ioc/main`; its other sibling repositories remain pinned to fixed commits. Its daily schedule also checks combinations where only the downstream IoC repository has changed. Both workflows verify fixture contracts and do not indicate production adoption.

## Verification

The integration suite exercises the public error matrix, capacity waits for all four domains, repeated contention, dropping an unaccepted task, closure wakeups, and same-domain nested waiting. Tests poll a pinned wait future once and assert `Pending` while a separate gate holds capacity; a timeout is only a deadlock watchdog.

The application consumer and runnable resource-budget example also poll a wait future before releasing the accepted task. The local documentation consumer verifies the sibling-checkout layout and Tokio features; because it resolves this repository's `.cargo/config.toml` patches, it is not a registry-isolation check. The [packaged documentation consumer workflow](../.github/workflows/packaged-documentation-consumer.yml) runs two independent jobs. Both package the crate, extract the archive outside the source tree, and build and run consumers using `quick_start.rs` and the complete `daily_report.rs` read from that archive. The source-integration job checks out the latest `dev-starfish` sources of `rs-tokio-executor`, `rs-rayon-executor`, and `rs-executor`, and permits exactly those three local executor packages alongside the consumer and archive target; all other packages must have a `registry+` source. The registry-only job runs `python3 .infra/tools/check-packaged-documentation-consumer.py --registry-only`; it excludes inherited Cargo override variables, uses an isolated Cargo home and target directory, fixes the crates.io source, rejects path/git/patch inputs in the archived manifest, and allows only the consumer and extracted target crate as local packages. In both modes the extracted target must have `source=None` and its exact archive manifest path, and the consumer must have its expected name and manifest path. After initial resolution creates the consumer lockfile, the verifier checks metadata with `cargo metadata --locked` and then runs with `cargo run --locked`, so source checks and execution share that lockfile. The source-integration check allows local overrides for the three executor crates; the registry-only check fails when required registry dependencies are unavailable. Both checks are required for release acceptance. Neither proves minimum-semver compatibility, nor does either establish production adoption. The IoC integration job runs its pinned consumer, tests, and Clippy independently of the core crate's feature matrix.

The bilingual user guides describe these same error, ownership, cancellation, and capacity contracts. Runnable examples and the package file list verify that documented code and design documents are present in the crate archive.

## Non-goals

This facade does not provide a fair or FIFO wait queue, a global resource budget, automatic deadlock detection, priority scheduling, forced interruption of synchronous code, or a universal cancellable handle. Those capabilities require separate application demand and a dedicated API and lifecycle design.
