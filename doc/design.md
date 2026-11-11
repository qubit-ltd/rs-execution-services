# Execution Services Design

[中文设计文档](design.zh_CN.md) · [User guide](user_guide.md)

## Purpose and boundaries

`qubit-execution-services` is an application-owned facade over four independent execution domains: a blocking thread pool, a Rayon CPU pool, Tokio `spawn_blocking`, and Tokio async IO. It centralizes construction, admission, snapshots, and shutdown while leaving Tokio runtime creation and configuration with the application.

Each domain owns its own capacity and queue. The facade is not a shared scheduler and does not create a process-wide thread or memory budget. Its builder requires at least one enabled domain and rejects options configured for disabled domains.

## Ownership and modules

`ExecutionServices` owns an `ExecutionServicesAdmission` and optional service values for each domain. The blocking service is held in `Arc` so accepted producers can submit child work through the same facade; the other service handles already provide shared ownership as required by their implementations.

- `execution_services.rs` owns the facade, builder entry point, domain inventory, and snapshot composition.
- `submission.rs` implements immediate submission and the shared native submission adapter.
- `waiting_submission.rs` implements the four capacity-wait APIs. All four domains share the retry control flow; blocking uses the owned-task wrapper, while CPU, Tokio blocking, and IO recover tasks through their native task-returning rejection APIs.
- `internal/execution_services_admission.rs` owns monotonic facade intent and resolves a requested optional domain.
- `internal/submission_retry.rs` shares the retry control flow across all four domains through `CapacityChangeReceiver`, waiting for a capacity-change notification between saturated attempts.
- `internal/capacity_change_receiver.rs` adapts native notification sources to the shared retry loop. Blocking, Tokio blocking, and IO use Tokio watch receivers; CPU retains Rayon's `CapacityChangesChanged` wrapper to preserve waker panic isolation.
- `internal/owned_wait_task.rs` retains the blocking callable while rejected attempt wrappers are dropped.
- `lifecycle.rs` closes each enabled domain and asynchronously waits for termination.

The Tokio `sync` feature remains a direct dependency because the blocking, Tokio-blocking, and IO capacity adapters use Tokio watch receivers.

## Submission and admission

The facade records one of three intents: `Running`, `ShuttingDown`, or `Stopping`. Shutdown and stop close admission before asking each enabled service to perform its lifecycle operation. Stop upgrades graceful shutdown and cannot be downgraded.

Both immediate and waiting submission first resolve the requested domain under admission. While intent is `Running`, an absent service returns `DomainDisabled`. Once intent has closed admission, `Rejected { domain, source: Shutdown }` takes precedence, even when the requested domain is disabled. The admission mutex is released before native submission, rejected task destruction, user callable execution, or future polling.

Admission and native acceptance are not one atomic transaction. A call that passed the facade check may overlap shutdown or stop; the underlying service decides whether it accepts that task. The facade does not promise strict linearizability across independent services.

| Facade intent | Domain state | Immediate submission | Capacity-wait submission |
| --- | --- | --- | --- |
| Running | Disabled | `DomainDisabled` | `DomainDisabled` |
| Closed | Disabled or enabled | `Rejected { domain, source: Shutdown }` | `Rejected { domain, source: Shutdown }` |
| Running | Enabled with available capacity | Native handle | Native handle |
| Running | Enabled at capacity | `Rejected { domain, source: Saturated }` | Pending; retry after notification |
| Closed during a wait | Previously enabled | Not applicable | Wake, recheck admission, return `Rejected { domain, source: Shutdown }` |

## Capacity-wait algorithm

A wait API returns a lazy future. It starts when first polled, resolves the domain, and subscribes to capacity changes before its first submission attempt. Blocking stores the callable in `OwnedWaitTask` and uses a fresh wrapper for each attempt; CPU, Tokio blocking, and IO retain the original callable or future in the waiting future and use native `try_*` APIs that return that task when rejected. All four return a handle after acceptance, retry only after saturation and a capacity-change notification, and return other errors immediately. If the notification channel closes, the wait returns Shutdown instead of remaining pending forever.

Capacity notifications are advisory: they indicate a state change, not a reservation. A competing submitter can consume the available capacity before a waiter retries. No FIFO ordering, fairness, or wait-time bound is guaranteed.

For blocking, `OwnedWaitTask` holds `Option<C>` behind a mutex shared with attempt wrappers. A rejected wrapper leaves the callable in the slot; an accepted wrapper takes it when executed. The other domains recover the original task directly from the native rejection value and retain it across retries. No path requires `Clone`, and a one-shot task is submitted for execution at most once.

Capacity limits count accepted work only. Waiters that have not been accepted are not included in `accepted_unfinished` snapshots or task-capacity limits, even though their futures retain user data. Applications that need a memory bound must limit concurrent producers or add application-level timeouts/backpressure. In the CPU domain, queued, running, and cancelling tasks all occupy capacity until service-side accounting is released. Captured-value cleanup for a cancelled queued task can therefore keep capacity occupied after a terminal result becomes observable; an immediate submission can still be saturated and a wait can remain pending. Cleanup completion releases accounting and sends a capacity-change notification, after which waiters compete again without FIFO, fairness, or bounded-wait guarantees. CPU snapshot counts expose queued, running, and cancelling states but do not reserve a slot.

## Cancellation and lifecycle

| Stage or handle | Contract |
| --- | --- |
| Wait future dropped before acceptance | Stops retries and drops the unaccepted task. |
| `TaskHandle` from blocking, CPU, or Tokio-blocking wait | Observes task completion; it has no cancellation method. Dropping it does not cancel work. |
| `TokioTaskHandle` from IO wait | Can request Tokio task abort; abort may race normal completion and does not guarantee user cleanup code runs. |
| Started synchronous callable | Cannot be forcibly interrupted by a handle. |
| `shutdown()` | Closes admission and requests graceful completion according to each enabled service's contract. |
| `stop()` | Closes admission and requests abrupt stopping; it cannot forcibly interrupt already-running synchronous code. Cancelling queued work can synchronously drop user-captured values on the calling thread, with no fixed duration bound. |

Keep the supplied Tokio runtime active until enabled Tokio services terminate. Stop external producers and wait for accepted producers that may submit child work before calling `shutdown()`. Keep destructors short and non-blocking: queued-task cleanup runs synchronously during `stop()`, so a blocked destructor can delay later domains from receiving their stop request. An asynchronous timeout cannot preempt a synchronous stop call. If cleanup must be asynchronous, drive it from a surviving owner; do not synchronously wait inside cleanup for this service to terminate, because termination may depend on that same cleanup.

Dropping the facade releases its service values; it does not coordinate `stop()` or wait for all accepted work. The resolved `qubit-thread-pool 0.12.1` implementation requests graceful shutdown when its `ThreadPool` value is dropped. The `qubit-rayon-executor 0.9.0` wrapper has no drop-time call to its explicit shutdown or stop methods; dropping the owned Rayon pool value signals pool termination, and Rayon worker threads terminate gradually after extant work completes. This is not a facade-level wait. The resolved `qubit-tokio-executor 0.12.0` blocking and IO service wrappers have no drop implementation that cancels accepted tasks. Tokio blocking tasks and IO futures are not aborted by dropping this facade. These differences do not provide a coordinated completion guarantee; call `shutdown()` and await `await_termination()` before dropping the facade when accepted work must finish before resources are released.

On success and ordinary error returns, preserve the operation result while requesting graceful shutdown and awaiting termination. If abrupt stop or Tokio abort is required, synchronous cleanup may run in the task's drop/cancellation path; asynchronous cleanup must be driven by a surviving owner before it releases the required resources. A future that has already been cancelled cannot resume to perform a later asynchronous cleanup wait. In particular, dropping an `await_termination()` future only cancels that waiter; a surviving owner may await termination again. Keep Tokio running until its accepted work and any required cleanup have completed.

A parent task remains counted against unfinished-task capacity until it returns. In CPU or Tokio blocking, a capacity-one parent that synchronously waits for a same-domain child submission can therefore form a wait cycle even when additional workers are available: the child cannot be accepted until the parent releases its slot, and the parent cannot return until the child is accepted and completes. A capacity-one IO parent awaiting a child submission similarly holds the only accepted-future slot. The blocking domain has a different queue-capacity model: its queue limit counts waiting tasks, while running work is limited by worker count. Put orchestration outside the domain, use distinct domains for independent work, or reason explicitly about worker availability, task capacity, and dependency depth. More workers or capacity alone are not proof against arbitrary recursive waits. The facade does not detect dependency cycles or create fallback workers.

## Downstream integration

The shutdown order depends on who owns the lifecycle. The manual and IoC paths below are different ownership contracts; callers must use the path matching their application.

### Manual ownership / 手工管理

When application code owns `ExecutionServices` directly, first stop external producers and wait for every producer that could submit child work. Then call `ExecutionServices::shutdown()`, await `await_termination()` while Tokio remains active, and only afterward release dependent resources and the runtime. This path lets accepted work finish gracefully before its dependencies are released.

### IoC ownership / IoC 管理

When the container owns `ExecutionServices`, model each consumer as depending on the services it uses and request application graceful shutdown. The container then drives dependent consumers' graceful callbacks and final flushes before the service's graceful shutdown and termination wait. A consumer's final flush must finish while service admission is still open; manually shutting down the service before asking the container to shut down reverses this order and is not appropriate for this ownership path. The fixture's [`FlushWorker`](../tests/fixtures/ioc_application_consumer/src/flush_worker.rs) uses `Managed::asynchronous_with_graceful` to express this dependency order.

### Build rollback / 构建回滚

Immediate shutdown and construction rollback use the managed `stop()` callback. `stop()` closes admission and requests stopping, but cannot interrupt already-running synchronous work; it can also synchronously drop queued tasks on the calling thread, so its duration is not bounded. Do not infer termination from the stop call alone. In this fixture, the managed wait callback separately awaits `ExecutionServices::await_termination()`.

`build_async()` can return a failure that owns rollback cleanup; when it does, the caller must drive `failure.wait_cleanup()` after an error. `build_settled()` instead waits for owned rollback on normal error completion and returns a settled failure containing the original cause and any cleanup report. A cleanup report describes the managed cleanup callbacks and may report unsuccessful cleanup; its presence or success does not independently prove service termination unless the managed wait callback actually awaited termination. For a cancelled build session, the build future cannot drive the asynchronous wait callback after it is dropped, so the surviving session owner must call `session.wait_cancelled_cleanup()` (or take and drive its cleanup handle). Keep Tokio running while these waits need it.

The existing [ioc-integration workflow](https://github.com/qubit-ltd/rs-execution-services/blob/main/.github/workflows/ci.yml) checks a fixed baseline. The [ioc-current-integration workflow](https://github.com/qubit-ltd/rs-execution-services/blob/main/.github/workflows/ioc-current-integration.yml) checks this repository revision from the triggering event together with `rs-ioc/main`; its other sibling repositories remain pinned to fixed commits. Its daily schedule also checks combinations where only the downstream IoC repository has changed. Both workflows verify fixture contracts and do not indicate production adoption.

## Verification

The integration suite exercises the public error matrix, capacity waits for all four domains, repeated contention, dropping an unaccepted task, closure wakeups, and same-domain nested waiting. The IO, CPU, and Tokio-blocking capacity-one parent cases poll the child wait once and assert `Pending`; the CPU and Tokio-blocking cases use configurations with spare workers to prove that task capacity also constrains admission. Shutdown in the latter cases wakes the parent and lets the test verify that the child never ran. Timeouts are deadlock watchdogs, not evidence for the capacity cycle.

The application consumer and runnable resource-budget example also poll a wait future before releasing the accepted task. The local documentation consumer verifies the sibling-checkout layout and Tokio features; because it resolves this repository's `.cargo/config.toml` patches, it is not a registry-isolation check. The [packaged documentation consumer workflow](https://github.com/qubit-ltd/rs-execution-services/blob/main/.github/workflows/packaged-documentation-consumer.yml) runs two independent jobs. Both package the crate, extract the archive outside the source tree, and build and run consumers using `quick_start.rs` and the complete `daily_report.rs` read from that archive. The source-integration job checks out the latest `dev-starfish` sources of `rs-tokio-executor`, `rs-rayon-executor`, and `rs-executor`, and permits exactly those three local executor packages alongside the consumer and archive target; all other packages must have a `registry+` source. The registry-only job runs `python3 .infra/tools/check-packaged-documentation-consumer.py --registry-only`; it excludes inherited Cargo override variables, uses an isolated Cargo home and target directory, fixes the crates.io source, rejects path/git/patch inputs in the archived manifest, and allows only the consumer and extracted target crate as local packages. In both modes the extracted target must have `source=None` and its exact archive manifest path, and the consumer must have its expected name and manifest path. After initial resolution creates the consumer lockfile, the verifier checks metadata with `cargo metadata --locked` and then runs with `cargo run --locked`, so source checks and execution share that lockfile. The source-integration check allows local overrides for the three executor crates; the registry-only check fails when required registry dependencies are unavailable. Both checks are required for release acceptance. The registry-only job also checks the manifest-declared Rayon lower bound against that exact crates.io version in a separate matrix entry; this proves compatibility for that Rayon version with the other dependencies resolved in that run, not that all transitive dependencies are simultaneously at their minimum versions. Neither job establishes production adoption. The IoC integration job runs its pinned consumer, tests, and Clippy independently of the core crate's feature matrix.

The bilingual user guides describe these same error, ownership, cancellation, and capacity contracts. Runnable examples and the package file list verify that documented code and design documents are present in the crate archive.

## Non-goals

This facade does not provide a fair or FIFO wait queue, a global resource budget, automatic deadlock detection, priority scheduling, forced interruption of synchronous code, or a universal cancellable handle. Those capabilities require separate application demand and a dedicated API and lifecycle design.
