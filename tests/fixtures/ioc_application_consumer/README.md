# IoC application consumer fixture

This standalone application exercises the public integration boundary among
`qubit-ioc` 0.3, `qubit-event-bus` 0.20, `qubit-fs-registry`, `qubit-fs-local`,
`qubit-fs`, and `qubit-execution-services`. It resolves unpublished IoC and
EventBus versions through sibling path dependencies. The Fs and FsRegistry
paths and crates.io patches keep transitive and direct types identical. Keep
the repositories as siblings in the Qubit workspace.

From the `rs-execution-services` root, run the same commands as CI:

```bash
cargo +1.94.0 check --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml --locked
cargo +1.94.0 test --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml --locked
cargo +1.94.0 run --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml --locked
cargo +1.94.0 clippy --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml --all-targets --locked -- -D warnings
```

`build_application` creates an IoC `Application` with a bounded `WaitPolicy`
and a real local `EventBus`. The example uses `bounded_with_total` with a 90
second application budget in addition to its per-component budgets; this is a
fixture value, not a universal production recommendation. It registers `EventBusRegistry`,
`ExecutionServices`, and a `FlushWorker` that depends on both resources. The
worker owns a typed subscription. After the selected roots' dependency graph
has been validated, the selected `FileSystemRegistry` factory creates
`report.csv` under
the caller-owned temporary root, registers a rooted `LocalFileSystemProvider`,
and returns the registry. The resource test resolves `file:///report.csv` and
checks the real provider's canonical URI and `stat` length of 22 bytes.

The normal executable submits a real IO task returning 43, then starts
`Application::begin_shutdown(ShutdownMode::Graceful)` while its Tokio runtime
and temporary directory remain alive. `FlushWorker` requests graceful stop
first. Its owned wait uses `ExecutionServices::spawn_io` to publish
`final-report:22` through the EventBus, awaits the task result 22 and the real
handler acknowledgement, then finishes. Dependency order lets the worker
flush while ExecutionServices and EventBus still admit work. The executable
checks both results, the final message, successful shutdown report, and
terminated services. Queries cloned from `application.context()` may remain
alive after the unique application owner starts shutdown.

`build_application` returns `ApplicationBuildError`, preserving construction
failures as the `Build` variant. The executable matches that variant directly,
calls `wait_cleanup(&mut self)` to await optional cleanup, and keeps the
original `BuildFailure` until cleanup has completed. A cancelled wait can be
resumed on the same failure. It checks the report, including cleanup failures,
before returning the original cause. If business work
fails after construction, it requests `Immediate` and waits for the shutdown
report before returning the business error. A shutdown failure prints the
report. The error paths do not claim a final flush.

The lifecycle integration tests register their `ExecutionServices` consumers
with `register_injected_async_factory::<u8, (Arc<ExecutionServices>,), _>`.
This exercises `FactoryArgs` dependency derivation across the fixture's public
crate boundary while keeping the same build-failure cleanup and cancellation
assertions. This fixture is a downstream contract test; it does not claim that
production services have adopted this registration style.

Managed `ExecutionServices` uses `shutdown()` for a graceful request,
`stop()` for Immediate/rollback, and `await_termination()` for its owned wait.
The EventBus adapter uses
`Managed::asynchronous_with_graceful_ticket` to call nonblocking
`request_shutdown` for both modes; the IoC adapter owns the ticket slot. The
wait future calls `ticket.wait_async()` for the selected ticket. Its Drop must
not cancel the bus shutdown, since an unused ticket may be discarded on
Immediate upgrade.
The `FlushWorker` also uses an asynchronous managed adapter: its wait owns the
final task and handler acknowledgement. All three actual resources provide a
wait callback because returning from their stop request does not confirm that
termination or the final flush has completed. Keep the Tokio runtime alive
until these waits finish, including when awaiting build-failure cleanup.
The synchronous `EventBus::shutdown`, including Immediate, waits for handler,
worker, and provider completion, so it cannot serve as an IoC abort callback.
Cancelling an observation does not resend the request or cancel background bus
shutdown. Neither mode can forcibly terminate blocked synchronous user code.

The external tests cover real graceful flush, real rooted Fs resolution,
Immediate cancellation of a running IO task, missing dependencies, explicit
build-failure cleanup, cancelled async build, blocked EventBus handler with
nonblocking requests and a resumed wait, plus injected grace and termination
budgets and an application-wide timeout. The total timer starts on the first
poll of `wait()`, survives cancellation, and is reported via
`ShutdownReport::overall_failure()`. A report with `incomplete` entries means termination was not
confirmed, not that resources were killed. After a termination timeout,
shutdown continues to later dependencies; an unfinished consumer may lose
those dependencies. The tests use bounded guards to detect hangs; a timeout
cannot interrupt arbitrary blocking code.

The EventBus adapter is fixture-specific. Its request callbacks must return
without blocking. Cancelling a borrowed `ShutdownHandle::wait()` future
preserves the already started ticket wait. An Immediate upgrade requests
stronger shutdown without discarding an already observed ticket; before wait
starts, its ticket replaces the pending graceful ticket. Keep the Tokio
runtime alive until shutdown finishes; neither IoC nor the adapter can interrupt
blocking synchronous callbacks.

The historical lane under `rs-ioc/tests/fixtures/application_consumer` now
pins EventBus 0.20 in its manifest, lockfile, and CI checkout, while retaining
its historical fixture source and four public-API regressions. It does not
cover this current fixture's EventBus request/ticket
adapter, graceful final flush, rooted Fs metadata, or deadline behavior.
The external current-lane IoC, EventBus, and consumer source SHA pins are
maintained in `rs-ioc/.github/workflows/downstream-contracts.yml`. Local path
checkouts and working source hashes do not prove remote CI coverage.
This is a downstream contract fixture, not evidence of production adoption.
