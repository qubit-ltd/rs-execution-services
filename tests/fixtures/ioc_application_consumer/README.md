# IoC application consumer fixture

This standalone application exercises the public integration boundary among
`qubit-ioc` 0.3, `qubit-event-bus` 0.18, `qubit-fs-registry`, `qubit-fs-local`,
`qubit-fs`, and `qubit-execution-services`. It resolves unpublished IoC and
EventBus versions through sibling path dependencies. The Fs and FsRegistry
paths and crates.io patches keep transitive and direct types identical. Keep
the repositories as siblings in the Qubit workspace.

From the `rs-execution-services` root, run `./ioc-ci-check.sh`. To run this
fixture directly:

```bash
cargo +1.94.0 run --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml --locked
cargo +1.94.0 test --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml --locked
```

`build_application` creates an IoC `Application` with a bounded `WaitPolicy`
and a real local `EventBus`. It registers `EventBusRegistry`,
`ExecutionServices`, and a `FlushWorker` that depends on both resources. The
worker owns a typed subscription. It also creates `report.csv` under a
caller-owned temporary root, registers a rooted `LocalFileSystemProvider`, and
makes it available through `FileSystemRegistry`. The resource test resolves
`file:///report.csv` and checks the real provider's canonical URI and
`stat` length of 22 bytes.

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

The executable also completes the error path: if construction returns a
`BuildFailure`, it separates the original cause from its cleanup handle and
explicitly waits for cleanup before returning the cause. If business work
fails after construction, it requests `Immediate` and waits for the shutdown
report before returning the business error. A shutdown failure prints the
report. The error paths do not claim a final flush.

Managed `ExecutionServices` uses `shutdown()` for a graceful request,
`stop()` for Immediate/rollback, and `await_termination()` for its owned wait.
The EventBus adapter calls nonblocking `request_shutdown`: its shared ticket
slot preserves a graceful ticket when Immediate strengthens that shutdown,
and its wait future calls `ticket.wait_async()` after releasing the slot lock.
The synchronous `EventBus::shutdown`, including Immediate, waits for handler,
worker, and provider completion, so it cannot serve as an IoC abort callback.
Cancelling an observation does not resend the request or cancel background bus
shutdown. Neither mode can forcibly terminate blocked synchronous user code.

The external tests cover real graceful flush, real rooted Fs resolution,
Immediate cancellation of a running IO task, missing dependencies, explicit
build-failure cleanup, cancelled async build, blocked EventBus handler with
nonblocking requests and a resumed wait, plus injected grace and termination
budgets. A report with `incomplete` entries means termination was not
confirmed, not that resources were killed. After a termination timeout,
shutdown continues to later dependencies; an unfinished consumer may lose
those dependencies. The tests use bounded guards to detect hangs; a timeout
cannot interrupt arbitrary blocking code.

The historical lane under `rs-ioc/tests/fixtures/application_consumer` uses
its original pinned EventBus 0.15 source and only four migrated public-API
regressions. It does not cover this current fixture's EventBus request/ticket
adapter, graceful final flush, rooted Fs metadata, or deadline behavior.
Final external current-lane IoC, EventBus, and consumer source SHA pins are
pending the real versioned commits in T11. Local path checkouts and working
source hashes do not prove that an unpublished commit contains these changes.
This is a downstream contract fixture, not evidence of production adoption.
