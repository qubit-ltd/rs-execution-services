# IoC application consumer fixture

This standalone application demonstrates the public integration boundary among
`qubit-ioc`, `qubit-event-bus`, `qubit-fs-registry`, and
`qubit-execution-services`. It uses local path dependencies so it can validate
the checked out source trees together. Keep the four repositories as siblings
in the Qubit workspace.

From the `rs-execution-services` repository root, run:

```bash
cargo run --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml
cargo test --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml
cargo clippy --manifest-path tests/fixtures/ioc_application_consumer/Cargo.toml --all-targets -- -D warnings
```

The example registers an `EventBusRegistry` with its local provider, creates an
`EventBus`, and shares it through the IoC context. It also registers an empty
`FileSystemRegistry` to show that registries are ordinary shared components;
the empty registry does not resolve filesystems. `ExecutionServices` receives a
Tokio runtime handle and executes a small IO task.

Shutdown is explicit: the application shuts down the event bus, requests
execution service shutdown, then waits for termination while the Tokio runtime
is still alive. A test also verifies that a missing `EventBusRegistry` is
reported before its factory runs.

This is a downstream contract fixture, not evidence that a production
application currently uses this integration.
