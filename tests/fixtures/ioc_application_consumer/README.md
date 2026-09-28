# IoC application consumer fixture

This standalone application demonstrates the public integration boundary among
`qubit-ioc`, `qubit-event-bus`, `qubit-fs-registry`, and
`qubit-execution-services`. It uses local path dependencies so it can validate
the checked out source trees together. Keep the four repositories as siblings
in the Qubit workspace.

From the `rs-execution-services` repository root, run:

```bash
./ioc-ci-check.sh
```

The example registers an `EventBusRegistry` with its local provider, creates an
`EventBus`, and shares it through the IoC context. It also registers an empty
`FileSystemRegistry` to show that registries are ordinary shared components;
the empty registry does not resolve filesystems. `ExecutionServices` receives a
Tokio runtime handle and executes a small IO task.

`EventBus` and `ExecutionServices` are registered as managed components. The
application drains `ExecutionServices` gracefully before calling
`ApplicationContext::begin_shutdown()` while the Tokio runtime is still alive.
The managed rollback callback uses `stop()` so a failed build does not wait for
unbounded application work. Tests verify missing dependencies, cleanup after a
later factory fails, and cleanup when an async build future is cancelled. Build
failure runs both stop and wait once; cancellation runs stop synchronously, and
the test waits for service termination outside the cancelled build.

The CI job checks out `rs-ioc` at `762660f56e425a5b1b442528a9f979a2c41a1a9d`,
`rs-event-bus` at `bf7ab432947070e6e8bbc6ffe02560eb1957ad27`, and
`rs-fs-registry` at `d3a6cacbc05bea970175db9cf6211680aa39c87c`. Those pinned
revisions are the integration compatibility baseline.

This is a downstream contract fixture, not evidence that a production
application currently uses this integration.
