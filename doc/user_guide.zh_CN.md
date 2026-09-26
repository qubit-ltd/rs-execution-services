# Qubit Execution Services 用户手册

[English user guide](user_guide.md) | [中文 README](../README.zh_CN.md)

本手册面向使用 `qubit-execution-services` 0.9.0 的 Rust 应用开发者，介绍如何按任务特点选择执行域、配置受本 crate 管理的线程池、处理提交和执行结果，并在应用退出时关闭服务。本 crate 面向应用层整合；如果库只需要一种执行能力，通常直接依赖对应的底层 crate 更合适。

## 手册目标与读者

应用往往同时包含可能阻塞线程的同步调用、CPU 密集型计算和异步任务。把这些工作交给同一类线程或调度器，可能让等待中的任务占用处理其他工作的资源。本手册说明如何通过一个 `ExecutionServices` 实例把任务分派到不同执行域。

## 概念模型

`ExecutionServices` 可持有以下四个执行域中的任意非空子集：

| 执行域 | 适用工作 | 实现与配置边界 |
| --- | --- | --- |
| `blocking` | 可能阻塞 OS 线程的同步工作 | 使用 `qubit-thread-pool` 的 `ThreadPool`；线程池和队列由本 crate 的 builder 配置。 |
| `cpu` | CPU 密集型同步工作 | 使用 Rayon-backed `RayonExecutorService`；本 crate 可限制已接收但未完成的任务数。 |
| `tokio_blocking` | Tokio 应用中的阻塞函数 | 通过 Tokio `spawn_blocking` 执行；runtime 与 blocking pool 的参数由应用负责。 |
| `io` | `Future` 任务 | 交给 Tokio async scheduler；future 的输出类型为 `Result<R, E>`。 |

这四个域各自调度任务，并不组成单一调度器。同步代码可能等待外部操作时，优先考虑 `blocking`；需要占用处理器进行计算时，使用 `cpu`。`spawn_io` 接受合适的异步 future，并不要求 future 一定在访问文件或网络。

## 贯穿场景：分发三类任务

假设应用在一次请求处理中要执行同步阻塞操作、CPU 计算和异步工作。目标是让每类任务进入合适的执行域、能够取得结果，并在不再接受新工作后统一关闭服务。

下面用简单计算展示调用路径和预期结果。接入应用时，可替换闭包内部工作，但应根据阻塞与计算特征选择执行域。

## 安装与最小配置

本 crate 要求 Rust 1.94 或更新版本。在应用中添加 crate 和 Tokio 依赖；创建 facade 前需要先有可用的 Tokio runtime：

```toml
[dependencies]
qubit-execution-services = "0.9"
tokio = { version = "1.53", features = ["rt", "time"] }
```

从源码开发本仓库时，先在 crate 根目录运行 `./.infra/tools/prepare-local-path-dependencies.sh`。该脚本会准备开发清单所用的相邻 `rs-thread-pool`、`rs-rayon-executor` 和 `rs-tokio-executor` 源码目录。应用依赖已发布的 crate 时不需要这些同级仓库；发布本 crate 前，registry 中必须已有清单所声明的 `qubit-thread-pool` 版本。

只启用应用需要的执行域。下面的例子选择 blocking、CPU 和 IO；Tokio blocking 未启用：

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
        let services = ExecutionServices::builder()
            .runtime(runtime.handle().clone())
            .enable_blocking()
            .enable_cpu()
            .enable_io()
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
        services.await_termination().await;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
```

`get()` 用于取得 blocking 与 CPU callable 的结果；Tokio blocking 或异步任务返回的 handle 则通过 `.await` 取得结果。每个 callable 或 future 都返回 `Result<R, E>`。提交是否成功由提交方法的返回值表示，任务执行结果由 handle 表示。

## 核心工作流

1. **创建 facade。** 从 `ExecutionServices::builder()` 开始，显式启用所需执行域；仅启用 Tokio 域时才调用 `runtime(handle)`。`ExecutionServices::new(runtime_handle)` 仍是启用四域的快捷构造。
2. **按任务特点路由。** 可能等待阻塞 API 的同步工作使用 `submit_blocking*`；CPU 密集型同步工作使用 `submit_cpu*`；需要接入 Tokio 的阻塞函数使用 `submit_tokio_blocking*`；异步 future 使用 `spawn_io`。
3. **选择结果观察方式。** `submit_*` runnable 方法只返回是否接收任务，不提供结果 handle。callable 方法返回可读取任务结果的 handle。需要查询状态或取消任务时，使用 `submit_tracked_*` 变体。
4. **停止接收任务并等待退出。** `shutdown()` 对已启用执行域请求有序关闭。之后等待 `await_termination()` 完成，再释放仍可能被任务使用的应用资源。所有已启用域终止后，该方法返回 `()`。

提交操作本身可能失败，应在调用处处理或向上传递其 `Result`。任务被接收后仍可能以错误结束；需要确认任务结果时，应检查 handle。

## 进阶用法

### 阻塞线程池的扩展和排队

blocking 队列默认最多容纳 1024 个等待任务；运行中的任务不计入此队列容量。有界队列满后，如果当前 worker 数尚未达到 `blocking_maximum_pool_size`，线程池还可以启动 worker；无法继续增加 worker 时，新的 blocking 提交才会因容量已满而返回 `SubmissionError::Saturated`。可通过 `blocking_queue_capacity(n)` 选择其他有限容量，也可以显式调用 `blocking_unbounded_queue()` 使用无界队列。应根据预期任务大小和提交速率选择容量；队列容量不会限制任务内存。

默认情况下，blocking 域的核心线程数和最大线程数都等于检测到的 CPU 并行度。长时间阻塞的调用可能占满所有 worker。队列尚有空间时，线程池不会扩容，因此该默认值限制并发数，不会随积压自动调整。应根据预期同时阻塞任务数和可接受积压量配置 `blocking_core_pool_size`、`blocking_maximum_pool_size` 和有限的 `blocking_queue_capacity`。有界队列满后，线程池可在达到最大线程数前增加 worker；无界队列会在核心线程数达到后继续排队，不会触发突发扩容。

builder 将阻塞域配置委托给 `ThreadPoolBuilder`。`blocking_pool_size(n)` 同时设置核心线程数和最大线程数。若希望突发负载下增加线程，可配置有界队列并把最大线程数设得高于核心线程数：

```rust
let services = ExecutionServices::builder()
    .enable_blocking()
    .blocking_core_pool_size(4)
    .blocking_maximum_pool_size(8)
    .blocking_queue_capacity(128)
    .build()?;
```

纯 blocking 和 CPU 应用无需提供 Tokio runtime：

```rust
let services = ExecutionServices::builder()
    .enable_blocking()
    .enable_cpu()
    .build()?;
```

纯 IO 应用只需提供 runtime 并启用 IO 域：

```rust
let services = ExecutionServices::builder()
    .runtime(tokio::runtime::Handle::current())
    .enable_io()
    .build()?;
```

选择 `blocking_unbounded_queue()` 后，超过核心线程处理能力的任务会继续排队；单独增大最大线程数不会触发突发扩容。其他 blocking 配置包括线程名前缀、栈大小、keep-alive 时长、是否允许核心线程超时，以及是否预启动核心线程。

### CPU 任务容量

`cpu_threads(n)` 配置 Rayon worker 数量。`cpu_task_capacity(n)` 限制已接收但尚未结束的 CPU 任务总数，其中包括正在运行和等待执行的任务。达到容量时，新提交会返回 `SubmissionError::Saturated`。已接收任务完成，或排队中的 tracked task 被取消后，容量可以重新释放。

### Tokio 任务容量

Tokio 阻塞域和 IO 域默认各自最多接收 1024 个尚未完成的任务。计数包括排队和运行中的工作；IO 域还包括已被 Tokio 接收但尚未 poll 的 future。达到容量时，提交返回 `SubmissionError::Saturated`。可在 facade builder 上通过 `tokio_blocking_task_capacity(NonZeroUsize)` 或 `io_task_capacity(NonZeroUsize)` 设置其他有限容量。任务完成后会释放容量。取消排队中的阻塞任务或 abort IO 任务后，底层任务被释放时会归还容量；已经开始运行的阻塞闭包无法被强制停止，会一直占用容量直到返回。

### Tokio runtime 的归属

builder 把传入的 `tokio::runtime::Handle` 交给已启用的 Tokio 执行域，并配置其任务准入容量。只启用 blocking 或 CPU 时不需要 Tokio handle。runtime 和调度器参数由创建 runtime 的应用配置。提交到未启用域会返回 `ExecutionServicesSubmissionError::DomainDisabled`；底层域拒绝任务时会返回 `ExecutionServicesSubmissionError::Rejected`。

### 有序关闭与强制停止

`shutdown()` 和 `stop()` 首先记录 facade 关闭准入的意图，然后只对已启用域请求对应操作。已经通过准入检查的提交可能与逐域 shutdown 或 stop 重叠，其接收或拒绝由对应底层执行域决定。任一操作返回后，已启用域都拒绝新的 facade 提交。`shutdown()` 要求已接收任务按照底层服务的行为完成；`stop()` 请求强制停止并返回 `ExecutionServicesStopReport`：启用域字段为 `Some(StopReport)`，未启用域字段为 `None`。报告不提供跨域总数，各域依次取样。Tokio IO 的 `running` 字段表示 stop 时已接收但尚未完成的 future，不代表该时刻正在 poll 的 future。应按各域的停止契约解释计数。

`await_termination()` 通过异步通知等待已启用执行域，不占用 Tokio blocking 线程。它可以由异步执行器轮询；builder 收到的 Tokio runtime 仍须持续运行，直到其中已接收的任务结束。在另一个 runtime 中 await 不会驱动已经停止运行的 current-thread runtime。丢弃等待 future 会释放该次等待的资源，不会停止服务。调用 shutdown 或 stop 之前，等待会保持未完成。

还可以通过 `lifecycle()`、`is_running()`、`is_shutting_down()`、`is_stopping()`、`is_not_running()` 与 `is_terminated()` 查询 facade 的总体生命周期。

应用需要由一个所有者统一提交多个执行域的任务并协调关闭时，可以使用此 facade。组件只需要一个执行域或该域的专有控制能力时，可以直接依赖对应的 executor crate。在当前 `rust-common` 检出目录中，`rs-task` 通过 Tokio 运行本地引擎，`rs-event-bus` 则由调用方驱动 future；它们都不是本 facade 的生产消费者。应用消费者 fixture 能验证公开 API 边界，但不能证明已有生产采用。

## 错误与诊断

- `ExecutionServicesBuilder::build()` 在没有启用域时返回 `NoDomains`；启用 Tokio 域却未设置 runtime 时返回 `MissingTokioRuntime`；启用的 blocking 或 CPU builder 配置无效时返回对应错误。
- 提交未启用域返回 `ExecutionServicesSubmissionError::DomainDisabled`。facade 关闭或已启用域拒绝任务时返回 `ExecutionServicesSubmissionError::Rejected`，其中保留底层 `SubmissionError`，例如 `Shutdown` 或 `Saturated`。
- 任务被接收后，通过对应 handle 获取执行结果。任务自身返回的错误与提交错误是两个阶段的问题；提交成功不代表任务执行成功。
- 检查关闭结果时，读取 `ExecutionServicesStopReport` 中各启用域的 `Option<StopReport>`；该类型不提供跨域总数。

## 排障

| 现象 | 检查方法 |
| --- | --- |
| `build()` 返回错误 | 根据 `Blocking` 或 `Cpu` 变体检查相应线程池配置。例如，blocking 最大线程数为零或 CPU worker 数为零都会被拒绝。 |
| CPU 或 Tokio 提交返回 `Saturated` | 增大对应任务容量、减少未完成任务，或在提交前增加背压。 |
| blocking 任务持续排队，没有增加 worker | 检查队列是否为无界队列。需要弹性扩展时，改用有界 `blocking_queue_capacity` 并设置更大的 `blocking_maximum_pool_size`。 |
| 关闭时提交新任务失败 | 检查 `lifecycle()` 状态；记录 shutdown 或 stop 意图后，新提交会被拒绝。已经通过准入检查的提交可能与逐域关闭重叠。 |
| `await_termination()` 一直未完成 | 检查已接收的 blocking 任务是否仍在运行或等待外部条件；有序关闭会等待底层服务终止。 |

## 限制与最佳实践

- facade 创建并持有 blocking 和 CPU 服务；Tokio runtime 的生命周期及调度设置仍归应用管理。使用 Tokio 执行域期间，应保持对应 runtime 可用。
- 若需要限制等待中的 blocking 工作量，请使用有界队列。无界队列没有配置队列容量上限。
- CPU 容量计算所有尚未完成的已接收任务，而非仅计算排队任务。设置容量时应同时考虑运行中和排队中的任务。
- 有序关闭会等待已接收任务完成。确实需要强制停止时使用 `stop()`，并结合报告和任务 handle 检查取消结果。
- 本 crate 负责分发任务和汇总生命周期，不提供应用级重试、优先级策略或 Tokio runtime 配置。

## 延伸阅读

- [中文 README](../README.zh_CN.md) 与 [English README](../README.md)
- [API 文档](https://docs.rs/qubit-execution-services)
- [English user guide](user_guide.md)
