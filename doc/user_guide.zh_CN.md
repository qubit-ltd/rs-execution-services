# Qubit Execution Services 用户手册

[English user guide](user_guide.md) | [中文 README](../README.zh_CN.md)

本手册面向使用 `qubit-execution-services` 0.8.0 的 Rust 应用开发者，介绍如何按任务特点选择执行域、配置受本 crate 管理的线程池、处理提交和执行结果，并在应用退出时关闭服务。本 crate 面向应用层整合；如果库只需要一种执行能力，通常直接依赖对应的底层 crate 更合适。

## 手册目标与读者

应用往往同时包含可能阻塞线程的同步调用、CPU 密集型计算和异步任务。把这些工作交给同一类线程或调度器，可能让等待中的任务占用处理其他工作的资源。本手册说明如何通过一个 `ExecutionServices` 实例把任务分派到不同执行域。

## 概念模型

`ExecutionServices` 持有四个执行域：

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
qubit-execution-services = "0.8"
tokio = { version = "1.53", features = ["rt", "time"] }
```

已有 Tokio runtime 的异步应用可以把当前 runtime 的 handle 交给 builder，再按任务类型提交工作：

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
services.await_termination().await;
# Ok(())
# }
```

`get()` 用于取得 blocking 与 CPU callable 的结果；Tokio blocking 或异步任务返回的 handle 则通过 `.await` 取得结果。每个 callable 或 future 都返回 `Result<R, E>`。提交是否成功由提交方法的返回值表示，任务执行结果由 handle 表示。

## 核心工作流

1. **创建 facade。** 调用 `ExecutionServices::builder(runtime_handle)`，按需调整由本 crate 管理的 blocking 和 CPU 线程池。`ExecutionServices::new(runtime_handle)` 使用默认 builder 配置。
2. **按任务特点路由。** 可能等待阻塞 API 的同步工作使用 `submit_blocking*`；CPU 密集型同步工作使用 `submit_cpu*`；需要接入 Tokio 的阻塞函数使用 `submit_tokio_blocking*`；异步 future 使用 `spawn_io`。
3. **选择结果观察方式。** `submit_*` runnable 方法只返回是否接收任务，不提供结果 handle。callable 方法返回可读取任务结果的 handle。需要查询状态或取消任务时，使用 `submit_tracked_*` 变体。
4. **停止接收任务并等待退出。** `shutdown()` 对所有执行域请求有序关闭。之后等待 `await_termination()` 完成，再释放仍可能被任务使用的应用资源。

提交操作本身可能失败，应在调用处处理或向上传递其 `Result`。任务被接收后仍可能以错误结束；需要确认任务结果时，应检查 handle。

## 进阶用法

### 阻塞线程池的扩展和排队

builder 将阻塞域配置委托给 `ThreadPoolBuilder`。`blocking_pool_size(n)` 同时设置核心线程数和最大线程数。若希望突发负载下增加线程，可配置有界队列并把最大线程数设得高于核心线程数：

```rust
let services = ExecutionServices::builder(tokio::runtime::Handle::current())
    .blocking_core_pool_size(4)
    .blocking_maximum_pool_size(8)
    .blocking_queue_capacity(128)
    .build()?;
```

选择 `blocking_unbounded_queue()` 后，超过核心线程处理能力的任务会继续排队；单独增大最大线程数不会触发突发扩容。其他 blocking 配置包括线程名前缀、栈大小、keep-alive 时长、是否允许核心线程超时，以及是否预启动核心线程。

### CPU 任务容量

`cpu_threads(n)` 配置 Rayon worker 数量。`cpu_task_capacity(n)` 限制已接收但尚未结束的 CPU 任务总数，其中包括正在运行和等待执行的任务。达到容量时，新提交会返回 `SubmissionError::Saturated`。已接收任务完成，或排队中的 tracked task 被取消后，容量可以重新释放。

### Tokio runtime 的归属

builder 把传入的 `tokio::runtime::Handle` 同时交给两个 Tokio 执行域。本 crate 没有为这两个域提供独立 builder；runtime 和调度器参数由创建 runtime 的应用配置。

### 有序关闭与强制停止

`shutdown()` 会拒绝新任务，并要求已接收任务按照底层服务的行为完成。`stop()` 请求强制停止并返回 `ExecutionServicesStopReport`，其中每个执行域都有一个 `StopReport`。`total_queued()`、`total_running()` 和 `total_cancelled()` 可分别汇总报告中的排队、运行和取消计数。这些计数是 stop 操作观察到的报告值。

还可以通过 `lifecycle()`、`is_running()`、`is_shutting_down()`、`is_stopping()`、`is_not_running()` 与 `is_terminated()` 查询 facade 的总体生命周期。

## 错误与诊断

- `ExecutionServicesBuilder::build()` 可能返回 `ExecutionServicesBuildError::Blocking` 或 `ExecutionServicesBuildError::Cpu`，分别表示 blocking 或 CPU builder 拒绝了配置。错误会保留底层 builder 错误作为来源。
- 提交方法通过 `SubmissionError` 报告执行域拒绝任务的情况。CPU 域达到配置容量时返回 `SubmissionError::Saturated`；开始关闭后，新任务会被拒绝。
- 任务被接收后，通过对应 handle 获取执行结果。任务自身返回的错误与提交错误是两个阶段的问题；提交成功不代表任务执行成功。
- 汇总关闭情况时，先检查 `ExecutionServicesStopReport` 的各域字段，再按需要读取总数。

## 排障

| 现象 | 检查方法 |
| --- | --- |
| `build()` 返回错误 | 根据 `Blocking` 或 `Cpu` 变体检查相应线程池配置。例如，blocking 最大线程数为零或 CPU worker 数为零都会被拒绝。 |
| CPU 提交返回 `Saturated` | 增大 `cpu_task_capacity`、减少未完成任务，或在提交前增加背压。 |
| blocking 任务持续排队，没有增加 worker | 检查队列是否为无界队列。需要弹性扩展时，改用有界 `blocking_queue_capacity` 并设置更大的 `blocking_maximum_pool_size`。 |
| 关闭时提交新任务失败 | 检查 `lifecycle()` 状态；开始关闭后停止提交。 |
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
