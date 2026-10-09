# Qubit Execution Services 用户手册

[中文 README](../README.zh_CN.md) · [English user guide](user_guide.md) · [API 文档](https://docs.rs/qubit-execution-services)

本文适用于 `qubit-execution-services` 0.10.1，要求 Rust 1.94 或更高版本。它面向需要在同一应用中安排同步阻塞工作、CPU 计算和异步任务的开发者。读到[检查报表结果](#检查报表结果)，即可完成一次业务任务；容量、监控和停机可在接入后按需查阅。只需要一种执行能力的组件，可以直接使用对应的底层 executor crate。

## 它解决什么问题

以每日结算报表为例：应用要从只提供同步接口的旧系统读取交易金额，计算总额，再通过异步接口保存报表。旧系统调用会阻塞线程，金额汇总主要消耗 CPU，保存操作要等待异步 I/O。把同步调用放在 Tokio 工作线程上，会阻碍该线程调度其他 future；把计算也交给等待旧系统的线程池，会让两类工作争用同一资源。

`ExecutionServices` 让应用按工作特点提交任务，并集中管理这些执行域的关闭。它不会替应用实现旧系统客户端、报表存储、业务重试或跨步骤事务。任务提交成功只表示对应域接纳了任务；报表是否保存成功，还要看任务结果及存储接口的返回契约。

## 从哪里开始

1. [接入结算报表任务](#接入结算报表任务)给出依赖、可运行的入门例子和实际项目中的职责划分。
2. [检查报表结果](#检查报表结果)区分提交、执行和业务完成三个阶段。
3. 按需查阅[选择执行域与核算资源](#选择执行域与核算资源)、[进阶用法](#进阶用法)、[应用关闭顺序](#应用关闭顺序)和[排障](#排障)。

## 接入结算报表任务

在应用中添加 crate 和 Tokio 依赖；启用 Tokio 执行域前需要先有可用的 runtime：

```toml
[dependencies]
qubit-execution-services = "0.10.1"
tokio = { version = "1.53", features = ["rt", "time"] }
```

先运行仓库的[入门示例](../examples/quick_start.rs)，确认环境能创建执行域、提交任务并等待结果。它只用计算演示调用路径，随后再接入真实业务：

```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates submitting and awaiting work in separate execution domains.

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

        assert_eq!(blocking.await?, 42);
        assert_eq!(cpu.await?, 55);
        assert_eq!(io.await?, 42);

        services.shutdown();
        services.await_termination().await;
        Ok::<(), Box<dyn std::error::Error>>(())
    })?;

    Ok(())
}
```

在仓库目录运行 `cargo run --example quick_start`；成功时程序没有输出，三个断言通过并正常退出。这段代码是入门练习，不代表真实业务。通过 `.await` 等待任务句柄，不会阻塞当前 Tokio 工作线程；`TaskHandle::get()` 会阻塞调用线程，只适合同步上下文。

### 在应用启动时创建服务

报表应用需要 `blocking` 运行旧系统的同步读取，`cpu` 汇总金额，`io` 保存报表。实际应用已有 Tokio runtime 时，使用它的 `Handle`；应用持有 `ExecutionServices`，把引用或 `Arc<ExecutionServices>` 注入报表模块：

```rust
use qubit_execution_services::ExecutionServices;

let services = ExecutionServices::builder()
    .runtime(tokio::runtime::Handle::current())
    .enable_blocking()
    .blocking_pool_size(4)
    .blocking_queue_capacity(32)
    .enable_cpu()
    .cpu_threads(2)
    .cpu_task_capacity(32)
    .enable_io()
    .build()?;
// 将 services 交给报表模块；应用关闭时仍持有它。
```

这些数值仅示范配置位置，需根据应用负载调整。`runtime()` 传入的 Tokio runtime 由应用创建和管理。只启用 blocking 或 CPU 时不需要 Tokio handle；启用 `io` 或 `tokio_blocking` 时必须提供。至少启用一个域后 `build()` 才能成功。

### 读取、汇总并保存一天的报表

下面是放进报表模块的集成片段。`LegacyLedger` 和 `ReportStore` 是**应用自行实现**的接口：前者封装旧系统的同步调用，后者封装异步存储。接口应在实际业务操作完成后返回可判断成败的结果。`Vec<u64>` 在这里表示以分为单位的金额；生产应用还需自行定义交易范围、货币和数据一致性规则。

```rust
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use qubit_execution_services::ExecutionServices;

pub trait LegacyLedger: Send + Sync {
    fn amounts_for_day(&self, day: &str) -> Result<Vec<u64>, io::Error>;
}

pub trait ReportStore: Send + Sync {
    fn save<'a>(
        &'a self,
        day: &'a str,
        total_cents: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send + 'a>>;
}

pub async fn make_daily_report(
    services: &ExecutionServices,
    ledger: Arc<dyn LegacyLedger>,
    store: Arc<dyn ReportStore>,
    day: String,
) -> Result<u64, Box<dyn std::error::Error>> {
    let read_day = day.clone();
    let read = services.submit_blocking_callable(move || ledger.amounts_for_day(&read_day))?;
    let amounts = read.await?;

    let calculate = services.submit_cpu_callable(move || {
        amounts.iter().copied().try_fold(0_u64, |sum, cents| {
            sum.checked_add(cents)
                .ok_or_else(|| io::Error::other("报表金额溢出"))
        })
    })?;
    let total_cents = calculate.await?;

    let save = services.spawn_io(async move {
        store.save(&day, total_cents).await?;
        Ok::<u64, io::Error>(total_cents)
    })?;
    Ok(save.await?)
}
```

报表入口调用 `make_daily_report` 后，成功返回当天总金额（分）；存储接口也已返回成功。读取和汇总分别在独立 worker 上执行，保存的 future 由 Tokio 调度。这里顺序等待每步，因为下一步依赖上一步。闭包和 future 都返回 `Result<R, E>`，捕获的对象须满足对应方法的 `Send` 和生命周期约束。报表应用可随后增加自己的日志、指标和请求响应映射。

### 完整可运行程序

可运行的[每日报表示例](../examples/daily_report.rs)用本地适配函数模拟旧账本和异步存储。金额格式错误或累计溢出会作为任务错误返回；真实应用可以替换为自己的依赖，保留执行域选择和关闭顺序。

```rust
// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates reading, calculating, and saving a daily report across domains.

use std::io;
use qubit_execution_services::ExecutionServices;
use tokio::runtime::Handle;

struct DailyReport {
    day: String,
    total_cents: u64,
}

fn read_legacy_ledger() -> io::Result<String> {
    Ok(String::from("120\n230\n"))
}

fn calculate_daily_report(day: String, entries: String) -> io::Result<DailyReport> {
    let total_cents = entries.lines().try_fold(0_u64, |total, entry| {
        let cents = entry.parse::<u64>().map_err(io::Error::other)?;
        total
            .checked_add(cents)
            .ok_or_else(|| io::Error::other("daily total overflow"))
    })?;
    Ok(DailyReport { day, total_cents })
}

async fn save_report(report: DailyReport) -> io::Result<u64> {
    if report.day.is_empty() {
        return Err(io::Error::other("report day must not be empty"));
    }
    Ok(report.total_cents)
}

async fn run_report(runtime: Handle) -> Result<u64, Box<dyn std::error::Error>> {
    let services = ExecutionServices::builder()
        .runtime(runtime)
        .enable_blocking()
        .enable_cpu()
        .enable_io()
        .blocking_pool_size(1)
        .cpu_threads(1)
        .build()?;

    let ledger = services.submit_blocking_callable(read_legacy_ledger)?.await?;
    let report = services
        .submit_cpu_callable(move || calculate_daily_report(String::from("2026-09-28"), ledger.clone()))?
        .await?;
    let saved = services.spawn_io(async move { save_report(report).await })?.await?;
    services.shutdown();
    services.await_termination().await;
    Ok(saved)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let total_cents = runtime.block_on(run_report(runtime.handle().clone()))?;
    assert_eq!(total_cents, 350);
    Ok(())
}
```

## 检查报表结果

| 阶段 | 观察方式 | 成功意味着什么 |
| --- | --- | --- |
| 构建 | `build()?` | 所选域和配置有效；不代表业务任务已运行。 |
| 提交 | `submit_*_callable(...)` 或 `spawn_io(...)` 返回 `Ok(handle)` | 对应域接纳任务；任务可能仍在排队。 |
| 执行 | `handle.await` 返回 `Ok(value)` | 闭包或 future 成功返回；业务持久化是否完成取决于应用接口契约。 |

报表失败时先区分提交失败与执行失败。提交失败表示这一步没有被执行域接纳；执行失败时，操作可能已产生部分外部副作用，须按旧系统和存储的实际语义补偿。若读取成功而保存失败，重试整个函数会重新读取，源数据在此期间可能变化；应用需决定是否使用幂等键、输入快照或覆盖策略。本 crate 不提供跨步骤事务和自动重试。

只需确认接纳、不需取得执行结果时，可调用 `submit_blocking`、`submit_cpu` 或 `submit_tokio_blocking` 等 runnable 方法；它们返回的 `Ok(())` 不能证明工作已完成。需要读取结果时用 callable；需要查询状态或取消时用 `submit_tracked_*` 变体。后面[错误与诊断](#错误与诊断)列出了构建与提交错误。

## 选择执行域与核算资源

报表例子使用了三个域；如果其他同步函数必须运行在应用的 Tokio 共享阻塞池上，可启用第四个域 `tokio_blocking`。根据操作是否阻塞、是否主要消耗 CPU 来选域；`spawn_io` 接受合适的异步 future，不限于文件或网络操作。

| 工作负载 | 执行域 | 执行资源 | 限制值控制什么 |
| --- | --- | --- | --- |
| 不依赖 Tokio 的同步文件操作或阻塞 SDK 调用 | `blocking` | 独立 `ThreadPool` | 线程池大小限制运行中的任务；队列容量限制等待任务。 |
| 图像编码等 CPU 密集型同步工作 | `cpu` | 独立 Rayon 线程池 | 任务容量统计所有已接收但未完成的任务，包括排队和运行中的任务。 |
| 必须使用应用 Tokio 阻塞池的阻塞函数 | `tokio_blocking` | Tokio 共享的 `spawn_blocking` 线程池 | facade 容量限制本服务已接收但未完成的任务；Tokio `max_blocking_threads` 限制 runtime 共享 worker 数。 |
| 异步 socket、HTTP 或其他 `Future` 工作 | `io` | 应用的 Tokio runtime | IO 容量限制已接收但未完成的 future，包括尚未 poll 的 future。 |

会等待外部操作的同步 API 使用 `blocking`；闭包主要消耗处理器时间时使用 `cpu`。当阻塞操作应运行在应用的 Tokio runtime 上时才选 `tokio_blocking`；它的 worker 与同一 runtime 上的其他 `spawn_blocking` 使用者共享。异步 future 使用 `io`。容量表示任务数或队列上限，不自动等于线程、连接、速率或内存限制。

核算资源预算时，逐项列出启用域并分别计算：

1. 根据 blocking 最大线程数和 CPU 线程数计算独立 worker。两者默认值分别基于 `available_parallelism()`；同时启用会创建两组独立线程池。
2. 加上应用配置的 Tokio async worker 和 `max_blocking_threads`。这两项由应用管理，阻塞线程池还与同一 runtime 的其他用户共享。
3. 用 `blocking_queue_capacity` 限制等待中的 blocking 任务；为 CPU、Tokio blocking 和 IO 设置已接收但尚未完成的任务容量。这些容量默认各为 1024，但不限制任务占用内存。
4. 决定生产者在有限容量已满时如何处理。提交可能返回 `SubmissionError::Saturated`；可在生产者处施加背压或减少未完成工作后再重试。

使用以下指标按域调整容量。应在有代表性的负载下记录峰值和饱和情况；表中的指标用于测量，不是通用容量建议。

| 执行域 | 配置上限 | 观测指标 | 饱和时的处理 |
| --- | --- | --- | --- |
| `blocking` | 核心/最大 worker 数和队列容量 | 队列深度、活动 worker、提交与端到端延迟 | 在生产者施加背压或减少未完成工作；根据测量调整队列和 worker 上限。 |
| `cpu` | Rayon worker 数和未完成任务容量 | 未完成任务、完成延迟、饱和次数 | 施加背压或减少在途 CPU 任务。 |
| `tokio_blocking` | facade 未完成任务容量与 runtime `max_blocking_threads` | facade 任务数、runtime 共享池使用量和竞争使用者 | 计入该 runtime 的所有使用者，分别调整上限或减少提交。 |
| `io` | 未完成 future 容量 | 已接收未完成 future、保留内存、延迟和饱和次数 | 施加背压或减少在途 future。 |

针对应用负载记录峰值到达速率、排队延迟、任务服务时长和 `Saturated` 次数。`snapshot()` 对各域独立采样，不能提供原子的进程级预算，也不能替代提交结果。

容量应由测量结果确定：记录峰值到达速率、任务服务时长和应用可接受的最大排队延迟；用到达速率乘服务时长估算各域的在途需求，再增加有依据的突发余量。blocking worker 数应覆盖需要同时运行的阻塞调用，有限队列另行限制允许的积压。CPU 容量从可用并行度出发，并限制已接纳任务以控制排队内存和延迟。Tokio blocking 的 facade 任务容量应与 runtime 共享的 `max_blocking_threads` 分别配置，并计入同一 runtime 的其他使用者。IO 容量则按并发 future 数及其保留内存估算。通过负载测试检查延迟、`snapshot()` 各域观测值和 `Saturated` 返回，再据实调整；快照只是独立采样，不能预留准入名额。

[资源预算示例](../examples/resource_budget.rs)使用演示数值：runtime 共享 Tokio 阻塞池最多四个线程、独立 blocking 池最多四个 worker（核心线程数为 2）、Rayon 池两个 worker、blocking 队列 32 个任务、CPU 未完成任务 32 个、Tokio blocking 未完成任务 8 个、IO 未完成 future 1 个。这些限制作用于不同资源，不构成通用推荐。

## 进阶用法

### 阻塞线程池的扩展和排队

blocking 队列默认最多容纳 1024 个等待任务；运行中的任务不计入此队列容量。有界队列满后，如果当前 worker 数尚未达到 `blocking_maximum_pool_size`，线程池还可以启动 worker；无法继续增加 worker 时，新的 blocking 提交才会因容量已满而返回 `SubmissionError::Saturated`。可通过 `blocking_queue_capacity(n)` 选择其他有限容量，也可以显式调用 `blocking_unbounded_queue()` 使用无界队列。应根据预期任务大小和提交速率选择容量；队列容量不会限制任务内存。

默认情况下，blocking 域的核心线程数和最大线程数都等于检测到的 CPU 并行度。长时间阻塞的调用可能占满所有 worker。队列尚有空间时，线程池不会扩容，因此该默认值限制并发数，不会随积压自动调整。应根据预期同时阻塞任务数和可接受积压量配置 `blocking_core_pool_size`、`blocking_maximum_pool_size` 和有限的 `blocking_queue_capacity`。有界队列满后，线程池可在达到最大线程数前增加 worker；无界队列会在核心线程数达到后继续排队，不会触发突发扩容。

CPU 池与 blocking 池相互独立，同时启用会创建两组 worker；Tokio 还会按应用的 runtime 设置使用调度线程和阻塞线程。前述单域默认值不是进程总线程数或内存预算。

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

这里有三个不同的限制：`tokio_blocking_task_capacity` 统计已接收但尚未完成的阻塞任务，无论它们处于排队还是运行状态；应用设置的 Tokio `max_blocking_threads` 限制的是该 runtime 的共享阻塞线程池，同一 runtime 上其他 `spawn_blocking` 调用也使用它，facade 不会预留其中的线程，也不配置专属的运行并发数；实际同时运行数还取决于共享线程池和其他使用者；`io_task_capacity` 统计已接收但尚未完成的 future，包括尚未被 poll 的 future。任务容量不会报告某一时刻实际正在运行或 poll 的任务数。

Tokio 阻塞域和 IO 域默认各自最多接收 1024 个尚未完成的任务。达到容量时，提交返回 `SubmissionError::Saturated`。可在 facade builder 上通过 `tokio_blocking_task_capacity(NonZeroUsize)` 或 `io_task_capacity(NonZeroUsize)` 设置其他有限容量。任务完成后会释放容量。取消排队中的阻塞任务或 abort IO 任务后，底层任务被释放时会归还容量；已经开始运行的阻塞闭包无法被强制停止，会一直占用容量直到返回。

如需查看同时配置池大小与任务容量的完整示例，请参阅可运行的[资源预算示例](../examples/resource_budget.rs)。示例数值仅用于说明 builder 用法，并非所有应用都适用的推荐上限。

### 容量满时等待，还是让上游稍后再试

例如报表高峰期，`cpu_task_capacity(32)` 已有 32 个未完成计算任务，第 33 个立即提交会返回 `SubmissionError::Saturated`。生产者可以限制并发、向上游报告繁忙，也可以等待容量恢复后再提交。若报表存储允许等待，可把前面的 `spawn_io` 换为：

```rust
// 接续 make_daily_report 中取得 total_cents、store 和 day 之后。
let save = services.spawn_io_wait(async move {
    store.save(&day, total_cents).await?;
    Ok::<u64, io::Error>(total_cents)
}).await?;
Ok(save.await?)
```

第一次 `.await` 等待任务**获接纳**，第二次 `.await` 等待存储**执行完成**。另外三个方法是 `submit_blocking_callable_wait`、`submit_cpu_callable_wait` 和 `submit_tokio_blocking_callable_wait`。这些 wait 方法是惰性 future，第一次被 poll 时才提交；开始首次尝试前会先订阅容量变化。通知只提示重新尝试，不会替某个等待者预留名额，因此不保证 FIFO、公平性或等待时间上限。

facade 正在运行时，未启用域返回 `DomainDisabled`；shutdown 或 stop 已关闭准入后，所有提交优先返回 `Rejected { source: Shutdown }`，包括目标域未启用的等待方法。容量已满时，立即提交返回 `Rejected { source: Saturated }`，wait 方法则等待通知后重试。丢弃尚未获接纳的等待 future 会释放其任务；这类等待者不计入域任务容量或 `snapshot().accepted_unfinished`。若需限制等待者内存，应由应用限制并发提交数或设置超时。

三个同步域的 wait 返回 `TaskHandle`，只能观察完成结果，没有 `cancel()`；丢弃结果句柄不会取消任务。IO wait 返回 `TokioTaskHandle`，可以请求 abort，但可能与正常完成竞争。已开始的同步工作不能被强制中断。IO 容量为 1 时，若父任务在 IO 域内等待同域子任务，父任务会占着唯一容量而子任务无法获接纳；应把编排逻辑放在该域外，或明确分析依赖深度并设置容量和超时。

用 `services.snapshot()` 可以按启用域观察积压。未启用域字段为 `None`；IO 域的 `accepted_unfinished` 包括已接纳但尚未 poll 的 future。`ThreadPoolStats::queue_capacity` 是配置的等待队列上限，无界队列为 `None`。各域统计独立采样，不对应同一时刻，不能相加或用于同步提交；真正的准入结果仍以提交方法的返回值为准。

### Tokio runtime 的归属

builder 把传入的 `tokio::runtime::Handle` 交给已启用的 Tokio 执行域，并配置其任务准入容量。只启用 blocking 或 CPU 时不需要 Tokio handle。runtime 和调度器参数由创建 runtime 的应用配置。准入开放时，提交到未启用域返回 `ExecutionServicesSubmissionError::DomainDisabled`。shutdown 或 stop 关闭准入后，所有提交优先返回 `ExecutionServicesSubmissionError::Rejected { source: SubmissionError::Shutdown }`，也包括未启用域。启用域的 `Saturated` 等拒绝原因保留在 `Rejected` 中。

### 有序关闭与强制停止

`shutdown()` 和 `stop()` 首先记录 facade 关闭准入的意图，然后只对已启用域请求对应操作。已经通过准入检查的提交可能与逐域 shutdown 或 stop 重叠，其接收或拒绝由对应底层执行域决定。任一操作返回后，已启用域都拒绝新的 facade 提交。`shutdown()` 要求已接收任务按照底层服务的行为完成；`stop()` 请求强制停止并返回 `ExecutionServicesStopReport`：启用域字段为 `Some(StopReport)`，未启用域字段为 `None`。报告不提供跨域总数，各域依次取样。Tokio IO 的 `running` 字段表示 stop 时已接收但尚未完成的 future，不代表该时刻正在 poll 的 future。应按各域的停止契约解释计数。

`await_termination()` 通过异步通知等待已启用执行域，不占用 Tokio blocking 线程。它可以由异步执行器轮询；builder 收到的 Tokio runtime 仍须持续运行，直到其中已接收的任务结束。在另一个 runtime 中 await 不会驱动已经停止运行的 current-thread runtime。丢弃等待 future 会释放该次等待的资源，不会停止服务。调用 shutdown 或 stop 之前，等待会保持未完成。

还可以通过 `lifecycle()`、`is_running()`、`is_shutting_down()`、`is_stopping()`、`is_not_running()` 与 `is_terminated()` 查询 facade 的总体生命周期。

### 应用关闭顺序

facade 只协调已启用的执行域，不知道哪些业务组件还会提交任务，也不知道任务依赖哪些外部资源。关闭请求发出后，已接收任务仍可继续运行；但它们之后通过此 facade 提交子任务会收到 `ExecutionServicesSubmissionError::Rejected { source: SubmissionError::Shutdown }`。因此，先停止报表入口，并等待所有可能继续提交子任务的生产者完成，再调用：

```rust
services.shutdown();
services.await_termination().await;
assert!(services.is_terminated());
```

等待期间要保持传入的 Tokio runtime 运行；所有报表任务完成后才释放它们使用的旧系统连接和存储客户端。可运行的[应用关闭示例](../examples/application_shutdown.rs)展示了跨域子任务完成后再关闭的顺序。底层执行器支持时，`stop()` 会取消排队任务，但不能强制中断已经开始运行的同步工作。

应用需要由一个所有者统一提交多个执行域的任务并协调关闭时，可以使用此 facade。组件只需要一个执行域或该域的专有控制能力时，可以直接依赖对应的 executor crate。

## 错误与诊断

- `ExecutionServicesBuilder::build()` 在没有启用域时返回 `NoDomains`；对未启用域设置配置时返回 `ConfigurationForDisabledDomain`；启用 Tokio 域却未设置 runtime 时返回 `MissingTokioRuntime`；启用的 blocking 或 CPU builder 配置无效时返回对应错误。
- 准入开放时，提交到未启用域返回 `ExecutionServicesSubmissionError::DomainDisabled`。shutdown 或 stop 关闭准入后，提交优先返回 `ExecutionServicesSubmissionError::Rejected { source: SubmissionError::Shutdown }`；已启用域的 `Saturated` 等拒绝原因保留在 `Rejected` 中。
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
| 报表保存任务返回错误 | 查看应用实现的 `ReportStore` 错误和存储端状态；提交成功不能证明报表已保存。 |

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
- [设计文档](design.zh_CN.md) 与 [Design documents](design.md)
- [每日报表示例](../examples/daily_report.rs)
