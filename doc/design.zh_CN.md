# Execution Services 设计

[English design document](design.md) · [中文用户手册](user_guide.zh_CN.md)

## 目的与边界

`qubit-execution-services` 是应用持有的门面，管理四个独立执行域：阻塞线程池、Rayon CPU 池、Tokio `spawn_blocking` 和 Tokio 异步 IO。它统一构建、准入、快照和关闭过程；Tokio runtime 仍由应用创建和配置。

各域分别维护容量和队列。门面不是共享调度器，也不提供进程级线程数或内存预算。Builder 至少启用一个域；给未启用域设置参数会报错。

## 所有权与模块职责

`ExecutionServices` 持有 `ExecutionServicesAdmission`，并保存每个域的可选服务。阻塞服务使用 `Arc`，使已接收的生产者可通过同一个门面提交子任务；其他服务句柄按其底层实现提供所需共享所有权。

- `execution_services.rs` 管理门面、Builder 入口、执行域清单和快照组合。
- `submission.rs` 实现立即提交和共享 native 提交适配。
- `waiting_submission.rs` 实现四个容量等待 API。blocking 使用任务所有权包装器和共享重试循环；CPU、Tokio blocking 和 IO 使用原生拒绝返回任务的 API 重试。
- `internal/execution_services_admission.rs` 管理单调关闭的门面意图并解析请求域。
- `internal/submission_retry.rs` 仅在 blocking 提交饱和时重试，并在尝试之间等待容量变化通知。
- `internal/owned_wait_task.rs` 在拒绝的尝试包装被释放后继续持有 blocking callable。
- `lifecycle.rs` 关闭各启用域，并通过异步通知等待终止。

由于重试模块直接使用 Tokio 的 `watch::Receiver` 类型，Tokio `sync` 是显式直接依赖 feature。

## 提交与准入

门面意图只有三种：`Running`、`ShuttingDown` 和 `Stopping`。shutdown 与 stop 都会先关闭准入，再通知各启用服务执行生命周期操作。stop 可以升级 graceful shutdown，不能被降级。

立即提交与等待提交都先在准入检查中解析目标域。意图处于 `Running` 时，服务缺失会返回 `DomainDisabled`。准入关闭后，`Rejected { source: Shutdown }` 优先，即使请求的域未启用也是如此。native 提交、拒绝任务析构、用户 callable 执行和 future poll 都在准入互斥锁释放之后发生。

准入检查和底层服务接收不是一个原子事务。已通过门面检查的调用仍可能与 shutdown 或 stop 重叠；是否接收由底层服务决定。门面不保证多个独立服务之间的严格线性化。

| 门面意图 | 执行域状态 | 立即提交 | 容量等待提交 |
| --- | --- | --- | --- |
| Running | 未启用 | `DomainDisabled` | `DomainDisabled` |
| 已关闭 | 未启用或已启用 | `Rejected(Shutdown)` | `Rejected(Shutdown)` |
| Running | 已启用且有容量 | 返回底层句柄 | 返回底层句柄 |
| Running | 已启用且容量已满 | `Rejected(Saturated)` | Pending；收到通知后重试 |
| 等待期间关闭 | 原本已启用 | 不适用 | 唤醒后重查准入，返回 `Rejected(Shutdown)` |

## 容量等待算法

wait API 返回惰性 future。首次 poll 时，它解析目标域，并在第一次提交尝试前订阅容量变化。blocking 将 callable 放入 `OwnedWaitTask`，每次尝试使用新的包装器；CPU、Tokio blocking 和 IO 则由等待 future 持有原 callable 或 future，并使用原生 `try_*` API 在拒绝时取回任务。四个 API 在接纳后返回句柄，仅在饱和并收到容量变化通知后重试，其他错误立即返回。若通知通道关闭，等待会返回 Shutdown，不会永久 Pending。

容量通知只说明状态发生变化，不会为等待者保留额度。其他提交者可能先取得可用容量。系统不保证 FIFO、公平性或等待时间上限。

容量仅统计已接收任务。尚未获接纳的等待者不计入 `accepted_unfinished` 快照或域任务容量，即使其 future 仍持有业务数据。应用若需要内存上限，应限制并发生产者，或在应用层设置超时和背压。

在 blocking 域中，`OwnedWaitTask` 用互斥保护共享的 `Option<C>`。被拒绝的包装器退出时，callable 仍在槽位中；接纳后的包装器在执行时取走 callable。其他域通过原生拒绝值直接取回任务，并在重试期间保留它。所有路径都不要求任务实现 `Clone`，一次性任务最多只会被接纳执行一次。

## 取消与生命周期

| 阶段或句柄 | 契约 |
| --- | --- |
| 接收前丢弃 wait future | 停止重试并释放未接收任务。 |
| 阻塞、CPU 或 Tokio 阻塞 wait 返回的 `TaskHandle` | 观察任务完成；没有取消方法。丢弃句柄不会取消任务。 |
| IO wait 返回的 `TokioTaskHandle` | 可请求 Tokio task abort；可能与正常完成竞争，也不保证用户清理代码运行。 |
| 已开始的同步 callable | 句柄不能强制中断。 |
| `shutdown()` | 关闭准入，并按各启用服务契约请求有序完成。 |
| `stop()` | 关闭准入并请求强制停止；不能强行中断已运行的同步代码。 |

在 Tokio 服务终止前，保持传入的 runtime 持续运行。调用 `shutdown()` 前先停止外部生产者，并等待可能提交子任务的已接收生产者退出。

丢弃 facade 只释放其持有的服务值；它不会统一请求 `stop()`，也不会等待所有已接收任务完成。Cargo metadata 解析到的 `qubit-thread-pool 0.12.1` 源码显示，`ThreadPool` 值析构时会请求 graceful shutdown。`qubit-rayon-executor 0.9.0` wrapper 析构时不会调用显式 shutdown 或 stop；Rayon pool 值析构会发出终止信号，现有工作完成后 worker 才会逐步退出，但 facade 不会等待。解析到的 `qubit-tokio-executor 0.12.0` blocking 和 IO service wrapper 没有会取消已接收任务的析构实现；丢弃 facade 不会 abort Tokio blocking 任务或 IO future。这些逐域行为不能组成统一的完成保证；如果必须在释放资源前等已接收工作完成，应调用 `shutdown()` 并等待 `await_termination()`，再丢弃 facade。

正常成功或返回错误时，应保留操作结果，同时请求 graceful shutdown 并等待终止。如果需要强制停止或 Tokio abort，同步清理可以在任务取消/析构路径中执行；异步清理必须由仍存活的所有者驱动，完成后才能释放所需资源。future 一旦取消，便不能恢复后再执行异步清理等待。单独丢弃 `await_termination()` future 只会取消本次等待；仍存活的所有者可以再次等待。Tokio runtime 应持续运行，直到其已接收工作及所需清理都完成。

容量为 1 的 IO 任务若在同一个已满 IO 域中等待子任务，会形成等待环：父任务占据唯一接收名额，子任务则等待名额释放。应将编排放到该域外；独立工作可使用不同域；若确需同域等待，应明确分析依赖深度和容量。单纯增加容量不能证明任意递归等待都安全。门面不检测依赖环，也不创建备用 worker。

## 下游集成

IoC fixture 将 `ExecutionServices` 作为受管理资源。构建失败或取消时，rollback 使用 `stop()` 关闭准入，不等待可能无限期占用服务的业务任务。异步 build 失败会运行每个 managed wait 回调并验证服务终止。如果 build future 本身被取消，会同步调用 stop 回调；由于被丢弃的 build 无法驱动异步 wait 回调，fixture 在 build 之外等待服务结束。

正常应用关闭顺序不同：先停止生产者，再调用 `ExecutionServices::shutdown()`，在 Tokio runtime 仍运行时等待服务终止，最后关闭 IoC context。cleanup 时重复 stop 不会重新开放准入。

现有 [ioc-integration 工作流](../.github/workflows/ci.yml)检查固定基线；[ioc-current-integration 工作流](../.github/workflows/ioc-current-integration.yml)检查本仓库触发事件对应的提交与 `rs-ioc/main`，其他兄弟仓库仍固定到指定提交。每日定时运行也会覆盖只有下游 IoC 仓库更新的组合。两个工作流都验证 fixture 契约，不代表生产环境已经采用此门面。

## 验证

集成测试通过公共 API 检查错误优先级、四域容量等待、多个等待者竞争、丢弃未接收任务、关闭唤醒以及同域嵌套等待。测试会固定已 pin 的 future，在独立 gate 仍占用容量时显式 poll 一次并断言 `Pending`；timeout 只用于发现测试卡死。

应用 consumer 和可运行资源预算示例也会在释放已接收任务前 poll 等待 future。本地文档 consumer 检查 sibling checkout 布局和 Tokio feature；它会解析本仓库 `.cargo/config.toml` 中的 patch，因此不属于 registry 隔离验证。`packaged-documentation-consumer` workflow 先运行验证器 self-test，再运行 `cargo package --locked`，把归档解压到源码树外，并在不加载源码树 Cargo 配置的目录中构建和运行消费者。README quick-start consumer 从归档读取 `examples/quick_start.rs`；指南 consumer 从归档读取并运行完整的 `examples/daily_report.rs`。当前 execution-services API 尚未在 registry 依赖版本组合中同时提供，因此 workflow 会检出 `rs-tokio-executor`、`rs-rayon-executor` 和 `rs-executor` 的最新 `dev-starfish` 源码并作为本地路径依赖。metadata 仅允许临时 consumer、解压目标包和这三个精确的本地路径包；其他每个包都必须具有 `registry+` 来源。解压目标包必须 `source=None`，且 manifest 路径精确指向归档。初次解析生成 consumer 锁文件后，验证器使用 `cargo metadata --locked` 检查最终 metadata，再使用 `cargo run --locked`，使来源检查和实际运行共享锁文件。该检查验证文档 feature 配置与最新本地 executor 源码及 Cargo 当前可解析的 registry 版本兼容；它不证明最小 semver 兼容性、不限定 registry 下载的物理端点，也不代表生产项目已经采用。独立 IoC CI job 会使用固定依赖运行 consumer、测试和 Clippy，不混入核心 crate 的 feature matrix。

中英文用户指南描述相同的错误、所有权、取消和容量契约。可运行示例与 Cargo 打包文件清单验证文档代码可执行且设计文档进入 crate 发布包。

## 非目标

本门面不提供公平或 FIFO 等待队列、全局资源预算、自动死锁检测、优先级调度、同步代码强制中断或统一可取消句柄。若出现真实应用需求，应另行设计 API 和生命周期契约。
