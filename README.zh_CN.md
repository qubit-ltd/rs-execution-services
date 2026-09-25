# Qubit Execution Services

[![Rust CI](https://github.com/qubit-ltd/rs-execution-services/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-execution-services/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-execution-services/coverage-badge.json)](https://qubit-ltd.github.io/rs-execution-services/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-execution-services.svg?color=blue)](https://crates.io/crates/qubit-execution-services)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

Qubit Execution Services 为 Rust 应用提供统一的任务分发入口：同步阻塞工作、CPU 密集型计算、Tokio 阻塞任务和异步 future 都可以交给各自适合的执行域。它让应用不必自行装配、持有和逐个关闭这些服务；只依赖一种执行能力的库仍可直接选用更小的 executor crate。

## 安装

在应用的 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
qubit-execution-services = "0.9"
tokio = { version = "1.53", features = ["rt", "time"] }
```

## 快速开始

下面的例子用同一个门面分别执行同步阻塞任务、CPU 计算和异步任务，取得结果后再关闭所有执行域：

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
        let services = ExecutionServices::builder(runtime.handle().clone())
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

## 能力与边界

- 将阻塞、CPU 密集型、Tokio 阻塞和 Tokio 异步任务分配到不同执行域。
- 通过 builder 配置受管理线程池，以及 Tokio 阻塞与 IO 域的有限容量；Tokio runtime 及其调度参数由应用管理。
- 支持无返回值任务、可取得结果的任务，以及可跟踪状态或取消的任务。
- 统一查询生命周期、发起有序关闭或强制停止，并汇总各执行域的停止计数。

blocking 线程池的核心数和最大线程数默认都等于检测到的 CPU 并行度，等待队列默认最多容纳 1024 个任务。长时间阻塞的调用可能占满所有 worker；队列尚未满时，等待任务不会触发扩容。请根据预期同时阻塞任务数和可接受积压量配置 `blocking_core_pool_size`、`blocking_maximum_pool_size` 和有限的 `blocking_queue_capacity`。有界队列满后，线程池可在达到配置的最大线程数前增加 worker；无界队列不会让线程池扩展到核心线程数之外。队列容量不会限制任务内存。CPU、Tokio 阻塞和 Tokio IO 域分别默认限制 1024 个尚未完成的已接收任务，并在达到容量时返回 `SubmissionError::Saturated`。可通过 `tokio_blocking_task_capacity` 和 `io_task_capacity` 配置 Tokio 域。facade 记录 shutdown 或 stop 意图后，新提交会在检查执行域容量前优先返回 `SubmissionError::Shutdown`；已通过准入检查的重叠提交仍可能被执行域以 `SubmissionError::Saturated` 拒绝。配置、取消和关闭流程详见用户手册。

facade 在委托底层执行域前检查准入。与 shutdown 或 stop 重叠的提交，可能被对应执行域接收或拒绝；任一关闭操作返回后，四个执行域都拒绝新的 facade 提交。facade 调用底层提交或销毁被拒绝任务时不持有准入锁。停止计数来自各执行域依次停止时的观测值。特别是，Tokio IO 的 `running` 还包括已接收但未完成的 future，不表示它们此刻正在被 poll；`total_running()` 不是全局并发度快照。

当应用需要由一个所有者统一提交多个执行域的任务并协调关闭时，可使用此 facade。只需要一个执行域或其专有控制能力的组件可以直接依赖对应的 executor crate。例如 `rs-task` 需要 `PoolJobTicket` 和 `ThreadPoolStats`，因此继续使用 `qubit-thread-pool`；其设计文档仅将本 facade 列为未来执行后端的候选。`rs-event-bus` 也直接使用底层 crate。尚未确认有生产下游使用本 facade。应用消费者 fixture 用于验证公开 API 边界，不能作为生产采用的证据。

## 延伸阅读

- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API 文档](https://docs.rs/qubit-execution-services)
- [English README](README.md)

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./ci-check.sh

# 检查代码覆盖率
./coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-execution-services](https://github.com/qubit-ltd/rs-execution-services)
