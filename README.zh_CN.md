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
qubit-execution-services = "0.10"
tokio = { version = "1.53", features = ["rt", "time"] }
```

## 快速开始

下面的例子用同一个门面分别执行同步阻塞任务、CPU 计算和异步任务，取得结果后再关闭已启用的执行域：

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

这里通过 `.await` 等待任务，让 Tokio 工作线程继续推进调度。`TaskHandle::get()` 会阻塞调用线程，只应在同步代码中使用。

## 能力与边界

- 按需启用 blocking、CPU、Tokio blocking 和 Tokio async 执行域。
- 通过 builder 配置受管理线程池，以及 Tokio 阻塞与 IO 域的有限容量；Tokio runtime 及其调度参数由应用管理。
- 支持无返回值任务、可取得结果的任务，以及可跟踪状态或取消的任务。
- 统一查询已启用域的生命周期、发起有序关闭或强制停止，并按域提供停止报告。

blocking 线程池可配置 worker 数量和队列容量；CPU 与 Tokio 执行域会限制已接收但尚未完成的任务数。用户手册详细说明默认值、容量错误、取消和关闭行为。

blocking 和 CPU 执行域会分别创建线程池，Tokio runtime 则由应用单独配置。各域默认值只是单域起点，不代表进程总线程数或内存预算。blocking 队列默认最多容纳 1024 个等待任务；CPU 和 Tokio 执行域默认各接受最多 1024 个未完成任务。应按预期并发量配置各域容量，并在达到容量前对提交端施加背压。

`tokio_blocking` 使用应用所拥有的 Tokio 共享阻塞线程池。它的任务容量限制已接收但尚未完成的任务数，不预留线程，也不配置专属的运行并发数；实际同时运行数还取决于 runtime 的共享阻塞池和其他使用者。完整配置见可运行的[资源预算示例](examples/resource_budget.rs)；其中数值仅用于演示，不是通用默认值。

facade 协调已启用执行域的任务提交与生命周期操作。停止报告按域提供可选观测值，不计算跨域总数。

应用需要统一向多个执行域提交任务并协调关闭时，可以使用此 facade。组件只需要一个执行域或该域的专有控制能力时，可以直接依赖对应的 executor crate。

关闭应用时，先停止会继续提交工作的业务组件，再调用 `shutdown()` 并等待 `await_termination()`；等待期间应保持 Tokio runtime 运行。完整顺序见[应用关闭示例](examples/application_shutdown.rs)和用户手册。

## 延伸阅读

- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API 文档](https://docs.rs/qubit-execution-services)
- [English README](README.md)

## 开发环境

开发本仓库时，Cargo 使用相邻目录中的 `rs-thread-pool`、`rs-rayon-executor` 和 `rs-tokio-executor` 源码。运行 Cargo 命令前，先准备这些本地依赖：

```bash
./.infra/tools/prepare-local-path-dependencies.sh
```

应用使用已发布版本时，只需依赖 crates.io 上对应版本。发布本 crate 前，registry 也必须已有清单声明的 `qubit-thread-pool` 版本。

检查已提交的锁文件能否原样解析时，请运行 `cargo test --locked` 和 `cargo test --locked --all-features`。

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
