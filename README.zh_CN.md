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
qubit-execution-services = "0.11"
tokio = { version = "1.53", features = ["rt", "time"] }
```

0.11 版本为 `ExecutionServicesSubmissionError::Rejected` 增加请求域，并移除无法获知域的 `SubmissionError` 自动转换。迁移说明见[用户指南](doc/user_guide.zh_CN.md)。

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
//! Demonstrates submitting and awaiting work in separate execution domains.

use std::future::Future;
#[cfg(not(test))]
use std::io;

use qubit_execution_services::ExecutionServices;

async fn shutdown_after<F>(services: &ExecutionServices, work: F) -> Result<(), Box<dyn std::error::Error>>
where
    F: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let result = work.await;
    services.shutdown();
    services.await_termination().await;
    result
}

#[cfg(not(test))]
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

        shutdown_after(&services, async {
            let blocking = services.submit_blocking_callable(|| Ok::<usize, io::Error>(40 + 2))?;
            let cpu = services.submit_cpu_callable(|| Ok::<usize, io::Error>((1..=10).sum()))?;
            let io = services.spawn_io(async { Ok::<usize, io::Error>(6 * 7) })?;

            assert_eq!(blocking.await?, 42);
            assert_eq!(cpu.await?, 55);
            assert_eq!(io.await?, 42);
            Ok::<(), Box<dyn std::error::Error>>(())
        })
        .await
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

需要等待容量时，可使用 `submit_blocking_callable_wait`、`submit_cpu_callable_wait`、`submit_tokio_blocking_callable_wait` 或 `spawn_io_wait`。这些惰性 future 会保留同一个任务并重试，最多接纳一次；获接纳前丢弃 future 会释放尚未提交的任务。三个同步 wait 方法返回只能观察结果的 `TaskHandle`，不提供取消方法；IO 返回的 `TokioTaskHandle` 可以请求 abort，但可能与正常完成竞争。准入、容量和取消契约见[设计文档](doc/design.zh_CN.md)。`snapshot()` 返回各域独立采样的监控信息，不是跨域原子快照。

按工作负载选择执行域并逐项核算资源的方法，见[选择执行域与核算资源](doc/user_guide.zh_CN.md#选择执行域与核算资源)。

facade 协调已启用执行域的任务提交与生命周期操作。停止报告按域提供可选观测值，不计算跨域总数。

应用需要统一向多个执行域提交任务并协调关闭时，可以使用此 facade。组件只需要一个执行域或该域的专有控制能力时，可以直接依赖对应的 executor crate。

关闭应用时，先停止入口并等待可能提交子任务的已接收生产者完成，再调用 `shutdown()`；准入关闭后，这类子提交会被拒绝。保持 Tokio runtime 运行，直到 `await_termination()` 完成；顺序见[跨域应用关闭示例](examples/application_shutdown.rs)。各域资源上限不构成进程级线程或内存预算，容量测量方法见[资源预算指南](doc/user_guide.zh_CN.md#选择执行域与核算资源)。

## 延伸阅读

- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API 文档](https://docs.rs/qubit-execution-services)
- [设计文档](doc/design.zh_CN.md) · [Design documents](doc/design.md)
- [每日报表示例](examples/daily_report.rs)
- [English README](README.md)

## 开发环境

当前工作树通过 `Cargo.toml` 中带版本约束的 path 依赖使用本地最新版 `rs-tokio-executor`、`rs-rayon-executor` 和 `rs-executor`；构建时需将这些仓库放在本仓库旁边。`.cargo/config.toml` 会将传递依赖及打包验证中的 executor 依赖也 patch 到这些 checkout。源码集成 documentation consumer 也使用这些本地 executor 兄弟仓库。IoC 集成 fixture 还使用本地 path 依赖：请将 `rs-ioc`、`rs-event-bus`、`rs-config`、`rs-fs-registry`、`rs-fs-local` 和 `rs-fs` 检出到本仓库旁边，并检出这些清单引用的传递 path 依赖（`rs-value`、`rs-local-files`、`rs-redact` 和 `rs-fs-testkit`）。固定集成工作流记录一组固定依赖提交；current 集成工作流跟踪 `rs-ioc/main`，使用本仓库维护的 current consumer 锁文件，并固定其他兄弟仓库。这些 fixture 检查验证本地集成布局，不代表 registry 发布验证。

[发布包文档 consumer workflow](https://github.com/qubit-ltd/rs-execution-services/blob/main/.github/workflows/packaged-documentation-consumer.yml) 运行源码集成检查和 registry-only 检查。源码集成消费者检查允许 `qubit-executor`、`qubit-rayon-executor` 和 `qubit-tokio-executor` 使用本地覆盖。registry-only 检查使用 registry 依赖构建并运行归档示例，仅允许消费者和解压目标 crate 为本地包。发布验收须同时通过两项检查；缺失 registry 依赖会导致检查失败并阻止发布验收。registry 矩阵还会单独检查 manifest 声明的 Rayon 下界对应的精确 crates.io 版本；它验证该 Rayon 版本与本次解析出的其他依赖组合兼容，不表示所有依赖都处于最低版本。发布验收前可运行严格检查：`python3 .infra/tools/check-packaged-documentation-consumer.py --registry-only`。运行 `cargo test --locked` 和 `cargo test --locked --all-features` 可检查已提交锁文件是否无需修改即可解析。

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./.infra/bin/ci-check.sh

# 检查代码覆盖率
./.infra/bin/coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./.infra/bin/align-ci.sh`格式化代码，运行`./.infra/bin/ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-execution-services](https://github.com/qubit-ltd/rs-execution-services)
