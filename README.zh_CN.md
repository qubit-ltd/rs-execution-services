# Qubit Execution Services

[![Rust CI](https://github.com/qubit-ltd/rs-execution-services/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-execution-services/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-execution-services/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-execution-services?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-execution-services.svg?color=blue)](https://crates.io/crates/qubit-execution-services)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Documentation](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

面向 Rust 应用的 execution-service 聚合门面。

## 概览

Qubit Execution Services 把应用常用的 Qubit executor 实现装配到一起：blocking OS 线程工作、CPU 密集型 Rayon 工作、Tokio blocking 工作和 async IO future。

本 crate 是应用层便利门面，不是基础抽象层。普通库通常应该直接依赖更小的 crate，例如 `qubit-executor`、`qubit-thread-pool`、`qubit-rayon-executor` 或 `qubit-tokio-executor`。

## 功能

- 提供 `ExecutionServices` 门面，包含独立的 blocking、CPU、Tokio blocking 与 async IO 域。
- 提供 `ExecutionServicesBuilder`，用于配置 blocking 线程池域与 CPU Rayon 域。
- 提供 `submit_blocking`、`submit_cpu` 与 `submit_tokio_blocking`，用于 fire-and-forget runnable 工作。
- 提供 `submit_blocking_callable`、`submit_cpu_callable` 与 `submit_tokio_blocking_callable`，用于带返回值的 callable 工作。
- 提供 `submit_tracked_*` 变体，用于需要状态或取消 handle 的工作。
- 提供 `spawn_io`，用于路由到 Tokio async scheduler 的 async future。
- 提供 `ExecutionServicesStopReport`，聚合所有执行域的 queued、running 与 cancelled 计数。
- 提供底层 executor service 与 task handle 的类型别名和 re-export。

## 执行域

Blocking 域使用 `qubit-thread-pool` 的 `ThreadPool`。它面向可能阻塞 OS 线程的同步工作，例如文件系统操作或遗留 blocking API。

CPU 域使用 `qubit-rayon-executor` 的 `RayonExecutorService`。它面向 CPU 密集型工作，适用于 Rayon 调度是正确执行模型的场景。

Tokio blocking 域使用 `qubit-tokio-executor` 的 `TokioExecutorService`。它面向 Tokio 应用中提交的 blocking 函数，并通过 `spawn_blocking` 执行。

IO 域使用 `qubit-tokio-executor` 的 `TokioIoExecutorService`。它面向 async future 与非阻塞 IO 工作，并通过 `tokio::spawn` 执行。

## Builder 配置

`ExecutionServicesBuilder` 将 blocking 域设置委托给 `ThreadPoolBuilder`，将 CPU 域设置委托给 `RayonExecutorServiceBuilder`。Tokio-backed 域目前使用默认构造方式，因为 Tokio runtime 和 scheduler 配置由 Tokio 自身及应用持有。

builder 暴露常用 blocking 线程池配置，包括 pool size、core size、maximum size、queue capacity、线程名前缀、栈大小、keep-alive、core 线程超时和预启动行为。它也暴露 CPU 域的 Rayon worker 数量、线程名前缀和栈大小配置。

## 关闭行为

`shutdown` 会对所有执行域请求有序关闭。新任务被拒绝，已接受任务按各底层服务的语义继续完成。

`stop` 会对所有执行域请求强制停止，并返回 `ExecutionServicesStopReport`，其中包含每个执行域一个 `StopReport`。该报告还提供 `total_queued`、`total_running` 与 `total_cancelled` 辅助方法，用于聚合统计。

`await_termination` 在所有底层服务都终止后完成。

## 快速开始

```rust
use std::io;

use qubit_execution_services::ExecutionServices;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let services = ExecutionServices::builder()
    .blocking_pool_size(4)
    .blocking_queue_capacity(1024)
    .cpu_threads(4)
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

## 如何选择依赖

当应用边界需要一个持有型门面，并希望按任务类型路由到不同执行域时，使用 `qubit-execution-services`。

只需要某一层或某一种运行时时，请直接使用更小的 crate：

- `qubit-executor` 用于 trait、task handle 与共享生命周期类型。
- `qubit-thread-pool` 用于不绑定 runtime 的 OS 线程池。
- `qubit-rayon-executor` 用于 CPU 密集型 Rayon 执行。
- `qubit-tokio-executor` 用于 Tokio blocking 与 async IO 执行。

## 测试

快速在本地跑一遍：

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

若要与持续集成（CI）保持一致，请在仓库根目录依次执行：`./align-ci.sh` 将本地工具链与配置对齐到 CI 规则，再执行 `./ci-check.sh` 复现流水线中的检查。需要查看或生成测试覆盖率时，使用 `./coverage.sh`。

## 参与贡献

欢迎通过 Issue 与 Pull Request 参与本仓库。建议：

- 报告缺陷、讨论设计或较大能力扩展时，可先开 Issue 对齐方向再投入实现。
- 单次 PR 尽量聚焦单一主题，便于代码审查与合并历史。
- 提交 PR 前请先运行 `./align-ci.sh`，再运行 `./ci-check.sh`，确保本地与 CI 使用同一套规则且能通过流水线等价检查。
- 若修改运行期行为，请补充或更新相应测试；若影响对外 API 或用户可见行为，请同步更新本文档或相关 rustdoc。
- 如果修改路由或关闭行为，请补充测试验证所有受影响的执行域。

向本仓库贡献内容即表示您同意以 [Apache License, Version 2.0](LICENSE)（与本项目相同）授权您的贡献。

## 许可证与版权

Copyright (c) 2026. Haixing Hu.

本软件依据 [Apache License, Version 2.0](LICENSE) 授权；完整许可文本见仓库根目录的 `LICENSE` 文件。

## 作者与维护

**Haixing Hu** — Qubit Co. Ltd.

| | |
| --- | --- |
| **源码仓库** | [github.com/qubit-ltd/rs-execution-services](https://github.com/qubit-ltd/rs-execution-services) |
| **API 文档** | [docs.rs/qubit-execution-services](https://docs.rs/qubit-execution-services) |
| **Crate 发布** | [crates.io/crates/qubit-execution-services](https://crates.io/crates/qubit-execution-services) |
