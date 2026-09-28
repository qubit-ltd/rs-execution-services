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
