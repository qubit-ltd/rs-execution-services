/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ExecutionServicesShutdownReport`].

use qubit_execution_services::{
    ExecutionServicesShutdownReport,
    ShutdownReport,
};

#[test]
fn test_execution_services_shutdown_report_totals() {
    let report = ExecutionServicesShutdownReport {
        blocking: ShutdownReport::new(1, 2, 3),
        cpu: ShutdownReport::new(4, 5, 6),
        tokio_blocking: ShutdownReport::new(7, 8, 9),
        io: ShutdownReport::new(10, 11, 12),
    };

    assert_eq!(report.total_queued(), 22);
    assert_eq!(report.total_running(), 26);
    assert_eq!(report.total_cancelled(), 30);
}
