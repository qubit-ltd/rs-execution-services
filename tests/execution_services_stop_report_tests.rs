/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`ExecutionServicesStopReport`].

use qubit_execution_services::{ExecutionServicesStopReport, StopReport};

#[test]
fn test_execution_services_stop_report_totals() {
    let report = ExecutionServicesStopReport {
        blocking: StopReport::new(1, 2, 3),
        cpu: StopReport::new(4, 5, 6),
        tokio_blocking: StopReport::new(7, 8, 9),
        io: StopReport::new(10, 11, 12),
    };

    assert_eq!(report.total_queued(), 22);
    assert_eq!(report.total_running(), 26);
    assert_eq!(report.total_cancelled(), 30);
}
