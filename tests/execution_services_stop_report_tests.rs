// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`ExecutionServicesStopReport`].

use qubit_execution_services::ExecutionServicesStopReport;
use qubit_executor::service::StopReport;

#[test]
fn test_execution_services_stop_report_has_only_per_domain_values() {
    let report = ExecutionServicesStopReport {
        blocking: Some(StopReport::new(1, 2, 3)),
        cpu: None,
        tokio_blocking: None,
        io: None,
    };

    assert_eq!(report.blocking, Some(StopReport::new(1, 2, 3)));
    assert_eq!(report.cpu, None);
    assert_eq!(report.tokio_blocking, None);
    assert_eq!(report.io, None);
}

#[test]
fn test_execution_services_stop_report_preserves_each_domain_independently() {
    let report = ExecutionServicesStopReport {
        blocking: Some(StopReport::new(1, 2, 3)),
        cpu: Some(StopReport::new(4, 5, 6)),
        tokio_blocking: Some(StopReport::new(7, 8, 9)),
        io: Some(StopReport::new(10, 11, 12)),
    };

    assert_eq!(report.blocking, Some(StopReport::new(1, 2, 3)));
    assert_eq!(report.cpu, Some(StopReport::new(4, 5, 6)));
    assert_eq!(report.tokio_blocking, Some(StopReport::new(7, 8, 9)));
    assert_eq!(report.io, Some(StopReport::new(10, 11, 12)));
}
