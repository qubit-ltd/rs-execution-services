// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Nonblocking EventBus requests with one retained shutdown observer.
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::ShutdownReport;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_ioc::CleanupError;
use qubit_ioc::Managed;

/// Formats and checks the provider report according to this fixture's strict
/// cleanup policy.
fn format_shutdown_report(report: &ShutdownReport) -> String {
    format!("event bus shutdown report: {report:?}")
}

/// Keeps provider shutdown degradation visible to the IoC container.
///
/// A provider timeout or known abandoned delivery is a cleanup diagnostic.
/// Provider uncertainty alone is recorded but does not imply confirmed loss.
fn check_shutdown_report(report: ShutdownReport) -> Result<(), CleanupError> {
    let diagnostic = format_shutdown_report(&report);
    let _ = writeln!(std::io::stderr().lock(), "{diagnostic}");
    let accepted = matches!(report.outcome, ShutdownOutcome::Complete)
        && report.known_abandoned_deliveries == 0;
    if accepted {
        Ok(())
    } else {
        Err(CleanupError::new(std::io::Error::other(diagnostic)))
    }
}

/// Adapts an existing bus. Abort and graceful requests return shutdown tickets
/// without waiting for handlers. Their errors remain in the IoC shutdown report,
/// and a cancelled wait resumes the same ticket observation.
pub fn managed_event_bus(bus: Arc<EventBus>) -> Managed<EventBus> {
    Managed::asynchronous_with_graceful_ticket(
        bus,
        |bus| {
            bus.request_shutdown(ShutdownMode::Immediate)
                .map_err(CleanupError::new)
        },
        |bus| {
            bus.request_shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(30),
            })
            .map_err(CleanupError::new)
        },
        |_, ticket| {
            Box::pin(async move {
                let report = ticket.wait_async().await.map_err(CleanupError::new)?;
                check_shutdown_report(report)
            })
        },
    )
}

#[cfg(test)]
mod tests {
    use qubit_event_bus::ShutdownReport;
    use qubit_event_bus::spi::ShutdownOutcome;

    use super::check_shutdown_report;
    use super::format_shutdown_report;

    #[test]
    fn shutdown_report_policy_table() {
        for outcome in [ShutdownOutcome::Complete, ShutdownOutcome::TimedOut] {
            for abandoned in [0, 1] {
                for uncertain in [false, true] {
                    let report = ShutdownReport {
                        outcome,
                        known_abandoned_deliveries: abandoned,
                        provider_may_have_abandoned_deliveries: uncertain,
                    };
                    let expected_ok = outcome == ShutdownOutcome::Complete && abandoned == 0;
                    let result = check_shutdown_report(report);
                    assert_eq!(result.is_ok(), expected_ok, "{report:?}");
                    let formatted = format_shutdown_report(&report);
                    assert!(formatted.contains(&format!("outcome: {outcome:?}")));
                    assert!(
                        formatted.contains(&format!("known_abandoned_deliveries: {abandoned}"))
                    );
                    assert!(formatted.contains(&format!(
                        "provider_may_have_abandoned_deliveries: {uncertain}"
                    )));
                    if !expected_ok {
                        let error = result.expect_err("degraded report must be diagnosed");
                        assert!(error.to_string().contains(&formatted));
                    }
                }
            }
        }
    }
}
