// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Checks that each published quick-start example matches the runnable example.

const QUICK_START: &str = include_str!("../examples/quick_start.rs");
const RESOURCE_BUDGET: &str = include_str!("../examples/resource_budget.rs");
const DAILY_REPORT: &str = include_str!("../examples/daily_report.rs");
const ENGLISH_README: &str = include_str!("../README.md");
const CHINESE_README: &str = include_str!("../README.zh_CN.md");
const ENGLISH_GUIDE: &str = include_str!("../doc/user_guide.md");
const CHINESE_GUIDE: &str = include_str!("../doc/user_guide.zh_CN.md");
const ENGLISH_DESIGN: &str = include_str!("../doc/design.md");
const CHINESE_DESIGN: &str = include_str!("../doc/design.zh_CN.md");

fn first_rust_code_block(markdown: &str) -> Option<String> {
    let mut lines = markdown.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "```rust" {
            continue;
        }

        let mut code = String::new();
        for line in lines.by_ref() {
            if line.trim() == "```" {
                return Some(code);
            }
            code.push_str(line);
            code.push('\n');
        }
        return None;
    }
    None
}

#[test]
fn test_documented_quick_start_examples_match_the_runnable_example() {
    let expected = QUICK_START.trim_end();
    for (path, markdown) in [
        ("README.md", ENGLISH_README),
        ("README.zh_CN.md", CHINESE_README),
        ("doc/user_guide.md", ENGLISH_GUIDE),
        ("doc/user_guide.zh_CN.md", CHINESE_GUIDE),
    ] {
        let actual = first_rust_code_block(markdown)
            .unwrap_or_else(|| panic!("{path} must contain a closed Rust quick-start block"));
        assert_eq!(actual.trim_end(), expected, "quick-start block in {path} drifted");
    }
}

#[test]
fn test_waiting_submission_and_snapshot_contract_is_documented_bilingually() {
    for (path, markdown) in [
        ("README.md", ENGLISH_README),
        ("README.zh_CN.md", CHINESE_README),
        ("doc/user_guide.md", ENGLISH_GUIDE),
        ("doc/user_guide.zh_CN.md", CHINESE_GUIDE),
    ] {
        assert!(
            markdown.contains("spawn_io_wait"),
            "{path} must mention waiting submission"
        );
        assert!(markdown.contains("snapshot()"), "{path} must explain snapshots");
    }
    assert!(RESOURCE_BUDGET.contains("spawn_io_wait"));
    assert!(RESOURCE_BUDGET.contains("snapshot()"));
}

#[test]
fn test_shutdown_example_and_resource_budget_are_linked_bilingually() {
    for guide in [ENGLISH_GUIDE, CHINESE_GUIDE] {
        for anchor in ["application_shutdown.rs", "shutdown()", "snapshot()", "Saturated"] {
            assert!(guide.contains(anchor), "missing {anchor} in a user guide");
        }
    }
    for readme in [ENGLISH_README, CHINESE_README] {
        assert!(readme.contains("examples/application_shutdown.rs"));
    }
}

#[test]
fn test_daily_report_example_is_linked_and_described_in_both_guides() {
    for (path, guide) in [
        ("doc/user_guide.md", ENGLISH_GUIDE),
        ("doc/user_guide.zh_CN.md", CHINESE_GUIDE),
    ] {
        assert!(
            guide.contains("examples/daily_report.rs"),
            "{path} must link the runnable daily-report example"
        );
        assert!(
            guide.contains("calculate_daily_report"),
            "{path} must describe report calculation"
        );
    }
    assert!(DAILY_REPORT.contains("fn main()"));
}

#[test]
fn test_design_contract_is_documented_bilingually_and_linked_from_guides() {
    for (path, document) in [
        ("doc/design.md", ENGLISH_DESIGN),
        ("doc/design.zh_CN.md", CHINESE_DESIGN),
    ] {
        for contract in ["ExecutionServicesAdmission", "Saturated", "FIFO"] {
            assert!(document.contains(contract), "{path} must describe {contract}");
        }
    }
    assert!(ENGLISH_GUIDE.contains("design.md"));
    assert!(CHINESE_GUIDE.contains("design.zh_CN.md"));
}
