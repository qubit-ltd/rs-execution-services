// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Checks that each published quick-start example matches the runnable example.

const QUICK_START: &str = include_str!("../examples/quick_start.rs");
const ENGLISH_README: &str = include_str!("../README.md");
const CHINESE_README: &str = include_str!("../README.zh_CN.md");
const ENGLISH_GUIDE: &str = include_str!("../doc/user_guide.md");
const CHINESE_GUIDE: &str = include_str!("../doc/user_guide.zh_CN.md");

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
