#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
sibling_root=$(cd "$project_root/.." && pwd -P)
for repository in rs-ioc rs-event-bus rs-fs-registry; do
    if [ ! -d "$sibling_root/$repository" ]; then
        echo "error: $sibling_root/$repository must be checked out beside rs-execution-services" >&2
        exit 1
    fi
done

fixture_manifest="$project_root/tests/fixtures/ioc_application_consumer/Cargo.toml"
CARGO_TARGET_DIR="$project_root/target/ioc-integration" \
    cargo run --locked --manifest-path "$fixture_manifest"
CARGO_TARGET_DIR="$project_root/target/ioc-integration" \
    cargo test --locked --manifest-path "$fixture_manifest"
CARGO_TARGET_DIR="$project_root/target/ioc-integration" \
    cargo clippy --locked --manifest-path "$fixture_manifest" --all-targets -- -D warnings
printf '%s\n' 'ioc-application-consumer: passed'
