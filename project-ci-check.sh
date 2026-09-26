#!/usr/bin/env bash
set -euo pipefail

project_root=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
"$project_root/.infra/tools/prepare-local-path-dependencies.sh"
CARGO_TARGET_DIR="$project_root/target/project-hook" \
    cargo run --locked --manifest-path \
    "$project_root/tests/fixtures/application_consumer/Cargo.toml"
printf '%s\n' 'application-consumer: passed'
