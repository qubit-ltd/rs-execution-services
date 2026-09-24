# Formatting and CI Checks

This crate uses the formatting and CI commands provided by its checked-in
scripts and `rs-infra` configuration.

## Format and align code

Run the alignment script from the crate root before submitting changes:

```bash
./align-ci.sh
```

The script prepares local path dependencies and runs the pinned `rs-infra-style`
formatter. Its toolchain and formatting configuration are selected by
`align-ci.sh` and `.infra/style/rustfmt.toml` when that file is present.

## Check style without changing files

```bash
./style-check.sh
```

## Run CI checks

```bash
./ci-check.sh
```

The CI workflow is `.github/workflows/ci.yml`. It runs the project's configured
verification, style, Clippy, feature-matrix, documentation, and coverage checks.
Use the workflow and the scripts above as the source of truth for available
checks and toolchain versions.
