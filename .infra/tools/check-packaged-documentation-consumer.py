#!/usr/bin/env python3
"""Build documented consumers against an extracted package without path patches."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib


PACKAGE_NAME = "qubit-execution-services"
CONSUMERS = (
    ("readme-current-thread", ["rt", "time"], "quick-start"),
    ("guide-multi-thread", ["rt", "rt-multi-thread", "time"], "documentation"),
)


def run(command: list[str], cwd: Path, show_stdout: bool = True) -> str:
    print(f"$ (cd {cwd} && {' '.join(command)})", flush=True)
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)
    if result.stdout and show_stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        raise RuntimeError(f"command failed with exit code {result.returncode}: {command[0]}")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result.stdout


def check_local_patch_configs(cwd: Path) -> list[Path]:
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
    candidates = {cargo_home / "config.toml", cargo_home / "config"}
    for directory in (cwd, *cwd.parents):
        candidates.add(directory / ".cargo" / "config.toml")
        candidates.add(directory / ".cargo" / "config")
    local_patches: list[Path] = []
    for config in sorted(candidates):
        if not config.is_file():
            continue
        try:
            parsed = tomllib.loads(config.read_text())
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise RuntimeError(f"cannot inspect Cargo config {config}: {error}") from error
        patch = parsed.get("patch", {}).get("crates-io", {})
        if any(isinstance(value, dict) and "path" in value for value in patch.values()):
            local_patches.append(config)
    if local_patches:
        paths = ", ".join(str(path) for path in local_patches)
        raise RuntimeError(f"Cargo global config contains crates.io path patches: {paths}")
    return [path for path in candidates if path.is_file()]


def check_metadata(metadata: dict, consumer_manifest: Path, package_manifest: Path) -> None:
    allowed_manifests = {consumer_manifest.resolve(), package_manifest.resolve()}
    unexpected = []
    for package in metadata.get("packages", []):
        source = package.get("source")
        manifest = Path(package["manifest_path"]).resolve()
        if source is None:
            if manifest not in allowed_manifests:
                unexpected.append(f"{package['name']} at {manifest}")
        elif not isinstance(source, str) or not source.startswith("registry+"):
            unexpected.append(f"{package['name']} from {source!r} at {manifest}")
    if unexpected:
        raise RuntimeError(
            "consumer resolved non-registry sources or unexpected local packages: "
            + "; ".join(unexpected)
        )
    target = next(
        (
            package
            for package in metadata.get("packages", [])
            if package.get("name") == PACKAGE_NAME
            and package.get("source") is None
            and Path(package["manifest_path"]).resolve() == package_manifest.resolve()
        ),
        None,
    )
    if target is None:
        raise RuntimeError(
            f"consumer metadata did not resolve {PACKAGE_NAME} from the extracted package "
            "with source=None and its exact manifest path"
        )


def self_test() -> None:
    consumer = Path("/tmp/consumer/Cargo.toml")
    package = Path("/tmp/package/Cargo.toml")

    def entry(name: str, source: object, manifest: Path) -> dict:
        return {"name": name, "source": source, "manifest_path": str(manifest)}

    def metadata(*packages: dict) -> dict:
        return {"packages": list(packages)}

    allowed_local = [entry("consumer", None, consumer), entry(PACKAGE_NAME, None, package)]
    check_metadata(
        metadata(
            *allowed_local,
            entry("registry-dependency", "registry+https://example.test/index", Path("/tmp/registry/Cargo.toml")),
        ),
        consumer,
        package,
    )

    rejected = (
        ("injected-local-patch", None, Path("/tmp/sibling/Cargo.toml")),
        ("git-override", "git+https://example.test/repo#abc", Path("/tmp/git/Cargo.toml")),
        ("unknown-source", "mystery+opaque", Path("/tmp/unknown/Cargo.toml")),
        ("invalid-source-type", 42, Path("/tmp/invalid/Cargo.toml")),
    )
    for name, source, manifest in rejected:
        try:
            check_metadata(metadata(*allowed_local, entry(name, source, manifest)), consumer, package)
        except RuntimeError as error:
            if name not in str(error) and repr(source) not in str(error):
                raise RuntimeError(f"self-test rejection omitted package/source: {error}") from error
        else:
            raise RuntimeError(f"self-test failed to reject {name} from {source!r}")

    for invalid_target in (
        metadata(entry("consumer", None, consumer)),
        metadata(
            entry("consumer", None, consumer),
            entry(PACKAGE_NAME, "registry+https://example.test/index", package),
        ),
        metadata(
            entry("consumer", None, consumer),
            entry(PACKAGE_NAME, "git+https://example.test/repo#abc", package),
        ),
    ):
        try:
            check_metadata(invalid_target, consumer, package)
        except RuntimeError as error:
            if PACKAGE_NAME not in str(error) and "target" not in str(error) and "extracted package" not in str(error):
                raise
        else:
            raise RuntimeError("self-test failed to reject missing or sourced target package")

    with tempfile.TemporaryDirectory(prefix="packaged-consumer-self-test-") as temporary:
        package_dir = Path(temporary)
        examples = package_dir / "examples"
        examples.mkdir()
        quick_start = "fn main() { println!(\"quick-start archive marker\"); }\n"
        daily_report = "fn main() { println!(\"daily-report archive marker\"); }\n"
        (examples / "quick_start.rs").write_text(quick_start)
        (examples / "daily_report.rs").write_text(daily_report)
        if consumer_program(package_dir, "quick-start") != quick_start:
            raise RuntimeError("self-test selected the wrong quick-start archive example")
        if consumer_program(package_dir, "documentation") != daily_report:
            raise RuntimeError("self-test selected the wrong daily-report archive example")
        try:
            consumer_program(package_dir, "unknown")
        except RuntimeError as error:
            if "unknown" not in str(error):
                raise
        else:
            raise RuntimeError("self-test accepted an unknown consumer program")
        (examples / "daily_report.rs").unlink()
        try:
            consumer_program(package_dir, "documentation")
        except RuntimeError as error:
            if "daily_report.rs" not in str(error):
                raise
        else:
            raise RuntimeError("self-test accepted a missing packaged daily-report example")

    print("metadata source matrix and packaged program self-tests passed")


def consumer_program(package_dir: Path, program: str) -> str:
    paths = {
        "quick-start": "examples/quick_start.rs",
        "documentation": "examples/daily_report.rs",
    }
    if program not in paths:
        raise RuntimeError(f"unknown consumer program: {program}")
    source = package_dir / paths[program]
    try:
        return source.read_text()
    except OSError as error:
        raise RuntimeError(f"cannot read packaged example {source}: {error}") from error


def consumer_manifest(name: str, package_path: Path, package_version: str, tokio_features: list[str]) -> str:
    features = ", ".join(json.dumps(feature) for feature in tokio_features)
    version_parts = package_version.split(".")
    if len(version_parts) < 2:
        raise RuntimeError(f"invalid package version: {package_version}")
    version_requirement = f"^{version_parts[0]}.{version_parts[1]}"
    return f'''[package]
name = "{name}"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
{PACKAGE_NAME} = {{ path = {json.dumps(str(package_path))}, version = {json.dumps(version_requirement)} }}
tokio = {{ version = "^1.54", features = [{features}] }}

[workspace]
'''


def build_consumers(project: Path) -> None:
    cargo_version = run(["cargo", "--version"], project).strip()
    package_version = json.loads(
        run(["cargo", "metadata", "--no-deps", "--format-version", "1"], project, show_stdout=False)
    )["packages"][0]["version"]
    print(f"Cargo: {cargo_version}; package: {PACKAGE_NAME} {package_version}")

    run(["cargo", "package", "--locked", "--allow-dirty", "--package", PACKAGE_NAME], project)
    archive = project / "target" / "package" / f"{PACKAGE_NAME}-{package_version}.crate"
    if not archive.is_file():
        raise RuntimeError(f"cargo package did not create expected archive: {archive}")

    with tempfile.TemporaryDirectory(prefix="qubit-execution-services-package-") as temporary:
        work = Path(temporary)
        config_files = check_local_patch_configs(work)
        print("Effective Cargo config files checked: " + (", ".join(map(str, config_files)) or "none found"))
        extracted_root = work / "archive"
        extracted_root.mkdir()
        with tarfile.open(archive, "r:gz") as package_archive:
            members = package_archive.getmembers()
            for member in members:
                destination = (extracted_root / member.name).resolve()
                if extracted_root.resolve() not in destination.parents and destination != extracted_root.resolve():
                    raise RuntimeError(f"package archive path escapes extraction directory: {member.name}")
                if not (member.isfile() or member.isdir()):
                    raise RuntimeError(f"unsupported file type in package archive: {member.name}")
            package_archive.extractall(extracted_root, members=members)

        package_dir = extracted_root / f"{PACKAGE_NAME}-{package_version}"
        package_manifest = package_dir / "Cargo.toml"
        if not package_manifest.is_file():
            raise RuntimeError(f"package archive is missing {package_manifest.name}")
        for name, features, program in CONSUMERS:
            consumer = work / name
            consumer.mkdir()
            manifest = consumer / "Cargo.toml"
            manifest.write_text(consumer_manifest(name.replace("-", "_"), package_dir, package_version, features))
            source = consumer_program(package_dir, program)
            (consumer / "src").mkdir()
            (consumer / "src" / "main.rs").write_text(source)

            run(
                ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest)],
                consumer,
                show_stdout=False,
            )
            metadata_output = run(
                [
                    "cargo",
                    "metadata",
                    "--locked",
                    "--format-version",
                    "1",
                    "--manifest-path",
                    str(manifest),
                ],
                consumer,
                show_stdout=False,
            )
            check_metadata(json.loads(metadata_output), manifest, package_manifest)
            run(["cargo", "run", "--locked", "--manifest-path", str(manifest)], consumer)


def main() -> int:
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        self_test()
        return 0
    if len(sys.argv) != 1:
        print(f"usage: {Path(sys.argv[0]).name} [--self-test]", file=sys.stderr)
        return 2
    project = Path(__file__).resolve().parents[2]
    try:
        build_consumers(project)
    except (OSError, RuntimeError, subprocess.SubprocessError, tarfile.TarError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
