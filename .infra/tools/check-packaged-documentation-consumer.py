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
        if package.get("source") is None:
            manifest = Path(package["manifest_path"]).resolve()
            if manifest not in allowed_manifests:
                unexpected.append(f"{package['name']} at {manifest}")
    if unexpected:
        raise RuntimeError(
            "consumer resolved local path dependencies other than itself and the extracted package: "
            + "; ".join(unexpected)
        )
    target = next(
        (
            package
            for package in metadata.get("packages", [])
            if package.get("name") == PACKAGE_NAME
            and Path(package["manifest_path"]).resolve() == package_manifest.resolve()
        ),
        None,
    )
    if target is None:
        raise RuntimeError("consumer metadata did not resolve the extracted package")


def self_test() -> None:
    consumer = Path("/tmp/consumer/Cargo.toml")
    package = Path("/tmp/package/Cargo.toml")
    metadata = {
        "packages": [
            {"name": "consumer", "source": None, "manifest_path": str(consumer)},
            {"name": PACKAGE_NAME, "source": None, "manifest_path": str(package)},
            {"name": "injected-local-patch", "source": None, "manifest_path": "/tmp/sibling/Cargo.toml"},
        ]
    }
    try:
        check_metadata(metadata, consumer, package)
    except RuntimeError as error:
        if "injected-local-patch" not in str(error):
            raise
    else:
        raise RuntimeError("self-test failed to reject an injected local path patch")
    print("local path-patch rejection self-test passed")


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
        quick_start = (project / "examples" / "quick_start.rs").read_text()
        documentation = (project / "tests" / "fixtures" / "documentation_consumer" / "src" / "main.rs").read_text()

        for name, features, program in CONSUMERS:
            consumer = work / name
            consumer.mkdir()
            manifest = consumer / "Cargo.toml"
            manifest.write_text(consumer_manifest(name.replace("-", "_"), package_dir, package_version, features))
            source = quick_start if program == "quick-start" else documentation
            (consumer / "src").mkdir()
            (consumer / "src" / "main.rs").write_text(source)

            metadata_output = run(
                ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest)],
                consumer,
                show_stdout=False,
            )
            metadata = json.loads(metadata_output)
            check_metadata(metadata, manifest, package_manifest)
            run(["cargo", "generate-lockfile", "--manifest-path", str(manifest)], consumer)
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
