#!/usr/bin/env python3
"""Build archived documentation consumers with source or registry dependencies."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from unittest import mock


PACKAGE_NAME = "qubit-execution-services"
CONSUMERS = (
    ("readme-current-thread", ["rt", "time"], "quick-start"),
    ("guide-multi-thread", ["rt", "rt-multi-thread", "time"], "documentation"),
)


def run(
    command: list[str],
    cwd: Path,
    show_stdout: bool = True,
    env: dict[str, str] | None = None,
) -> str:
    print(f"$ (cd {cwd} && {' '.join(command)})", flush=True)
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False, env=env)
    if result.stdout and show_stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        raise RuntimeError(f"command failed with exit code {result.returncode}: {command[0]}")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result.stdout


def check_local_patch_configs(cwd: Path, cargo_home: Path | None = None) -> list[Path]:
    cargo_home = cargo_home or Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
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


def check_metadata(
    metadata: dict,
    consumer_manifest: Path,
    package_manifest: Path,
    path_dependency_manifests: dict[str, Path],
    consumer_package_name: str | None = None,
    registry_only: bool = False,
) -> None:
    allowed_manifests = {consumer_manifest.resolve(), package_manifest.resolve()}
    expected_path_manifests = {
        manifest.resolve(): name for name, manifest in path_dependency_manifests.items()
    }
    resolved_path_dependencies: set[str] = set()
    unexpected = []
    for package in metadata.get("packages", []):
        source = package.get("source")
        manifest = Path(package["manifest_path"]).resolve()
        if source is None:
            expected_name = expected_path_manifests.get(manifest)
            if manifest in allowed_manifests:
                continue
            if expected_name is None:
                unexpected.append(f"{package['name']} at {manifest}")
            elif package.get("name") != expected_name:
                unexpected.append(
                    f"{package['name']} at {manifest} (expected {expected_name})"
                )
            else:
                resolved_path_dependencies.add(expected_name)
        elif (
            not isinstance(source, str)
            or not source.startswith("registry+")
            or (registry_only and source != "registry+https://github.com/rust-lang/crates.io-index")
        ):
            unexpected.append(f"{package['name']} from {source!r} at {manifest}")
    missing_path_dependencies = set(path_dependency_manifests) - resolved_path_dependencies
    if missing_path_dependencies:
        unexpected.append(
            "missing expected path dependencies: " + ", ".join(sorted(missing_path_dependencies))
        )
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
    if consumer_package_name is not None:
        consumer_package = next(
            (
                item for item in metadata.get("packages", [])
                if Path(item["manifest_path"]).resolve() == consumer_manifest.resolve()
                and item.get("source") is None
            ),
            None,
        )
        if consumer_package is None or consumer_package.get("name") != consumer_package_name:
            raise RuntimeError(
                f"consumer metadata did not resolve expected {consumer_package_name} from "
                f"{consumer_manifest}"
            )


CRATES_IO_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"
RAYON_CRATE = "qubit-rayon-executor"


def _exact_version(value: str) -> tuple[int, int, int]:
    parts = value.split(".")
    if len(parts) != 3 or any(
        not part.isdigit() or (len(part) > 1 and part.startswith("0"))
        for part in parts
    ):
        raise RuntimeError(f"Rayon version must be an exact x.y.z version: {value!r}")
    return tuple(map(int, parts))


def _rayon_lower_bound(package_manifest: Path) -> str:
    try:
        parsed = tomllib.loads(package_manifest.read_text())
        requirement = parsed["dependencies"][RAYON_CRATE]["version"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError(
            f"cannot read {RAYON_CRATE} dependency from {package_manifest}: {error}"
        ) from error
    if not isinstance(requirement, str) or not requirement.startswith("^"):
        raise RuntimeError(
            f"{RAYON_CRATE} dependency must declare a caret lower bound, got {requirement!r}"
        )
    lower = requirement[1:]
    parts = lower.split(".")
    if len(parts) == 2:
        lower += ".0"
    _exact_version(lower)
    return lower


def check_rayon_resolution(
    metadata: dict,
    package_manifest: Path,
    rayon_version: str,
    registry_only: bool,
) -> None:
    if not registry_only:
        raise RuntimeError("--rayon-version is only valid with --registry-only")
    _exact_version(rayon_version)
    lower_bound = _rayon_lower_bound(package_manifest)
    if rayon_version != lower_bound:
        raise RuntimeError(
            f"Rayon pin {rayon_version} does not match facade manifest lower bound {lower_bound}"
        )
    packages = metadata.get("packages", [])
    targets = [
        item for item in packages
        if item.get("name") == PACKAGE_NAME
        and item.get("source") is None
        and Path(item.get("manifest_path", "")).resolve() == package_manifest.resolve()
    ]
    if len(targets) != 1 or not targets[0].get("id"):
        raise RuntimeError("metadata lacks the extracted facade package ID")
    target = targets[0]
    rayon_packages = [item for item in packages if item.get("name") == RAYON_CRATE]
    exact_packages = [item for item in rayon_packages if item.get("version") == rayon_version]
    if len(exact_packages) != 1:
        raise RuntimeError(
            f"metadata must contain exactly one {RAYON_CRATE} {rayon_version} package; "
            f"found {[item.get('version') for item in rayon_packages]}"
        )
    rayon = exact_packages[0]
    if rayon.get("source") != CRATES_IO_SOURCE or not rayon.get("id"):
        raise RuntimeError(f"{RAYON_CRATE} {rayon_version} is not from the crates.io registry")
    nodes = metadata.get("resolve", {}).get("nodes", [])
    facade_nodes = [node for node in nodes if node.get("id") == target["id"]]
    if len(facade_nodes) != 1:
        raise RuntimeError("metadata resolve graph lacks a unique facade node")
    rayon_edges = [
        dep for dep in facade_nodes[0].get("deps", [])
        if dep.get("name") == "qubit_rayon_executor"
    ]
    if len(rayon_edges) != 1 or rayon_edges[0].get("pkg") != rayon["id"]:
        raise RuntimeError(
            f"facade dependency edge does not resolve to {RAYON_CRATE} {rayon_version}"
        )
def self_test() -> None:
    consumer = Path("/tmp/consumer/Cargo.toml")
    package = Path("/tmp/package/Cargo.toml")

    def entry(name: str, source: object, manifest: Path) -> dict:
        return {"name": name, "source": source, "manifest_path": str(manifest)}

    def metadata(*packages: dict) -> dict:
        return {"packages": list(packages)}

    allowed_local = [entry("consumer", None, consumer), entry(PACKAGE_NAME, None, package)]
    latest_path_dependencies = [
        entry("qubit-tokio-executor", None, Path("/tmp/rs-tokio-executor/Cargo.toml")),
        entry("qubit-rayon-executor", None, Path("/tmp/rs-rayon-executor/Cargo.toml")),
        entry("qubit-executor", None, Path("/tmp/rs-executor/Cargo.toml")),
    ]
    local_dependency_manifests = {
        package["name"]: Path(package["manifest_path"])
        for package in latest_path_dependencies
    }
    registry_graph = metadata(
        *allowed_local,
        entry("qubit-executor", "registry+https://github.com/rust-lang/crates.io-index", Path("/tmp/registry/qubit-executor/Cargo.toml")),
    )
    check_metadata(
        registry_graph, consumer, package, {},
        consumer_package_name="consumer", registry_only=True,
    )
    with tempfile.TemporaryDirectory(prefix="rayon-resolution-self-test-") as temporary:
        root = Path(temporary)
        archive = root / "archive"
        archive.mkdir()
        archive_manifest = archive / "Cargo.toml"
        archive_manifest.write_text(
            '[package]\nname="qubit-execution-services"\nversion="0.10.4"\n'
            '[dependencies]\nqubit-rayon-executor={version="^0.9"}\n'
        )
        facade_id = "qubit-execution-services 0.10.4 (path+file:///tmp/package)"
        rayon_090_id = "qubit-rayon-executor 0.9.0 (registry+https://github.com/rust-lang/crates.io-index)"
        rayon_091_id = "qubit-rayon-executor 0.9.1 (registry+https://github.com/rust-lang/crates.io-index)"

        def rayon_meta(
            version_id: str,
            versions: tuple[str, ...] = ("0.9.0",),
            edge_id: str | None = None,
        ) -> dict:
            rayon_packages = [
                {"name": RAYON_CRATE, "version": version, "id": version_id if version == "0.9.0" else rayon_091_id,
                 "source": CRATES_IO_SOURCE, "manifest_path": f"/tmp/registry/rayon-{version}/Cargo.toml"}
                for version in versions
            ]
            return {
                "packages": [
                    {"name": PACKAGE_NAME, "version": "0.10.4", "id": facade_id,
                     "source": None, "manifest_path": str(archive_manifest)},
                    *rayon_packages,
                ],
                "resolve": {"nodes": [{"id": facade_id, "deps": [
                    {"name": "qubit_rayon_executor", "pkg": edge_id or version_id}
                ]}]},
            }

        rejected_rayon_cases = (
            (rayon_meta(rayon_090_id, ("0.9.0", "0.9.1"), edge_id=rayon_091_id), "0.9.0", True,
             "accepted a mismatched facade dependency edge"),
            (rayon_meta(rayon_091_id, ("0.9.1",)), "0.9.0", True,
             "accepted a same-name package with only a different version"),
            (rayon_meta(rayon_090_id), "1.0.0", True,
             "accepted a pin outside the facade caret requirement"),
            (rayon_meta(rayon_090_id), "^0.9.0", True,
             "accepted a range instead of an exact pin"),
            (rayon_meta(rayon_090_id), "", True,
             "accepted an empty Rayon pin"),
            (rayon_meta(rayon_090_id), "00.9.0", True,
             "accepted a non-canonical Rayon version"),
            (rayon_meta(rayon_090_id), "0.9.0", False,
             "accepted a Rayon pin in non-registry mode"),
        )
        for graph, pin, registry, message in rejected_rayon_cases:
            try:
                check_rayon_resolution(graph, archive_manifest, pin, registry_only=registry)
            except RuntimeError:
                pass
            else:
                raise RuntimeError(f"registry verifier {message}")
        check_rayon_resolution(rayon_meta(rayon_090_id), archive_manifest, "0.9.0", registry_only=True)
    try:
        check_metadata(
            metadata(*allowed_local, entry("alternate-registry", "registry+https://example.test/index", Path("/tmp/registry/other/Cargo.toml"))),
            consumer, package, {}, registry_only=True,
        )
    except RuntimeError:
        pass
    else:
        raise RuntimeError("registry-only accepted a non-crates.io registry source")
    try:
        check_metadata(
            metadata(*allowed_local, *latest_path_dependencies), consumer, package, {}
        )
    except RuntimeError:
        pass
    else:
        raise RuntimeError("registry-only accepted local executor dependencies")

    strict_manifest = consumer_manifest("strict_consumer", package.parent, "0.10.4", ["rt", "time"], None, registry_only=True)
    if (
        "[patch" in strict_manifest
        or "rs-executor" in strict_manifest
        or "rs-rayon-executor" in strict_manifest
        or "rs-tokio-executor" in strict_manifest
        or 'version = "^1.53"' not in strict_manifest
    ):
        raise RuntimeError("registry-only consumer manifest did not preserve strict dependency constraints")
    pinned_manifest = consumer_manifest(
        "pinned_consumer", package.parent, "0.10.4", ["rt"], None,
        registry_only=True, rayon_version="0.9.0",
    )
    if (
        'qubit-rayon-executor = { version = "=0.9.0", features = ["async-wait"] }'
        not in pinned_manifest
    ):
        raise RuntimeError("registry-only consumer manifest did not exact-pin Rayon with async-wait")
    try:
        check_metadata(
            metadata(entry("renamed_consumer", None, consumer), entry(PACKAGE_NAME, None, package)),
            consumer, package, {}, consumer_package_name="strict_consumer", registry_only=True,
        )
    except RuntimeError:
        pass
    else:
        raise RuntimeError("registry-only accepted an unexpected consumer package name")
    for source_manifest in (
        '[package]\nname="bad"\nversion="0.1.0"\n[dependencies]\nx={path="../sibling"}\n',
        '[package]\nname="bad"\nversion="0.1.0"\n[dependencies]\nx={git="https://example.test/x"}\n',
        '[package]\nname="bad"\nversion="0.1.0"\n[patch.crates-io]\nx={path="../sibling"}\n',
    ):
        with tempfile.TemporaryDirectory(prefix="registry-manifest-self-test-") as temporary:
            candidate = Path(temporary) / "Cargo.toml"
            candidate.write_text(source_manifest)
            try:
                check_manifest_sources(candidate, {})
            except RuntimeError:
                pass
            else:
                raise RuntimeError("registry-only manifest source validation accepted path/git/patch input")
    with tempfile.TemporaryDirectory(prefix="registry-allowed-path-self-test-") as temporary:
        temporary_path = Path(temporary)
        candidate = temporary_path / "Cargo.toml"
        archive_package = temporary_path / "archive-package"
        archive_package.mkdir()
        (archive_package / "Cargo.toml").write_text('[package]\nname="qubit-execution-services"\n')
        candidate.write_text(
            f'[dependencies]\n{PACKAGE_NAME}={{path={json.dumps(str(archive_package))}}}\n'
        )
        check_manifest_sources(candidate, {PACKAGE_NAME: archive_package})
    with tempfile.TemporaryDirectory(prefix="registry-env-self-test-") as temporary:
        cargo_home = Path(temporary) / "cargo-home"
        target_dir = Path(temporary) / "target"
        cargo_home.mkdir()
        target_dir.mkdir()
        with mock.patch.dict(os.environ, {
            "HOME": temporary,
            "RUSTUP_HOME": str(Path(temporary) / "rustup"),
            "RUSTUP_TOOLCHAIN": "1.94.0",
            "CARGO_HOME": "/dev/null/inherited-cargo-home",
            "CARGO_TARGET_DIR": "/dev/null/inherited-target",
            "CARGO_REGISTRIES_CRATES_IO_INDEX": "file:///dev/null",
            "CARGO_NET_OFFLINE": "true",
        }):
            isolated_env = registry_environment(cargo_home, target_dir)
        if set(key for key in isolated_env if key.startswith("CARGO_")) != {"CARGO_HOME", "CARGO_TARGET_DIR"}:
            raise RuntimeError("registry-only environment retained inherited Cargo override variables")
        if isolated_env["CARGO_HOME"] != str(cargo_home) or isolated_env["CARGO_TARGET_DIR"] != str(target_dir):
            raise RuntimeError("registry-only environment did not use isolated Cargo directories")
        if isolated_env.get("HOME") != temporary or isolated_env.get("RUSTUP_TOOLCHAIN") != "1.94.0":
            raise RuntimeError("registry-only environment did not preserve HOME and the selected toolchain")
        if isolated_env.get("RUSTUP_HOME") != str(Path(temporary) / "rustup"):
            raise RuntimeError("registry-only environment did not preserve RUSTUP_HOME")
        consumer_dir = Path(temporary) / "work" / "consumer"
        consumer_dir.mkdir(parents=True)
        checked_configs = check_strict_cargo_configs(consumer_dir, cargo_home)
        if checked_configs != [cargo_home / "config.toml"]:
            raise RuntimeError("registry-only did not report its isolated Cargo config")
        ancestor_config_dir = Path(temporary) / "work" / ".cargo"
        ancestor_config_dir.mkdir()
        (ancestor_config_dir / "config.toml").write_text(
            '[source.crates-io]\nreplace-with = "local-registry"\n'
        )
        try:
            check_strict_cargo_configs(consumer_dir, cargo_home)
        except RuntimeError as error:
            if "inherited Cargo configuration" not in str(error):
                raise
        else:
            raise RuntimeError("registry-only accepted an ancestor Cargo source override")
    source_manifest = consumer_manifest("source_consumer", package.parent, "0.10.4", ["rt"], Path("/tmp/rs-tokio-executor"))
    if "[patch.crates-io]" not in source_manifest or 'version = "^1.53"' not in source_manifest:
        raise RuntimeError("source consumer manifest lost its executor patches or Tokio constraint")

    with mock.patch("subprocess.run", return_value=mock.Mock(returncode=7, stdout="", stderr="cargo failed")) as cargo_run:
        try:
            run(["cargo", "metadata"], Path("/tmp/consumer"), show_stdout=False)
        except RuntimeError:
            pass
        else:
            raise RuntimeError("self-test swallowed a failed Cargo command")
        if cargo_run.call_count != 1:
            raise RuntimeError("self-test continued after a failed Cargo command")
    try:
        check_metadata(
            metadata(*allowed_local, *latest_path_dependencies),
            consumer,
            package,
            local_dependency_manifests,
        )
    except RuntimeError as error:
        raise RuntimeError("self-test failed to allow the latest Tokio executor path dependencies") from error

    check_metadata(
        metadata(
            *allowed_local,
            *latest_path_dependencies,
            entry("registry-dependency", "registry+https://example.test/index", Path("/tmp/registry/Cargo.toml")),
        ),
        consumer,
        package,
        local_dependency_manifests,
    )

    rejected = (
        ("injected-local-patch", None, Path("/tmp/sibling/Cargo.toml")),
        ("git-override", "git+https://example.test/repo#abc", Path("/tmp/git/Cargo.toml")),
        ("unknown-source", "mystery+opaque", Path("/tmp/unknown/Cargo.toml")),
        ("invalid-source-type", 42, Path("/tmp/invalid/Cargo.toml")),
    )
    for name, source, manifest in rejected:
        try:
            check_metadata(
                metadata(*allowed_local, *latest_path_dependencies, entry(name, source, manifest)),
                consumer,
                package,
                local_dependency_manifests,
            )
        except RuntimeError as error:
            if name not in str(error) and repr(source) not in str(error):
                raise RuntimeError(f"self-test rejection omitted package/source: {error}") from error
        else:
            raise RuntimeError(f"self-test failed to reject {name} from {source!r}")

    wrong_local_path = metadata(
        *allowed_local,
        *latest_path_dependencies,
        entry("qubit-tokio-executor", None, Path("/tmp/alternate/Cargo.toml")),
    )
    try:
        check_metadata(wrong_local_path, consumer, package, local_dependency_manifests)
    except RuntimeError as error:
        if "alternate" not in str(error):
            raise
    else:
        raise RuntimeError("self-test accepted a Tokio executor from an unapproved local path")

    missing_local_path = metadata(*allowed_local, latest_path_dependencies[0])
    try:
        check_metadata(missing_local_path, consumer, package, local_dependency_manifests)
    except RuntimeError as error:
        if "qubit-executor" not in str(error):
            raise
    else:
        raise RuntimeError("self-test accepted metadata missing the expected qubit-executor path")

    for invalid_target in (
        metadata(entry("consumer", None, consumer), *latest_path_dependencies),
        metadata(
            entry("consumer", None, consumer),
            entry(PACKAGE_NAME, "registry+https://example.test/index", package),
            *latest_path_dependencies,
        ),
        metadata(
            entry("consumer", None, consumer),
            entry(PACKAGE_NAME, "git+https://example.test/repo#abc", package),
            *latest_path_dependencies,
        ),
        metadata(
            entry("consumer", None, consumer),
            entry(PACKAGE_NAME, None, Path("/tmp/alternate-target/Cargo.toml")),
            *latest_path_dependencies,
        ),
    ):
        try:
            check_metadata(invalid_target, consumer, package, local_dependency_manifests)
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


def consumer_manifest(
    name: str,
    package_path: Path,
    package_version: str,
    tokio_features: list[str],
    tokio_executor_path: Path | None,
    registry_only: bool = False,
    rayon_version: str | None = None,
) -> str:
    if rayon_version is not None:
        if not registry_only:
            raise RuntimeError("--rayon-version is only valid with --registry-only")
        _exact_version(rayon_version)
    features = ", ".join(json.dumps(feature) for feature in tokio_features)
    version_parts = package_version.split(".")
    if len(version_parts) < 2:
        raise RuntimeError(f"invalid package version: {package_version}")
    version_requirement = f"^{version_parts[0]}.{version_parts[1]}"
    patches = ""
    rayon_pin = ""
    if rayon_version is not None:
        rayon_pin = (
            f'{RAYON_CRATE} = {{ version = "={rayon_version}", '
            'features = ["async-wait"] }\n'
        )
    if not registry_only:
        if tokio_executor_path is None:
            raise RuntimeError("source consumer requires a Tokio executor path")
        patches = "[patch.crates-io]\n"
        patches += f'qubit-tokio-executor = {{ path = {json.dumps(str(tokio_executor_path))} }}\n'
        patches += f'qubit-rayon-executor = {{ path = {json.dumps(str(tokio_executor_path.parent / "rs-rayon-executor"))} }}\n'
        patches += f'qubit-executor = {{ path = {json.dumps(str(tokio_executor_path.parent / "rs-executor"))} }}\n'
    return f'''[package]
name = "{name}"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
{PACKAGE_NAME} = {{ path = {json.dumps(str(package_path))}, version = {json.dumps(version_requirement)} }}
{rayon_pin}tokio = {{ version = "^1.53", features = [{features}] }}

{patches}

[workspace]
'''


def registry_environment(cargo_home: Path, target_dir: Path) -> dict[str, str]:
    keep = {
        "PATH",
        "HOME",
        "RUSTUP_HOME",
        "RUSTUP_TOOLCHAIN",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "CURL_CA_BUNDLE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    }
    environment = {key: value for key, value in os.environ.items() if key in keep}
    if "RUSTUP_HOME" not in environment:
        environment["RUSTUP_HOME"] = str(Path(environment.get("HOME", str(Path.home()))) / ".rustup")
    environment.update({"CARGO_HOME": str(cargo_home), "CARGO_TARGET_DIR": str(target_dir)})
    (cargo_home / "config.toml").write_text(
        '[source.crates-io]\nregistry = "https://github.com/rust-lang/crates.io-index"\n'
    )
    return environment


def check_strict_cargo_configs(cwd: Path, cargo_home: Path) -> list[Path]:
    expected_config = (cargo_home / "config.toml").resolve()
    config_files = {
        config.resolve()
        for directory in (cwd.resolve(), *cwd.resolve().parents)
        for config in (directory / ".cargo" / "config.toml", directory / ".cargo" / "config")
        if config.is_file()
    }
    config_files.update(
        config.resolve()
        for config in (cargo_home / "config.toml", cargo_home / "config")
        if config.is_file()
    )
    unexpected = config_files - {expected_config}
    if unexpected:
        raise RuntimeError(
            "registry-only consumer found inherited Cargo configuration: "
            + ", ".join(map(str, sorted(unexpected)))
        )
    try:
        parsed = tomllib.loads(expected_config.read_text())
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError(f"cannot inspect isolated Cargo config {expected_config}: {error}") from error
    expected = {
        "source": {
            "crates-io": {
                "registry": "https://github.com/rust-lang/crates.io-index",
            }
        }
    }
    if parsed != expected:
        raise RuntimeError(f"isolated Cargo config does not pin crates.io: {expected_config}")
    return sorted(config_files)


def check_manifest_sources(manifest: Path, allowed_path_dependencies: dict[str, Path]) -> None:
    try:
        parsed = tomllib.loads(manifest.read_text())
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise RuntimeError(f"cannot inspect registry-only manifest {manifest}: {error}") from error
    if "patch" in parsed:
        raise RuntimeError(f"registry-only manifest {manifest} must not contain [patch]")

    def inspect_dependencies(dependencies: dict) -> None:
        for name, dependency in dependencies.items():
            if not isinstance(dependency, dict):
                continue
            if "git" in dependency:
                raise RuntimeError(f"registry-only dependency {name} uses a git source")
            if "path" in dependency:
                expected_path = allowed_path_dependencies.get(name)
                dependency_path = (manifest.parent / dependency["path"]).resolve()
                if expected_path is None or dependency_path != expected_path.resolve():
                    raise RuntimeError(f"registry-only dependency {name} uses unexpected path {dependency['path']}")

    dependency_sections = ("dependencies", "dev-dependencies", "build-dependencies")
    for section in dependency_sections:
        inspect_dependencies(parsed.get(section, {}))
    for target in parsed.get("target", {}).values():
        if isinstance(target, dict):
            for section in dependency_sections:
                inspect_dependencies(target.get(section, {}))


def local_path_dependency_manifests(project: Path) -> dict[str, Path]:
    workspace_root = Path(os.environ.get("QUBIT_WORKSPACE_ROOT", project.parent)).resolve()
    manifests = {
        "qubit-tokio-executor": workspace_root / "rs-tokio-executor" / "Cargo.toml",
        "qubit-rayon-executor": workspace_root / "rs-rayon-executor" / "Cargo.toml",
        "qubit-executor": workspace_root / "rs-executor" / "Cargo.toml",
    }
    for expected_name, manifest in manifests.items():
        if not manifest.is_file():
            raise RuntimeError(f"required latest path dependency manifest is missing: {manifest}")
        try:
            actual_name = tomllib.loads(manifest.read_text())["package"]["name"]
        except (OSError, KeyError, tomllib.TOMLDecodeError) as error:
            raise RuntimeError(f"cannot inspect path dependency manifest {manifest}: {error}") from error
        if actual_name != expected_name:
            raise RuntimeError(
                f"path dependency manifest {manifest} declares {actual_name!r}, expected {expected_name!r}"
            )
    return manifests


def build_consumers(
    project: Path,
    registry_only: bool = False,
    rayon_version: str | None = None,
) -> None:
    if rayon_version is not None and not registry_only:
        raise RuntimeError("--rayon-version is only valid with --registry-only")
    local_manifests = {} if registry_only else local_path_dependency_manifests(project)
    tokio_executor_path = (
        None if registry_only else local_manifests["qubit-tokio-executor"].parent.resolve()
    )
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
        print("Consumer mode: " + ("registry-only" if registry_only else "source-integration"))
        print(f"Archive SHA-256: {hashlib.sha256(archive.read_bytes()).hexdigest()}")
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
        if registry_only:
            check_manifest_sources(package_manifest, {})
        strict_home = work / "cargo-home"
        strict_target = work / "target"
        strict_home.mkdir()
        strict_target.mkdir()
        if not registry_only:
            config_files = check_local_patch_configs(work)
            print("Cargo config files checked: " + (", ".join(map(str, config_files)) or "none found"))
        for name, features, program in CONSUMERS:
            consumer = work / name
            consumer.mkdir()
            manifest = consumer / "Cargo.toml"
            manifest.write_text(
                consumer_manifest(
                    name.replace("-", "_"),
                    package_dir,
                    package_version,
                    features,
                    tokio_executor_path,
                    registry_only=registry_only,
                    rayon_version=rayon_version,
                )
            )
            source = consumer_program(package_dir, program)
            (consumer / "src").mkdir()
            (consumer / "src" / "main.rs").write_text(source)

            cargo_env = None
            if registry_only:
                cargo_env = registry_environment(strict_home, strict_target)
                config_files = check_strict_cargo_configs(consumer, strict_home)
                print(
                    "Registry-only Cargo config files checked: "
                    + (", ".join(map(str, config_files)) or "none found")
                )
                check_manifest_sources(manifest, {PACKAGE_NAME: package_manifest.parent})
            run(
                ["cargo", "metadata", "--format-version", "1", "--manifest-path", str(manifest)],
                consumer,
                show_stdout=False,
                env=cargo_env,
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
                env=cargo_env,
            )
            resolved_metadata = json.loads(metadata_output)
            check_metadata(
                resolved_metadata,
                manifest,
                package_manifest,
                local_manifests,
                consumer_package_name=name.replace("-", "_"),
                registry_only=registry_only,
            )
            if rayon_version is not None:
                check_rayon_resolution(
                    resolved_metadata,
                    package_manifest,
                    rayon_version,
                    registry_only=registry_only,
                )
            checked_sources = sorted(
                f"{package['name']}={package['source']}"
                for package in resolved_metadata["packages"]
                if package.get("source") is not None
            )
            print(f"{name}: verified sources: " + "; ".join(checked_sources))
            run(["cargo", "run", "--locked", "--manifest-path", str(manifest)], consumer, env=cargo_env)


def main() -> int:
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        self_test()
        return 0
    args = sys.argv[1:]
    registry_only = "--registry-only" in args
    rayon_version = None
    if "--rayon-version" in args:
        index = args.index("--rayon-version")
        if index + 1 >= len(args):
            rayon_version = ""
        else:
            rayon_version = args[index + 1]
        del args[index:index + 2]
    if args == ["--registry-only"]:
        pass
    elif args == [] and rayon_version is None:
        pass
    else:
        print(
            f"usage: {Path(sys.argv[0]).name} [--self-test | --registry-only [--rayon-version x.y.z]]",
            file=sys.stderr,
        )
        return 2
    if rayon_version is not None:
        if not registry_only:
            print("error: --rayon-version is only valid with --registry-only", file=sys.stderr)
            return 2
        try:
            _exact_version(rayon_version)
        except RuntimeError as error:
            print(f"error: {error}", file=sys.stderr)
            return 2
    project = Path(__file__).resolve().parents[2]
    try:
        build_consumers(project, registry_only=registry_only, rayon_version=rayon_version)
    except (OSError, RuntimeError, subprocess.SubprocessError, tarfile.TarError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
