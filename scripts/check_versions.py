#!/usr/bin/env python3
"""Check release metadata against Cargo's authoritative workspace version."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path


def field(text, pattern):
    matches = re.findall(pattern, text, flags=re.MULTILINE)
    if len(matches) != 1:
        raise ValueError(f"expected exactly one match for {pattern!r}, got {len(matches)}")
    return matches[0]


def check(root, metadata, ref=""):
    errors = []
    members = set(metadata["workspace_members"])
    packages = [p for p in metadata["packages"] if p["id"] in members]
    version = next(p["version"] for p in packages if p["name"] == "dggs-stream-core")
    for package in packages:
        if package["version"] != version:
            errors.append(f"{package['name']}: version {package['version']} != {version}")
        expected_publish = None if package["name"] == "dggs-stream-core" else []
        if package["publish"] != expected_publish:
            errors.append(f"{package['name']}: only dggs-stream-core is publishable")
        for dep in package["dependencies"]:
            if dep.get("path") and dep["req"] != f"^{version}":
                errors.append(f"{package['name']}: {dep['name']} must require {version}")

    path_dep = r'^{}\s*=\s*\{{[^\n]*version\s*=\s*"([^"]+)"'.format
    # The standalone workspaces (fuzz, dggs-stream-sedona) are outside `cargo metadata`.
    fields = [
        ("fuzz/Cargo.toml", path_dep("dggs-stream-core")),
        ("crates/dggs-stream-sedona/Cargo.toml", r'^version\s*=\s*"([^"]+)"'),
        ("crates/dggs-stream-sedona/Cargo.toml", path_dep("dggs-stream-core")),
        ("crates/dggs-stream-sedona/python/Cargo.toml", r'^version\s*=\s*"([^"]+)"'),
        ("crates/dggs-stream-sedona/python/Cargo.toml", path_dep("dggs-stream-sedona")),
        ("crates/dggs-stream-python/pyproject.toml", r'^version\s*=\s*"([^"]+)"'),
        ("crates/dggs-stream-pg/dggs_stream.control", r"^default_version\s*=\s*'([^']+)'"),
        # The extension under its former name, kept for existing installs.
        ("crates/dggs-stream-pg/raster_h3.control", r"^default_version\s*=\s*'([^']+)'"),
        ("packaging/duckdb-community/description.yml", r"^  version:\s*(\S+)"),
        ("crates/dggs-stream-pg/src/fmgr.rs", r'version: c"([^"]+)"\.as_ptr\(\)'),
    ]
    for path, pattern in fields:
        try:
            actual = field((root / path).read_text(), pattern)
            if actual != version:
                errors.append(f"{path}: {actual} != {version}")
        except (OSError, ValueError) as exc:
            errors.append(f"{path}: {exc}")

    for ext in ("dggs_stream", "raster_h3"):
        sql = root / f"crates/dggs-stream-pg/sql/{ext}--{version}.sql"
        if not sql.is_file():
            errors.append(f"missing PostgreSQL install script: {sql.name}")
    if ref.startswith("refs/tags/") and ref != f"refs/tags/v{version}":
        errors.append(f"release tag {ref} != v{version}")

    # Documentation describes dependency roles; Cargo.toml owns their versions.
    readme = (root / "README.md").read_text()
    if re.search(r"https://crates\.io/crates/[^)]+\)\s+`v[0-9]", readme):
        errors.append("README.md: do not duplicate dependency versions from Cargo.toml")
    if (root / "LICENSE").read_text() != (root / "crates/dggs-stream-core/LICENSE").read_text():
        errors.append("core crate LICENSE must match the workspace LICENSE")
    return errors


def main():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version=1", "--locked"],
        cwd=root, check=True, capture_output=True, text=True,
    )
    errors = check(root, json.loads(result.stdout), os.environ.get("GITHUB_REF", ""))
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print("Workspace, wheel, PostgreSQL, community manifest, and release tag agree.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
