import subprocess
import sys
import tarfile
import zipfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
API_ALLOWLIST = ROOT / "tests" / "runtime" / "api_file_allowlist.txt"


@pytest.fixture(scope="session")
def built_dist(tmp_path_factory):
    dist_dir = tmp_path_factory.mktemp("dist")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "build",
            "--sdist",
            "--wheel",
            "--outdir",
            str(dist_dir),
            str(ROOT),
        ],
        check=True,
    )
    return dist_dir


def _latest(dist_dir: Path, pattern: str) -> Path:
    matches = sorted(dist_dir.glob(pattern))
    assert matches, pattern
    return matches[-1]


def _contains_docs_tree(name: str) -> bool:
    return name == "docs" or name.startswith("docs/") or "/docs/" in name or name.endswith("/docs")


def _expected_api_files() -> set:
    return {
        line.strip()
        for line in API_ALLOWLIST.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def test_built_wheel_has_lark_channel_and_no_lark_oapi(built_dist):
    wheel = _latest(built_dist, "lark_channel_sdk-*.whl")
    with zipfile.ZipFile(wheel) as zf:
        names = set(zf.namelist())
    assert any(name.startswith("lark_channel/") for name in names)
    assert not any(name.startswith("lark_oapi/") for name in names)
    assert not any(_contains_docs_tree(name) for name in names)
    assert {name for name in names if name.startswith("lark_channel/api/") and name.endswith(".py")} == _expected_api_files()


def test_built_wheel_declares_vendored_third_party_notices(built_dist):
    wheel = _latest(built_dist, "lark_channel_sdk-*.whl")
    with zipfile.ZipFile(wheel) as zf:
        names = set(zf.namelist())
        metadata_name = next(
            name for name in names if name.endswith(".dist-info/METADATA")
        )
        metadata = zf.read(metadata_name).decode("utf-8")

    assert "License: MIT AND BSD-3-Clause" in metadata
    assert "License-File: LICENSE" in metadata
    assert "License-File: THIRD_PARTY_NOTICES.md" in metadata
    assert any(
        ".dist-info/" in name and name.endswith("THIRD_PARTY_NOTICES.md")
        for name in names
    )


def test_built_sdist_has_lark_channel_and_no_lark_oapi(built_dist):
    sdist = _latest(built_dist, "lark_channel_sdk-*.tar.gz")
    with tarfile.open(sdist) as tf:
        names = set(tf.getnames())
    assert any("/lark_channel/" in name for name in names)
    assert not any("/lark_oapi/" in name for name in names)
    assert any(name.endswith("/docs/quickstart.md") for name in names)
    assert any(name.endswith("/docs/reference.md") for name in names)
    actual_api_files = {
        "/".join(name.split("/")[1:])
        for name in names
        if "/lark_channel/api/" in name and name.endswith(".py")
    }
    assert actual_api_files == _expected_api_files()


def test_built_sdist_includes_third_party_notices(built_dist):
    sdist = _latest(built_dist, "lark_channel_sdk-*.tar.gz")
    with tarfile.open(sdist) as tf:
        names = set(tf.getnames())

    assert any(name.endswith("/THIRD_PARTY_NOTICES.md") for name in names)
