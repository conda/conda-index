from __future__ import annotations

import json
import sys
from io import StringIO
from os.path import join
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

import conda_index.cli
import conda_index.index.logutil
from conda_index.cli import cli

try:
    from conda_index.postgres.cache import PsqlCache
except ImportError:
    PsqlCache = None

from .utils import fake_download

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

HERE = Path(__file__).parent
ARBITRARY_YML = HERE / "environment.yml"


def test_cli(tmp_path):
    """
    Coverage testing for the argparse cli.
    """

    (tmp_path / "noarch").mkdir()  # makes valid channel
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    current_index_versions = tmp_path / "current_index_versions.yml"

    current_index_versions.write_text(
        """
python:
    - 3.8
    - 3.9
"""
    )

    # fancy separate output directory run
    args = [
        f"--output={output_dir}",
        f"--current-index-versions-file={current_index_versions}",
        "--channeldata",
        str(tmp_path),
        "--rss",
        "--bz2",
        "--zst",
        "--verbose",
    ]

    try:
        cli(args)
        success = True
    except SystemExit as e:
        success = e.code == 0 or e.code is None

    assert success, "CLI should exit successfully"
    assert (output_dir / "channeldata.json").exists()

    # plain run
    assert not (tmp_path / "noarch" / "repodata.json").exists()
    cli([str(tmp_path)])
    assert not (tmp_path / "channeldata.json").exists()
    assert (tmp_path / "noarch" / "repodata.json").exists()


@pytest.mark.parametrize(
    "cli_option", ["--current-repodata", "--run-exports", "--channeldata"]
)
def test_mutual_exclusion_mononlithic_repodata(
    cli_option: str, tmp_path, mocker: MockerFixture
):
    """Test that 'cli_option' is blocked when repodata.json is not written."""
    mock_stderr = mocker.patch("sys.stderr", new_callable=StringIO)

    with pytest.raises(SystemExit) as exc_info:
        cli(["--no-write-monolithic", cli_option, str(tmp_path)])

    assert exc_info.value.code == 1
    error_output = mock_stderr.getvalue()
    assert "Conflicting arguments" in error_output
    assert cli_option in error_output


@pytest.mark.needs_postgresql
@pytest.mark.xfail(
    PsqlCache is None,
    reason="Should fail with an import error if postgres dependencies are not installed",
)
def test_postgres_backend(tmp_path, postgresql_database):
    """Test running the index with postgres database backends."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    args = [
        f"--output={output_dir}",
        "--channeldata",
        str(tmp_path),
        "--rss",
        "--bz2",
        "--zst",
        "--db=postgresql",
        f"--db-url={postgresql_database.url}",
    ]

    try:
        cli(args)
        success = True
    except SystemExit as e:
        success = e.code == 0 or e.code is None

    assert success, "CLI should exit successfully"
    assert (output_dir / "channeldata.json").exists()


def test_index(tmp_path):
    """Test that conda index will create an index with packages"""
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    packages_dir = tmp_path / "packages"

    test_package_path = join(
        packages_dir, "noarch", "conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/noarch/conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    fake_download(test_package_url, test_package_path)

    cli_args = [
        f"--output={output_dir}",
        "--channeldata",
        f"{str(packages_dir)}",
    ]

    try:
        cli(cli_args)
        success = True
    except SystemExit as e:
        success = e.code == 0 or e.code is None

    assert success, "CLI should exit successfully"

    channeldata_path = output_dir / "channeldata.json"
    assert channeldata_path.exists()
    channeldata = json.loads(channeldata_path.read_text())
    assert len(channeldata["packages"]) == 1
    assert "conda-index-pkg-a" in channeldata["packages"]


def test_update_cache(tmp_path):
    """Test disabling updating the cache should not add new packages to the output"""
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    packages_dir = tmp_path / "packages"

    test_package_path = join(
        packages_dir, "noarch", "conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/noarch/conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    fake_download(test_package_url, test_package_path)

    args = [
        f"--output={output_dir}",
        "--channeldata",
        f"{str(packages_dir)}",
        "--bz2",
        "--zst",
        "--no-update-cache",
    ]

    try:
        cli(args)
        success = True
    except SystemExit as e:
        success = e.code == 0 or e.code is None

    assert success, "CLI should exit successfully"

    channeldata_path = output_dir / "channeldata.json"
    assert channeldata_path.exists()
    channeldata = json.loads(channeldata_path.read_text())
    assert channeldata["packages"] == {}, (
        "Expected no packages when cache is not updated"
    )


def test_patch_generator(tmp_path):
    """Test that --patch-generator applies a Python patch function to repodata."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()

    packages_dir = tmp_path / "packages"

    test_package_path = join(
        packages_dir, "noarch", "conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    )
    test_package_url = "https://conda.anaconda.org/conda-test/noarch/conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    fake_download(test_package_url, test_package_path)

    patch_file = HERE / "archives" / "conda-index-pkg-a-patch.py"

    try:
        cli(
            [
                f"--output={output_dir}",
                f"--patch-generator={patch_file}",
                str(packages_dir),
            ]
        )
        success = True
    except SystemExit as e:
        success = e.code == 0 or e.code is None

    assert success, "CLI should exit successfully"

    repodata_path = output_dir / "noarch" / "repodata.json"
    assert repodata_path.exists()
    repodata = json.loads(repodata_path.read_text())

    pkg_filename = "conda-index-pkg-a-1.0-pyhed9eced_1.tar.bz2"
    assert pkg_filename in repodata["packages"]
    assert "patched-dep" in repodata["packages"][pkg_filename].get("depends", [])


def test_postgresql_missing_dependencies(mocker: MockerFixture):
    """Test that --db=postgresql exits with an error when postgres deps are missing."""
    mocker.patch.dict(sys.modules, {"conda_index.postgres.cache": None})
    mock_stderr = mocker.patch("sys.stderr", new_callable=StringIO)

    with pytest.raises(SystemExit) as exc_info:
        cli(
            [
                "--db=postgresql",
                "--db-url=postgresql://user:pass@localhost/dbname",
                "--output=/tmp/output",
                "--channeldata",
                ".",
            ]
        )

    assert exc_info.value.code == 1
    assert "Missing dependencies for postgresql" in mock_stderr.getvalue()


def test_cli_impl(monkeypatch):
    """Execute code in cli _main_impl function."""
    # don't allow it to mess up logging
    monkeypatch.setattr(conda_index.index.logutil, "configure", lambda: None)

    # Save the original changed_packages method to restore it after the test.
    # _main_impl modifies this class attribute when update_cache=False,
    # which would otherwise persist and break subsequent tests.
    from conda_index.index.sqlitecache import CondaIndexCache

    original_changed_packages = CondaIndexCache.changed_packages
    monkeypatch.setattr(
        CondaIndexCache,
        "changed_packages",
        original_changed_packages,
    )

    container = {"mock_index": None}
    called = {}

    class DoNothingChannelIndex(conda_index.index.ChannelIndex):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            container["mock_index"] = self

        def index(self, *args, **kwargs):
            called["index"] = True

        def update_channeldata(self, rss=False):
            called["update_channeldata"] = True

    monkeypatch.setattr(conda_index.cli, "ChannelIndex", DoNothingChannelIndex)

    conda_index.cli._main_impl(
        dir="/tmp/fake",
        patch_generator="/tmp/fake2",
        subdir=(),
        output="/tmp/fake-output",
        channeldata=True,
        verbose=True,
        threads=1,
        current_index_versions_file=ARBITRARY_YML,
        channel_name="test-channel",
        bz2=False,
        zst=True,
        rss=True,
        run_exports=True,
        compact=True,
        base_url=None,
        update_cache=False,
        upstream_stage="fs",
        current_repodata=True,
        write_monolithic=True,
        write_shards=True,
        db="sqlite3",
        db_url="",
        html_dependencies=True,
        update_only=False,
        repodata_next=True,
    )

    assert container["mock_index"]
    assert called["index"]
    assert called["update_channeldata"]
