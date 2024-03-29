"""
Tests primarily to increase coverage.
"""

import os.path
import sqlite3
import tarfile

import pytest

import conda_index.api
import conda_index.index
import conda_index.index.convert_cache
import conda_index.yaml
from conda_index.utils import _checksum

PATCH_GENERATOR = os.path.join(os.path.dirname(__file__), "gen_patch.py")


def test_coverage_1():
    conda_index.index.logging_config()


def test_dummy_executor():
    ex = conda_index.index.DummyExecutor()
    assert list(ex.map(lambda x: x * 2, range(4))) == [0, 2, 4, 6]

    assert [ex.submit(lambda x: x * 2, n).result() for n in range(4)] == [0, 2, 4, 6]


def test_ensure_valid_channel(testing_workdir):
    conda_index.index._ensure_valid_channel(
        os.path.join(testing_workdir, "valid_channel"), "linux-64"
    )


def test_bad_subdir(testing_workdir):
    with pytest.raises(ValueError):
        conda_index.api.update_index(os.path.join(testing_workdir, "osx-64"))


def test_no_noarch_and_patch_generator(testing_workdir):
    # logs a warning if noarch is not indexed
    conda_index.api.update_index(
        testing_workdir, subdir="osx-64", patch_generator=PATCH_GENERATOR, threads=1
    )

    with pytest.raises(ValueError):
        conda_index.api.update_index(
            testing_workdir,
            subdir="osx-64",
            patch_generator="patch-generator-does-not-exist",
            threads=1,
        )


def test_migrate_1():
    conn = sqlite3.connect(":memory:")
    conda_index.index.convert_cache.create(conn)
    conda_index.index.convert_cache.migrate(conn)

    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA user_version=0")
    conda_index.index.convert_cache.create(conn)
    conda_index.index.convert_cache.migrate(conn)

    conda_index.index.convert_cache.remove_prefix(conn)

    conn = sqlite3.connect(":memory:")
    conda_index.index.convert_cache.create(conn)
    conn.execute("PRAGMA user_version=42")
    with pytest.raises(ValueError):
        conda_index.index.convert_cache.migrate(conn)


def test_unknown_hash_algorithm():
    with pytest.raises(AttributeError):
        _checksum("not-a-real-file.txt", "sha0")


def test_apply_instructions():
    # how does this work? in any case, "revoke" requires "depends".
    conda_index.index._apply_instructions(
        "noarch",
        {
            "packages": {
                "jim.tar.bz2": {"depends": []},
                "bob.tar.bz2": {"depends": []},
            },
            "packages.conda": {
                "jim.conda": {"depends": []},
                "bob.conda": {"depends": []},
            },
        },
        {"revoke": ["jim.tar.bz2"], "remove": ["bob.tar.bz2"]},
    )


def test_bad_yaml():
    # unclosed string
    assert conda_index.yaml.determined_load("'not yaml") == {}


def test_main():
    """
    Run module for coverage.
    """
    with pytest.raises(SystemExit):
        import conda_index.__main__  # noqa: F401


def test_patch_instructions_coverage(tmp_path):
    """
    Used to load patch instructions into conda-index.
    """
    (tmp_path / "noarch").mkdir()

    index = conda_index.index.ChannelIndex(tmp_path, "noarch")

    index._load_instructions("noarch")

    patch = tmp_path / "noarch" / "patch_instructions.json"

    patch.write_text("{}")

    index._load_instructions("noarch")

    patch_tarball = tmp_path / "patch.tar.bz2"
    with tarfile.open(patch_tarball, "w:bz2") as tar:
        tar.add(patch, arcname="noarch/patch_instructions.json")

    index._load_patch_instructions_tarball("noarch", patch_tarball)

    # non-importable path (would normally be a .py)
    with pytest.raises(ImportError):
        index._create_patch_instructions("noarch", {}, str(patch))

    # unsupported patch version
    patch.write_text('{"patch_instructions_version":42}')
    with pytest.raises(RuntimeError):
        index._load_instructions("noarch")

    # unsupported patch version in tarball triggers separate RuntimeError check
    patch_invalid = tmp_path / "patch_invalid.tar.bz2"
    with tarfile.open(patch_invalid, "w:bz2") as tar:
        tar.add(patch, arcname="noarch/patch_instructions.json")

    with pytest.raises(RuntimeError):
        index._patch_repodata("noarch", {}, str(patch_invalid))
