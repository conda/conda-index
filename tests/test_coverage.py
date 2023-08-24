"""
Tests primarily to increase coverage.
"""

import os.path
import sqlite3

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
        import conda_index.__main__
