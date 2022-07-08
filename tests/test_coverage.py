"""
Tests primarily to increase coverage.
"""

import os.path
import sqlite3

import pytest

import conda_index.api
import conda_index.index
import conda_index.index.convert_cache
from conda_index.utils import _checksum


def test_coverage_1():
    conda_index.index.logging_config()
    conda_index.index.ensure_binary(b"")


def test_dummy_executor():
    ex = conda_index.index.DummyExecutor()
    assert list(ex.map(lambda x: x * 2, range(4))) == [0, 2, 4, 6]

    assert [ex.submit(lambda x: x * 2, n).result() for n in range(4)] == [0, 2, 4, 6]


def test_ensure_valid_channel(testing_workdir):
    conda_index.index._ensure_valid_channel(
        os.path.join(testing_workdir, "valid_channel"), "linux-64"
    )


def test_bad_subdir(testing_workdir):
    with pytest.raises(SystemExit):
        conda_index.api.update_index(os.path.join(testing_workdir, "osx-64"))


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
