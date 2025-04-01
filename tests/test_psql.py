"""
Test ability to index off diverse filesystems using fsspec.
"""

import json

import fsspec.core
import pytest

from conda_index.alchemy.psqlcache import PsqlCache
from conda_index.index import ChannelIndex
from conda_index.index.fs import FsspecFS


def test_psql(tmp_path, postgresql_database):
    """
    Test that conda-index can store its cache in postgresql.
    """
    channel_root = tmp_path / "channel"  # used for sqlite cache in this mode
    channel_root.mkdir()
    output = tmp_path / "output"  # default same as channel
    output.mkdir()

    test_subdirs = ("noarch", "osx-64")

    channel_index = ChannelIndex(
        channel_root,
        channel_name="psql",
        output_root=output,
        subdirs=test_subdirs,  # listdir('tests/index_data/packages')?
        write_bz2=False,
        write_zst=False,
        threads=1,
        write_run_exports=False,
        compact_json=True,
        cache_class=PsqlCache,
        cache_kwargs={"db_url": postgresql_database.url},
    )

    channel_index.index(
        patch_generator=None, current_index_versions=None, progress=False
    )

    print("Indexed?")
