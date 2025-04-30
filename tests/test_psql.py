"""
Test ability to index off diverse filesystems using fsspec.
"""

from pathlib import Path

import pytest

from conda_index.index import ChannelIndex

try:
    from conda_index.alchemy.psqlcache import PsqlCache
except ImportError:
    PsqlCache = None


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_psql(tmp_path: Path, index_data: Path, postgresql_database):
    """
    Test that conda-index can store its cache in postgresql.
    """
    channel_root = index_data / "packages"
    output = tmp_path / "output"  # default same as channel
    output.mkdir()

    test_subdirs = ("noarch", "osx-64")

    assert PsqlCache

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
        write_shards=True,
        write_monolithic=True,
    )

    channel_index.index(
        patch_generator=None, current_index_versions=None, progress=False
    )

    print("Done")
