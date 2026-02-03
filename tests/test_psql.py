"""
Test postgresql support.
"""

from pathlib import Path

import pytest

from conda_index.index import ChannelIndex

try:
    from conda_index.postgres.cache import PsqlCache
except ImportError:
    PsqlCache = None


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_load_all_from_cache_filters_by_stage_and_path(tmp_path: Path):
    """
    Verify load_all_from_cache() filters by both stage AND path.
    """

    class MockResult:
        def first(self):
            return None

    class MockConnection:
        def __init__(self):
            self.executed = []

        def execute(self, query, *args, **kwargs):
            self.executed.append(query)
            return MockResult()

    class MockBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, *args):
            return False

    class MockEngine:
        def __init__(self):
            self.connection = MockConnection()

        def begin(self):
            return MockBegin(self.connection)

    cache = PsqlCache(tmp_path, "noarch", db_url="postgresql://example")
    mock_engine = MockEngine()
    cache.engine = mock_engine  # type: ignore

    cache.load_all_from_cache("test-package.conda")

    assert len(mock_engine.connection.executed) == 1
    query = str(mock_engine.connection.executed[0])
    # Both stage and path conditions must be in the WHERE clause
    assert "stat.stage" in query
    assert "stat.path" in query


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
