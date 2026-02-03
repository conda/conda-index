"""
Test postgresql support.
"""

import json
from pathlib import Path

import pytest

from conda_index.index import ChannelIndex

try:
    from conda_index.postgres.cache import PsqlCache
except ImportError:
    pytest.skip("Could not import PsqlCache", allow_module_level=True)


@pytest.fixture
def mock_engine():
    """Mock SQLAlchemy engine that captures executed queries."""
    executed = []

    class MockResult:
        def first(self):
            return None

    class MockConnection:
        def execute(self, query, *args, **kwargs):
            executed.append(query)
            return MockResult()

    class MockBegin:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self.conn

        def __exit__(self, *args):
            return False

    class MockEngine:
        def begin(self):
            return MockBegin(MockConnection())

    engine = MockEngine()
    engine.executed = executed  # type: ignore
    return engine


@pytest.mark.skipif(PsqlCache is None, reason="Could not import PsqlCache")
def test_load_all_from_cache_filters_by_stage_and_path(tmp_path: Path, mock_engine):
    """
    Verify load_all_from_cache() filters by both stage AND path.
    """
    cache = PsqlCache(tmp_path, "noarch", db_url="postgresql://example")
    cache.engine = mock_engine  # type: ignore

    cache.load_all_from_cache("test-package.conda")

    assert len(mock_engine.executed) == 1
    query = str(mock_engine.executed[0])
    # Both conditions must be in the WHERE clause (joined by AND)
    assert "WHERE stat.stage" in query
    assert "AND stat.path" in query


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


def repodata_paths(repodata: dict):
    """
    Yield every key in repodata['packages'], repodata['packages.conda']
    """
    for group in ("packages", "packages.conda"):
        yield from repodata[group]


def test_repodata_paths():
    """
    Test helper function.
    """
    assert list(repodata_paths({"packages": {"a": 1}, "packages.conda": {"b": 2}})) == [
        "a",
        "b",
    ]


# conda-index is expected to attempt to index, but skip these files.
INVALID_PACKAGES_IN_SUITE = {
    "subfolder.tar.bz2",
    "a.tar.bz2",
    "subfolder2.tar.bz2",
    "b.tar.bz2",
}


def test_psql_channel_separation(
    tmp_path: Path, index_data: Path, archives_data: Path, postgresql_database
):
    """
    Test that conda-index can store multiple channels in PostgreSQL and keep
    them separate.
    """
    output_base = tmp_path / "output"

    # Twice so that combined packages are written to first output if the bug
    # occurs:
    for _ in range(2):
        for channel_root in (index_data, archives_data):
            # tmp_path is the same as used for the fixtures, contains the
            # index_data, archives_data directories
            output = output_base / channel_root.name  # default same as channel
            output.mkdir(parents=True, exist_ok=True)

            channel_index = ChannelIndex(
                channel_root,
                channel_name="psql",
                output_root=output,
                write_bz2=False,
                write_zst=False,
                threads=1,
                write_run_exports=True,
                compact_json=True,
                cache_class=PsqlCache,
                cache_kwargs={"db_url": postgresql_database.url},
                write_shards=True,
                write_monolithic=True,
            )

            channel_index.index(
                patch_generator=None, current_index_versions=None, progress=False
            )

            for subdir in output.glob("*"):
                if not subdir.is_dir():
                    continue
                expected_packages = (
                    set(
                        p.name
                        for p in (channel_root / subdir.name).glob("*")
                        if p.name.endswith((".tar.bz2", ".conda"))
                    )
                    - INVALID_PACKAGES_IN_SUITE
                )
                actual_packages = set(
                    repodata_paths(json.loads((subdir / "repodata.json").read_text()))
                )
                assert actual_packages == expected_packages
